package catalog

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"example.com/go-api-reliability-lab/internal/db"
	"example.com/go-api-reliability-lab/internal/db/queries"
	"example.com/go-api-reliability-lab/internal/provider"
)

var externalIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

type Service struct {
	pool     *pgxpool.Pool
	queries  *queries.Queries
	provider provider.Client
	now      func() time.Time
}

func New(pool *pgxpool.Pool, client provider.Client, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{pool: pool, queries: queries.New(pool), provider: client, now: now}
}

func (s *Service) clock() time.Time { return s.now().UTC().Truncate(time.Microsecond) }

// preparedSave contains only normalized provider data. Lookup failures can be
// carried to the keyed transaction so a concurrently completed receipt wins.
type preparedSave struct {
	resource   queries.Resource
	unknown    bool
	document   provider.Document
	verifiedAt time.Time
	lookupErr  error
}

func normalizeSave(input SaveInput) (SaveInput, error) {
	input.Note = strings.TrimSpace(input.Note)
	if !externalIDPattern.MatchString(input.ExternalID) || !validText(input.Note, 1000) {
		return SaveInput{}, ErrInvalidInput
	}
	return input, nil
}

func (s *Service) prepareSave(ctx context.Context, input SaveInput) (preparedSave, error) {
	existing, err := s.queries.FindResource(ctx, input.ExternalID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return preparedSave{}, storageError(ctx, err)
	}
	prepared := preparedSave{resource: existing, unknown: errors.Is(err, pgx.ErrNoRows)}
	if prepared.unknown {
		// Slow provider work never holds a transaction or a database connection.
		prepared.document, prepared.lookupErr = s.lookup(ctx, input.ExternalID)
		if prepared.lookupErr == nil {
			prepared.verifiedAt = s.clock()
		}
	}
	return prepared, nil
}

func (s *Service) Save(ctx context.Context, input SaveInput) (Reference, error) {
	input, err := normalizeSave(input)
	if err != nil {
		return Reference{}, err
	}
	prepared, err := s.prepareSave(ctx, input)
	if err != nil {
		return Reference{}, err
	}
	if prepared.lookupErr != nil {
		return Reference{}, prepared.lookupErr
	}
	var result Reference
	err = db.WithinTx(ctx, s.pool, func(tx pgx.Tx) error {
		result, err = s.insertPreparedReference(ctx, s.queries.WithTx(tx), input, prepared)
		return err
	})
	if err != nil {
		return Reference{}, storageError(ctx, err)
	}
	return result, nil
}

// Both save paths use the same mutation under their caller's transaction.
func (s *Service) insertPreparedReference(ctx context.Context, q *queries.Queries, input SaveInput, prepared preparedSave) (Reference, error) {
	resource := prepared.resource
	if prepared.unknown {
		var err error
		resource, err = q.InsertResource(ctx, queries.InsertResourceParams{
			ID: uuid.New(), ExternalID: input.ExternalID, FirstVerifiedAt: timestamp(prepared.verifiedAt),
		})
		if errors.Is(err, pgx.ErrNoRows) {
			// Read Committed gives this statement a fresh snapshot of the winner.
			resource, err = q.FindResource(ctx, input.ExternalID)
		} else if err == nil {
			err = q.PutMetadata(ctx, metadataParams(resource.ID, prepared.document, prepared.verifiedAt))
		}
		if err != nil {
			return Reference{}, err
		}
	}
	if resource.ID == uuid.Nil || !finite(resource.FirstVerifiedAt) {
		return Reference{}, ErrInvariant
	}
	created, err := q.InsertReference(ctx, queries.InsertReferenceParams{
		ID: uuid.New(), ResourceID: resource.ID, Note: input.Note, CreatedAt: timestamp(s.clock()),
	})
	if err != nil {
		return Reference{}, err
	}
	return Reference{ID: created.ID, Note: created.Note, CreatedAt: created.CreatedAt.Time.UTC(),
		Resource: Resource{ID: resource.ID, ExternalID: resource.ExternalID, FirstVerifiedAt: resource.FirstVerifiedAt.Time.UTC()}}, nil
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (View, error) {
	if id == uuid.Nil {
		return View{}, ErrInvalidInput
	}
	row, err := s.queries.GetReference(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return View{}, ErrReferenceNotFound
	}
	if err != nil {
		return View{}, storageError(ctx, err)
	}
	if row.ResourceID == uuid.Nil || !finite(row.FirstVerifiedAt) || !finite(row.CreatedAt) ||
		!externalIDPattern.MatchString(row.ExternalID) || !validText(row.Note, 1000) {
		return View{}, ErrInvariant
	}
	view := View{Reference: Reference{ID: row.ID, Note: row.Note, CreatedAt: row.CreatedAt.Time.UTC(),
		Resource: Resource{ID: row.ResourceID, ExternalID: row.ExternalID, FirstVerifiedAt: row.FirstVerifiedAt.Time.UTC()}}, MetadataState: "missing"}
	metadata, err := s.readMetadata(ctx, row.ResourceID)
	if err != nil {
		return View{}, err
	}
	if metadata != nil && s.clock().Before(metadata.FetchedAt.Add(FreshFor)) {
		view.Metadata, view.MetadataState = metadata, "fresh"
		return view, nil
	}
	document, lookupErr := s.lookup(ctx, row.ExternalID)
	if ctx.Err() != nil {
		return View{}, ctx.Err()
	}
	if lookupErr == nil {
		fetchedAt := s.clock()
		if err := s.queries.PutMetadata(ctx, metadataParams(row.ResourceID, document, fetchedAt)); err != nil {
			return View{}, storageError(ctx, err)
		}
		// Read the currently committed snapshot; another refresh may have won.
		view.Metadata, err = s.readMetadata(ctx, row.ResourceID)
		if err != nil {
			return View{}, err
		}
		if view.Metadata != nil {
			if s.clock().Before(view.Metadata.FetchedAt.Add(FreshFor)) {
				view.MetadataState = "fresh"
			} else {
				// An unexpected clock advance cannot silently label old data fresh.
				view.Metadata = nil
			}
		}
		return view, nil
	}
	if errors.Is(lookupErr, provider.ErrNotFound) {
		if _, err := s.queries.EvictMetadata(ctx, row.ResourceID); err != nil {
			return View{}, storageError(ctx, err)
		}
		return view, nil
	}
	// Re-read after I/O, so an already committed eviction is respected and a
	// concurrent successful refresh can supply the current snapshot.
	if errors.Is(lookupErr, provider.ErrUnavailable) {
		metadata, err = s.readMetadata(ctx, row.ResourceID)
		if err != nil {
			return View{}, err
		}
		if metadata != nil && s.clock().Before(metadata.FetchedAt.Add(FreshFor+StaleGrace)) {
			view.Metadata, view.MetadataState = metadata, "stale"
			if s.clock().Before(metadata.FetchedAt.Add(FreshFor)) {
				view.MetadataState = "fresh"
			}
		}
	}
	// Invalid provider output does not justify stale fallback or a storage write.
	return view, nil
}

func (s *Service) Evict(ctx context.Context, externalID string) (int64, error) {
	if !externalIDPattern.MatchString(externalID) {
		return 0, ErrInvalidInput
	}
	resource, err := s.queries.FindResource(ctx, externalID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, storageError(ctx, err)
	}
	count, err := s.queries.EvictMetadata(ctx, resource.ID)
	return count, storageError(ctx, err)
}

func (s *Service) Purge(ctx context.Context, limit int32) (int64, error) {
	if limit < 1 || limit > MaxPurgeBatch {
		return 0, ErrInvalidInput
	}
	count, err := s.queries.PurgeMetadata(ctx, queries.PurgeMetadataParams{Cutoff: timestamp(s.clock().Add(-Retention)), BatchLimit: limit})
	return count, storageError(ctx, err)
}

func (s *Service) lookup(ctx context.Context, externalID string) (provider.Document, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, ProviderTimeout)
	defer cancel()
	document, err := s.provider.Lookup(lookupCtx, externalID)
	if ctx.Err() != nil {
		return provider.Document{}, ctx.Err()
	}
	if lookupCtx.Err() != nil {
		return provider.Document{}, provider.ErrUnavailable
	}
	if err != nil {
		switch {
		case errors.Is(err, provider.ErrNotFound):
			return provider.Document{}, provider.ErrNotFound
		case errors.Is(err, provider.ErrUnavailable), errors.Is(err, context.DeadlineExceeded):
			return provider.Document{}, provider.ErrUnavailable
		default:
			return provider.Document{}, provider.ErrMalformed
		}
	}
	if document.ExternalID != externalID || !validText(document.Title, 200) || strings.TrimSpace(document.Title) != document.Title ||
		(document.Summary != nil && (!validText(*document.Summary, 2000) || strings.TrimSpace(*document.Summary) != *document.Summary)) {
		return provider.Document{}, provider.ErrMalformed
	}
	return document, nil
}

func (s *Service) readMetadata(ctx context.Context, id uuid.UUID) (*Metadata, error) {
	row, err := s.queries.GetMetadata(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, storageError(ctx, err)
	}
	if !finite(row.FetchedAt) || !validText(row.Title, 200) || strings.TrimSpace(row.Title) != row.Title {
		return nil, ErrInvariant
	}
	result := &Metadata{Title: row.Title, FetchedAt: row.FetchedAt.Time.UTC()}
	if row.Summary.Valid {
		if !validText(row.Summary.String, 2000) || strings.TrimSpace(row.Summary.String) != row.Summary.String {
			return nil, ErrInvariant
		}
		result.Summary = &row.Summary.String
	}
	return result, nil
}

func metadataParams(id uuid.UUID, document provider.Document, fetchedAt time.Time) queries.PutMetadataParams {
	params := queries.PutMetadataParams{ResourceID: id, Title: document.Title, FetchedAt: timestamp(fetchedAt)}
	if document.Summary != nil {
		params.Summary = pgtype.Text{String: *document.Summary, Valid: true}
	}
	return params
}

func timestamp(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: true}
}
func finite(value pgtype.Timestamptz) bool {
	return value.Valid && value.InfinityModifier == pgtype.Finite && !value.Time.IsZero()
}
func validText(value string, max int) bool {
	return utf8.ValidString(value) && strings.TrimSpace(value) != "" && utf8.RuneCountInString(value) <= max && !strings.ContainsRune(value, '\x00')
}

// Return stable classifications, never raw SQL errors or provider payloads.
func storageError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, ErrInvariant) {
		return ErrInvariant
	}
	var sqlErr *pgconn.PgError
	if errors.As(err, &sqlErr) {
		if strings.HasPrefix(sqlErr.Code, "08") || strings.HasPrefix(sqlErr.Code, "53") || strings.HasPrefix(sqlErr.Code, "57") || sqlErr.Code == "55P03" {
			return ErrStoreUnavailable
		}
		return ErrInvariant
	}
	return ErrStoreUnavailable
}
