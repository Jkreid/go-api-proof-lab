package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"example.com/go-api-reliability-lab/internal/db"
	"example.com/go-api-reliability-lab/internal/db/queries"
	"example.com/go-api-reliability-lab/internal/provider"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	ReceiptRetention   = 24 * time.Hour
	MaxReceiptBytes    = 8192
	referenceOperation = "reference.create.v1"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with payload")
	ErrIdempotencyBusy     = errors.New("idempotent write is busy")
	idempotencyKeyPattern  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// CreateReceipt is the successful create representation, already encoded before
// commit. HTTP fixes status to 201, content type to application/json, and derives
// Location from ReferenceID. No provider display data belongs in this receipt.
type CreateReceipt struct {
	ReferenceID uuid.UUID
	Body        []byte
	Replayed    bool
}

func ValidIdempotencyKey(key string) bool { return idempotencyKeyPattern.MatchString(key) }

// Fingerprints cover semantic input in a fixed versioned shape, not raw JSON.
// The operation version also freezes the response encoding for live receipts.
func saveFingerprint(input SaveInput) [sha256.Size]byte {
	encoded, _ := json.Marshal(struct {
		Operation  string `json:"operation"`
		ExternalID string `json:"externalId"`
		Note       string `json:"note"`
	}{referenceOperation, input.ExternalID, input.Note})
	return sha256.Sum256(encoded)
}

func (s *Service) SaveIdempotent(ctx context.Context, actorID uuid.UUID, key string, input SaveInput) (CreateReceipt, error) {
	if actorID == uuid.Nil || !ValidIdempotencyKey(key) {
		return CreateReceipt{}, ErrInvalidInput
	}
	input, err := normalizeSave(input)
	if err != nil {
		return CreateReceipt{}, err
	}
	digest := sha256.Sum256([]byte(key))
	fingerprint := saveFingerprint(input)
	identity := queries.FindReferenceReceiptParams{ActorID: actorID, Operation: referenceOperation, KeyDigest: digest[:]}
	row, err := s.queries.FindReferenceReceipt(ctx, identity)
	if ctx.Err() != nil {
		return CreateReceipt{}, ctx.Err()
	}
	if err == nil {
		receipt, active, err := inspectReceipt(row, s.clock(), fingerprint[:])
		if err != nil || active {
			return receipt, err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return CreateReceipt{}, storageError(ctx, err)
	}

	prepared, err := s.prepareSave(ctx, input)
	if err != nil {
		return CreateReceipt{}, err
	}
	if ctx.Err() != nil {
		return CreateReceipt{}, ctx.Err()
	}
	var result CreateReceipt
	err = db.WithinTx(ctx, s.pool, func(tx pgx.Tx) error {
		// Per lock acquisition, not the total operation budget. The HTTP context
		// also bounds pool acquisition, provider preparation, SQL, and commit.
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '250ms'"); err != nil {
			return err
		}
		q := s.queries.WithTx(tx)
		row, err := q.AcquireReferenceReceipt(ctx, queries.AcquireReferenceReceiptParams{
			ActorID: actorID, Operation: referenceOperation, KeyDigest: digest[:], RequestFingerprint: fingerprint[:],
		})
		if err != nil {
			return err
		}
		// The row lock can wait across expiry. Never classify using a timestamp
		// captured before acquiring it.
		now := s.clock()
		switch row.State {
		case "succeeded":
			receipt, active, err := inspectReceipt(row, now, fingerprint[:])
			if err != nil {
				return err
			}
			if active {
				result = receipt
				return nil
			}
			count, err := q.ReclaimReferenceReceipt(ctx, queries.ReclaimReferenceReceiptParams{
				ActorID: actorID, Operation: referenceOperation, KeyDigest: digest[:], RequestFingerprint: fingerprint[:],
			})
			if err != nil {
				return err
			}
			if count != 1 {
				return ErrInvariant
			}
		case "claimed":
			// Only our new insert can return a claim: the deferred database
			// constraint prevents another transaction from committing one.
			if !bytes.Equal(row.RequestFingerprint, fingerprint[:]) || row.ReferenceID.Valid ||
				row.ResponseBody != nil || row.CompletedAt.Valid || row.ExpiresAt.Valid {
				return ErrInvariant
			}
		default:
			return ErrInvariant
		}
		// A redundant failed lookup must not hide the winning same-key receipt.
		// Without that receipt, preparation errors retain the unkeyed behavior.
		if prepared.lookupErr != nil {
			return prepared.lookupErr
		}
		reference, err := s.insertPreparedReference(ctx, q, input, prepared)
		if err != nil {
			return err
		}
		body, err := encodeCreateReceipt(reference)
		if err != nil {
			return err
		}
		completedAt := s.clock()
		completed, err := q.CompleteReferenceReceipt(ctx, queries.CompleteReferenceReceiptParams{
			ActorID: actorID, Operation: referenceOperation, KeyDigest: digest[:],
			ReferenceID: pgtype.UUID{Bytes: reference.ID, Valid: true}, ResponseBody: body,
			CompletedAt: timestamp(completedAt), ExpiresAt: timestamp(completedAt.Add(ReceiptRetention)),
		})
		if err != nil {
			return err
		}
		// Validate what PostgreSQL returns as well as what we sent. A failed
		// finalization/encoding check rolls back all four tables together.
		result, err = decodeCreateReceipt(completed)
		return err
	})
	if err != nil {
		if ctx.Err() != nil {
			return CreateReceipt{}, ctx.Err()
		}
		if errors.Is(err, ErrIdempotencyConflict) || errors.Is(err, provider.ErrNotFound) ||
			errors.Is(err, provider.ErrUnavailable) || errors.Is(err, provider.ErrMalformed) {
			return CreateReceipt{}, err
		}
		var sqlErr *pgconn.PgError
		if errors.As(err, &sqlErr) && sqlErr.Code == "55P03" {
			return CreateReceipt{}, ErrIdempotencyBusy
		}
		// A commit error can have an uncertain outcome. Retrying the same key
		// within its window resolves against persisted state; never invent success.
		return CreateReceipt{}, storageError(ctx, err)
	}
	return result, nil
}

func (s *Service) PurgeReceipts(ctx context.Context, limit int32) (int64, error) {
	if limit < 1 || limit > MaxPurgeBatch {
		return 0, ErrInvalidInput
	}
	count, err := s.queries.PurgeReferenceReceipts(ctx, queries.PurgeReferenceReceiptsParams{
		Cutoff: timestamp(s.clock()), BatchLimit: limit,
	})
	return count, storageError(ctx, err)
}

func inspectReceipt(row queries.ReferenceWriteReceipt, now time.Time, fingerprint []byte) (CreateReceipt, bool, error) {
	if err := validateReceiptEnvelope(row); err != nil {
		return CreateReceipt{}, false, err
	}
	// Obsolete response bytes are not needed for reuse. Structure/timestamps
	// remain validated, but an expired body is discarded without decoding it.
	if !now.Before(row.ExpiresAt.Time) {
		return CreateReceipt{}, false, nil
	}
	receipt, err := decodeCreateReceipt(row)
	if err != nil {
		return CreateReceipt{}, false, err
	}
	if !bytes.Equal(row.RequestFingerprint, fingerprint) {
		return CreateReceipt{}, false, ErrIdempotencyConflict
	}
	receipt.Replayed = true
	return receipt, true, nil
}

func validateReceiptEnvelope(row queries.ReferenceWriteReceipt) error {
	if row.State != "succeeded" || row.ActorID == uuid.Nil || row.Operation != referenceOperation ||
		len(row.KeyDigest) != sha256.Size || len(row.RequestFingerprint) != sha256.Size ||
		!row.ReferenceID.Valid || uuid.UUID(row.ReferenceID.Bytes) == uuid.Nil ||
		len(row.ResponseBody) == 0 || len(row.ResponseBody) > MaxReceiptBytes ||
		!finite(row.CompletedAt) || !finite(row.ExpiresAt) ||
		!row.ExpiresAt.Time.Equal(row.CompletedAt.Time.Add(ReceiptRetention)) {
		return ErrInvariant
	}
	return nil
}

func encodeCreateReceipt(reference Reference) ([]byte, error) {
	input, err := normalizeSave(SaveInput{ExternalID: reference.Resource.ExternalID, Note: reference.Note})
	if err != nil || input.Note != reference.Note || reference.ID == uuid.Nil || reference.Resource.ID == uuid.Nil ||
		reference.CreatedAt.IsZero() || reference.Resource.FirstVerifiedAt.IsZero() {
		return nil, ErrInvariant
	}
	encoded, err := json.Marshal(reference)
	if err != nil || len(encoded)+1 > MaxReceiptBytes {
		return nil, ErrInvariant
	}
	return append(encoded, '\n'), nil
}

func decodeCreateReceipt(row queries.ReferenceWriteReceipt) (CreateReceipt, error) {
	if err := validateReceiptEnvelope(row); err != nil {
		return CreateReceipt{}, err
	}
	var reference Reference
	if err := json.Unmarshal(row.ResponseBody, &reference); err != nil {
		return CreateReceipt{}, ErrInvariant
	}
	canonical, err := encodeCreateReceipt(reference)
	// Re-encoding the frozen v1 shape rejects duplicates, unknown fields,
	// missing fields, trailing data, and noncanonical text/encoding.
	if err != nil || !bytes.Equal(canonical, row.ResponseBody) || reference.ID != uuid.UUID(row.ReferenceID.Bytes) {
		return CreateReceipt{}, ErrInvariant
	}
	fingerprint := saveFingerprint(SaveInput{ExternalID: reference.Resource.ExternalID, Note: reference.Note})
	if !bytes.Equal(fingerprint[:], row.RequestFingerprint) {
		return CreateReceipt{}, ErrInvariant
	}
	return CreateReceipt{ReferenceID: reference.ID, Body: bytes.Clone(row.ResponseBody)}, nil
}
