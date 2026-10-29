//go:build integration

package catalog_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/go-api-reliability-lab/internal/catalog"
	"example.com/go-api-reliability-lab/internal/provider"
	"example.com/go-api-reliability-lab/internal/testutil"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Each test uses an independently migrated schema in the disposable test cluster.
// The clock changes policy time without sleeping or changing the database clock.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

type fixture struct {
	pool     *pgxpool.Pool
	provider *provider.Simulator
	clock    *manualClock
	service  *catalog.Service
	start    time.Time
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{
		pool:     testutil.OpenDB(t),
		provider: provider.NewSimulator(),
		start:    time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	f.clock = &manualClock{now: f.start}
	f.service = catalog.New(f.pool, f.provider, f.clock.Now)
	return f
}

func save(t *testing.T, service *catalog.Service, externalID, note string) catalog.Reference {
	t.Helper()
	reference, err := service.Save(t.Context(), catalog.SaveInput{ExternalID: externalID, Note: note})
	if err != nil {
		t.Fatalf("save %s: %v", externalID, err)
	}
	return reference
}

func get(t *testing.T, service *catalog.Service, id uuid.UUID) catalog.View {
	t.Helper()
	view, err := service.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	return view
}

func counts(t *testing.T, pool *pgxpool.Pool, resources, references, metadata int) {
	t.Helper()
	var gotResources, gotReferences, gotMetadata int
	err := pool.QueryRow(t.Context(), `SELECT
		(SELECT count(*) FROM resources),
		(SELECT count(*) FROM saved_references),
		(SELECT count(*) FROM resource_metadata)`).Scan(&gotResources, &gotReferences, &gotMetadata)
	if err != nil {
		t.Fatal(err)
	}
	if gotResources != resources || gotReferences != references || gotMetadata != metadata {
		t.Fatalf("rows = resources:%d references:%d metadata:%d; want %d/%d/%d", gotResources, gotReferences, gotMetadata, resources, references, metadata)
	}
}

func requireState(t *testing.T, view catalog.View, state string) {
	t.Helper()
	if view.MetadataState != state {
		t.Fatalf("metadata state = %q, want %q", view.MetadataState, state)
	}
	if (view.Metadata == nil) != (state == "missing") {
		t.Fatalf("metadata/state disagree: %#v", view)
	}
}

func requireIdentity(t *testing.T, got catalog.Reference, initial catalog.Reference) {
	t.Helper()
	if got.Resource.ID != initial.Resource.ID || got.Resource.ExternalID != initial.Resource.ExternalID || !got.Resource.FirstVerifiedAt.Equal(initial.Resource.FirstVerifiedAt) {
		t.Fatalf("durable identity changed: got %#v, want %#v", got.Resource, initial.Resource)
	}
}

func storedFetchTime(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) time.Time {
	t.Helper()
	var fetched time.Time
	if err := pool.QueryRow(t.Context(), `SELECT fetched_at FROM resource_metadata WHERE resource_id=$1`, id).Scan(&fetched); err != nil {
		t.Fatal(err)
	}
	return fetched
}

type lookupFunc func(context.Context, string) (provider.Document, error)

func (fn lookupFunc) Lookup(ctx context.Context, externalID string) (provider.Document, error) {
	return fn(ctx, externalID)
}

func clonePool(t *testing.T, original *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.NewWithConfig(t.Context(), original.Config())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestConcurrentFirstUseSharesIdentity(t *testing.T) {
	f := newFixture(t)
	const contenders = 6
	var mu sync.Mutex
	arrived := 0
	allLookingUp := make(chan struct{})
	client := lookupFunc(func(ctx context.Context, externalID string) (provider.Document, error) {
		mu.Lock()
		arrived++
		if arrived == contenders {
			close(allLookingUp)
		}
		mu.Unlock()
		select {
		case <-allLookingUp:
			return provider.Document{ExternalID: externalID, Title: "Shared document"}, nil
		case <-ctx.Done():
			return provider.Document{}, ctx.Err()
		}
	})
	// Independent pools prevent an application-local mutex from satisfying the test.
	services := []*catalog.Service{
		catalog.New(f.pool, client, f.clock.Now),
		catalog.New(clonePool(t, f.pool), client, f.clock.Now),
	}
	type result struct {
		reference catalog.Reference
		err       error
	}
	results := make(chan result, contenders)
	for i := range contenders {
		go func() {
			r, err := services[i%len(services)].Save(t.Context(), catalog.SaveInput{ExternalID: "doc-101", Note: fmt.Sprintf("Reader %d", i)})
			results <- result{r, err}
		}()
	}
	var resourceID uuid.UUID
	referenceIDs := make(map[uuid.UUID]bool)
	for range contenders {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent save: %v", r.err)
		}
		if resourceID == uuid.Nil {
			resourceID = r.reference.Resource.ID
		}
		if r.reference.Resource.ID != resourceID {
			t.Fatal("concurrent saves created different resource IDs")
		}
		if referenceIDs[r.reference.ID] {
			t.Fatal("repeated saves reused an application reference")
		}
		referenceIDs[r.reference.ID] = true
	}
	counts(t, f.pool, 1, contenders, 1)
}

func TestInitialSaveRollsBackEveryTable(t *testing.T) {
	for _, point := range []struct{ name, trigger string }{
		{"metadata insert", `CREATE TRIGGER reject_insert BEFORE INSERT ON resource_metadata FOR EACH ROW EXECUTE FUNCTION reject_write()`},
		{"reference insert", `CREATE TRIGGER reject_insert BEFORE INSERT ON saved_references FOR EACH ROW EXECUTE FUNCTION reject_write()`},
		{"deferred commit", `CREATE CONSTRAINT TRIGGER reject_commit AFTER INSERT ON saved_references DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_write()`},
	} {
		t.Run(point.name, func(t *testing.T) {
			f := newFixture(t)
			_, err := f.pool.Exec(t.Context(), `CREATE FUNCTION reject_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic write failure'; END $$; `+point.trigger)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.service.Save(t.Context(), catalog.SaveInput{ExternalID: "doc-101", Note: "Keep atomic"})
			if err == nil {
				t.Fatal("save succeeded despite database rejection")
			}
			counts(t, f.pool, 0, 0, 0)
		})
	}
}

func TestFreshnessAndStaleGraceBoundaries(t *testing.T) {
	f := newFixture(t)
	reference := save(t, f.service, "doc-101", "Application-owned note")
	f.provider.SetError("doc-101", provider.ErrUnavailable)
	for _, boundary := range []struct {
		name  string
		age   time.Duration
		state string
		calls int
	}{
		{"just before freshness ends", catalog.FreshFor - time.Nanosecond, "fresh", 1},
		{"at freshness end", catalog.FreshFor, "stale", 2},
		{"just before grace ends", catalog.FreshFor + catalog.StaleGrace - time.Nanosecond, "stale", 3},
		{"at grace end", catalog.FreshFor + catalog.StaleGrace, "missing", 4},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			f.clock.Set(f.start.Add(boundary.age))
			view := get(t, f.service, reference.ID)
			requireState(t, view, boundary.state)
			requireIdentity(t, view.Reference, reference)
			if view.Note != reference.Note || view.ID != reference.ID {
				t.Fatal("provider outage changed application data")
			}
			if f.provider.Calls() != boundary.calls {
				t.Fatalf("lookups = %d, want %d", f.provider.Calls(), boundary.calls)
			}
			if !storedFetchTime(t, f.pool, reference.Resource.ID).Equal(f.start) {
				t.Fatal("failed refresh advanced the stored fetch time")
			}
		})
	}
}

func TestRefreshChangesOnlySnapshot(t *testing.T) {
	f := newFixture(t)
	first := save(t, f.service, "doc-101", "First reader")
	second := save(t, f.service, "doc-101", "Second reader")
	if f.provider.Calls() != 1 {
		t.Fatal("known identity unnecessarily called provider")
	}
	requireIdentity(t, second, first)
	f.clock.Set(f.start.Add(catalog.FreshFor))
	f.provider.SetDocument(provider.Document{ExternalID: "doc-101", Title: " Revised title "})
	view := get(t, f.service, first.ID)
	requireState(t, view, "fresh")
	requireIdentity(t, view.Reference, first)
	if view.Note != first.Note || !view.CreatedAt.Equal(first.CreatedAt) {
		t.Fatal("refresh rewrote the application reference")
	}
	if view.Metadata.Title != "Revised title" || !view.Metadata.FetchedAt.Equal(f.clock.Now()) {
		t.Fatalf("wrong replacement snapshot: %#v", view.Metadata)
	}
	other := get(t, f.service, second.ID)
	if other.Note != second.Note || other.Metadata.Title != "Revised title" {
		t.Fatal("shared snapshot did not preserve the other reference")
	}
	if f.provider.Calls() != 2 {
		t.Fatal("fresh shared snapshot did not avoid lookup")
	}
	counts(t, f.pool, 1, 2, 1)
}

func TestFallbackRechecksTimeAfterLookup(t *testing.T) {
	f := newFixture(t)
	reference := save(t, f.service, "doc-101", "Still useful without display data")
	f.clock.Set(f.start.Add(catalog.FreshFor + catalog.StaleGrace - time.Nanosecond))
	client := lookupFunc(func(context.Context, string) (provider.Document, error) {
		f.clock.Set(f.start.Add(catalog.FreshFor + catalog.StaleGrace))
		return provider.Document{}, provider.ErrUnavailable
	})
	view := get(t, catalog.New(f.pool, client, f.clock.Now), reference.ID)
	requireState(t, view, "missing")
	if !storedFetchTime(t, f.pool, reference.Resource.ID).Equal(f.start) {
		t.Fatal("fallback changed the snapshot")
	}
}

func TestVerificationTimestampFollowsSuccessfulLookup(t *testing.T) {
	f := newFixture(t)
	completed := f.start.Add(2 * time.Minute)
	client := lookupFunc(func(context.Context, string) (provider.Document, error) {
		f.clock.Set(completed)
		return provider.Document{ExternalID: "doc-101", Title: "Verified after lookup"}, nil
	})
	reference := save(t, catalog.New(f.pool, client, f.clock.Now), "doc-101", "A note")
	if !reference.Resource.FirstVerifiedAt.Equal(completed) || !storedFetchTime(t, f.pool, reference.Resource.ID).Equal(completed) {
		t.Fatal("timestamps describe request start rather than successful verification")
	}
}

func TestUnknownIdentityRejectsFailedVerification(t *testing.T) {
	for _, outcome := range []struct {
		name string
		err  error
	}{
		{"not found", provider.ErrNotFound},
		{"outage", provider.ErrUnavailable},
		{"malformed", provider.ErrMalformed},
	} {
		t.Run(outcome.name, func(t *testing.T) {
			f := newFixture(t)
			f.provider.SetError("doc-101", outcome.err)
			_, err := f.service.Save(t.Context(), catalog.SaveInput{ExternalID: "doc-101", Note: "Unverified"})
			if !errors.Is(err, outcome.err) {
				t.Fatalf("save error = %v, want %v", err, outcome.err)
			}
			counts(t, f.pool, 0, 0, 0)
		})
	}
}

func TestMalformedRefreshDoesNotServeOrReplaceSnapshot(t *testing.T) {
	for _, raw := range []string{
		`{"externalId":"different-id","title":"Wrong identity"}`,
		`{"externalId":"doc-101","title":"   "}`,
		`{"externalId":"doc-101","title":17}`,
	} {
		t.Run(raw, func(t *testing.T) {
			f := newFixture(t)
			reference := save(t, f.service, "doc-101", "Persist me")
			f.clock.Set(f.start.Add(catalog.FreshFor))
			f.provider.SetRaw("doc-101", []byte(raw))
			view := get(t, f.service, reference.ID)
			requireState(t, view, "missing")
			requireIdentity(t, view.Reference, reference)
			if !storedFetchTime(t, f.pool, reference.Resource.ID).Equal(f.start) {
				t.Fatal("malformed output advanced snapshot time")
			}
			var title string
			if err := f.pool.QueryRow(t.Context(), `SELECT title FROM resource_metadata WHERE resource_id=$1`, reference.Resource.ID).Scan(&title); err != nil {
				t.Fatal(err)
			}
			if title != "Transaction boundaries" {
				t.Fatal("malformed output overwrote previous metadata")
			}
		})
	}
}

func TestEvictionAndNotFoundPreserveHistoricalVerification(t *testing.T) {
	f := newFixture(t)
	first := save(t, f.service, "doc-101", "Durable annotation")
	removed, err := f.service.Evict(t.Context(), "doc-101")
	if err != nil || removed != 1 {
		t.Fatalf("evict = %d, %v", removed, err)
	}
	counts(t, f.pool, 1, 1, 0)
	f.provider.SetError("doc-101", provider.ErrUnavailable)
	before := f.provider.Calls()
	second := save(t, f.service, "doc-101", "Saved after eviction")
	requireIdentity(t, second, first)
	if before != f.provider.Calls() {
		t.Fatal("evicted metadata erased historical verification")
	}
	requireState(t, get(t, f.service, first.ID), "missing")
	f.clock.Set(f.start.Add(2 * time.Minute))
	f.provider.SetDocument(provider.Document{ExternalID: "doc-101", Title: "Recovered"})
	view := get(t, f.service, first.ID)
	requireState(t, view, "fresh")
	requireIdentity(t, view.Reference, first)
	if view.Metadata.Title != "Recovered" {
		t.Fatal("recovery did not refill metadata")
	}
	f.clock.Set(f.clock.Now().Add(catalog.FreshFor))
	f.provider.SetError("doc-101", provider.ErrNotFound)
	requireState(t, get(t, f.service, second.ID), "missing")
	counts(t, f.pool, 1, 2, 0)
	before = f.provider.Calls()
	third := save(t, f.service, "doc-101", "Historical reference after removal")
	requireIdentity(t, third, first)
	if before != f.provider.Calls() {
		t.Fatal("not-found revoked historical verification")
	}
	counts(t, f.pool, 1, 3, 0)
}

func TestProviderTimeoutAndRequestCancellation(t *testing.T) {
	t.Run("bounded provider timeout allows stale display", func(t *testing.T) {
		f := newFixture(t)
		reference := save(t, f.service, "doc-101", "Bounded waiting")
		f.clock.Set(f.start.Add(catalog.FreshFor))
		f.provider.SetDelay("doc-101", 5*time.Second)
		started := time.Now()
		view := get(t, f.service, reference.ID)
		requireState(t, view, "stale")
		if time.Since(started) > 2*time.Second {
			t.Fatal("provider timeout exceeded HTTP operation budget")
		}
		if !storedFetchTime(t, f.pool, reference.Resource.ID).Equal(f.start) {
			t.Fatal("timeout advanced snapshot time")
		}
	})
	t.Run("request cancellation remains an error", func(t *testing.T) {
		f := newFixture(t)
		reference := save(t, f.service, "doc-101", "Cancel the request")
		f.clock.Set(f.start.Add(catalog.FreshFor))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		client := lookupFunc(func(ctx context.Context, _ string) (provider.Document, error) {
			cancel()
			<-ctx.Done()
			return provider.Document{}, ctx.Err()
		})
		_, err := catalog.New(f.pool, client, f.clock.Now).Get(ctx, reference.ID)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation became a fallback response: %v", err)
		}
	})
}

func TestPurgeBoundaryLimitAndLockedRows(t *testing.T) {
	f := newFixture(t)
	first := save(t, f.service, "doc-101", "One")
	save(t, f.service, "doc-202", "Two")
	f.provider.SetDocument(provider.Document{ExternalID: "doc-303", Title: "Third document"})
	save(t, f.service, "doc-303", "Three")
	f.clock.Set(f.start.Add(catalog.Retention - time.Nanosecond))
	removed, err := f.service.Purge(t.Context(), 1)
	if err != nil || removed != 0 {
		t.Fatalf("pre-expiry purge = %d, %v", removed, err)
	}
	f.clock.Set(f.start.Add(catalog.Retention))
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(t.Context(), `SELECT 1 FROM resource_metadata WHERE resource_id=$1 FOR UPDATE`, first.Resource.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	removed, err = f.service.Purge(ctx, 1)
	if err != nil || removed != 1 {
		t.Fatalf("purge must skip locked row and honor limit: %d, %v", removed, err)
	}
	counts(t, f.pool, 3, 3, 2)
	if !storedFetchTime(t, f.pool, first.Resource.ID).Equal(f.start) {
		t.Fatal("locked row changed")
	}
	removed, err = f.service.Purge(t.Context(), 1)
	if err != nil || removed != 1 {
		t.Fatalf("second bounded purge = %d, %v", removed, err)
	}
	counts(t, f.pool, 3, 3, 1)
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	removed, err = f.service.Purge(t.Context(), 1)
	if err != nil || removed != 1 {
		t.Fatalf("unlocked expiry purge = %d, %v", removed, err)
	}
	counts(t, f.pool, 3, 3, 0)
	requireIdentity(t, save(t, f.service, "doc-101", "After cleanup"), first)
}

func TestRestartUsesPersistedIdentityAndSnapshot(t *testing.T) {
	f := newFixture(t)
	firstPool := clonePool(t, f.pool)
	first := save(t, catalog.New(firstPool, f.provider, f.clock.Now), "doc-101", "Survives restart")
	firstPool.Close()
	secondProvider := provider.NewSimulator()
	secondProvider.SetError("doc-101", provider.ErrUnavailable)
	restarted := catalog.New(clonePool(t, f.pool), secondProvider, f.clock.Now)
	view := get(t, restarted, first.ID)
	requireState(t, view, "fresh")
	requireIdentity(t, view.Reference, first)
	if view.Note != first.Note {
		t.Fatal("restart lost authored note")
	}
	requireIdentity(t, save(t, restarted, "doc-101", "After restart"), first)
	if secondProvider.Calls() != 0 {
		t.Fatal("restart required provider for durable state or fresh metadata")
	}
	counts(t, f.pool, 1, 2, 1)
}

func TestLookupEvidenceNeverEntersStorage(t *testing.T) {
	f := newFixture(t)
	const evidence = "synthetic-disposable-diagnostic-7293"
	f.provider.SetRaw("doc-101", []byte(`{"externalId":"doc-101","title":"  Normalized title  ","summary":"  Useful summary  ","diagnostic":"`+evidence+`","candidates":[{"score":0.97}]}`))
	reference := save(t, f.service, "doc-101", "Keep only my note")
	view := get(t, f.service, reference.ID)
	if view.Metadata.Title != "Normalized title" || view.Metadata.Summary == nil || *view.Metadata.Summary != "Useful summary" {
		t.Fatalf("unnormalized metadata: %#v", view.Metadata)
	}
	var persisted string
	err := f.pool.QueryRow(t.Context(), `SELECT
		(SELECT json_agg(r)::text FROM resources r) ||
		(SELECT json_agg(s)::text FROM saved_references s) ||
		(SELECT json_agg(m)::text FROM resource_metadata m)`).Scan(&persisted)
	if err != nil {
		t.Fatal(err)
	}
	for _, excluded := range []string{evidence, "diagnostic", "candidates", "score"} {
		if strings.Contains(persisted, excluded) {
			t.Fatalf("lookup evidence %q entered storage", excluded)
		}
	}
}

func TestDatabaseFailureIsNotMissingMetadata(t *testing.T) {
	f := newFixture(t)
	reference := save(t, f.service, "doc-101", "Database errors stay errors")
	if _, err := f.pool.Exec(t.Context(), `DROP TABLE resource_metadata`); err != nil {
		t.Fatal(err)
	}
	_, err := f.service.Get(t.Context(), reference.ID)
	if err == nil {
		t.Fatal("database failure became a missing-metadata success")
	}
}

func TestProviderLookupDoesNotHoldDatabaseConnection(t *testing.T) {
	f := newFixture(t)
	config := f.pool.Config()
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	calls := 0
	client := lookupFunc(func(ctx context.Context, externalID string) (provider.Document, error) {
		// A transaction held by Save or Get would exhaust this one-connection pool.
		// The provider timeout bounds that failure instead of hanging the test.
		var resources int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM resources`).Scan(&resources); err != nil {
			return provider.Document{}, err
		}
		calls++
		return provider.Document{ExternalID: externalID, Title: "Independent provider I/O"}, nil
	})
	service := catalog.New(pool, client, f.clock.Now)
	reference := save(t, service, "doc-101", "Do not hold a connection during I/O")
	f.clock.Set(f.start.Add(catalog.FreshFor))
	requireState(t, get(t, service, reference.ID), "fresh")
	if calls != 2 {
		t.Fatalf("provider calls = %d, want initial verification and refresh", calls)
	}
}
