//go:build integration

package catalog_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/go-api-reliability-lab/internal/catalog"
	"example.com/go-api-reliability-lab/internal/provider"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createReceipt(t *testing.T, service *catalog.Service, actor uuid.UUID, key string, input catalog.SaveInput) catalog.CreateReceipt {
	t.Helper()
	receipt, err := service.SaveIdempotent(t.Context(), actor, key, input)
	if err != nil {
		t.Fatalf("create receipt: %v", err)
	}
	var reference catalog.Reference
	if err := json.Unmarshal(receipt.Body, &reference); err != nil || reference.ID == uuid.Nil || reference.ID != receipt.ReferenceID {
		t.Fatalf("invalid successful response receipt: %v", err)
	}
	return receipt
}

func receiptCount(t *testing.T, pool *pgxpool.Pool, expected int) {
	t.Helper()
	var count int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM reference_write_receipts`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != expected {
		t.Fatalf("receipt count = %d, want %d", count, expected)
	}
}

func requireReplay(t *testing.T, got, original catalog.CreateReceipt) {
	t.Helper()
	if !got.Replayed || got.ReferenceID != original.ReferenceID || !bytes.Equal(got.Body, original.Body) {
		t.Fatal("retry did not replay the original reference ID and exact response bytes")
	}
}

func TestReceiptReplaySurvivesEvictionAndRestart(t *testing.T) {
	f := newFixture(t)
	actor, key := uuid.New(), uuid.NewString()
	input := catalog.SaveInput{ExternalID: "doc-101", Note: "  A durable note\n"}
	firstPool := clonePool(t, f.pool)
	original := createReceipt(t, catalog.New(firstPool, f.provider, f.clock.Now), actor, key, input)
	if original.Replayed {
		t.Fatal("first creation was marked as replay")
	}
	firstPool.Close()
	if _, err := f.service.Evict(t.Context(), input.ExternalID); err != nil {
		t.Fatal(err)
	}
	// A receipt is a historical response, independent of later domain changes.
	if _, err := f.pool.Exec(t.Context(), `UPDATE saved_references SET note='Later synthetic fixture edit' WHERE id=$1`, original.ReferenceID); err != nil {
		t.Fatal(err)
	}
	calls := 0
	missingProvider := lookupFunc(func(context.Context, string) (provider.Document, error) {
		calls++
		return provider.Document{}, provider.ErrNotFound
	})
	restarted := catalog.New(clonePool(t, f.pool), missingProvider, f.clock.Now)
	input.Note = "A durable note"
	requireReplay(t, createReceipt(t, restarted, actor, key, input), original)
	for _, changed := range []catalog.SaveInput{
		{ExternalID: "doc-101", Note: "A  durable note"},
		{ExternalID: "doc-404", Note: "A durable note"},
	} {
		_, err := restarted.SaveIdempotent(t.Context(), actor, key, changed)
		if !errors.Is(err, catalog.ErrIdempotencyConflict) {
			t.Fatalf("different normalized input error = %v, want conflict", err)
		}
	}
	if calls != 0 {
		t.Fatal("live receipt replay or conflict unnecessarily called the provider")
	}
	counts(t, f.pool, 1, 1, 0)
	receiptCount(t, f.pool, 1)
	var keyLength, fingerprintLength int
	var persistedBody []byte
	var completedAt, expiresAt time.Time
	if err := f.pool.QueryRow(t.Context(), `SELECT octet_length(key_digest), octet_length(request_fingerprint), response_body, completed_at, expires_at FROM reference_write_receipts`).Scan(&keyLength, &fingerprintLength, &persistedBody, &completedAt, &expiresAt); err != nil {
		t.Fatal(err)
	}
	if keyLength != 32 || fingerprintLength != 32 || !bytes.Equal(persistedBody, original.Body) || !expiresAt.Equal(completedAt.Add(catalog.ReceiptRetention)) {
		t.Fatal("stored receipt did not preserve its digest, exact body, and retention contract")
	}
}

func TestConcurrentEquivalentReceiptsCreateOneReference(t *testing.T) {
	f := newFixture(t)
	const contenders = 6
	var mu sync.Mutex
	arrived := 0
	ready := make(chan struct{})
	client := lookupFunc(func(ctx context.Context, externalID string) (provider.Document, error) {
		mu.Lock()
		arrived++
		if arrived == contenders {
			close(ready)
		}
		mu.Unlock()
		select {
		case <-ready:
			return provider.Document{ExternalID: externalID, Title: "Shared synthetic document"}, nil
		case <-ctx.Done():
			return provider.Document{}, ctx.Err()
		}
	})
	services := []*catalog.Service{
		catalog.New(f.pool, client, f.clock.Now),
		catalog.New(clonePool(t, f.pool), client, f.clock.Now),
	}
	actor, key := uuid.New(), uuid.NewString()
	type result struct {
		receipt catalog.CreateReceipt
		err     error
	}
	results := make(chan result, contenders)
	for i := range contenders {
		go func() {
			note := "One logical write"
			if i%2 == 0 {
				note = "  " + note + "\n"
			}
			receipt, err := services[i%2].SaveIdempotent(t.Context(), actor, key, catalog.SaveInput{ExternalID: "doc-101", Note: note})
			results <- result{receipt, err}
		}()
	}
	var first catalog.CreateReceipt
	created := 0
	for range contenders {
		got := <-results
		if got.err != nil {
			t.Fatalf("concurrent equivalent retry: %v", got.err)
		}
		if first.ReferenceID == uuid.Nil {
			first = got.receipt
		}
		if first.ReferenceID != got.receipt.ReferenceID || !bytes.Equal(first.Body, got.receipt.Body) {
			t.Fatal("concurrent retries returned different receipts")
		}
		if !got.receipt.Replayed {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("fresh creations = %d, want 1", created)
	}
	counts(t, f.pool, 1, 1, 1)
	receiptCount(t, f.pool, 1)
}

func TestConcurrentDifferentPayloadsConflict(t *testing.T) {
	f := newFixture(t)
	var mu sync.Mutex
	arrived := 0
	ready := make(chan struct{})
	client := lookupFunc(func(ctx context.Context, id string) (provider.Document, error) {
		mu.Lock()
		arrived++
		if arrived == 2 {
			close(ready)
		}
		mu.Unlock()
		select {
		case <-ready:
			return provider.Document{ExternalID: id, Title: "Validated synthetic document"}, nil
		case <-ctx.Done():
			return provider.Document{}, ctx.Err()
		}
	})
	actor, key := uuid.New(), uuid.NewString()
	services := []*catalog.Service{catalog.New(f.pool, client, f.clock.Now), catalog.New(clonePool(t, f.pool), client, f.clock.Now)}
	results := make(chan error, 2)
	for i := range 2 {
		go func() {
			_, err := services[i].SaveIdempotent(t.Context(), actor, key, catalog.SaveInput{ExternalID: fmt.Sprintf("doc-%d", 101+i), Note: "Different resources are different requests"})
			results <- err
		}()
	}
	succeeded, conflicted := 0, 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, catalog.ErrIdempotencyConflict):
			conflicted++
		default:
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	if succeeded != 1 || conflicted != 1 {
		t.Fatalf("success/conflict = %d/%d, want 1/1", succeeded, conflicted)
	}
	counts(t, f.pool, 1, 1, 1)
	receiptCount(t, f.pool, 1)
}

func TestReceiptScopeSeparatesActorsAndKeys(t *testing.T) {
	f := newFixture(t)
	actor, key := uuid.New(), uuid.NewString()
	input := catalog.SaveInput{ExternalID: "doc-101", Note: "Independent write scope"}
	first := createReceipt(t, f.service, actor, key, input)
	f.clock.Set(f.start.Add(catalog.ReceiptRetention))
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(t.Context(), `SELECT 1 FROM reference_write_receipts WHERE actor_id=$1 FOR UPDATE`, actor); err != nil {
		t.Fatal(err)
	}
	config := f.pool.Config()
	application := "receipt_scope_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	config.ConnConfig.RuntimeParams["application_name"] = application
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	type result struct {
		receipt catalog.CreateReceipt
		err     error
	}
	results := make(chan result, 1)
	go func() {
		receipt, err := catalog.New(pool, f.provider, f.clock.Now).SaveIdempotent(t.Context(), actor, key, input)
		results <- result{receipt, err}
	}()
	waitForReceiptLock(t, f.pool, application)
	// One blocked scope must not serialize another actor or another key.
	otherPool := catalog.New(clonePool(t, f.pool), f.provider, f.clock.Now)
	otherActor := createReceipt(t, otherPool, uuid.New(), key, input)
	otherKey := createReceipt(t, otherPool, actor, uuid.NewString(), input)
	if first.ReferenceID == otherActor.ReferenceID || first.ReferenceID == otherKey.ReferenceID || otherActor.ReferenceID == otherKey.ReferenceID || otherActor.Replayed || otherKey.Replayed {
		t.Fatal("independent actor/key scopes shared a response receipt")
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := <-results
	if got.err != nil || got.receipt.Replayed || got.receipt.ReferenceID == first.ReferenceID {
		t.Fatalf("original scope could not resume independently: %v", got.err)
	}
	counts(t, f.pool, 1, 4, 1)
	receiptCount(t, f.pool, 3)
}

func TestReceiptFinalizationFailureRollsBackAllTables(t *testing.T) {
	for _, point := range []struct{ name, trigger string }{
		{"finalization", `CREATE TRIGGER reject_receipt BEFORE UPDATE ON reference_write_receipts FOR EACH ROW WHEN (NEW.state='succeeded') EXECUTE FUNCTION reject_receipt_write()`},
		{"deferred commit", `CREATE CONSTRAINT TRIGGER reject_receipt AFTER UPDATE ON reference_write_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.state='succeeded') EXECUTE FUNCTION reject_receipt_write()`},
	} {
		t.Run(point.name, func(t *testing.T) {
			f := newFixture(t)
			if _, err := f.pool.Exec(t.Context(), `CREATE FUNCTION reject_receipt_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic receipt failure'; END $$; `+point.trigger); err != nil {
				t.Fatal(err)
			}
			actor, key := uuid.New(), uuid.NewString()
			input := catalog.SaveInput{ExternalID: "doc-101", Note: "All changes must be atomic"}
			if _, err := f.service.SaveIdempotent(t.Context(), actor, key, input); err == nil {
				t.Fatal("receipt finalization failure was reported as success")
			}
			counts(t, f.pool, 0, 0, 0)
			receiptCount(t, f.pool, 0)
			if _, err := f.pool.Exec(t.Context(), `DROP TRIGGER reject_receipt ON reference_write_receipts`); err != nil {
				t.Fatal(err)
			}
			if receipt := createReceipt(t, f.service, actor, key, input); receipt.Replayed {
				t.Fatal("rolled-back claim prevented a fresh retry")
			}
			counts(t, f.pool, 1, 1, 1)
			receiptCount(t, f.pool, 1)
		})
	}
}

func TestProviderFailureLeavesNoReceipt(t *testing.T) {
	for _, failure := range []error{provider.ErrNotFound, provider.ErrUnavailable, provider.ErrMalformed} {
		t.Run(failure.Error(), func(t *testing.T) {
			f := newFixture(t)
			f.provider.SetError("doc-101", failure)
			_, err := f.service.SaveIdempotent(t.Context(), uuid.New(), uuid.NewString(), catalog.SaveInput{ExternalID: "doc-101", Note: "Unverified write"})
			if !errors.Is(err, failure) {
				t.Fatalf("provider failure = %v, want %v", err, failure)
			}
			counts(t, f.pool, 0, 0, 0)
			receiptCount(t, f.pool, 0)
		})
	}
}

func TestConcurrentReceiptTakesPrecedenceOverProviderFailure(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		failure  error
		mismatch bool
	}{
		{"outage", provider.ErrUnavailable, false},
		{"not found", provider.ErrNotFound, false},
		{"malformed", provider.ErrMalformed, false},
		{"payload conflict", provider.ErrUnavailable, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newFixture(t)
			entered, release := make(chan struct{}), make(chan struct{})
			client := lookupFunc(func(ctx context.Context, _ string) (provider.Document, error) {
				close(entered)
				select {
				case <-release:
					return provider.Document{}, scenario.failure
				case <-ctx.Done():
					return provider.Document{}, ctx.Err()
				}
			})
			actor, key := uuid.New(), uuid.NewString()
			input := catalog.SaveInput{ExternalID: "doc-101", Note: "Winning input"}
			contenderInput := input
			if scenario.mismatch {
				contenderInput.Note = "Different input"
			}
			type result struct {
				receipt catalog.CreateReceipt
				err     error
			}
			resultCh := make(chan result, 1)
			contender := catalog.New(clonePool(t, f.pool), client, f.clock.Now)
			go func() {
				receipt, err := contender.SaveIdempotent(t.Context(), actor, key, contenderInput)
				resultCh <- result{receipt, err}
			}()
			<-entered
			winner := createReceipt(t, f.service, actor, key, input)
			close(release)
			got := <-resultCh
			if scenario.mismatch {
				if !errors.Is(got.err, catalog.ErrIdempotencyConflict) {
					t.Fatalf("provider failure masked concurrent conflict: %v", got.err)
				}
			} else {
				if got.err != nil {
					t.Fatalf("provider failure masked concurrent success: %v", got.err)
				}
				requireReplay(t, got.receipt, winner)
			}
			counts(t, f.pool, 1, 1, 1)
			receiptCount(t, f.pool, 1)
		})
	}
}

func TestReceiptExpiryAllowsAtomicKeyReuse(t *testing.T) {
	f := newFixture(t)
	actor, key := uuid.New(), uuid.NewString()
	originalInput := catalog.SaveInput{ExternalID: "doc-101", Note: "Original write"}
	original := createReceipt(t, f.service, actor, key, originalInput)
	f.clock.Set(f.start.Add(catalog.ReceiptRetention - time.Nanosecond))
	requireReplay(t, createReceipt(t, f.service, actor, key, originalInput), original)
	replacementInput := catalog.SaveInput{ExternalID: "doc-202", Note: "A new use after expiry"}
	if _, err := f.service.SaveIdempotent(t.Context(), actor, key, replacementInput); !errors.Is(err, catalog.ErrIdempotencyConflict) {
		t.Fatalf("key reused before expiry: %v", err)
	}
	f.clock.Set(f.start.Add(catalog.ReceiptRetention))
	replacement := createReceipt(t, f.service, actor, key, replacementInput)
	if replacement.Replayed || replacement.ReferenceID == original.ReferenceID {
		t.Fatal("key was not recaptured at exact expiry")
	}
	requireReplay(t, createReceipt(t, f.service, actor, key, replacementInput), replacement)
	if _, err := f.service.SaveIdempotent(t.Context(), actor, key, originalInput); !errors.Is(err, catalog.ErrIdempotencyConflict) {
		t.Fatalf("old input matched newly recaptured receipt: %v", err)
	}
	counts(t, f.pool, 2, 2, 2)
	receiptCount(t, f.pool, 1)
}

func TestConcurrentExpiredKeyReuseCreatesOneNewReference(t *testing.T) {
	f := newFixture(t)
	actor, key := uuid.New(), uuid.NewString()
	input := catalog.SaveInput{ExternalID: "doc-101", Note: "First use"}
	old := createReceipt(t, f.service, actor, key, input)
	f.clock.Set(f.start.Add(catalog.ReceiptRetention))
	input.Note = "Second use"
	const contenders = 6
	services := []*catalog.Service{f.service, catalog.New(clonePool(t, f.pool), f.provider, f.clock.Now)}
	type result struct {
		receipt catalog.CreateReceipt
		err     error
	}
	results := make(chan result, contenders)
	start := make(chan struct{})
	for i := range contenders {
		go func() {
			<-start
			receipt, err := services[i%2].SaveIdempotent(t.Context(), actor, key, input)
			results <- result{receipt, err}
		}()
	}
	close(start)
	created := 0
	var first catalog.CreateReceipt
	for range contenders {
		got := <-results
		if got.err != nil {
			t.Fatalf("concurrent recapture: %v", got.err)
		}
		if first.ReferenceID == uuid.Nil {
			first = got.receipt
		}
		if got.receipt.ReferenceID == old.ReferenceID || got.receipt.ReferenceID != first.ReferenceID || !bytes.Equal(got.receipt.Body, first.Body) {
			t.Fatal("expired key recapture produced inconsistent receipts")
		}
		if !got.receipt.Replayed {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("new creations after expiry = %d, want 1", created)
	}
	counts(t, f.pool, 1, 2, 1)
	receiptCount(t, f.pool, 1)
}

// Observe a database lock wait rather than assuming a goroutine has reached SQL
// after an arbitrary sleep. The timeout is shorter than the receipt lock budget.
func waitForReceiptLock(t *testing.T, pool *pgxpool.Pool, application string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND state='active')`, application).Scan(&waiting)
		if err != nil {
			t.Fatalf("observe receipt lock wait: %v", err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("contender did not reach the receipt lock")
		case <-ticker.C:
		}
	}
}

func TestReceiptDecisionAfterAcquiringContendedLock(t *testing.T) {
	for _, scenario := range []string{"expiry rechecked", "bounded wait", "cancellation"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			actor, key := uuid.New(), uuid.NewString()
			input := catalog.SaveInput{ExternalID: "doc-101", Note: "Contended creation"}
			entered, releaseProvider := make(chan struct{}), make(chan struct{})
			client := lookupFunc(func(ctx context.Context, externalID string) (provider.Document, error) {
				close(entered)
				select {
				case <-releaseProvider:
					return provider.Document{ExternalID: externalID, Title: "Verified contender"}, nil
				case <-ctx.Done():
					return provider.Document{}, ctx.Err()
				}
			})
			config := f.pool.Config()
			application := "receipt_wait_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			config.ConnConfig.RuntimeParams["application_name"] = application
			pool, err := pgxpool.NewWithConfig(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type result struct {
				receipt catalog.CreateReceipt
				err     error
			}
			results := make(chan result, 1)
			go func() {
				receipt, err := catalog.New(pool, client, f.clock.Now).SaveIdempotent(ctx, actor, key, input)
				results <- result{receipt, err}
			}()
			<-entered // This contender's preflight saw no receipt.
			winner := createReceipt(t, f.service, actor, key, input)
			tx, err := f.pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background())
			if _, err := tx.Exec(t.Context(), `SELECT 1 FROM reference_write_receipts WHERE actor_id=$1 FOR UPDATE`, actor); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			close(releaseProvider)
			waitForReceiptLock(t, f.pool, application)
			switch scenario {
			case "expiry rechecked":
				f.clock.Set(f.start.Add(catalog.ReceiptRetention))
				if err := tx.Rollback(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "cancellation":
				cancel()
			}
			got := <-results
			switch scenario {
			case "expiry rechecked":
				if got.err != nil || got.receipt.Replayed || got.receipt.ReferenceID == winner.ReferenceID {
					t.Fatalf("expiry was not rechecked after lock acquisition: replay=%v err=%v", got.receipt.Replayed, got.err)
				}
				counts(t, f.pool, 1, 2, 1)
			case "bounded wait":
				if !errors.Is(got.err, catalog.ErrIdempotencyBusy) {
					t.Fatalf("contended receipt error = %v, want busy", got.err)
				}
				if time.Since(started) > time.Second {
					t.Fatal("receipt lock wait exceeded its bounded budget")
				}
				counts(t, f.pool, 1, 1, 1)
			case "cancellation":
				if !errors.Is(got.err, context.Canceled) {
					t.Fatalf("request cancellation error = %v", got.err)
				}
				counts(t, f.pool, 1, 1, 1)
			}
			receiptCount(t, f.pool, 1)
		})
	}
}

func TestReceiptPurgeBoundaryLimitAndConcurrentReuse(t *testing.T) {
	f := newFixture(t)
	actor, key := uuid.New(), uuid.NewString()
	input := catalog.SaveInput{ExternalID: "doc-101", Note: "Kept after receipt cleanup"}
	first := createReceipt(t, f.service, actor, key, input)
	createReceipt(t, f.service, actor, uuid.NewString(), input)
	createReceipt(t, f.service, actor, uuid.NewString(), input)
	f.clock.Set(f.start.Add(catalog.ReceiptRetention - time.Nanosecond))
	if removed, err := f.service.PurgeReceipts(t.Context(), 1); err != nil || removed != 0 {
		t.Fatalf("purge before expiry = %d, %v", removed, err)
	}
	f.clock.Set(f.start.Add(catalog.ReceiptRetention))
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(t.Context(), `SELECT 1 FROM reference_write_receipts WHERE reference_id=$1 FOR UPDATE`, first.ReferenceID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if removed, err := f.service.PurgeReceipts(ctx, 1); err != nil || removed != 1 {
		t.Fatalf("bounded purge must skip locked receipt: %d, %v", removed, err)
	}
	receiptCount(t, f.pool, 2)
	config := f.pool.Config()
	application := "receipt_reuse_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	config.ConnConfig.RuntimeParams["application_name"] = application
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	type result struct {
		receipt catalog.CreateReceipt
		err     error
	}
	results := make(chan result, 1)
	go func() {
		receipt, err := catalog.New(pool, f.provider, f.clock.Now).SaveIdempotent(t.Context(), actor, key, input)
		results <- result{receipt, err}
	}()
	waitForReceiptLock(t, f.pool, application)
	if removed, err := f.service.PurgeReceipts(t.Context(), 1); err != nil || removed != 1 {
		t.Fatalf("cleanup beside waiting reuse = %d, %v", removed, err)
	}
	if err := tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := <-results
	if got.err != nil || got.receipt.Replayed || got.receipt.ReferenceID == first.ReferenceID {
		t.Fatalf("reuse after concurrent cleanup = replay:%v err:%v", got.receipt.Replayed, got.err)
	}
	if removed, err := f.service.PurgeReceipts(t.Context(), 1); err != nil || removed != 0 {
		t.Fatalf("cleanup removed live replacement receipt: %d, %v", removed, err)
	}
	counts(t, f.pool, 1, 4, 1)
	receiptCount(t, f.pool, 1)
	var originalStillPresent bool
	if err := f.pool.QueryRow(t.Context(), `SELECT EXISTS (SELECT 1 FROM saved_references WHERE id=$1)`, first.ReferenceID).Scan(&originalStillPresent); err != nil {
		t.Fatal(err)
	}
	if !originalStillPresent {
		t.Fatal("receipt cleanup removed application history")
	}
}

func TestIdempotentProviderLookupDoesNotHoldDatabaseConnection(t *testing.T) {
	f := newFixture(t)
	config := f.pool.Config()
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client := lookupFunc(func(ctx context.Context, externalID string) (provider.Document, error) {
		var receipts int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM reference_write_receipts`).Scan(&receipts); err != nil {
			return provider.Document{}, err
		}
		if receipts != 0 {
			return provider.Document{}, errors.New("provider observed a committed claim")
		}
		return provider.Document{ExternalID: externalID, Title: "Provider I/O without a held connection"}, nil
	})
	createReceipt(t, catalog.New(pool, client, f.clock.Now), uuid.New(), uuid.NewString(), catalog.SaveInput{ExternalID: "doc-101", Note: "Keep provider work outside the transaction"})
	counts(t, f.pool, 1, 1, 1)
	receiptCount(t, f.pool, 1)
}

func TestMalformedStoredReceiptFailsClosed(t *testing.T) {
	for _, corruption := range []string{"invalid JSON", "mismatched reference ID", "missing response fields"} {
		t.Run(corruption, func(t *testing.T) {
			f := newFixture(t)
			actor, key := uuid.New(), uuid.NewString()
			input := catalog.SaveInput{ExternalID: "doc-101", Note: "A validated receipt"}
			original := createReceipt(t, f.service, actor, key, input)
			var damaged []byte
			switch corruption {
			case "invalid JSON":
				damaged = []byte(`{`)
			case "missing response fields":
				damaged = []byte(`{}`)
			case "mismatched reference ID":
				var body catalog.Reference
				if err := json.Unmarshal(original.Body, &body); err != nil {
					t.Fatal(err)
				}
				body.ID = uuid.New()
				damaged, _ = json.Marshal(body)
				damaged = append(damaged, '\n')
			}
			if _, err := f.pool.Exec(t.Context(), `UPDATE reference_write_receipts SET response_body=$1 WHERE actor_id=$2`, damaged, actor); err != nil {
				t.Fatal(err)
			}
			if _, err := f.service.SaveIdempotent(t.Context(), actor, key, input); !errors.Is(err, catalog.ErrInvariant) {
				t.Fatalf("corrupt stored receipt error = %v, want invariant failure", err)
			}
			counts(t, f.pool, 1, 1, 1)
			receiptCount(t, f.pool, 1)
		})
	}
}

func TestDatabaseRejectsUnfinishedAndOversizedReceipts(t *testing.T) {
	f := newFixture(t)
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(t.Context(), `INSERT INTO reference_write_receipts (actor_id, operation, key_digest, request_fingerprint, state) VALUES ($1,'reference.create.v1',$2,$3,'claimed')`, uuid.New(), make([]byte, 32), bytes.Repeat([]byte{1}, 32)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(t.Context()); err == nil {
		t.Fatal("unfinished receipt claim committed")
	}
	receiptCount(t, f.pool, 0)
	actor := uuid.New()
	createReceipt(t, f.service, actor, uuid.NewString(), catalog.SaveInput{ExternalID: "doc-101", Note: "Bounded response storage"})
	if _, err := f.pool.Exec(t.Context(), `UPDATE reference_write_receipts SET response_body=$1 WHERE actor_id=$2`, bytes.Repeat([]byte{'x'}, catalog.MaxReceiptBytes+1), actor); err == nil {
		t.Fatal("database accepted an oversized receipt")
	}
	counts(t, f.pool, 1, 1, 1)
	receiptCount(t, f.pool, 1)
}

func TestReceiptCleanupDeletionBeforeWaitingRetry(t *testing.T) {
	f := newFixture(t)
	actor, key := uuid.New(), uuid.NewString()
	input := catalog.SaveInput{ExternalID: "doc-101", Note: "Retry after cleanup"}
	original := createReceipt(t, f.service, actor, key, input)
	f.clock.Set(f.start.Add(catalog.ReceiptRetention))
	// Pause a cleanup deletion before commit. The retry's preflight can still
	// see the expired row, but acquisition must resolve the concurrent delete.
	tx, err := f.pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	deleted, err := tx.Exec(t.Context(), `DELETE FROM reference_write_receipts WHERE reference_id=$1 AND expires_at <= $2`, original.ReferenceID, f.clock.Now())
	if err != nil || deleted.RowsAffected() != 1 {
		t.Fatalf("delete expired receipt = %d, %v", deleted.RowsAffected(), err)
	}
	config := f.pool.Config()
	application := "receipt_deleted_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	config.ConnConfig.RuntimeParams["application_name"] = application
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	type result struct {
		receipt catalog.CreateReceipt
		err     error
	}
	results := make(chan result, 1)
	go func() {
		receipt, err := catalog.New(pool, f.provider, f.clock.Now).SaveIdempotent(t.Context(), actor, key, input)
		results <- result{receipt, err}
	}()
	waitForReceiptLock(t, f.pool, application)
	if err := tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := <-results
	if got.err != nil || got.receipt.Replayed || got.receipt.ReferenceID == original.ReferenceID {
		t.Fatalf("retry after committed deletion = replay:%v err:%v", got.receipt.Replayed, got.err)
	}
	requireReplay(t, createReceipt(t, f.service, actor, key, input), got.receipt)
	if f.provider.Calls() != 1 {
		t.Fatal("receipt deletion discarded durable resource verification")
	}
	counts(t, f.pool, 1, 2, 1)
	receiptCount(t, f.pool, 1)
}

func TestFailedExpiredRecaptureRestoresOriginalReceipt(t *testing.T) {
	f := newFixture(t)
	actor, key := uuid.New(), uuid.NewString()
	original := createReceipt(t, f.service, actor, key, catalog.SaveInput{ExternalID: "doc-101", Note: "First use"})
	var originalFingerprint []byte
	if err := f.pool.QueryRow(t.Context(), `SELECT request_fingerprint FROM reference_write_receipts WHERE actor_id=$1`, actor).Scan(&originalFingerprint); err != nil {
		t.Fatal(err)
	}
	f.clock.Set(f.start.Add(catalog.ReceiptRetention))
	// Reject only claimed -> succeeded, after replacement domain rows exist.
	// The acquisition UPSERT of the old succeeded row must still be allowed.
	if _, err := f.pool.Exec(t.Context(), `
		CREATE FUNCTION reject_recapture_finalization() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'synthetic recapture failure'; END $$;
		CREATE TRIGGER reject_recapture BEFORE UPDATE ON reference_write_receipts
		FOR EACH ROW WHEN (OLD.state='claimed' AND NEW.state='succeeded')
		EXECUTE FUNCTION reject_recapture_finalization()`); err != nil {
		t.Fatal(err)
	}
	replacementInput := catalog.SaveInput{ExternalID: "doc-202", Note: "Replacement use"}
	if _, err := f.service.SaveIdempotent(t.Context(), actor, key, replacementInput); err == nil {
		t.Fatal("expired recapture succeeded despite finalization failure")
	}
	counts(t, f.pool, 1, 1, 1)
	receiptCount(t, f.pool, 1)
	var referenceID uuid.UUID
	var body, fingerprint []byte
	var state string
	var completedAt, expiresAt time.Time
	if err := f.pool.QueryRow(t.Context(), `SELECT reference_id, response_body, request_fingerprint, state, completed_at, expires_at FROM reference_write_receipts WHERE actor_id=$1`, actor).Scan(&referenceID, &body, &fingerprint, &state, &completedAt, &expiresAt); err != nil {
		t.Fatal(err)
	}
	if referenceID != original.ReferenceID || !bytes.Equal(body, original.Body) || !bytes.Equal(fingerprint, originalFingerprint) || state != "succeeded" || !completedAt.Equal(f.start) || !expiresAt.Equal(f.start.Add(catalog.ReceiptRetention)) {
		t.Fatal("rolled-back recapture changed the previous receipt")
	}
	if _, err := f.pool.Exec(t.Context(), `DROP TRIGGER reject_recapture ON reference_write_receipts`); err != nil {
		t.Fatal(err)
	}
	replacement := createReceipt(t, f.service, actor, key, replacementInput)
	if replacement.Replayed || replacement.ReferenceID == original.ReferenceID {
		t.Fatal("failed recapture prevented subsequent key reuse")
	}
	counts(t, f.pool, 2, 2, 2)
	receiptCount(t, f.pool, 1)
}

func TestSuccessfulReceiptExcludesProviderDisplayAndLookupEvidence(t *testing.T) {
	f := newFixture(t)
	const title = "synthetic-provider-title-7293"
	const summary = "synthetic-provider-summary-7293"
	const diagnostic = "synthetic-disposable-evidence-7293"
	f.provider.SetRaw("doc-101", []byte(`{"externalId":"doc-101","title":"  `+title+`  ","summary":"  `+summary+`  ","diagnostic":"`+diagnostic+`","candidates":[{"score":0.97}]}`))
	actor := uuid.New()
	receipt := createReceipt(t, f.service, actor, uuid.NewString(), catalog.SaveInput{ExternalID: "doc-101", Note: "Application-owned note"})
	var persistedBody []byte
	if err := f.pool.QueryRow(t.Context(), `SELECT response_body FROM reference_write_receipts WHERE actor_id=$1`, actor).Scan(&persistedBody); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(persistedBody, receipt.Body) {
		t.Fatal("returned response differs from the stored receipt")
	}
	for _, excluded := range []string{title, summary, diagnostic, `"title"`, `"summary"`, `"diagnostic"`, `"candidates"`} {
		if bytes.Contains(receipt.Body, []byte(excluded)) {
			t.Fatalf("provider-only value %q appeared in successful receipt bytes", excluded)
		}
	}
	var storedTitle, storedSummary string
	if err := f.pool.QueryRow(t.Context(), `SELECT title, summary FROM resource_metadata`).Scan(&storedTitle, &storedSummary); err != nil {
		t.Fatal(err)
	}
	if storedTitle != title || storedSummary != summary {
		t.Fatal("provider display data was not normalized into its separate snapshot")
	}
	counts(t, f.pool, 1, 1, 1)
	receiptCount(t, f.pool, 1)
}
