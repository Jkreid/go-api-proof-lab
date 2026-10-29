// Command demo checks the complete lifecycle using the real HTTP handler and
// PostgreSQL, with a synthetic provider and an advancing application clock.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"

	"example.com/go-api-reliability-lab/internal/catalog"
	"example.com/go-api-reliability-lab/internal/db"
	"example.com/go-api-reliability-lab/internal/httpapi"
	"example.com/go-api-reliability-lab/internal/provider"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	discardedEvidence = "synthetic-disposable-diagnostic-7293"
	demoKey           = "demo-save-001"
	demoKeyedNote     = "A save whose response did not reach the client"
)

type demoClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *demoClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *demoClock) Set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
}

type walkthrough struct {
	ctx                  context.Context
	pool                 *pgxpool.Pool
	provider             *provider.Simulator
	clock                *demoClock
	service              *catalog.Service
	handler              http.Handler
	server               *httptest.Server
	client               *http.Client
	first, second        catalog.Reference
	lastFetched          time.Time
	receiptBody          []byte
	receiptReference     catalog.Reference
	receiptExpires       time.Time
	receiptProviderCalls int
}

func main() {
	if err := run(); err != nil {
		// Helpers below return bounded descriptions, never raw SQL or HTTP bodies.
		fmt.Fprintln(os.Stderr, "Demo failed:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := db.OpenSample(ctx)
	if err != nil {
		return errors.New("cannot open disposable database; run make demo")
	}
	defer pool.Close()
	clock := &demoClock{now: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}
	simulator := provider.NewSimulator()
	// The simulator drops extra fields at the provider boundary. Nothing below
	// stores or prints this response document or its synthetic diagnostic value.
	simulator.SetRaw("doc-101", []byte(`{"externalId":"doc-101","title":"  Transaction boundaries  ","summary":"  Notes on durable application state.  ","diagnostic":"`+discardedEvidence+`","candidates":[{"score":0.97}]}`))
	service := catalog.New(pool, simulator, clock.Now)
	handler := httpapi.NewHandler(service)
	server := httptest.NewServer(handler)
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	d := &walkthrough{ctx: ctx, pool: pool, provider: simulator, clock: clock, service: service, handler: handler, server: server, client: client}

	fmt.Println("Durable identity, disposable metadata, and bounded retries: ten checked scenarios")
	fmt.Println("All documents and notes are synthetic. Time advances without sleeping.")
	for index, step := range []struct {
		name string
		run  func() error
	}{
		{"Two references share one verified identity", d.sharedIdentity},
		{"Fresh reads avoid lookup; refresh replaces only display data", d.replaceSnapshot},
		{"An outage permits labeled stale display within the grace window", d.staleFallback},
		{"At the grace boundary, application data survives without display metadata", d.expiredFallback},
		{"Eviction preserves verification; recovery refills the same identity", d.evictAndRecover},
		{"Definitive absence evicts display data without revoking history", d.notFound},
		{"A committed write survives simulated response delivery loss", d.lostResponseAndReplay},
		{"The same key rejects a changed note within the retry window", d.keyConflict},
		{"Receipt replay survives metadata eviction and a provider outage", d.replayWithoutMetadata},
		{"At exactly 24 hours the key can create a new reference", d.reuseExpiredKey},
	} {
		fmt.Printf("\n%d. %s\n", index+1, step.name)
		if err := step.run(); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}
	fmt.Println("\nPASS: all ten scenarios passed; metadata eviction and receipt expiry left durable history intact.")
	return nil
}

func (d *walkthrough) sharedIdentity() error {
	var err error
	d.first, err = d.post("A note owned by the application")
	if err != nil {
		return err
	}
	d.second, err = d.post("Another reader's independent note")
	if err != nil {
		return err
	}
	if d.first.ID == d.second.ID || !sameResource(d.first.Resource, d.second.Resource) || d.provider.Calls() != 1 {
		return errors.New("repeated saves did not produce independent references sharing one verification")
	}
	if err := d.checkEvidenceExcluded(); err != nil {
		return err
	}
	fmt.Println("   Extra lookup fields are absent from HTTP responses and persisted rows.")
	return d.checkCounts(1, 2, 1)
}

func (d *walkthrough) replaceSnapshot() error {
	view, err := d.get(d.first.ID)
	if err != nil {
		return err
	}
	if view.Metadata == nil || view.MetadataState != "fresh" || view.Metadata.Title != "Transaction boundaries" || d.provider.Calls() != 1 {
		return errors.New("fresh normalized metadata did not avoid provider lookup")
	}
	d.clock.Set(view.Metadata.FetchedAt.Add(catalog.FreshFor))
	d.provider.SetDocument(provider.Document{ExternalID: "doc-101", Title: "Transaction boundaries, revised"})
	view, err = d.get(d.first.ID)
	if err != nil {
		return err
	}
	if view.Metadata == nil || view.MetadataState != "fresh" || view.Metadata.Title != "Transaction boundaries, revised" || !sameReference(view.Reference, d.first) {
		return errors.New("refresh changed application data or failed to replace display metadata")
	}
	d.lastFetched = view.Metadata.FetchedAt
	other, err := d.get(d.second.ID)
	if err != nil {
		return err
	}
	if other.Metadata == nil || other.Metadata.Title != view.Metadata.Title || !sameReference(other.Reference, d.second) || d.provider.Calls() != 2 {
		return errors.New("replacement snapshot was not shared by both unchanged references")
	}
	return d.checkCounts(1, 2, 1)
}

func (d *walkthrough) staleFallback() error {
	d.clock.Set(d.lastFetched.Add(catalog.FreshFor))
	d.provider.SetError("doc-101", provider.ErrUnavailable)
	view, err := d.get(d.first.ID)
	if err != nil {
		return err
	}
	if view.Metadata == nil || view.MetadataState != "stale" || !view.Metadata.FetchedAt.Equal(d.lastFetched) || !sameReference(view.Reference, d.first) || d.provider.Calls() != 3 {
		return errors.New("outage did not preserve a labeled stale snapshot and unchanged reference")
	}
	if err := d.checkStoredFetchTime(); err != nil {
		return err
	}
	return d.checkCounts(1, 2, 1)
}

func (d *walkthrough) expiredFallback() error {
	d.clock.Set(d.lastFetched.Add(catalog.FreshFor + catalog.StaleGrace))
	view, err := d.get(d.first.ID)
	if err != nil {
		return err
	}
	if view.Metadata != nil || view.MetadataState != "missing" || !sameReference(view.Reference, d.first) || d.provider.Calls() != 4 {
		return errors.New("grace boundary did not remove stale display while retaining application data")
	}
	if err := d.checkStoredFetchTime(); err != nil {
		return err
	}
	fmt.Println("   The snapshot is retained but no longer eligible for stale display.")
	return d.checkCounts(1, 2, 1)
}

func (d *walkthrough) evictAndRecover() error {
	removed, err := d.service.Evict(d.ctx, "doc-101")
	if err != nil || removed != 1 {
		return errors.New("explicit eviction did not remove exactly one snapshot")
	}
	fmt.Println("   Local maintenance evicted the display snapshot.")
	if err := d.checkCounts(1, 2, 0); err != nil {
		return err
	}
	third, err := d.post("A reference saved during the provider outage")
	if err != nil {
		return err
	}
	if !sameResource(third.Resource, d.first.Resource) || d.provider.Calls() != 4 {
		return errors.New("save after eviction did not reuse historical verification without lookup")
	}
	if err := d.checkCounts(1, 3, 0); err != nil {
		return err
	}
	d.provider.SetDocument(provider.Document{ExternalID: "doc-101", Title: "Recovered display metadata"})
	view, err := d.get(d.first.ID)
	if err != nil {
		return err
	}
	if view.Metadata == nil || view.MetadataState != "fresh" || view.Metadata.Title != "Recovered display metadata" || !sameReference(view.Reference, d.first) || d.provider.Calls() != 5 {
		return errors.New("recovery did not refill metadata under the original identity")
	}
	d.lastFetched = view.Metadata.FetchedAt
	return d.checkCounts(1, 3, 1)
}

func (d *walkthrough) notFound() error {
	d.clock.Set(d.lastFetched.Add(catalog.FreshFor))
	d.provider.SetError("doc-101", provider.ErrNotFound)
	view, err := d.get(d.second.ID)
	if err != nil {
		return err
	}
	if view.Metadata != nil || view.MetadataState != "missing" || !sameReference(view.Reference, d.second) || d.provider.Calls() != 6 {
		return errors.New("not-found did not preserve the application reference without display metadata")
	}
	if err := d.checkCounts(1, 3, 0); err != nil {
		return err
	}
	fourth, err := d.post("A historical reference after provider removal")
	if err != nil {
		return err
	}
	if !sameResource(fourth.Resource, d.first.Resource) || d.provider.Calls() != 6 {
		return errors.New("not-found incorrectly revoked historical verification")
	}
	return d.checkCounts(1, 4, 0)
}

// failedDelivery models a committed server response that the client cannot
// receive. The harness records the attempted bytes for comparison; Write fails.
type failedDelivery struct {
	header http.Header
	status int
	body   []byte
	failed bool
}

func (w *failedDelivery) Header() http.Header    { return w.header }
func (w *failedDelivery) WriteHeader(status int) { w.status = status }
func (w *failedDelivery) Write(body []byte) (int, error) {
	w.body = append(w.body, body...)
	w.failed = true
	return 0, errors.New("simulated response delivery failure")
}

func (d *walkthrough) lostResponseAndReplay() error {
	// Restore a snapshot so the later eviction can demonstrate that receipts
	// preserve only the create result, independently of current display data.
	d.provider.SetDocument(provider.Document{ExternalID: "doc-101", Title: "Display restored before the retry example"})
	view, err := d.get(d.first.ID)
	if err != nil || view.Metadata == nil {
		return errors.New("restore snapshot before keyed-write walkthrough")
	}
	d.receiptProviderCalls = d.provider.Calls()
	encoded, err := json.Marshal(catalog.SaveInput{ExternalID: "doc-101", Note: demoKeyedNote})
	if err != nil {
		return errors.New("encode synthetic keyed input")
	}
	req := httptest.NewRequest(http.MethodPost, "/references", bytes.NewReader(encoded)).WithContext(d.ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", demoKey)
	failed := &failedDelivery{header: make(http.Header)}
	d.handler.ServeHTTP(failed, req)
	if !failed.failed || failed.status != http.StatusCreated || failed.header.Get("Idempotency-Replayed") != "false" {
		return errors.New("first keyed write did not commit before simulated delivery failure")
	}
	if json.Unmarshal(failed.body, &d.receiptReference) != nil || d.receiptReference.ID == uuid.Nil {
		return errors.New("decode attempted create response")
	}
	if failed.header.Get("Location") != "/references/"+d.receiptReference.ID.String() || failed.header.Get("Content-Type") != "application/json" {
		return errors.New("original create response has incorrect representation headers")
	}
	d.receiptBody = append([]byte(nil), failed.body...)
	if bytes.Contains(d.receiptBody, []byte(`"metadata"`)) || bytes.Contains(d.receiptBody, []byte(`"title"`)) {
		return errors.New("create receipt unexpectedly contains provider display metadata")
	}
	if err := d.pool.QueryRow(d.ctx, `SELECT expires_at FROM reference_write_receipts`).Scan(&d.receiptExpires); err != nil {
		return errors.New("read committed receipt expiry")
	}
	if !d.receiptExpires.Equal(d.clock.Now().Add(catalog.ReceiptRetention)) {
		return errors.New("receipt expiry does not match the 24-hour completion window")
	}
	fmt.Println("   POST /references -> committed 201; the response writer simulated delivery failure.")
	// Equivalent surrounding whitespace is canonicalized before fingerprinting.
	body, headers, err := d.keyedPost("  "+demoKeyedNote+"  ", http.StatusCreated)
	if err != nil {
		return err
	}
	if err := d.assertReplay(body, headers); err != nil {
		return err
	}
	fmt.Printf("   Retry -> 201 replayed=true reference=%s; body and Location match the committed response.\n", d.receiptReference.ID)
	if err := d.checkReceiptCount(1); err != nil {
		return err
	}
	return d.checkCounts(1, 5, 1)
}

func (d *walkthrough) keyConflict() error {
	body, _, err := d.keyedPost("A different note under the same key", http.StatusConflict)
	if err != nil {
		return err
	}
	var result struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &result) != nil || result.Error.Code != "idempotency_conflict" {
		return errors.New("changed keyed payload did not return idempotency_conflict")
	}
	if d.provider.Calls() != d.receiptProviderCalls {
		return errors.New("key conflict called the provider")
	}
	fmt.Println("   Changed note -> 409 idempotency_conflict; no additional reference or lookup.")
	if err := d.checkReceiptCount(1); err != nil {
		return err
	}
	return d.checkCounts(1, 5, 1)
}

func (d *walkthrough) replayWithoutMetadata() error {
	removed, err := d.service.Evict(d.ctx, "doc-101")
	if err != nil || removed != 1 {
		return errors.New("evict snapshot before replay")
	}
	d.provider.SetError("doc-101", provider.ErrUnavailable)
	body, headers, err := d.keyedPost(demoKeyedNote, http.StatusCreated)
	if err != nil {
		return err
	}
	if err := d.assertReplay(body, headers); err != nil {
		return err
	}
	fmt.Println("   Evict snapshot, simulate outage, retry -> byte-identical 201; no provider lookup.")
	if err := d.checkReceiptCount(1); err != nil {
		return err
	}
	return d.checkCounts(1, 5, 0)
}

func (d *walkthrough) reuseExpiredKey() error {
	d.clock.Set(d.receiptExpires)
	body, headers, err := d.keyedPost("A new save after the previous receipt expired", http.StatusCreated)
	if err != nil {
		return err
	}
	var reference catalog.Reference
	if json.Unmarshal(body, &reference) != nil || reference.ID == uuid.Nil || reference.ID == d.receiptReference.ID ||
		!sameResource(reference.Resource, d.receiptReference.Resource) || headers.Get("Idempotency-Replayed") != "false" ||
		headers.Get("Location") != "/references/"+reference.ID.String() || d.provider.Calls() != d.receiptProviderCalls {
		return errors.New("expired key did not create a new reference using historical verification")
	}
	fmt.Printf("   At expiresAt=%s -> 201 replayed=false reference=%s; the previous reference remains.\n", d.clock.Now().Format(time.RFC3339), reference.ID)
	if err := d.checkReceiptCount(1); err != nil {
		return err
	}
	return d.checkCounts(1, 6, 0)
}

func (d *walkthrough) keyedPost(note string, status int) ([]byte, http.Header, error) {
	encoded, err := json.Marshal(catalog.SaveInput{ExternalID: "doc-101", Note: note})
	if err != nil {
		return nil, nil, errors.New("encode synthetic keyed input")
	}
	return d.requestWithKey(http.MethodPost, "/references", encoded, status, demoKey)
}

func (d *walkthrough) assertReplay(body []byte, headers http.Header) error {
	if !bytes.Equal(body, d.receiptBody) || headers.Get("Idempotency-Replayed") != "true" ||
		headers.Get("Location") != "/references/"+d.receiptReference.ID.String() || headers.Get("Content-Type") != "application/json" ||
		d.provider.Calls() != d.receiptProviderCalls {
		return errors.New("retry changed receipt bytes, response headers, or provider call count")
	}
	return nil
}

func (d *walkthrough) checkReceiptCount(expected int) error {
	var count int
	if err := d.pool.QueryRow(d.ctx, `SELECT count(*) FROM reference_write_receipts`).Scan(&count); err != nil || count != expected {
		return errors.New("unexpected retained receipt count")
	}
	return nil
}

func (d *walkthrough) post(note string) (catalog.Reference, error) {
	encoded, err := json.Marshal(catalog.SaveInput{ExternalID: "doc-101", Note: note})
	if err != nil {
		return catalog.Reference{}, errors.New("encode synthetic save input")
	}
	body, err := d.request(http.MethodPost, "/references", encoded, http.StatusCreated)
	if err != nil {
		return catalog.Reference{}, err
	}
	var reference catalog.Reference
	if json.Unmarshal(body, &reference) != nil || reference.ID == uuid.Nil || reference.Resource.ID == uuid.Nil {
		return catalog.Reference{}, errors.New("decode saved reference")
	}
	fmt.Printf("   POST /references -> 201 reference=%s resource=%s note=%q\n", reference.ID, reference.Resource.ID, reference.Note)
	return reference, nil
}

func (d *walkthrough) get(id uuid.UUID) (catalog.View, error) {
	body, err := d.request(http.MethodGet, "/references/"+id.String(), nil, http.StatusOK)
	if err != nil {
		return catalog.View{}, err
	}
	var view catalog.View
	if json.Unmarshal(body, &view) != nil || view.ID != id {
		return catalog.View{}, errors.New("decode reference view")
	}
	fmt.Printf("   GET /references/%s -> 200 resource=%s state=%s", id, view.Resource.ID, view.MetadataState)
	if view.Metadata != nil {
		fmt.Printf(" title=%q fetchedAt=%s", view.Metadata.Title, view.Metadata.FetchedAt.Format(time.RFC3339))
	}
	fmt.Println()
	return view, nil
}

func (d *walkthrough) request(method, path string, body []byte, status int) ([]byte, error) {
	result, _, err := d.requestWithKey(method, path, body, status, "")
	return result, err
}

func (d *walkthrough) requestWithKey(method, path string, body []byte, status int, key string) ([]byte, http.Header, error) {
	req, err := http.NewRequestWithContext(d.ctx, method, d.server.URL+path, bytes.NewReader(body))
	if err != nil {
		return nil, nil, errors.New("construct local HTTP request")
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	response, err := d.client.Do(req)
	if err != nil {
		return nil, nil, errors.New("local HTTP request failed or exceeded its deadline")
	}
	defer response.Body.Close()
	const maxResponseBytes = 64 << 10
	result, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(result) > maxResponseBytes {
		return nil, nil, errors.New("read bounded HTTP response")
	}
	if response.StatusCode != status {
		return nil, nil, fmt.Errorf("HTTP status = %d, expected %d", response.StatusCode, status)
	}
	if bytes.Contains(result, []byte(discardedEvidence)) || bytes.Contains(result, []byte(`"diagnostic"`)) || bytes.Contains(result, []byte(`"candidates"`)) {
		return nil, nil, errors.New("discarded lookup evidence appeared in an HTTP response")
	}
	return result, response.Header.Clone(), nil
}

func (d *walkthrough) checkCounts(resources, references, metadata int) error {
	var r, s, m, receipts int
	err := d.pool.QueryRow(d.ctx, `SELECT
		(SELECT count(*) FROM resources),
		(SELECT count(*) FROM saved_references),
		(SELECT count(*) FROM resource_metadata),
		(SELECT count(*) FROM reference_write_receipts)`).Scan(&r, &s, &m, &receipts)
	if err != nil {
		return errors.New("read table counts")
	}
	if r != resources || s != references || m != metadata {
		return fmt.Errorf("row counts = %d/%d/%d; expected %d/%d/%d", r, s, m, resources, references, metadata)
	}
	fmt.Printf("   providerCalls=%d rows: resources=%d references=%d metadata=%d receipts=%d\n", d.provider.Calls(), r, s, m, receipts)
	return nil
}

func (d *walkthrough) checkStoredFetchTime() error {
	var fetchedAt time.Time
	if err := d.pool.QueryRow(d.ctx, `SELECT fetched_at FROM resource_metadata WHERE resource_id=$1`, d.first.Resource.ID).Scan(&fetchedAt); err != nil {
		return errors.New("read stored fetch timestamp")
	}
	if !fetchedAt.Equal(d.lastFetched) {
		return errors.New("failed refresh advanced the stored timestamp")
	}
	return nil
}

func (d *walkthrough) checkEvidenceExcluded() error {
	var persisted string
	err := d.pool.QueryRow(d.ctx, `SELECT
		(SELECT json_agg(r)::text FROM resources r) ||
		(SELECT json_agg(s)::text FROM saved_references s) ||
		(SELECT json_agg(m)::text FROM resource_metadata m)`).Scan(&persisted)
	if err != nil {
		return errors.New("inspect normalized persisted fields")
	}
	for _, excluded := range []string{discardedEvidence, `"diagnostic"`, `"candidates"`, `"score"`} {
		if strings.Contains(persisted, excluded) {
			return errors.New("discarded lookup evidence entered storage")
		}
	}
	return nil
}

func sameResource(a, b catalog.Resource) bool {
	return a.ID == b.ID && a.ExternalID == b.ExternalID && a.FirstVerifiedAt.Equal(b.FirstVerifiedAt)
}

func sameReference(a, b catalog.Reference) bool {
	return a.ID == b.ID && a.Note == b.Note && a.CreatedAt.Equal(b.CreatedAt) && sameResource(a.Resource, b.Resource)
}
