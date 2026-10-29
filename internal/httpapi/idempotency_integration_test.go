//go:build integration

package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/go-api-reliability-lab/internal/catalog"
	"example.com/go-api-reliability-lab/internal/provider"
	"example.com/go-api-reliability-lab/internal/testutil"
	"github.com/google/uuid"
)

type lostDelivery struct {
	header http.Header
	status int
	body   []byte
	failed bool
}

func (w *lostDelivery) Header() http.Header    { return w.header }
func (w *lostDelivery) WriteHeader(status int) { w.status = status }
func (w *lostDelivery) Write(body []byte) (int, error) {
	w.failed = true
	w.body = bytes.Clone(body)
	return 0, errors.New("synthetic response delivery failure")
}

func TestCommittedHTTPWriteReplaysAfterDeliveryFailure(t *testing.T) {
	pool := testutil.OpenDB(t)
	simulator := provider.NewSimulator()
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	handler := NewHandler(catalog.New(pool, simulator, func() time.Time { return now }))
	key := uuid.NewString()
	request := httptest.NewRequest(http.MethodPost, "/references", strings.NewReader(`{"externalId":"doc-101","note":"A saved note"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	failed := &lostDelivery{header: make(http.Header)}
	handler.ServeHTTP(failed, request)
	if !failed.failed || failed.status != 201 || failed.header.Get("Idempotency-Replayed") != "false" {
		t.Fatalf("first response did not attempt201 delivery: status=%d", failed.status)
	}
	var original catalog.Reference
	if err := json.Unmarshal(failed.body, &original); err != nil || original.ID == uuid.Nil {
		t.Fatalf("original body: %v", err)
	}
	var refs, receipts int
	if err := pool.QueryRow(t.Context(), `SELECT (SELECT count(*) FROM saved_references),(SELECT count(*) FROM reference_write_receipts WHERE state='succeeded')`).Scan(&refs, &receipts); err != nil {
		t.Fatal(err)
	}
	if refs != 1 || receipts != 1 {
		t.Fatalf("write/receipt not committed before faileddelivery: %d/%d", refs, receipts)
	}
	calls := simulator.Calls()

	server := httptest.NewServer(handler)
	defer server.Close()
	retry, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/references", strings.NewReader(`{ "note": "  A saved note  ", "externalId": "doc-101" }`))
	if err != nil {
		t.Fatal(err)
	}
	retry.Header.Set("Content-Type", "application/json")
	retry.Header.Set("Idempotency-Key", key)
	client := server.Client()
	client.Timeout = 3 * time.Second
	response, err := client.Do(retry)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, catalog.MaxReceiptBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 201 || !bytes.Equal(body, failed.body) || response.Header.Get("Location") != failed.header.Get("Location") ||
		response.Header.Get("Content-Type") != failed.header.Get("Content-Type") || response.Header.Get("Idempotency-Replayed") != "true" || simulator.Calls() != calls {
		t.Fatal("HTTP retry did not replay the exact successful response without provider I/O")
	}
	if err := pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM saved_references),(SELECT count(*) FROM reference_write_receipts)`).Scan(&refs, &receipts); err != nil {
		t.Fatal(err)
	}
	if refs != 1 || receipts != 1 {
		t.Fatal("HTTP retry duplicated the mutation")
	}
}
