package httpapi

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/go-api-reliability-lab/internal/catalog"
	"github.com/google/uuid"
)

func TestOptionalIdempotencyHeaders(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  []string
		present bool
		valid   bool
	}{
		{"absent", nil, false, true},
		{"short", []string{"A"}, true, true},
		{"punctuation", []string{"aBc-0_.:9"}, true, true},
		{"max length", []string{strings.Repeat("A", 128)}, true, true},
		{"empty", []string{""}, true, false},
		{"empty values", nil, true, false},
		{"duplicate", []string{"same", "same"}, true, false},
		{"comma joined", []string{"one,two"}, true, false},
		{"space", []string{"one two"}, true, false},
		{"leading space", []string{" one"}, true, false},
		{"unicode", []string{"café"}, true, false},
		{"too long", []string{strings.Repeat("a", 129)}, true, false},
		{"first punctuation", []string{"_abc"}, true, false},
		{"newline", []string{"a\n"}, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, keyedCalls := 0, 0
			id := uuid.New()
			service := stubService{
				save: func(context.Context, catalog.SaveInput) (catalog.Reference, error) {
					calls++
					return catalog.Reference{ID: id}, nil
				},
				keyed: func(context.Context, uuid.UUID, string, catalog.SaveInput) (catalog.CreateReceipt, error) {
					keyedCalls++
					return catalog.CreateReceipt{ReferenceID: id, Body: []byte("{}\n")}, nil
				},
			}
			req := httptest.NewRequest(http.MethodPost, "/references", strings.NewReader(`{"externalId":"doc-101","note":"note"}`))
			req.Header.Set("Content-Type", "application/json")
			if tc.present {
				req.Header["Idempotency-Key"] = tc.values
			}
			response := httptest.NewRecorder()
			NewHandler(service).ServeHTTP(response, req)
			if !tc.valid {
				if response.Code != 400 || calls+keyedCalls != 0 {
					t.Fatalf("invalid header reached service: status=%d calls=%d", response.Code, calls+keyedCalls)
				}
			} else if response.Code != 201 || (tc.present && (keyedCalls != 1 || calls != 0)) || (!tc.present && (calls != 1 || keyedCalls != 0)) {
				t.Fatalf("wrong dispatch: status=%d unkeyed=%d keyed=%d", response.Code, calls, keyedCalls)
			}
			if !tc.present && response.Header().Get("Idempotency-Replayed") != "" {
				t.Fatal("unkeyed response gained replay header")
			}
		})
	}
}

func TestKeyedHTTPUsesServerActorAndExactReceipt(t *testing.T) {
	actor, id := uuid.New(), uuid.New()
	original := []byte(fmt.Sprintf("{\"note\":\"Application text\",\"id\":%q}\n", id.String()))
	for _, replayed := range []bool{false, true} {
		service := stubService{keyed: func(ctx context.Context, gotActor uuid.UUID, key string, input catalog.SaveInput) (catalog.CreateReceipt, error) {
			if gotActor != actor || key != "Case-Sensitive-001" || input.ExternalID != "doc-101" {
				t.Fatal("key or trusted actor changed")
			}
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) > OperationTimeout {
				t.Fatal("keyed operation lacks deadline")
			}
			return catalog.CreateReceipt{ReferenceID: id, Body: original, Replayed: replayed}, nil
		}}
		req := httptest.NewRequest(http.MethodPost, "/references", strings.NewReader(`{"externalId":"doc-101","note":"Application text"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "Case-Sensitive-001")
		req.Header.Set("X-Demo-Actor", uuid.NewString())
		response := httptest.NewRecorder()
		NewHandlerForActor(service, actor).ServeHTTP(response, req)
		if response.Code != 201 || !bytes.Equal(response.Body.Bytes(), original) || response.Header().Get("Location") != "/references/"+id.String() ||
			response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Idempotency-Replayed") != fmt.Sprint(replayed) {
			t.Fatalf("receipt changed at HTTP boundary: status=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
		}
	}
}

func TestIdempotencyHTTPFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		err         error
		status      int
		code, retry string
	}{
		{catalog.ErrIdempotencyConflict, 409, "idempotency_conflict", ""},
		{catalog.ErrIdempotencyBusy, 503, "idempotency_busy", "1"},
		{catalog.ErrInvariant, 500, "internal_error", ""},
		{catalog.ErrStoreUnavailable, 503, "service_unavailable", ""},
	} {
		service := stubService{keyed: func(context.Context, uuid.UUID, string, catalog.SaveInput) (catalog.CreateReceipt, error) {
			return catalog.CreateReceipt{}, tc.err
		}}
		req := httptest.NewRequest(http.MethodPost, "/references", strings.NewReader(`{"externalId":"doc-101","note":"note"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "retry-001")
		response := httptest.NewRecorder()
		NewHandler(service).ServeHTTP(response, req)
		if response.Code != tc.status || response.Body.String() != fmt.Sprintf("{\"error\":{\"code\":%q}}\n", tc.code) || response.Header().Get("Retry-After") != tc.retry {
			t.Fatalf("error response status=%d body=%s headers=%v", response.Code, response.Body.String(), response.Header())
		}
	}
}
