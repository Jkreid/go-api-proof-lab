package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/go-api-reliability-lab/internal/catalog"
	"example.com/go-api-reliability-lab/internal/provider"
	"github.com/google/uuid"
)

type stubService struct {
	keyed func(context.Context, uuid.UUID, string, catalog.SaveInput) (catalog.CreateReceipt, error)
	save  func(context.Context, catalog.SaveInput) (catalog.Reference, error)
	get   func(context.Context, uuid.UUID) (catalog.View, error)
}

func (s stubService) Save(ctx context.Context, input catalog.SaveInput) (catalog.Reference, error) {
	return s.save(ctx, input)
}

func (s stubService) Get(ctx context.Context, id uuid.UUID) (catalog.View, error) {
	return s.get(ctx, id)
}

func (s stubService) SaveIdempotent(ctx context.Context, actor uuid.UUID, key string, input catalog.SaveInput) (catalog.CreateReceipt, error) {
	return s.keyed(ctx, actor, key, input)
}

func TestCreateReference(t *testing.T) {
	id := uuid.New()
	service := stubService{save: func(ctx context.Context, input catalog.SaveInput) (catalog.Reference, error) {
		if input.ExternalID != "doc-101" || input.Note != "Review transaction boundaries" {
			t.Fatalf("input = %#v", input)
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > OperationTimeout {
			t.Fatal("HTTP operation has no bounded deadline")
		}
		return catalog.Reference{ID: id, Note: input.Note}, nil
	}}
	request := httptest.NewRequest(http.MethodPost, "/references", strings.NewReader(`{"externalId":"doc-101","note":"Review transaction boundaries"}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	response := httptest.NewRecorder()
	NewHandler(service).ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Header().Get("Location") != "/references/"+id.String() {
		t.Fatalf("status = %d, location = %s; body = %s", response.Code, response.Header().Get("Location"), response.Body.String())
	}
	var actual catalog.Reference
	if err := json.Unmarshal(response.Body.Bytes(), &actual); err != nil || actual.ID != id {
		t.Fatalf("response = %#v, error = %v", actual, err)
	}
	if response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("headers = %#v", response.Header())
	}
}

func TestRejectAmbiguousAndInvalidBodies(t *testing.T) {
	service := stubService{save: func(context.Context, catalog.SaveInput) (catalog.Reference, error) {
		t.Fatal("invalid request reached service")
		return catalog.Reference{}, nil
	}}
	for name, body := range map[string]string{
		"empty":        "",
		"array":        "[]",
		"null":         "null",
		"unknown":      `{"externalId":"doc-101","note":"note","unexpected":true}`,
		"duplicate":    `{"externalId":"doc-101","externalId":"doc-202","note":"note"}`,
		"escaped key":  `{"externalId":"doc-101","note":"first","n\u006fte":"second"}`,
		"null note":    `{"externalId":"doc-101","note":null}`,
		"number":       `{"externalId":101,"note":"note"}`,
		"object":       `{"externalId":"doc-101","note":{}}`,
		"missing":      `{"externalId":"doc-101"}`,
		"trailing":     `{"externalId":"doc-101","note":"note"} {}`,
		"malformed":    `{"externalId":"doc-101","note":"note"`,
		"invalid utf8": "{\"externalId\":\"doc-101\",\"note\":\"\xff\"}",
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/references", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			NewHandler(service).ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestBodyAndMediaTypeLimits(t *testing.T) {
	for _, test := range []struct {
		mediaType string
		body      string
		status    int
	}{
		{"", `{}`, 415},
		{"text/plain", `{}`, 415},
		{"application/json; charset=iso-8859-1", `{}`, 415},
		{"application/json", strings.Repeat(" ", MaxBodyBytes+1), 413},
	} {
		request := httptest.NewRequest(http.MethodPost, "/references", strings.NewReader(test.body))
		request.Header.Set("Content-Type", test.mediaType)
		response := httptest.NewRecorder()
		NewHandler(stubService{}).ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("media type %q status = %d, want %d", test.mediaType, response.Code, test.status)
		}
	}
}

func TestSafeServiceErrors(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
		code   string
	}{
		{catalog.ErrInvalidInput, 400, "invalid_input"},
		{catalog.ErrReferenceNotFound, 404, "reference_not_found"},
		{provider.ErrNotFound, 404, "external_resource_not_found"},
		{provider.ErrUnavailable, 503, "service_unavailable"},
		{provider.ErrMalformed, 502, "malformed_provider_result"},
		{catalog.ErrStoreUnavailable, 503, "service_unavailable"},
		{context.DeadlineExceeded, 503, "service_unavailable"},
		{context.Canceled, 503, "service_unavailable"},
		{catalog.ErrInvariant, 500, "internal_error"},
		{errors.New("unknown failure"), 500, "internal_error"},
	} {
		t.Run(test.err.Error(), func(t *testing.T) {
			service := stubService{save: func(context.Context, catalog.SaveInput) (catalog.Reference, error) {
				return catalog.Reference{}, fmt.Errorf("SYNTHETIC_PRIVATE_ERROR_DETAIL: %w", test.err)
			}}
			request := httptest.NewRequest(http.MethodPost, "/references", strings.NewReader(`{"externalId":"doc-101","note":"note"}`))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			NewHandler(service).ServeHTTP(response, request)
			want := fmt.Sprintf("{\"error\":{\"code\":%q}}\n", test.code)
			if response.Code != test.status || response.Body.String() != want {
				t.Fatalf("status/body = %d %s, want %d %s", response.Code, response.Body.String(), test.status, want)
			}
		})
	}
}

func TestReadReferenceAndRouting(t *testing.T) {
	id := uuid.New()
	service := stubService{get: func(ctx context.Context, actualID uuid.UUID) (catalog.View, error) {
		if actualID != id {
			t.Fatalf("id = %v, want %v", actualID, id)
		}
		return catalog.View{Reference: catalog.Reference{ID: id, Note: "Application-owned note"}, MetadataState: "missing"}, nil
	}}
	for _, test := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/references/" + id.String(), 200},
		{http.MethodGet, "/references/not-an-id", 400},
		{http.MethodGet, "/references", 405},
		{http.MethodPost, "/references/" + id.String(), 405},
		{http.MethodGet, "/unavailable", 404},
		{http.MethodGet, "/references/extra/path", 404},
	} {
		response := httptest.NewRecorder()
		NewHandler(service).ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		if response.Code != test.status {
			t.Fatalf("%s %s = %d, want %d", test.method, test.path, response.Code, test.status)
		}
		if test.status == 200 && (!strings.Contains(response.Body.String(), `"metadataState":"missing"`) || !strings.Contains(response.Body.String(), `"metadata":null`)) {
			t.Fatalf("missing metadata response = %s", response.Body.String())
		}
	}
}
