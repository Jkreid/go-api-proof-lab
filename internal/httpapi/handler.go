// Package httpapi exposes the sample's two catalog operations.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"example.com/go-api-reliability-lab/internal/catalog"
	"example.com/go-api-reliability-lab/internal/provider"
	"github.com/google/uuid"
)

const (
	MaxBodyBytes     = 16 * 1024
	OperationTimeout = 2 * time.Second
)

type Service interface {
	Save(context.Context, catalog.SaveInput) (catalog.Reference, error)
	SaveIdempotent(context.Context, uuid.UUID, string, catalog.SaveInput) (catalog.CreateReceipt, error)
	Get(context.Context, uuid.UUID) (catalog.View, error)
}

type handler struct {
	service Service
	actorID uuid.UUID
}

// The local server has one synthetic actor. It is a key namespace, not an
// authentication or access-control mechanism.
func NewHandler(service Service) http.Handler {
	return NewHandlerForActor(service, uuid.MustParse("11111111-1111-4111-8111-111111111111"))
}

// NewHandlerForActor binds an actor in trusted server/test setup. Request headers
// never select the actor, and no accounts or actor rows are needed by this sample.
func NewHandlerForActor(service Service, actorID uuid.UUID) http.Handler {
	if actorID == uuid.Nil {
		panic("HTTP handler requires a nonzero synthetic actor")
	}
	return &handler{service: service, actorID: actorID}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), OperationTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	// A context alone does not interrupt a slow client's request body reads.
	// The concrete HTTP server supports this; in-memory response recorders may not.
	deadline, _ := ctx.Deadline()
	_ = http.NewResponseController(w).SetReadDeadline(deadline)
	w.Header().Set("Cache-Control", "no-store")
	switch {
	case r.URL.Path == "/references":
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		h.save(w, r)
	case strings.HasPrefix(r.URL.Path, "/references/") && !strings.Contains(strings.TrimPrefix(r.URL.Path, "/references/"), "/"):
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		idText := strings.TrimPrefix(r.URL.Path, "/references/")
		id, err := uuid.Parse(idText)
		if err != nil || id.String() != idText {
			writeError(w, http.StatusBadRequest, "invalid_input")
			return
		}
		view, err := h.service.Get(ctx, id)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	default:
		writeError(w, http.StatusNotFound, "route_not_found")
	}
}

func (h *handler) save(w http.ResponseWriter, r *http.Request) {
	keys, keyed := r.Header[http.CanonicalHeaderKey("Idempotency-Key")]
	if keyed && (len(keys) != 1 || !catalog.ValidIdempotencyKey(keys[0])) {
		writeError(w, http.StatusBadRequest, "invalid_input")
		return
	}
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" || (params["charset"] != "" && !strings.EqualFold(params["charset"], "utf-8")) {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		var networkError net.Error
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "body_too_large")
		} else if r.Context().Err() != nil || (errors.As(err, &networkError) && networkError.Timeout()) {
			writeError(w, http.StatusServiceUnavailable, "service_unavailable")
		} else {
			writeError(w, http.StatusBadRequest, "invalid_input")
		}
		return
	}
	if !utf8.Valid(body) {
		writeError(w, http.StatusBadRequest, "invalid_input")
		return
	}
	input, err := decodeInput(bytes.NewReader(body))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_input")
		return
	}
	if keyed {
		receipt, err := h.service.SaveIdempotent(r.Context(), h.actorID, keys[0], input)
		if err != nil {
			writeServiceError(w, err)
			return
		}
		w.Header().Set("Location", "/references/"+receipt.ReferenceID.String())
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Idempotency-Replayed", strconv.FormatBool(receipt.Replayed))
		w.WriteHeader(http.StatusCreated)
		// Encoding and validation occurred inside the database transaction.
		// Response delivery can fail after commit; leave the receipt available.
		_, _ = w.Write(receipt.Body)
		return
	}
	reference, err := h.service.Save(r.Context(), input)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	w.Header().Set("Location", "/references/"+reference.ID.String())
	writeJSON(w, http.StatusCreated, reference)
}

// Read top-level fields explicitly because encoding/json otherwise accepts
// duplicate names, giving ambiguous input a last-field-wins interpretation.
func decodeInput(reader io.Reader) (catalog.SaveInput, error) {
	decoder := json.NewDecoder(reader)
	input := catalog.SaveInput{}
	start, err := decoder.Token()
	if err != nil {
		return input, err
	}
	if start != json.Delim('{') {
		return input, catalog.ErrInvalidInput
	}
	seen := make(map[string]bool, 2)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return input, err
		}
		name, ok := token.(string)
		if !ok || seen[name] || (name != "externalId" && name != "note") {
			return input, catalog.ErrInvalidInput
		}
		seen[name] = true
		value, err := decoder.Token()
		if err != nil {
			return input, err
		}
		text, ok := value.(string)
		if !ok {
			return input, catalog.ErrInvalidInput
		}
		if name == "externalId" {
			input.ExternalID = text
		} else {
			input.Note = text
		}
	}
	if _, err := decoder.Token(); err != nil {
		return input, err
	}
	if !seen["externalId"] || !seen["note"] {
		return input, catalog.ErrInvalidInput
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return input, err
		}
		return input, catalog.ErrInvalidInput
	}
	return input, nil
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, catalog.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict")
	case errors.Is(err, catalog.ErrIdempotencyBusy):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "idempotency_busy")
	case errors.Is(err, catalog.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, "invalid_input")
	case errors.Is(err, catalog.ErrReferenceNotFound):
		writeError(w, http.StatusNotFound, "reference_not_found")
	case errors.Is(err, provider.ErrNotFound):
		writeError(w, http.StatusNotFound, "external_resource_not_found")
	case errors.Is(err, provider.ErrMalformed):
		writeError(w, http.StatusBadGateway, "malformed_provider_result")
	case errors.Is(err, provider.ErrUnavailable), errors.Is(err, catalog.ErrStoreUnavailable), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "service_unavailable")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error")
	}
}

func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	encoded, err := json.Marshal(body)
	if err != nil {
		status = http.StatusInternalServerError
		encoded = []byte(`{"error":{"code":"internal_error"}}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(encoded, '\n'))
}
