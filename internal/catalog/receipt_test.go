package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"example.com/go-api-reliability-lab/internal/db/queries"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func receiptFixture(t *testing.T) queries.ReferenceWriteReceipt {
	t.Helper()
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	reference := Reference{ID: uuid.New(), Note: "Application note", CreatedAt: now,
		Resource: Resource{ID: uuid.New(), ExternalID: "doc-101", FirstVerifiedAt: now}}
	body, err := encodeCreateReceipt(reference)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("synthetic-key"))
	fingerprint := saveFingerprint(SaveInput{ExternalID: reference.Resource.ExternalID, Note: reference.Note})
	return queries.ReferenceWriteReceipt{
		ActorID: uuid.New(), Operation: referenceOperation, KeyDigest: digest[:], RequestFingerprint: fingerprint[:],
		State: "succeeded", ReferenceID: pgtype.UUID{Bytes: reference.ID, Valid: true}, ResponseBody: body,
		CompletedAt: timestamp(now), ExpiresAt: timestamp(now.Add(ReceiptRetention)),
	}
}

func TestFingerprintUsesNormalizedSemanticInput(t *testing.T) {
	first, err := normalizeSave(SaveInput{ExternalID: "doc-101", Note: "  two words\n"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := normalizeSave(SaveInput{ExternalID: "doc-101", Note: "two words"})
	if err != nil {
		t.Fatal(err)
	}
	if saveFingerprint(first) != saveFingerprint(second) {
		t.Fatal("outer whitespace changed fingerprint")
	}
	for _, changed := range []SaveInput{
		{ExternalID: "doc-202", Note: first.Note},
		{ExternalID: first.ExternalID, Note: "two  words"},
		{ExternalID: first.ExternalID, Note: "Two words"},
	} {
		if saveFingerprint(first) == saveFingerprint(changed) {
			t.Fatal("meaningful input change reused fingerprint")
		}
	}
}

func TestReceiptReplaysExactBytesAndCopiesBuffer(t *testing.T) {
	row := receiptFixture(t)
	result, active, err := inspectReceipt(row, row.CompletedAt.Time, row.RequestFingerprint)
	if err != nil || !active || !result.Replayed || !bytes.Equal(result.Body, row.ResponseBody) {
		t.Fatalf("receipt did not replay: active=%v err=%v", active, err)
	}
	result.Body[0] = '!'
	if row.ResponseBody[0] != '{' {
		t.Fatal("returned body aliases storage buffer")
	}
	different := sha256.Sum256([]byte("different"))
	if _, _, err := inspectReceipt(row, row.CompletedAt.Time, different[:]); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("mismatch: %v", err)
	}
	if _, active, err := inspectReceipt(row, row.ExpiresAt.Time, different[:]); err != nil || active {
		t.Fatalf("exact expiry must allow reuse: active=%v err=%v", active, err)
	}
}

func TestReceiptRejectsCorruptLiveRepresentations(t *testing.T) {
	for name, corrupt := range map[string]func(*queries.ReferenceWriteReceipt){
		"unknown field": func(r *queries.ReferenceWriteReceipt) {
			r.ResponseBody = append([]byte(`{"metadata":{"title":"provider-only"},`), r.ResponseBody[1:]...)
		},
		"duplicate field": func(r *queries.ReferenceWriteReceipt) {
			r.ResponseBody = append([]byte(`{"note":"Application note",`), r.ResponseBody[1:]...)
		},
		"trailing document": func(r *queries.ReferenceWriteReceipt) { r.ResponseBody = append(r.ResponseBody, []byte(`{}`)...) },
		"truncated":         func(r *queries.ReferenceWriteReceipt) { r.ResponseBody = r.ResponseBody[:len(r.ResponseBody)/2] },
		"oversized":         func(r *queries.ReferenceWriteReceipt) { r.ResponseBody = bytes.Repeat([]byte("x"), MaxReceiptBytes+1) },
		"unknown version":   func(r *queries.ReferenceWriteReceipt) { r.Operation = "reference.create.v2" },
		"wrong reference":   func(r *queries.ReferenceWriteReceipt) { r.ReferenceID.Bytes = uuid.New() },
		"wrong fingerprint": func(r *queries.ReferenceWriteReceipt) { r.RequestFingerprint = bytes.Repeat([]byte{7}, 32) },
		"invalid expiry":    func(r *queries.ReferenceWriteReceipt) { r.ExpiresAt = timestamp(r.CompletedAt.Time) },
		"zero resource": func(r *queries.ReferenceWriteReceipt) {
			var v Reference
			if err := json.Unmarshal(r.ResponseBody, &v); err != nil {
				t.Fatal(err)
			}
			v.Resource.ID = uuid.Nil
			b, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			r.ResponseBody = append(b, '\n')
		},
	} {
		t.Run(name, func(t *testing.T) {
			row := receiptFixture(t)
			corrupt(&row)
			if _, _, err := inspectReceipt(row, row.CompletedAt.Time, row.RequestFingerprint); !errors.Is(err, ErrInvariant) {
				t.Fatalf("corrupt receipt not rejected: %v", err)
			}
		})
	}
}

func TestExpiredBodyIsDisposable(t *testing.T) {
	row := receiptFixture(t)
	row.ResponseBody = []byte("obsolete invalid body")
	if _, active, err := inspectReceipt(row, row.ExpiresAt.Time, row.RequestFingerprint); err != nil || active {
		t.Fatalf("expired body must not prevent reuse: active=%v err=%v", active, err)
	}
}

func TestMaximumEscapedNoteFitsReceiptBound(t *testing.T) {
	row := receiptFixture(t)
	var reference Reference
	if err := json.Unmarshal(row.ResponseBody, &reference); err != nil {
		t.Fatal(err)
	}
	// Every character expands to six bytes in JSON; it is valid authored text.
	reference.Note = strings.Repeat("\x01", 1000)
	body, err := encodeCreateReceipt(reference)
	if err != nil || len(body) > MaxReceiptBytes {
		t.Fatalf("maximum note: bytes=%d err=%v", len(body), err)
	}
	if body[len(body)-1] != '\n' {
		t.Fatal("receipt lost terminal newline")
	}
}
