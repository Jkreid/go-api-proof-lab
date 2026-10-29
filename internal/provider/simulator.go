package provider

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"unicode/utf8"
)

type reply struct {
	raw   []byte
	err   error
	delay time.Duration
}

// Simulator is an in-process provider. Mutation methods belong to demo/test
// harnesses; the HTTP API exposes none of them.
type Simulator struct {
	mu      sync.Mutex
	replies map[string]reply
	calls   int
}

func NewSimulator() *Simulator {
	s := &Simulator{replies: make(map[string]reply)}
	summary := "An introduction to transaction boundaries and durable state."
	s.SetDocument(Document{ExternalID: "doc-101", Title: "Transaction boundaries", Summary: &summary})
	s.SetDocument(Document{ExternalID: "doc-202", Title: "Designing replaceable metadata"})
	return s
}

func (s *Simulator) SetDocument(document Document) {
	if !utf8.ValidString(document.Title) || (document.Summary != nil && !utf8.ValidString(*document.Summary)) {
		s.SetError(document.ExternalID, ErrMalformed)
		return
	}
	raw, _ := json.Marshal(document) // This string-only type cannot fail to marshal.
	s.SetRaw(document.ExternalID, raw)
}

// SetRaw permits synthetic extra fields and malformed responses in scenarios.
// Lookup drops fields outside Document instead of archiving lookup evidence.
func (s *Simulator) SetRaw(externalID string, raw []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies[externalID] = reply{raw: append([]byte(nil), raw...)}
}

func (s *Simulator) SetError(externalID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replies[externalID] = reply{err: err}
}

// SetDelay makes timeout behavior observable without a real network service.
func (s *Simulator) SetDelay(externalID string, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.replies[externalID]
	r.delay = delay
	s.replies[externalID] = r
}

func (s *Simulator) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *Simulator) Lookup(ctx context.Context, externalID string) (Document, error) {
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	s.mu.Lock()
	s.calls++
	r, ok := s.replies[externalID]
	s.mu.Unlock()
	if !ok {
		return Document{}, ErrNotFound
	}
	if r.delay > 0 {
		timer := time.NewTimer(r.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return Document{}, ctx.Err()
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return Document{}, err
	}
	if r.err != nil {
		return Document{}, r.err
	}
	var document Document
	if !utf8.Valid(r.raw) || json.Unmarshal(r.raw, &document) != nil {
		return Document{}, ErrMalformed
	}
	return Normalize(document, externalID)
}
