package provider

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNormalize(t *testing.T) {
	summary := "  useful summary\n"
	document, err := Normalize(Document{ExternalID: "doc-101", Title: "  A document\n", Summary: &summary}, "doc-101")
	if err != nil || document.Title != "A document" || document.Summary == nil || *document.Summary != "useful summary" {
		t.Fatalf("normalized document = %#v, error = %v", document, err)
	}
	blank := " \t\n"
	document, err = Normalize(Document{ExternalID: "doc-101", Title: "A document", Summary: &blank}, "doc-101")
	if err != nil || document.Summary != nil {
		t.Fatalf("blank summary = %#v, error = %v", document.Summary, err)
	}
	tooLong := strings.Repeat("a", 2001)
	invalid := string([]byte{0xff})
	for name, document := range map[string]Document{
		"identity mismatch": {ExternalID: "doc-202", Title: "A document"},
		"blank title":       {ExternalID: "doc-101", Title: "  "},
		"long title":        {ExternalID: "doc-101", Title: strings.Repeat("a", 201)},
		"long summary":      {ExternalID: "doc-101", Title: "A document", Summary: &tooLong},
		"invalid title":     {ExternalID: "doc-101", Title: invalid},
		"invalid summary":   {ExternalID: "doc-101", Title: "A document", Summary: &invalid},
		"NUL title":         {ExternalID: "doc-101", Title: "a\x00b"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Normalize(document, "doc-101"); !errors.Is(err, ErrMalformed) {
				t.Fatalf("error = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestSimulatorDropsLookupEvidence(t *testing.T) {
	s := NewSimulator()
	raw := []byte(`{"externalId":"doc-101","title":"  Transaction boundaries ","summary":"  Durable state  ","diagnostics":{"evidence":"SYNTHETIC_DISCARD_ME"},"candidateIds":["discard-me"]}`)
	s.SetRaw("doc-101", raw)
	raw[0] = '!'
	document, err := s.Lookup(context.Background(), "doc-101")
	if err != nil || document.ExternalID != "doc-101" || document.Title != "Transaction boundaries" || document.Summary == nil || *document.Summary != "Durable state" {
		t.Fatalf("document = %#v, error = %v", document, err)
	}
	*document.Summary = "modified by caller"
	again, err := s.Lookup(context.Background(), "doc-101")
	if err != nil || *again.Summary != "Durable state" || s.Calls() != 2 {
		t.Fatalf("subsequent document = %#v, error = %v; calls = %d", again, err, s.Calls())
	}
}

func TestSimulatorOutcomes(t *testing.T) {
	s := NewSimulator()
	if _, err := s.Lookup(context.Background(), "absent"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown resource: %v", err)
	}
	s.SetError("doc-101", ErrUnavailable)
	if _, err := s.Lookup(context.Background(), "doc-101"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("outage: %v", err)
	}
	for _, raw := range [][]byte{
		[]byte(`{"externalId":"doc-202","title":"wrong identity"}`),
		[]byte(`{"externalId":"doc-101","title":42}`),
		[]byte(`{"externalId":"doc-101","title":""}`),
		[]byte(`{"externalId":"doc-101","title":"ok"} trailing`),
		[]byte("{\"externalId\":\"doc-101\",\"title\":\"\xff\"}"),
	} {
		s.SetRaw("doc-101", raw)
		if _, err := s.Lookup(context.Background(), "doc-101"); !errors.Is(err, ErrMalformed) {
			t.Fatalf("malformed result produced error %v", err)
		}
	}
}

func TestSimulatorHonorsCancellation(t *testing.T) {
	s := NewSimulator()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Lookup(ctx, "doc-101"); !errors.Is(err, context.Canceled) || s.Calls() != 0 {
		t.Fatalf("cancelled lookup error = %v, calls = %d", err, s.Calls())
	}
	s.SetDelay("doc-101", time.Hour)
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := s.Lookup(ctx, "doc-101"); !errors.Is(err, context.DeadlineExceeded) || s.Calls() != 1 {
		t.Fatalf("timed out lookup error = %v, calls = %d", err, s.Calls())
	}
}

func TestSimulatorConcurrentControlsAndLookups(t *testing.T) {
	s := NewSimulator()
	var group sync.WaitGroup
	for i := 0; i < 24; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			if i%2 == 0 {
				s.SetDocument(Document{ExternalID: "doc-101", Title: "Updated document"})
			} else {
				s.SetError("doc-101", ErrUnavailable)
			}
			_, err := s.Lookup(context.Background(), "doc-101")
			if err != nil && !errors.Is(err, ErrUnavailable) {
				t.Errorf("lookup error: %v", err)
			}
		}(i)
	}
	group.Wait()
	if s.Calls() != 24 {
		t.Fatalf("calls = %d, want 24", s.Calls())
	}
}
