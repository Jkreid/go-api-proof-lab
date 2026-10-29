// Package provider defines the lookup boundary and a synthetic catalog.
package provider

import (
	"context"
	"errors"
	"strings"
	"unicode/utf8"
)

var (
	ErrNotFound    = errors.New("external resource not found")
	ErrUnavailable = errors.New("provider unavailable")
	ErrMalformed   = errors.New("malformed provider result")
)

// Document is the normalized subset of provider data the application uses.
// Raw response bodies and provider diagnostics do not cross this boundary.
type Document struct {
	ExternalID string  `json:"externalId"`
	Title      string  `json:"title"`
	Summary    *string `json:"summary"`
}

type Client interface {
	Lookup(context.Context, string) (Document, error)
}

// Normalize validates identity before a document can become a snapshot.
// Callers can apply this even when using a Client other than Simulator.
func Normalize(document Document, expectedID string) (Document, error) {
	if document.ExternalID != expectedID || expectedID == "" || !utf8.ValidString(document.ExternalID) || !utf8.ValidString(document.Title) || strings.ContainsRune(document.Title, '\x00') {
		return Document{}, ErrMalformed
	}
	document.Title = strings.TrimSpace(document.Title)
	if count := utf8.RuneCountInString(document.Title); count == 0 || count > 200 {
		return Document{}, ErrMalformed
	}
	if document.Summary != nil {
		if !utf8.ValidString(*document.Summary) || strings.ContainsRune(*document.Summary, '\x00') {
			return Document{}, ErrMalformed
		}
		summary := strings.TrimSpace(*document.Summary)
		if utf8.RuneCountInString(summary) > 2000 {
			return Document{}, ErrMalformed
		}
		document.Summary = nil
		if summary != "" {
			document.Summary = &summary
		}
	}
	return document, nil
}
