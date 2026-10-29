// Package catalog separates application-owned references and historical identity
// verification from replaceable provider display data.
package catalog

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

const (
	FreshFor              = time.Minute
	StaleGrace            = 10 * time.Minute
	Retention             = time.Hour
	ProviderTimeout       = 500 * time.Millisecond
	MaxPurgeBatch   int32 = 1000
)

var (
	ErrInvalidInput      = errors.New("invalid input")
	ErrReferenceNotFound = errors.New("reference not found")
	ErrStoreUnavailable  = errors.New("storage unavailable")
	ErrInvariant         = errors.New("invalid stored state")
)

type SaveInput struct {
	ExternalID string `json:"externalId"`
	Note       string `json:"note"`
}

type Resource struct {
	ID              uuid.UUID `json:"id"`
	ExternalID      string    `json:"externalId"`
	FirstVerifiedAt time.Time `json:"firstVerifiedAt"`
}

type Reference struct {
	ID        uuid.UUID `json:"id"`
	Resource  Resource  `json:"resource"`
	Note      string    `json:"note"`
	CreatedAt time.Time `json:"createdAt"`
}

type Metadata struct {
	Title     string    `json:"title"`
	Summary   *string   `json:"summary"`
	FetchedAt time.Time `json:"fetchedAt"`
}

type View struct {
	Reference
	Metadata      *Metadata `json:"metadata"`
	MetadataState string    `json:"metadataState"`
}
