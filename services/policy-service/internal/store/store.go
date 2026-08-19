// Package store defines the policy repository contract shared by the
// Postgres implementation and the in-memory fake used in tests.
package store

import (
	"context"
	"errors"
	"time"

	"dabet/services/policy-service/internal/policy"
)

// Sentinel errors mapped to API codes by the handlers.
var (
	// ErrNotFound: no such row. Handlers render it as 404, which is also
	// the ownership-mismatch answer (docs §4.1).
	ErrNotFound = errors.New("policy not found")
	// ErrDuplicate: (scope, scope_id) already has a policy — 409 conflict
	// (docs §6.1).
	ErrDuplicate = errors.New("policy already exists at this scope")
)

// Cursor is the opaque list cursor payload: strictly-increasing
// (created_at, id) position of the last item returned.
type Cursor struct {
	CreatedAtUnixNano int64  `json:"t"`
	ID                string `json:"id"`
}

// ListFilter narrows List. Zero values mean "no filter".
type ListFilter struct {
	Scope   policy.Scope
	ScopeID string
}

// Repo is the policy repository. All reads return defensive copies.
type Repo interface {
	// Create inserts p. ErrDuplicate when (scope, scope_id) is taken.
	Create(ctx context.Context, p *policy.Policy) error
	// GetByID returns the policy regardless of owner; ownership is the
	// handler's check so that "other creator's policy" and "absent" both
	// come out as 404.
	GetByID(ctx context.Context, id string) (*policy.Policy, error)
	// GetByScope returns the policy at exactly (scope, scopeID).
	GetByScope(ctx context.Context, scope policy.Scope, scopeID string) (*policy.Policy, error)
	// List returns up to limit of the creator's policies ordered by
	// (created_at, id), starting after the cursor when non-nil.
	List(ctx context.Context, creatorID string, f ListFilter, after *Cursor, limit int) ([]*policy.Policy, error)
	// Update replaces the stored document of p.ID with p's document and
	// updated_at. Scope fields are never changed. ErrNotFound if absent.
	Update(ctx context.Context, p *policy.Policy) error
	// Delete removes the policy. ErrNotFound if absent.
	Delete(ctx context.Context, id string) error
}

// Now returns the repository timestamp: UTC, microsecond precision, so the
// fake and Postgres (timestamptz, microseconds) agree exactly.
func Now() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}
