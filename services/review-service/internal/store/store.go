// Package store persists the review cursors of docs §7.6 — the only
// state review-service owns. One small row per creator: which flagged.v1
// partition their queue lives on and the next offset to review from.
package store

import (
	"context"
	"time"
)

// Cursor is one review.review_cursors row.
type Cursor struct {
	CreatorID  string
	Partition  int32
	NextOffset int64
	UpdatedAt  time.Time
}

// Store is the cursor repository. The Postgres implementation is the real
// one; memstore backs handler tests.
type Store interface {
	// GetOrInit returns the creator's cursor, first inserting
	// (partition, offset) if no row exists. Initialisation is not an
	// advance: a plain read never moves an existing cursor.
	GetOrInit(ctx context.Context, creatorID string, partition int32, offset int64) (Cursor, error)

	// SetNextOffset moves next_offset from -> to iff it still equals
	// from (compare-and-swap; used by the snap-forward of §7.6.3).
	// Returns whether the swap applied.
	SetNextOffset(ctx context.Context, creatorID string, from, to int64) (bool, error)

	// Reset unconditionally rewrites the cursor row (used when the
	// topic's partition count changed and the creator maps to a new
	// partition).
	Reset(ctx context.Context, creatorID string, partition int32, offset int64) error

	// Advance moves next_offset from -> to in one transaction on the
	// cursor row, calling mid while the row is locked and before the
	// update commits. If next_offset no longer equals from, nothing runs
	// and (false, nil) is returned — the caller's window is stale.
	Advance(ctx context.Context, creatorID string, from, to int64, mid func(ctx context.Context) error) (bool, error)
}
