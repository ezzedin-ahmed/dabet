// Package ledger defines the billing ledger of docs §5.3: an append-only
// credit_entries table with a unique idempotency key, and a
// creator_balances table maintained on write. Apply is the single
// mutation; everything else reads.
package ledger

import (
	"context"
	"time"
)

// Entry reasons per §5.3. Usage entries use the usage.v1 event_type
// verbatim (messages_processed | messages_reclustered).
const (
	ReasonTopup      = "topup"
	ReasonAdjustment = "adjustment"
)

// Entry is one ledger row.
type Entry struct {
	ID             int64          `json:"id"`
	CreatorID      string         `json:"-"`
	Delta          int64          `json:"delta"`
	Reason         string         `json:"reason"`
	IdempotencyKey string         `json:"idempotency_key"`
	Metadata       map[string]any `json:"metadata"`
	CreatedAt      time.Time      `json:"created_at"`
}

// ApplyResult reports what Apply did.
type ApplyResult struct {
	// Replayed is true when the idempotency key had already been used:
	// nothing was inserted and Balance is the existing balance.
	Replayed bool
	// Balance is the creator's balance after the call.
	Balance int64
}

// Repository is the ledger storage contract. The Postgres implementation
// lives in internal/repo; Memory below has identical semantics for tests.
type Repository interface {
	// Apply runs the §5.3 transaction: insert the entry with
	// ON CONFLICT (idempotency_key) DO NOTHING; if nothing was inserted
	// this is a replay — commit and return the existing balance —
	// otherwise upsert creator_balances += delta. Balances may go
	// negative (refund, dispute); that is allowed.
	Apply(ctx context.Context, creatorID string, delta int64, reason, idempotencyKey string, metadata map[string]any) (ApplyResult, error)

	// Balance returns the creator's balance row. found is false when the
	// creator has no row yet (treat as balance 0).
	Balance(ctx context.Context, creatorID string) (balance int64, updatedAt time.Time, found bool, err error)

	// Entries returns up to limit ledger rows for the creator, newest
	// first (descending id). beforeID = 0 starts from the newest;
	// otherwise only rows with id < beforeID are returned.
	Entries(ctx context.Context, creatorID string, beforeID int64, limit int) ([]Entry, error)

	// LastTopup returns the delta of the creator's most recent entry with
	// reason 'topup'. found is false when there has never been one.
	LastTopup(ctx context.Context, creatorID string) (delta int64, found bool, err error)

	// NegativeBalances counts creators whose balance is below zero.
	NegativeBalances(ctx context.Context) (int64, error)
}
