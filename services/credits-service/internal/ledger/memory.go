package ledger

import (
	"context"
	"sync"
	"time"
)

// Memory is an in-memory Repository with semantics identical to the
// Postgres implementation (internal/repo): same replay behaviour, same
// ordering, same zero-row handling. It backs unit tests; the SQL
// implementation is covered by a POSTGRES_DSN-gated test in internal/repo.
type Memory struct {
	mu       sync.Mutex
	nextID   int64
	entries  []Entry
	balances map[string]balanceRow
	now      func() time.Time
}

type balanceRow struct {
	balance   int64
	updatedAt time.Time
}

// NewMemory returns an empty in-memory ledger.
func NewMemory() *Memory {
	return &Memory{nextID: 1, balances: make(map[string]balanceRow), now: time.Now}
}

// SetNow overrides the clock (tests).
func (m *Memory) SetNow(now func() time.Time) { m.now = now }

// Apply implements Repository.
func (m *Memory) Apply(_ context.Context, creatorID string, delta int64, reason, idempotencyKey string, metadata map[string]any) (ApplyResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, e := range m.entries {
		if e.IdempotencyKey == idempotencyKey {
			// Replay: nothing inserted, existing balance returned.
			return ApplyResult{Replayed: true, Balance: m.balances[creatorID].balance}, nil
		}
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	now := m.now().UTC()
	m.entries = append(m.entries, Entry{
		ID:             m.nextID,
		CreatorID:      creatorID,
		Delta:          delta,
		Reason:         reason,
		IdempotencyKey: idempotencyKey,
		Metadata:       metadata,
		CreatedAt:      now,
	})
	m.nextID++
	row := m.balances[creatorID]
	row.balance += delta
	row.updatedAt = now
	m.balances[creatorID] = row
	return ApplyResult{Replayed: false, Balance: row.balance}, nil
}

// Balance implements Repository.
func (m *Memory) Balance(_ context.Context, creatorID string) (int64, time.Time, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.balances[creatorID]
	if !ok {
		return 0, time.Time{}, false, nil
	}
	return row.balance, row.updatedAt, true, nil
}

// Entries implements Repository.
func (m *Memory) Entries(_ context.Context, creatorID string, beforeID int64, limit int) ([]Entry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Entry
	for i := len(m.entries) - 1; i >= 0 && len(out) < limit; i-- {
		e := m.entries[i]
		if e.CreatorID != creatorID {
			continue
		}
		if beforeID != 0 && e.ID >= beforeID {
			continue
		}
		out = append(out, e)
	}
	return out, nil
}

// LastTopup implements Repository.
func (m *Memory) LastTopup(_ context.Context, creatorID string) (int64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.entries) - 1; i >= 0; i-- {
		e := m.entries[i]
		if e.CreatorID == creatorID && e.Reason == ReasonTopup {
			return e.Delta, true, nil
		}
	}
	return 0, false, nil
}

// NegativeBalances implements Repository.
func (m *Memory) NegativeBalances(context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, row := range m.balances {
		if row.balance < 0 {
			n++
		}
	}
	return n, nil
}
