// Package memstore is an in-memory store.Store for handler tests, with
// the same CAS and transactional-advance semantics as the Postgres
// implementation.
package memstore

import (
	"context"
	"sync"
	"time"

	"dabet/services/review-service/internal/store"
)

// Mem implements store.Store in memory.
type Mem struct {
	mu      sync.Mutex
	cursors map[string]store.Cursor
	now     func() time.Time
}

// New builds an empty Mem.
func New() *Mem {
	return &Mem{cursors: make(map[string]store.Cursor), now: time.Now}
}

// Get returns the raw cursor row for test assertions.
func (m *Mem) Get(creatorID string) (store.Cursor, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cursors[creatorID]
	return c, ok
}

// GetOrInit implements store.Store.
func (m *Mem) GetOrInit(_ context.Context, creatorID string, partition int32, offset int64) (store.Cursor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.cursors[creatorID]; ok {
		return c, nil
	}
	c := store.Cursor{CreatorID: creatorID, Partition: partition, NextOffset: offset, UpdatedAt: m.now()}
	m.cursors[creatorID] = c
	return c, nil
}

// SetNextOffset implements store.Store.
func (m *Mem) SetNextOffset(_ context.Context, creatorID string, from, to int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cursors[creatorID]
	if !ok || c.NextOffset != from {
		return false, nil
	}
	c.NextOffset = to
	c.UpdatedAt = m.now()
	m.cursors[creatorID] = c
	return true, nil
}

// Reset implements store.Store.
func (m *Mem) Reset(_ context.Context, creatorID string, partition int32, offset int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cursors[creatorID] = store.Cursor{
		CreatorID: creatorID, Partition: partition, NextOffset: offset, UpdatedAt: m.now(),
	}
	return nil
}

// Advance implements store.Store.
func (m *Mem) Advance(ctx context.Context, creatorID string, from, to int64, mid func(ctx context.Context) error) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.cursors[creatorID]
	if !ok || c.NextOffset != from {
		return false, nil
	}
	if mid != nil {
		if err := mid(ctx); err != nil {
			return false, err
		}
	}
	c.NextOffset = to
	c.UpdatedAt = m.now()
	m.cursors[creatorID] = c
	return true, nil
}
