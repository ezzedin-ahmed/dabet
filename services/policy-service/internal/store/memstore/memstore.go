// Package memstore is the in-memory fake store.Repo used by unit tests.
// It mirrors the Postgres repository's observable behaviour: sentinel
// errors, (scope, scope_id) uniqueness, (created_at, id) list ordering,
// and defensive copies on every read and write.
package memstore

import (
	"context"
	"sort"
	"sync"

	"dabet/services/policy-service/internal/policy"
	"dabet/services/policy-service/internal/store"
)

// Mem is a threadsafe in-memory store.Repo.
type Mem struct {
	mu   sync.Mutex
	byID map[string]*policy.Policy

	// Calls counts repository method invocations by name, so cache tests
	// can assert that a cached resolve never touched the repository.
	calls map[string]int
}

// New returns an empty Mem.
func New() *Mem {
	return &Mem{byID: map[string]*policy.Policy{}, calls: map[string]int{}}
}

var _ store.Repo = (*Mem)(nil)

// Calls returns how many times the named method has been invoked.
func (m *Mem) Calls(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[method]
}

func clone(p *policy.Policy) *policy.Policy {
	c := *p
	c.RestrictedWords = append([]string(nil), p.RestrictedWords...)
	c.RestrictedContent = make([]policy.RestrictedContentEntry, len(p.RestrictedContent))
	for i, e := range p.RestrictedContent {
		e.Examples = append([]string(nil), e.Examples...)
		c.RestrictedContent[i] = e
	}
	if p.RateLimitMessages != nil {
		v := *p.RateLimitMessages
		c.RateLimitMessages = &v
	}
	if p.RateLimitSeconds != nil {
		v := *p.RateLimitSeconds
		c.RateLimitSeconds = &v
	}
	return &c
}

// Create implements store.Repo.
func (m *Mem) Create(_ context.Context, p *policy.Policy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls["Create"]++
	for _, existing := range m.byID {
		if existing.Scope == p.Scope && existing.ScopeID == p.ScopeID {
			return store.ErrDuplicate
		}
	}
	m.byID[p.ID] = clone(p)
	return nil
}

// GetByID implements store.Repo.
func (m *Mem) GetByID(_ context.Context, id string) (*policy.Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls["GetByID"]++
	p, ok := m.byID[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return clone(p), nil
}

// GetByScope implements store.Repo.
func (m *Mem) GetByScope(_ context.Context, scope policy.Scope, scopeID string) (*policy.Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls["GetByScope"]++
	for _, p := range m.byID {
		if p.Scope == scope && p.ScopeID == scopeID {
			return clone(p), nil
		}
	}
	return nil, store.ErrNotFound
}

// List implements store.Repo.
func (m *Mem) List(_ context.Context, creatorID string, f store.ListFilter, after *store.Cursor, limit int) ([]*policy.Policy, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls["List"]++
	var all []*policy.Policy
	for _, p := range m.byID {
		if p.CreatorID != creatorID {
			continue
		}
		if f.Scope != "" && p.Scope != f.Scope {
			continue
		}
		if f.ScopeID != "" && p.ScopeID != f.ScopeID {
			continue
		}
		all = append(all, p)
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.Before(all[j].CreatedAt)
		}
		return all[i].ID < all[j].ID
	})
	var out []*policy.Policy
	for _, p := range all {
		if after != nil {
			t := p.CreatedAt.UnixNano()
			if t < after.CreatedAtUnixNano || (t == after.CreatedAtUnixNano && p.ID <= after.ID) {
				continue
			}
		}
		out = append(out, clone(p))
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// Update implements store.Repo.
func (m *Mem) Update(_ context.Context, p *policy.Policy) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls["Update"]++
	existing, ok := m.byID[p.ID]
	if !ok {
		return store.ErrNotFound
	}
	next := clone(p)
	// Immutable fields keep their stored values regardless of input.
	next.CreatorID = existing.CreatorID
	next.Scope = existing.Scope
	next.ScopeID = existing.ScopeID
	next.CreatedAt = existing.CreatedAt
	m.byID[p.ID] = next
	return nil
}

// Delete implements store.Repo.
func (m *Mem) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls["Delete"]++
	if _, ok := m.byID[id]; !ok {
		return store.ErrNotFound
	}
	delete(m.byID, id)
	return nil
}
