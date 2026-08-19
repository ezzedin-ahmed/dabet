package api

import (
	"sync"
	"time"
)

// idemTTL is the §4.1 idempotency replay window.
const idemTTL = 24 * time.Hour

// idemStore replays stored POST responses per (creator_id, key) for 24h
// (docs §4.1). It is per-instance and in-memory — a documented deviation
// from a shared durable store, acceptable here because POST /v1/reviews is
// already naturally idempotent at the cursor level (§7.6): the header is a
// best-effort exact-response replay on top of a safe no-op.
type idemStore struct {
	mu  sync.Mutex
	m   map[string]idemEntry
	now func() time.Time
}

type idemEntry struct {
	status int
	body   []byte
	at     time.Time
}

func newIdemStore(now func() time.Time) *idemStore {
	if now == nil {
		now = time.Now
	}
	return &idemStore{m: make(map[string]idemEntry), now: now}
}

func idemKey(creatorID, key string) string { return creatorID + "\x00" + key }

// Get returns the stored response for (creator, key), if fresh.
func (s *idemStore) Get(creatorID, key string) (status int, body []byte, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, found := s.m[idemKey(creatorID, key)]
	if !found || s.now().Sub(e.at) > idemTTL {
		return 0, nil, false
	}
	return e.status, e.body, true
}

// Put stores a response and opportunistically sweeps expired entries.
func (s *idemStore) Put(creatorID, key string, status int, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if len(s.m) > 0 && len(s.m)%1024 == 0 {
		for k, e := range s.m {
			if now.Sub(e.at) > idemTTL {
				delete(s.m, k)
			}
		}
	}
	s.m[idemKey(creatorID, key)] = idemEntry{status: status, body: body, at: now}
}
