// Package cachetest is the in-memory fake Memcached used by unit tests:
// a map behind the cache.Client interface, with switches to simulate a
// Memcached outage.
package cachetest

import (
	"errors"
	"sync"

	"dabet/services/policy-service/internal/cache"
)

// ErrDown is what the fake returns while Down is set — deliberately not
// cache.ErrMiss, so callers exercise the outage path, not the miss path.
var ErrDown = errors.New("memcached down")

// Fake is a threadsafe in-memory cache.Client.
type Fake struct {
	mu   sync.Mutex
	data map[string][]byte

	// Down makes every Get and Set fail with ErrDown.
	Down bool

	gets, sets int
}

// New returns an empty Fake.
func New() *Fake { return &Fake{data: map[string][]byte{}} }

var _ cache.Client = (*Fake)(nil)

// Get implements cache.Client.
func (f *Fake) Get(key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets++
	if f.Down {
		return nil, ErrDown
	}
	v, ok := f.data[key]
	if !ok {
		return nil, cache.ErrMiss
	}
	return append([]byte(nil), v...), nil
}

// Set implements cache.Client. The TTL is recorded nowhere: unit tests
// assert read-through behaviour, not clock behaviour.
func (f *Fake) Set(key string, value []byte, _ int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sets++
	if f.Down {
		return ErrDown
	}
	f.data[key] = append([]byte(nil), value...)
	return nil
}

// SetDown toggles the simulated outage.
func (f *Fake) SetDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Down = down
}

// Len returns the number of stored entries.
func (f *Fake) Len() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.data)
}
