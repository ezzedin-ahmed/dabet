// Package cache is a minimal interface over Memcached (docs §6.8), so the
// resolver can be tested against a fake and so a Memcached outage is a
// value (an error return) rather than a panic. TTL-only; there is no
// invalidation bus by design.
package cache

import (
	"errors"

	"github.com/bradfitz/gomemcache/memcache"
)

// ErrMiss is returned by Get when the key is absent. Any other error means
// the cache itself failed and the caller should read through to Postgres.
var ErrMiss = errors.New("cache miss")

// Client is the read-through cache used by the resolver.
type Client interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte, ttlSeconds int32) error
}

// Memcached adapts *memcache.Client to Client.
type Memcached struct {
	mc *memcache.Client
}

// NewMemcached builds a Memcached client over the given server addresses.
func NewMemcached(addrs ...string) *Memcached {
	return &Memcached{mc: memcache.New(addrs...)}
}

var _ Client = (*Memcached)(nil)

// Get implements Client, translating the miss sentinel.
func (m *Memcached) Get(key string) ([]byte, error) {
	it, err := m.mc.Get(key)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			return nil, ErrMiss
		}
		return nil, err
	}
	return it.Value, nil
}

// Set implements Client.
func (m *Memcached) Set(key string, value []byte, ttlSeconds int32) error {
	return m.mc.Set(&memcache.Item{Key: key, Value: value, Expiration: ttlSeconds})
}
