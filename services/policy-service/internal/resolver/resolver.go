// Package resolver implements policy resolution (docs §6.2) behind a
// Memcached read-through (docs §6.8): content → platform → creator → none,
// first match wins, whole document, no field merge, ever.
//
// A negative result is a first-class, cacheable answer (§6.7): "this
// content has no policy" is stored with the same TTL as a positive one,
// because at 500K msg/s an uncached negative is a database read per
// message on every unconfigured content.
package resolver

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"dabet/pkg/obs"

	"dabet/services/policy-service/internal/cache"
	"dabet/services/policy-service/internal/metrics"
	"dabet/services/policy-service/internal/policy"
	"dabet/services/policy-service/internal/store"
)

// DefaultTTL is the Memcached TTL from docs §6.8 (assumption A10). The
// deployed value is env-overridable; this is the default, not a constant
// truth.
const DefaultTTL = 300 * time.Second

// Result values for policy_resolve_total{result}.
const (
	ResultContent  = "content"
	ResultPlatform = "platform"
	ResultCreator  = "creator"
	ResultNone     = "none"
)

// entry is the cached representation: either a policy or a first-class
// negative.
type entry struct {
	Found  bool           `json:"found"`
	Result string         `json:"result"`
	Policy *policy.Policy `json:"policy,omitempty"`
}

// Resolver resolves effective policies with a read-through cache.
type Resolver struct {
	repo store.Repo
	c    cache.Client
	m    *metrics.Metrics
	std  *obs.Metrics
	ttl  int32
}

// New builds a Resolver. ttl rounds down to whole seconds for Memcached.
func New(repo store.Repo, c cache.Client, m *metrics.Metrics, std *obs.Metrics, ttl time.Duration) *Resolver {
	return &Resolver{repo: repo, c: c, m: m, std: std, ttl: int32(ttl / time.Second)}
}

func cacheKey(creatorID, platform, contentID string) string {
	// Memcached keys must be <250 bytes with no whitespace: creator_id is
	// a UUID, platform an enum word, content_id an opaque string ≤64
	// chars (§4.2), so the concatenation is always safe.
	return "pol:v1:" + creatorID + "|" + platform + "|" + contentID
}

// Resolve returns the effective policy for (creator, platform, content)
// and the winning scope name, or (nil, "none") when no policy exists at
// any scope. An error is returned only when Postgres itself fails — a
// Memcached failure reads through and continues (docs §6.8).
func (r *Resolver) Resolve(ctx context.Context, creatorID, platform, contentID string) (*policy.Policy, string, error) {
	key := cacheKey(creatorID, platform, contentID)

	start := time.Now()
	raw, err := r.c.Get(key)
	switch {
	case err == nil:
		r.std.DependencyUp.WithLabelValues("memcached").Set(1)
		var e entry
		if jsonErr := json.Unmarshal(raw, &e); jsonErr == nil {
			r.m.CacheHits.WithLabelValues("memcached", "true").Inc()
			r.m.ResolveDuration.WithLabelValues("memcached").Observe(time.Since(start).Seconds())
			r.m.ResolveTotal.WithLabelValues(e.Result).Inc()
			return e.Policy, e.Result, nil
		}
		// A corrupt entry is treated as a miss and overwritten below.
	case errors.Is(err, cache.ErrMiss):
		r.std.DependencyUp.WithLabelValues("memcached").Set(1)
		r.m.CacheHits.WithLabelValues("memcached", "false").Inc()
	default:
		// Memcached is down: read through to Postgres and continue.
		r.std.DependencyUp.WithLabelValues("memcached").Set(0)
	}

	dbStart := time.Now()
	p, result, dbErr := r.resolveFromDB(ctx, creatorID, platform, contentID)
	if dbErr != nil {
		return nil, "", dbErr
	}
	r.m.ResolveDuration.WithLabelValues("postgres").Observe(time.Since(dbStart).Seconds())
	r.m.ResolveTotal.WithLabelValues(result).Inc()

	if buf, jsonErr := json.Marshal(entry{Found: p != nil, Result: result, Policy: p}); jsonErr == nil {
		if setErr := r.c.Set(key, buf, r.ttl); setErr != nil {
			r.std.DependencyUp.WithLabelValues("memcached").Set(0)
		}
	}
	return p, result, nil
}

// resolveFromDB walks the precedence chain (docs §6.2). Absence at a scope
// is not an error; any other repository error aborts the resolve so the
// caller (moderation-service) can fail open.
func (r *Resolver) resolveFromDB(ctx context.Context, creatorID, platform, contentID string) (*policy.Policy, string, error) {
	type candidate struct {
		scope   policy.Scope
		scopeID string
		result  string
	}
	candidates := make([]candidate, 0, 3)
	if contentID != "" {
		candidates = append(candidates, candidate{policy.ScopeContent, contentID, ResultContent})
	}
	if platform != "" {
		candidates = append(candidates, candidate{policy.ScopePlatform, creatorID + ":" + platform, ResultPlatform})
	}
	candidates = append(candidates, candidate{policy.ScopeCreator, creatorID, ResultCreator})

	for _, c := range candidates {
		p, err := r.repo.GetByScope(ctx, c.scope, c.scopeID)
		if err == nil {
			return p, c.result, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return nil, "", err
		}
	}
	return nil, ResultNone, nil
}
