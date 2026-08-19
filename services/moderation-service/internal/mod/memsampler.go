package mod

import (
	"sync"
	"time"
)

// MemSampler is the per-instance in-memory fallback sampler used only
// while Redis is down. DEVIATION from §4.7 (documented): the spec's table
// covers rate/dup/semantic but is silent on the sampler; treating every
// message as sampled would turn a Redis outage into an LLM stampede, and
// treating none as sampled would silently disable the LLM stage. A local
// bucket keeps the per-content ceiling approximately intact — it is
// per-instance rather than global, so with N instances the effective
// ceiling is up to N× during the outage.
type MemSampler struct {
	capacity     float64
	refillPerSec float64
	idleTTL      time.Duration

	mu      sync.Mutex
	buckets map[string]*memBucket
	ops     int
}

type memBucket struct {
	tokens float64
	ts     time.Time
}

// NewMemSampler mirrors the Redis sampler parameters (§7.5).
func NewMemSampler(capacity, refillPerMin float64, idleTTL time.Duration) *MemSampler {
	return &MemSampler{
		capacity:     capacity,
		refillPerSec: refillPerMin / 60,
		idleTTL:      idleTTL,
		buckets:      make(map[string]*memBucket),
	}
}

// Allow takes one token from the content's bucket, creating it full.
func (s *MemSampler) Allow(contentID string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.buckets[contentID]
	if !ok {
		b = &memBucket{tokens: s.capacity, ts: now}
		s.buckets[contentID] = b
	}
	if el := now.Sub(b.ts).Seconds(); el > 0 {
		b.tokens += el * s.refillPerSec
		if b.tokens > s.capacity {
			b.tokens = s.capacity
		}
	}
	b.ts = now
	allowed := b.tokens >= 1
	if allowed {
		b.tokens--
	}
	s.ops++
	if s.ops%4096 == 0 {
		s.sweep(now)
	}
	return allowed
}

// sweep drops buckets idle beyond the TTL, mirroring Redis key expiry.
// Called with mu held.
func (s *MemSampler) sweep(now time.Time) {
	for k, b := range s.buckets {
		if now.Sub(b.ts) > s.idleTTL {
			delete(s.buckets, k)
		}
	}
}
