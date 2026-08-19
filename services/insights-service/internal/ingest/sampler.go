package ingest

import "time"

// Sampler is the per-content embedding ceiling of docs §8.4 — the same token
// bucket shape as the LLM sampler (§7.5): each content refills at a fixed
// rate up to a fixed capacity, and a message that finds no token is dropped
// with reason "sampled". Low-volume content is embedded exhaustively;
// firehose content is sampled down to a fixed cost, which is the lever that
// controls the dominant storage cost of the system.
//
// The bucket is in-process, per instance. A content's messages span
// partitions (messages.v1 is keyed by hash(author_id, content_id), §4.2), so
// each consumer instance holds an independent bucket and the effective
// ceiling is approximately per-instance × instances. This approximation is
// accepted: the ceiling is a statistical cost control, not an exact quota.
//
// Sampler is not safe for concurrent use; it is owned by the pipeline loop.
type Sampler struct {
	refillPerSec float64
	capacity     float64
	maxEntries   int
	buckets      map[string]*sampleBucket
}

type sampleBucket struct {
	tokens float64
	last   time.Time
}

// NewSampler builds a sampler refilling refillPerMinute tokens per minute per
// content, holding at most capacity tokens, tracking at most maxEntries
// contents.
func NewSampler(refillPerMinute, capacity float64, maxEntries int) *Sampler {
	return &Sampler{
		refillPerSec: refillPerMinute / 60,
		capacity:     capacity,
		maxEntries:   maxEntries,
		buckets:      make(map[string]*sampleBucket),
	}
}

// Allow reports whether a message on contentID may be embedded at now,
// consuming one token when it may.
func (s *Sampler) Allow(contentID string, now time.Time) bool {
	b, ok := s.buckets[contentID]
	if !ok {
		s.evictIfFull(now)
		b = &sampleBucket{tokens: s.capacity, last: now}
		s.buckets[contentID] = b
	} else {
		elapsed := now.Sub(b.last).Seconds()
		if elapsed > 0 {
			b.tokens = min(s.capacity, b.tokens+elapsed*s.refillPerSec)
			b.last = now
		}
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// evictIfFull bounds the bucket map. Contents idle long enough to have
// refilled to capacity carry no state worth keeping (a fresh bucket starts
// full), so they are evicted first; if none qualify, arbitrary entries go —
// resetting an active bucket to full briefly over-samples one content, which
// is preferable to unbounded memory.
func (s *Sampler) evictIfFull(now time.Time) {
	if len(s.buckets) < s.maxEntries {
		return
	}
	for id, b := range s.buckets {
		idle := now.Sub(b.last).Seconds()
		if b.tokens+idle*s.refillPerSec >= s.capacity {
			delete(s.buckets, id)
		}
	}
	for id := range s.buckets {
		if len(s.buckets) < s.maxEntries {
			break
		}
		delete(s.buckets, id)
	}
}
