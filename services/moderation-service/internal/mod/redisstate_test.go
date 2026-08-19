package mod

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"dabet/pkg/rediskeys"
)

func newTestState(t *testing.T) (*RedisState, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedisState(rdb), mr
}

func TestSeenGuard(t *testing.T) {
	s, mr := newTestState(t)
	ctx := context.Background()

	already, err := s.Seen(ctx, "msg-1", 5*time.Minute)
	if err != nil || already {
		t.Fatalf("first Seen = (%v, %v), want (false, nil)", already, err)
	}
	already, err = s.Seen(ctx, "msg-1", 5*time.Minute)
	if err != nil || !already {
		t.Fatalf("second Seen = (%v, %v), want (true, nil)", already, err)
	}
	if ttl := mr.TTL(rediskeys.Seen("msg-1")); ttl != 5*time.Minute {
		t.Fatalf("seen TTL = %v, want 5m", ttl)
	}
	// A different message is unaffected.
	if already, _ := s.Seen(ctx, "msg-2", 5*time.Minute); already {
		t.Fatal("distinct message must not be seen")
	}
}

// Rate-limit bucket math: burst up to capacity, then refill at
// messages/seconds per second. The clock is passed as a script argument,
// so a fake clock drives refill deterministically.
func TestRateLimitBucketBurstThenRefill(t *testing.T) {
	s, mr := newTestState(t)
	ctx := context.Background()
	key := rediskeys.Rate("ct", "au")
	now := t0
	const capacity, refillPerSec = 3.0, 1.0 // 3 msgs / 3 s

	for i := 0; i < 3; i++ {
		ok, err := s.TakeToken(ctx, key, capacity, refillPerSec, now, 6*time.Second)
		if err != nil || !ok {
			t.Fatalf("burst token %d = (%v, %v), want allowed", i+1, ok, err)
		}
	}
	if ok, _ := s.TakeToken(ctx, key, capacity, refillPerSec, now, 6*time.Second); ok {
		t.Fatal("bucket exhausted, 4th take must be denied")
	}

	// 500 ms refills only half a token: still denied.
	if ok, _ := s.TakeToken(ctx, key, capacity, refillPerSec, now.Add(500*time.Millisecond), 6*time.Second); ok {
		t.Fatal("half a token must not admit")
	}
	// A further second refills past 1 (0.5 - already spent? no: denied takes
	// consume nothing), so at +1.5s the bucket holds 1.5 tokens.
	if ok, err := s.TakeToken(ctx, key, capacity, refillPerSec, now.Add(1500*time.Millisecond), 6*time.Second); err != nil || !ok {
		t.Fatalf("refilled token = (%v, %v), want allowed", ok, err)
	}
	// Refill never exceeds capacity: after an hour only 3 tokens exist.
	later := now.Add(time.Hour)
	allowed := 0
	for i := 0; i < 5; i++ {
		if ok, _ := s.TakeToken(ctx, key, capacity, refillPerSec, later, 6*time.Second); ok {
			allowed++
		}
	}
	if allowed != 3 {
		t.Fatalf("after idle hour got %d tokens, want capacity 3", allowed)
	}
	// TTL is 2x the window.
	if ttl := mr.TTL(key); ttl != 6*time.Second {
		t.Fatalf("rate key TTL = %v, want 6s", ttl)
	}
}

func TestDupCheckMembershipAndDepth(t *testing.T) {
	s, mr := newTestState(t)
	ctx := context.Background()
	key := rediskeys.Dup("ct", "au")

	hit, err := s.DupCheck(ctx, key, "h1", 3, 5*time.Minute)
	if err != nil || hit {
		t.Fatalf("first hash = (%v, %v), want miss", hit, err)
	}
	hit, _ = s.DupCheck(ctx, key, "h1", 3, 5*time.Minute)
	if !hit {
		t.Fatal("repeated hash must hit")
	}
	// Push h2..h4 (depth 3): h1 gets evicted from the window.
	for _, h := range []string{"h2", "h3", "h4"} {
		if hit, _ := s.DupCheck(ctx, key, h, 3, 5*time.Minute); hit {
			t.Fatalf("fresh hash %s must miss", h)
		}
	}
	if hit, _ := s.DupCheck(ctx, key, "h1", 3, 5*time.Minute); hit {
		t.Fatal("h1 aged out of the depth window, must miss")
	}
	if ttl := mr.TTL(key); ttl != 5*time.Minute {
		t.Fatalf("dup key TTL = %v, want 5m", ttl)
	}
}

// Sampler ceiling (§7.5): capacity tokens burst, then refill-limited.
func TestSamplerCeiling(t *testing.T) {
	s, _ := newTestState(t)
	ctx := context.Background()
	key := rediskeys.Samp("ct")
	now := t0
	const capacity, perMin = 30.0, 30.0

	granted := 0
	for i := 0; i < 100; i++ {
		if ok, err := s.TakeToken(ctx, key, capacity, perMin/60, now, 5*time.Minute); err != nil {
			t.Fatal(err)
		} else if ok {
			granted++
		}
	}
	if granted != 30 {
		t.Fatalf("burst granted %d, want ceiling 30", granted)
	}
	// One minute later the bucket has refilled exactly 30.
	granted = 0
	for i := 0; i < 100; i++ {
		if ok, _ := s.TakeToken(ctx, key, capacity, perMin/60, now.Add(time.Minute), 5*time.Minute); ok {
			granted++
		}
	}
	if granted != 30 {
		t.Fatalf("after 1m granted %d, want 30", granted)
	}
}

func TestEmbSimilarityAndWindow(t *testing.T) {
	s, mr := newTestState(t)
	ctx := context.Background()
	key := rediskeys.Emb("ct", "au")
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}
	almostA := []float32{0.99, 0.05, 0}

	sim, err := s.EmbMaxSimilarity(ctx, key, a, 2, 5*time.Minute)
	if err != nil || sim != 0 {
		t.Fatalf("empty history sim = (%v, %v), want 0", sim, err)
	}
	sim, _ = s.EmbMaxSimilarity(ctx, key, b, 2, 5*time.Minute)
	if sim >= 0.5 {
		t.Fatalf("orthogonal sim = %v, want ~0", sim)
	}
	sim, _ = s.EmbMaxSimilarity(ctx, key, almostA, 2, 5*time.Minute)
	if sim < 0.95 {
		t.Fatalf("near-duplicate sim = %v, want >= 0.95", sim)
	}
	// Depth 2: a has been evicted (list holds almostA, b).
	sim, _ = s.EmbMaxSimilarity(ctx, key, a, 2, 5*time.Minute)
	if sim < 0.9 { // still similar to almostA
		t.Fatalf("sim vs retained almostA = %v", sim)
	}
	if ttl := mr.TTL(key); ttl != 5*time.Minute {
		t.Fatalf("emb key TTL = %v, want 5m", ttl)
	}
}

func TestVectorPackRoundTrip(t *testing.T) {
	v := []float32{0.25, -1, 3.5, 0}
	got, ok := unpackVector(packVector(v))
	if !ok || len(got) != len(v) {
		t.Fatalf("round trip failed: %v %v", got, ok)
	}
	for i := range v {
		if got[i] != v[i] {
			t.Fatalf("element %d = %v, want %v", i, got[i], v[i])
		}
	}
	if _, ok := unpackVector([]byte{1, 2, 3}); ok {
		t.Fatal("truncated payload must not unpack")
	}
}

func TestMemSamplerFallback(t *testing.T) {
	s := NewMemSampler(2, 60, 5*time.Minute) // 1 token/s refill
	now := t0
	if !s.Allow("ct", now) || !s.Allow("ct", now) {
		t.Fatal("burst of capacity 2 must be allowed")
	}
	if s.Allow("ct", now) {
		t.Fatal("3rd take at same instant must be denied")
	}
	if !s.Allow("ct", now.Add(time.Second)) {
		t.Fatal("refilled token must be allowed")
	}
	if !s.Allow("other", now) {
		t.Fatal("buckets are per content")
	}
}
