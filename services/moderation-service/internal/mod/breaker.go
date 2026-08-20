package mod

import (
	"sync"
	"time"
)

// Breaker is the shared circuit breaker in front of the Redis-backed
// cascade stages (1 seen, 4 rate, 5 dup, 6 semantic, 8 sampler).
//
// WHY IT EXISTS. §4.7's table is normative: "Redis down → **skip**
// rate/dup/semantic stages, continue to word + LLM stages". Skipping is
// not the same as trying and failing. Deciding availability per message
// means every message of an outage re-pays the client's full failure cost
// — dial timeout plus the client's own retry ladder — on the consuming
// goroutine, which is single-threaded per instance. A 30 s Redis drill
// measured on the reference stack collapsed consumption from 400 msg/s to
// ~11 msg/s per instance and built 12 620 messages of backlog, while the
// LLM and policy outages (which have bounded timeouts and, for the LLM,
// run off the consumer goroutine) cost nothing at all. The breaker turns
// "try and fail" into "skip".
//
// SHAPE. Ordinary three-state breaker, but with two properties the
// pipeline depends on:
//
//   - While open, Allow returns false and the caller must not touch the
//     client at all. That — not the counting, not the metric — is the fix.
//   - Recovery rides real traffic. After the cooldown one message, and
//     exactly one, is handed the half-open probe token; every concurrent
//     message keeps skipping. So the worst-case cost of an outage is one
//     failed call per cooldown, not one per message.
//
// ANTI-THRASH. Each trip lengthens the next cooldown geometrically, from
// cooldown up to maxCooldown, so a dependency that flaps does not get
// probed at the base rate forever. The ladder resets only after the
// breaker has stayed closed and healthy for maxCooldown, which means a
// genuine recovery pays at most one extra probe interval and a flapping
// one settles at the cap.
//
// Safe for concurrent use: several kafkax partition workers may drive the
// same pipeline, so every field is mutex-guarded and the probe token is
// handed out at most once at a time.
type Breaker struct {
	threshold   int
	cooldown    time.Duration
	maxCooldown time.Duration

	mu        sync.Mutex
	failures  int       // consecutive failures while closed
	open      bool      // true = calls are being skipped
	openUntil time.Time // when the next probe becomes eligible
	probing   bool      // a half-open probe is in flight
	trips     int       // consecutive trips; drives the backoff ladder
	closedAt  time.Time // when the breaker last returned to closed
}

// NewBreaker builds a breaker that trips after threshold consecutive
// failures and admits one probe every cooldown, backing that interval off
// to at most maxCooldown while failures continue. Nonsensical values are
// clamped rather than rejected: this is on the hot path and a bad env var
// must degrade, not crash (P2).
func NewBreaker(threshold int, cooldown, maxCooldown time.Duration) *Breaker {
	if threshold < 1 {
		threshold = 1
	}
	if cooldown <= 0 {
		cooldown = 500 * time.Millisecond
	}
	if maxCooldown < cooldown {
		maxCooldown = cooldown
	}
	return &Breaker{threshold: threshold, cooldown: cooldown, maxCooldown: maxCooldown}
}

// Allow reports whether a call may be attempted now. probe is true when
// this caller holds the single half-open token, in which case the result
// it reports back decides whether the breaker closes. A caller that gets
// allowed == true MUST report exactly one Succeed or Fail with the same
// probe value.
func (b *Breaker) Allow(now time.Time) (allowed, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open {
		return true, false
	}
	if now.Before(b.openUntil) || b.probing {
		return false, false
	}
	b.probing = true
	return true, true
}

// Succeed records a successful call.
func (b *Breaker) Succeed(now time.Time, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
		if b.open {
			b.open = false
			b.closedAt = now
		}
	}
	b.failures = 0
	// The backoff ladder decays only after a sustained healthy stretch, so
	// a flapping dependency keeps its longer cooldown.
	if !b.open && !b.closedAt.IsZero() && !now.Before(b.closedAt.Add(b.maxCooldown)) {
		b.trips = 0
		b.closedAt = time.Time{}
	}
}

// Fail records a failed call and trips the breaker when the consecutive
// failure threshold is reached (or immediately, when the failure is the
// half-open probe).
func (b *Breaker) Fail(now time.Time, probe bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if probe {
		b.probing = false
		b.trip(now)
		return
	}
	if b.open {
		// A call that started before the trip finished after it. It has
		// already been counted; do not extend the open window with it.
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.failures = 0
		b.trip(now)
	}
}

// trip opens the breaker for the next backoff interval. Called with mu.
func (b *Breaker) trip(now time.Time) {
	b.trips++
	b.open = true
	b.openUntil = now.Add(b.backoff())
	b.closedAt = time.Time{}
}

// backoff is cooldown doubled once per consecutive trip, capped at
// maxCooldown. Called with mu.
func (b *Breaker) backoff() time.Duration {
	d := b.cooldown
	for i := 1; i < b.trips; i++ {
		if d >= b.maxCooldown {
			break
		}
		d *= 2
	}
	if d > b.maxCooldown {
		d = b.maxCooldown
	}
	return d
}

// Open reports whether calls are currently being skipped. Used for the
// dependency_up gauge and by tests.
func (b *Breaker) Open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}
