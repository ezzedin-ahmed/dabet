// Package quota is the daily-unit budget the YouTube driver paces itself
// against. It is the API-cost analogue of a token bucket: a project gets a
// fixed number of units per rolling day and every call spends some, so the
// only lever an ingest process has is how often it calls.
//
// Two knobs come out of the same budget:
//
//   - Pace gives the minimum interval between calls that keeps the
//     projected daily spend inside the allowance. It is smooth: the driver
//     simply waits longer between polls when it is watching more streams.
//   - Reserve is the hard stop. It blocks until the bucket has actually
//     refilled enough units for the call, so a burst (a rebalance handing
//     an instance a hundred new streams at once) cannot overshoot.
//
// The bucket refills continuously at Daily/24h rather than resetting at
// midnight Pacific, which is how YouTube actually resets it. A continuous
// refill is strictly more conservative — it never lets the driver spend
// tomorrow's units today — and it removes a timezone from the hot path.
package quota

import (
	"context"
	"sync"
	"time"
)

// Day is the window the daily allowance is expressed over.
const Day = 24 * time.Hour

// Budget is a refilling allowance of API units. The zero value is not
// usable; call New.
type Budget struct {
	// Now is the clock; injectable so tests need no real time.
	Now func() time.Time

	mu     sync.Mutex
	daily  float64
	tokens float64
	last   time.Time
}

// New returns a Budget of daily units, starting full. daily <= 0 means an
// unlimited budget (Pace returns 0 and Reserve never blocks), which is what
// tests and self-hosted quota-exempt deployments want.
func New(daily int) *Budget {
	b := &Budget{Now: time.Now, daily: float64(daily)}
	b.tokens = b.daily
	return b
}

// Unlimited returns a budget that never throttles.
func Unlimited() *Budget { return New(0) }

func (b *Budget) unlimited() bool { return b.daily <= 0 }

// refillLocked adds the units that accrued since the last observation.
func (b *Budget) refillLocked(now time.Time) {
	if b.last.IsZero() {
		b.last = now
		return
	}
	elapsed := now.Sub(b.last)
	if elapsed <= 0 {
		return
	}
	b.last = now
	b.tokens = min(b.daily, b.tokens+b.daily*elapsed.Seconds()/Day.Seconds())
}

// Remaining reports the units currently available.
func (b *Budget) Remaining() float64 {
	if b.unlimited() {
		return -1
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(b.Now())
	return b.tokens
}

// Pace returns the minimum interval between calls of the given unit cost
// such that streams concurrent callers together stay inside the daily
// allowance:
//
//	interval >= cost * streams * 24h / daily
//
// streams < 1 is treated as 1.
func (b *Budget) Pace(cost, streams int) time.Duration {
	if b.unlimited() || cost <= 0 {
		return 0
	}
	if streams < 1 {
		streams = 1
	}
	b.mu.Lock()
	daily := b.daily
	b.mu.Unlock()
	return time.Duration(float64(cost) * float64(streams) * Day.Seconds() / daily * float64(time.Second))
}

// Reserve blocks until cost units are available and then spends them,
// returning ctx.Err() if the context is cancelled first. A cost larger than
// the whole daily allowance is spent immediately rather than deadlocking;
// the caller has already lost that argument.
func (b *Budget) Reserve(ctx context.Context, cost int) error {
	if b.unlimited() || cost <= 0 {
		return ctx.Err()
	}
	for {
		wait := b.reserveOrWait(cost)
		if wait <= 0 {
			return nil
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// reserveOrWait spends cost and returns 0, or returns how long until
// enough units have accrued.
func (b *Budget) reserveOrWait(cost int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(b.Now())
	need := float64(cost)
	if need > b.daily {
		// Unsatisfiable by construction: spend what there is and proceed
		// rather than block forever.
		b.tokens = 0
		return 0
	}
	if b.tokens >= need {
		b.tokens -= need
		return 0
	}
	missing := need - b.tokens
	return time.Duration(missing / b.daily * Day.Seconds() * float64(time.Second))
}

// Drain empties the budget. The YouTube driver calls it when the API says
// quotaExceeded: the provider is the authority on the real balance, and our
// local accounting has evidently drifted low.
func (b *Budget) Drain() {
	if b.unlimited() {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked(b.Now())
	b.tokens = 0
}
