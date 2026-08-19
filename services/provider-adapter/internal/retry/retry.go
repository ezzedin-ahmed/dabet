// Package retry is the shared reconnect policy for the platform drivers.
//
// P2 splits provider failures in two. A transient failure (socket dropped,
// 5xx, network timeout, provider rate limit) is retried with exponential
// backoff and jitter — jitter matters because an adapter instance holds
// thousands of connections (A13) and a provider blip would otherwise make
// them all reconnect in lockstep. A permanent failure (revoked token,
// deleted channel, disallowed intent) ends the watch with a terminal error
// so the ingest manager stops instead of hammering a call that can never
// succeed.
package retry

import (
	"context"
	"math/rand/v2"
	"time"
)

// Backoff produces the delay before the next attempt.
type Backoff struct {
	// Base is the first delay; each attempt doubles it up to Max.
	Base time.Duration
	// Max caps the delay.
	Max time.Duration
	// Jitter returns a uniform float in [0,1); nil means rand.Float64.
	// Injectable so tests are deterministic.
	Jitter func() float64

	attempt int
}

// DefaultBackoff is the policy the drivers use unless overridden: fast
// enough that a one-off socket drop costs a second of chat, slow enough
// that a provider outage does not turn into a retry storm.
func DefaultBackoff() Backoff {
	return Backoff{Base: 500 * time.Millisecond, Max: 30 * time.Second}
}

// Reset returns the backoff to its first step. Drivers call this after a
// successful connect so a long-lived session that eventually drops does not
// inherit the previous outage's delay.
func (b *Backoff) Reset() { b.attempt = 0 }

// Next advances the backoff and returns the next delay, jittered to
// 0.5x-1.5x of the exponential step.
func (b *Backoff) Next() time.Duration {
	base := b.Base
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	maxD := b.Max
	if maxD <= 0 {
		maxD = 30 * time.Second
	}
	d := base
	for i := 0; i < b.attempt && d < maxD; i++ {
		d *= 2
	}
	d = min(d, maxD)
	b.attempt++

	jitter := b.Jitter
	if jitter == nil {
		jitter = rand.Float64
	}
	return d/2 + time.Duration(jitter()*float64(d))
}

// Wait sleeps for the next backoff step, returning ctx.Err() if the
// context is cancelled first. Every driver reconnect path goes through
// this, which is what makes cancellation prompt during a backoff.
func (b *Backoff) Wait(ctx context.Context) error {
	return Sleep(ctx, b.Next())
}

// Sleep waits for d, or returns early with ctx.Err() on cancellation.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
