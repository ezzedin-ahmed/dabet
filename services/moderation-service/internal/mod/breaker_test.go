package mod

import (
	"sync"
	"testing"
	"time"
)

func TestBreakerTripsOnlyOnConsecutiveFailures(t *testing.T) {
	b := NewBreaker(3, 100*time.Millisecond, time.Second)
	now := t0

	// Two failures then a success: the run is broken, nothing trips.
	b.Fail(now, false)
	b.Fail(now, false)
	b.Succeed(now, false)
	b.Fail(now, false)
	b.Fail(now, false)
	if allowed, _ := b.Allow(now); !allowed || b.Open() {
		t.Fatal("breaker tripped on non-consecutive failures")
	}

	b.Fail(now, false)
	if allowed, _ := b.Allow(now); allowed || !b.Open() {
		t.Fatal("third consecutive failure must trip the breaker")
	}
}

func TestBreakerSkipsWhileOpenAndAdmitsOneProbe(t *testing.T) {
	b := NewBreaker(1, 100*time.Millisecond, time.Second)
	now := t0
	b.Fail(now, false)

	// Inside the cooldown nothing is admitted, however many callers ask.
	for i := 0; i < 100; i++ {
		if allowed, _ := b.Allow(now.Add(99 * time.Millisecond)); allowed {
			t.Fatal("breaker admitted a call inside the cooldown")
		}
	}

	// After it, exactly one caller gets the probe token.
	now = now.Add(150 * time.Millisecond)
	allowed, probe := b.Allow(now)
	if !allowed || !probe {
		t.Fatal("first caller after the cooldown must get the probe")
	}
	for i := 0; i < 10; i++ {
		if a, _ := b.Allow(now); a {
			t.Fatal("a second probe was admitted while the first was in flight")
		}
	}

	// A failing probe re-opens without waiting for the threshold again.
	b.Fail(now, true)
	if a, _ := b.Allow(now); a || !b.Open() {
		t.Fatal("failed probe must re-open the breaker immediately")
	}
}

func TestBreakerProbeSuccessClosesPromptly(t *testing.T) {
	b := NewBreaker(2, 100*time.Millisecond, time.Second)
	now := t0
	b.Fail(now, false)
	b.Fail(now, false)

	now = now.Add(101 * time.Millisecond)
	allowed, probe := b.Allow(now)
	if !allowed || !probe {
		t.Fatal("probe not admitted after the cooldown")
	}
	b.Succeed(now, true)

	if b.Open() {
		t.Fatal("successful probe must close the breaker")
	}
	for i := 0; i < 10; i++ {
		if a, p := b.Allow(now); !a || p {
			t.Fatal("a closed breaker admits every caller, none of them a probe")
		}
	}
}

// The open window grows per consecutive trip so a dependency that flaps is
// not probed at the base rate forever, and it decays again only after a
// sustained healthy stretch.
func TestBreakerBackoffLadderAndDecay(t *testing.T) {
	b := NewBreaker(1, 100*time.Millisecond, 400*time.Millisecond)
	now := t0

	want := []time.Duration{100, 200, 400, 400}
	for i, w := range want {
		w *= time.Millisecond
		if i == 0 {
			b.Fail(now, false)
		} else {
			now = now.Add(w) // comfortably past the previous window
			allowed, probe := b.Allow(now)
			if !allowed || !probe {
				t.Fatalf("trip %d: probe not admitted", i)
			}
			b.Fail(now, true) // the dependency is still broken
		}
		if got := b.openUntil.Sub(now); got != w {
			t.Fatalf("trip %d: open window = %v, want %v", i, got, w)
		}
		// Traffic is still skipped right up to the boundary.
		if a, _ := b.Allow(now.Add(w - time.Millisecond)); a {
			t.Fatalf("trip %d: admitted before the %v window elapsed", i, w)
		}
	}

	// Recover, then stay healthy longer than maxCooldown: the ladder resets
	// so the NEXT outage starts from the base cooldown again.
	now = now.Add(400 * time.Millisecond)
	_, probe := b.Allow(now)
	b.Succeed(now, probe)
	now = now.Add(500 * time.Millisecond)
	b.Succeed(now, false)

	b.Fail(now, false)
	if a, _ := b.Allow(now.Add(100 * time.Millisecond)); !a {
		t.Fatal("after a sustained healthy stretch the ladder must reset to the base cooldown")
	}
}

func TestBreakerClampsNonsenseConfig(t *testing.T) {
	b := NewBreaker(0, -time.Second, time.Nanosecond)
	if b.threshold != 1 {
		t.Fatalf("threshold = %d, want a clamp to 1", b.threshold)
	}
	if b.cooldown <= 0 || b.maxCooldown < b.cooldown {
		t.Fatalf("cooldown = %v, max = %v: bad env must degrade, not crash", b.cooldown, b.maxCooldown)
	}
}

// The breaker is shared across partition workers, so every transition has
// to hold under concurrent callers. Run under -race.
func TestBreakerConcurrentCallers(t *testing.T) {
	b := NewBreaker(4, time.Millisecond, 10*time.Millisecond)
	var wg sync.WaitGroup
	var mu sync.Mutex
	probes := 0
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				now := t0.Add(time.Duration(j) * time.Millisecond)
				allowed, probe := b.Allow(now)
				if !allowed {
					continue
				}
				if probe {
					mu.Lock()
					probes++
					mu.Unlock()
				}
				if j%3 == 0 {
					b.Succeed(now, probe)
				} else {
					b.Fail(now, probe)
				}
			}
		}()
	}
	wg.Wait()
	if probes == 0 {
		t.Fatal("no probe was ever handed out; recovery would never happen")
	}
}
