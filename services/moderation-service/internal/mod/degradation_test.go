package mod

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/contracts"
	"dabet/pkg/policyapi"
)

// breakerConfig is the cascade config used by the degradation tests: a
// short, deterministic breaker and a policy-independent cascade.
func breakerConfig(threshold int, cooldown, maxCooldown time.Duration) Config {
	cfg := DefaultConfig()
	cfg.RedisBreakerThreshold = threshold
	cfg.RedisBreakerCooldown = cooldown
	cfg.RedisBreakerMaxCooldown = maxCooldown
	return cfg
}

// fullCascadePolicy exercises every Redis-backed stage plus the in-memory
// word stage, so a skip is visible from either side.
func fullCascadePolicy() *policyapi.ResolvedPolicy {
	p := rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)
	p.RateLimitMessages = i32(100)
	p.RateLimitSeconds = i32(10)
	p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
	p.RestrictedWords = []string{"badword"}
	return p
}

// This is the F2 fix, stated as an assertion: once the breaker is open the
// Redis-backed stages are skipped WITHOUT a call reaching the client. The
// old code re-attempted the seen: guard for every message of an outage and
// re-paid the client's failure latency on the consumer goroutine.
func TestRedisOutageSkipsStagesWithoutCallingRedis(t *testing.T) {
	const threshold, messages = 3, 200
	env := newPipeEnv(t, breakerConfig(threshold, time.Second, 5*time.Second))
	env.policy.val = cachedPolicy(fullCascadePolicy())
	base := env.warmRedis(t)
	env.mr.SetError("redis is down")

	for i := 0; i < messages; i++ {
		env.process(t, testMessage(fmt.Sprintf("m%d", i), "a badword appears"))
	}

	// Exactly `threshold` calls got through: the ones that tripped it. The
	// clock never advances, so no probe is due.
	if got := env.probe.count() - base; got != threshold {
		t.Fatalf("%d commands reached Redis over %d messages, want %d: an open breaker must not call at all",
			got, messages, threshold)
	}
	if !env.pipe.breaker.Open() {
		t.Fatal("breaker should be open after a sustained outage")
	}
	// Every message counted its fail-open exactly once, over all five
	// Redis-backed stages (§4.7).
	if got := env.failOpen(t, "redis", ""); got != messages {
		t.Fatalf("fail_open{redis} = %v, want %d (exactly one per message)", got, messages)
	}
	// The embedding stage is skipped as part of the Redis skip, not
	// counted as its own fail-open.
	if got := env.failOpen(t, "embedding", ""); got != 0 {
		t.Fatalf("fail_open{embedding} = %v, want 0", got)
	}
	if got := testutil.ToFloat64(env.met.DependencyUp.WithLabelValues("redis")); got != 0 {
		t.Fatal("dependency_up{redis} must read 0 while the breaker is open")
	}
	// §4.7: continue to the word stage. Every message still gets moderated
	// by the detectors that do not need Redis.
	if got := len(env.prod.byTopic(contracts.TopicFlagged)); got != messages {
		t.Fatalf("flagged %d of %d: the word stage must keep running with Redis down", got, messages)
	}
}

// What F2 actually measured was throughput, so assert on time. With a
// slow-failing client the per-message cost must fall back to the
// no-Redis cost, not the failure-latency cost.
func TestRedisOutageDoesNotRepayFailureLatencyPerMessage(t *testing.T) {
	const threshold, messages = 5, 200
	const failLatency = 5 * time.Millisecond

	// Same run twice, differing only in how slow a failing Redis call is.
	// The delta is what the outage costs; comparing against a same-machine
	// baseline keeps the assertion honest under -race, where the cascade's
	// own cost dwarfs a few milliseconds of injected latency.
	run := func(delay time.Duration) (time.Duration, int) {
		env := newPipeEnv(t, breakerConfig(threshold, time.Minute, time.Minute))
		env.policy.val = cachedPolicy(fullCascadePolicy())
		base := env.warmRedis(t)
		env.mr.SetError("redis is down")
		env.probe.setDelay(delay)

		start := time.Now()
		for i := 0; i < messages; i++ {
			env.process(t, testMessage(fmt.Sprintf("m%d", i), "a badword appears"))
		}
		return time.Since(start), env.probe.count() - base
	}

	fast, fastCalls := run(0)
	slow, slowCalls := run(failLatency)

	// Without the breaker the outage costs failLatency per MESSAGE; with
	// it, per TRIP. The budget is a quarter of the unfixed cost, which is
	// an order of magnitude above the ~threshold×failLatency expected.
	unfixed := messages * failLatency
	if cost, budget := slow-fast, unfixed/4; cost > budget {
		t.Fatalf("a Redis outage cost %v extra over %d messages (baseline %v, with latency %v); "+
			"the unfixed path costs ~%v and the budget is %v",
			cost, messages, fast, slow, unfixed, budget)
	}
	if fastCalls != threshold || slowCalls != threshold {
		t.Fatalf("commands reaching the failing client: %d fast, %d slow, want %d each",
			fastCalls, slowCalls, threshold)
	}
}

// Recovery has to be automatic and prompt: one probe rides the next
// message after the cooldown, and normal processing resumes from there.
func TestRedisRecoveryProbeClosesTheBreaker(t *testing.T) {
	const threshold = 2
	cooldown := 100 * time.Millisecond
	env := newPipeEnv(t, breakerConfig(threshold, cooldown, time.Second))
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.Spam = policyapi.SpamMode_SPAM_MODE_IDENTICAL
	}))
	env.mr.SetError("redis is down")

	for i := 0; i < 20; i++ {
		env.process(t, testMessage(fmt.Sprintf("down%d", i), "text"))
	}
	if !env.pipe.breaker.Open() {
		t.Fatal("breaker did not open")
	}
	callsWhileDown := env.probe.count()

	// Redis comes back, but nothing probes until the cooldown expires.
	env.mr.SetError("")
	env.process(t, testMessage("early", "text"))
	if env.probe.count() != callsWhileDown {
		t.Fatal("a call was made before the cooldown expired")
	}
	if env.failOpen(t, "redis", "") != 21 {
		t.Fatal("messages during the open window each count one fail-open")
	}

	// After it, the next message carries the probe, it succeeds, and the
	// breaker closes for everyone.
	env.clock.Advance(cooldown)
	env.process(t, testMessage("probe", "text"))
	if env.pipe.breaker.Open() {
		t.Fatal("a successful probe must close the breaker")
	}
	if got := testutil.ToFloat64(env.met.DependencyUp.WithLabelValues("redis")); got != 1 {
		t.Fatal("dependency_up{redis} must return to 1")
	}

	// Normal processing resumes: the redelivery guard and the duplicate
	// detector work again, and no further fail-opens are counted.
	before := env.failOpen(t, "redis", "")
	env.process(t, testMessage("r1", "recovered text"))
	env.process(t, testMessage("r2", "recovered text")) // duplicate
	if got := env.failOpen(t, "redis", ""); got != before {
		t.Fatalf("fail_open{redis} = %v, want %v: a closed breaker fails nothing open", got, before)
	}
	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 1 {
		t.Fatalf("flagged %d, want 1: the duplicate stage must work again", len(flagged))
	}
	if f := unmarshalFlagged(t, flagged[0]); f.Detector != contracts.DetectorDuplicate {
		t.Fatalf("detector = %s, want duplicate", f.Detector)
	}
}

// A dependency that flaps must not put the breaker back into per-message
// retrying: the cost of each flap stays bounded by the threshold, and the
// open window lengthens rather than being re-probed at the base rate.
func TestFlappingRedisDoesNotThrash(t *testing.T) {
	const threshold, perRound = 2, 50
	cooldown := 10 * time.Millisecond
	env := newPipeEnv(t, breakerConfig(threshold, cooldown, 160*time.Millisecond))
	env.policy.val = cachedPolicy(testPolicy())
	base := env.warmRedis(t)

	n := 0
	next := func() contracts.Message {
		n++
		return testMessage(fmt.Sprintf("f%d", n), "text")
	}

	env.mr.SetError("flap")
	for round := 0; round < 5; round++ {
		for i := 0; i < perRound; i++ {
			env.process(t, next())
		}
		// The dependency comes back just long enough for one probe, then
		// breaks again before the traffic behind it arrives.
		env.clock.Advance(env.pipe.breaker.openUntil.Sub(env.clock.Now()))
		env.mr.SetError("")
		env.process(t, next())
		env.mr.SetError("flap")
	}

	// One more failing stretch, to observe the window the ladder has
	// reached rather than the one the last probe left behind.
	for i := 0; i < perRound; i++ {
		env.process(t, next())
	}

	// Per flap: at most one successful probe plus `threshold` failures to
	// re-trip, plus the final stretch's threshold. Everything else was
	// skipped without a call.
	const bound = 5*(1+threshold) + threshold
	if got := env.probe.count() - base; got > bound {
		t.Fatalf("%d commands reached a flapping Redis over %d messages, want <= %d",
			got, n, bound)
	}
	// And the ladder has backed the probe interval off well past the base,
	// so the flapping does not settle into probing at the base rate.
	if got := env.pipe.breaker.openUntil.Sub(env.clock.Now()); got <= cooldown {
		t.Fatalf("open window after five flaps = %v, want > the %v base cooldown", got, cooldown)
	}
}

// The documented deviation survives: with Redis unavailable the sampler
// falls back to the per-instance in-memory bucket rather than admitting
// everything or nothing — and it does so with the breaker open, i.e.
// without a call.
func TestBreakerOpenSamplerStillUsesMemoryBucket(t *testing.T) {
	cfg := breakerConfig(1, time.Minute, time.Minute)
	cfg.SamplerCapacity = 1
	cfg.SamplerPerMin = 60
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
	env.mr.SetError("redis is down")

	env.process(t, testMessage("m1", "first"))  // trips the breaker
	callsAfterTrip := env.probe.count()         //
	env.process(t, testMessage("m2", "second")) // skipped outright
	env.process(t, testMessage("m3", "third"))

	if env.probe.count() != callsAfterTrip {
		t.Fatal("the sampler stage called Redis with the breaker open")
	}
	if got := testutil.ToFloat64(env.met.SamplerSkipped); got != 2 {
		t.Fatalf("sampler_skipped = %v, want 2: the in-memory bucket still enforces a ceiling", got)
	}
}
