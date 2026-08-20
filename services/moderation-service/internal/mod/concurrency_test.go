package mod

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/contracts"
	"dabet/pkg/policyapi"
)

// One Pipeline may be driven by several kafkax partition workers, so
// Process has to be safe under concurrency and the accounting has to stay
// exact. Run under -race; the assertions catch lost counts even without it.
func TestConcurrentProcessAccountsEveryMessage(t *testing.T) {
	const workers, perWorker = 8, 60
	cfg := DefaultConfig()
	cfg.LLMBatchSize = 5
	cfg.SamplerCapacity = 1000 // admit everything: exercise the batcher hard
	cfg.SamplerPerMin = 60000
	env := newPipeEnv(t, cfg)
	// Redis (seen + dup), the word matcher, the sampler and the batcher
	// all run for every message; texts are unique so no detector fires and
	// everything reaches stage 9. No rate limit — a shared bucket would
	// flag most of the traffic before it got there.
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
	env.policy.val.Policy.Spam = policyapi.SpamMode_SPAM_MODE_IDENTICAL
	env.policy.val.Policy.RestrictedWords = []string{"badword"}
	env.policy.val.Matcher = NewMatcher(env.policy.val.Policy.RestrictedWords)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				env.process(t, testMessage(fmt.Sprintf("w%d-m%d", w, i), fmt.Sprintf("message %d %d", w, i)))
			}
		}(w)
	}
	wg.Wait()
	env.pipe.Shutdown(context.Background())

	const total = workers * perWorker
	// Every message reaches exactly one terminal outcome. Redis is healthy
	// here, so nothing is dropped by the redelivery guard.
	sum := env.outcome(t, "clean") + env.outcome(t, "flagged") + env.outcome(t, "skipped")
	if sum != total {
		t.Fatalf("outcomes sum to %v over %d messages: counts were lost or double-counted", sum, total)
	}
	if got := env.failOpen(t, "redis", ""); got != 0 {
		t.Fatalf("fail_open{redis} = %v with a healthy Redis, want 0", got)
	}
	// Usage is billed once per processed message (§7.10), and the flush
	// must have emitted exactly that many.
	usage := usageQuantity(t, env)
	if usage != total {
		t.Fatalf("usage quantity = %d, want %d", usage, total)
	}
	// Nothing was left in the batcher after Shutdown.
	env.pipe.batcher.mu.Lock()
	pending := len(env.pipe.batcher.pending)
	env.pipe.batcher.mu.Unlock()
	if pending != 0 {
		t.Fatalf("%d policies still pending after Shutdown", pending)
	}
}

// Redis failing WHILE several workers are running is the case the shared
// breaker has to get right: the trip is global, so the call count stays
// near the threshold no matter how many goroutines are hammering it, and
// the fail-open count stays exactly one per message.
func TestConcurrentProcessWithRedisDown(t *testing.T) {
	const workers, perWorker, threshold = 8, 50, 4
	env := newPipeEnv(t, breakerConfig(threshold, time.Minute, time.Minute))
	env.policy.val = cachedPolicy(fullCascadePolicy())
	base := env.warmRedis(t)
	env.mr.SetError("redis is down")

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				env.process(t, testMessage(fmt.Sprintf("w%d-m%d", w, i), "a badword appears"))
			}
		}(w)
	}
	wg.Wait()

	const total = workers * perWorker
	if got := env.failOpen(t, "redis", ""); got != total {
		t.Fatalf("fail_open{redis} = %v, want exactly %d (one per message)", got, total)
	}
	// The bound scales with CONCURRENCY, not with message count, which is
	// the whole claim: the threshold, plus the calls each worker already
	// had in flight when the trip landed, plus go-redis' per-connection
	// handshake as the pool grows to serve eight goroutines. The defect
	// this replaces cost one call per message — 400 of them.
	if got, max := env.probe.count()-base, threshold+4*workers; got > max {
		t.Fatalf("%d commands reached a dead Redis over %d messages, want <= %d", got, total, max)
	}
}

// Shutdown races with the workers in a real drain: kafkax may still be
// mid-record when the context is cancelled. A dispatch arriving then must
// not call wg.Add concurrently with wg.Wait — documented WaitGroup misuse,
// and reachable only once several partition workers drive one pipeline —
// and it must still be accounted rather than dropped on the floor.
//
// The runtime's misuse check needs a narrow interleaving, so this test
// leans on the accounting assertion for its teeth: nothing may be lost in
// the drain, whichever side of it a message lands on. Run under -race.
func TestProcessDuringShutdownIsSafe(t *testing.T) {
	const rounds, workers, perWorker = 8, 4, 25
	for round := 0; round < rounds; round++ {
		cfg := DefaultConfig()
		// One dispatch per message, so the in-flight counter oscillates
		// between 0 and 1 throughout the drain — which is exactly the
		// window in which Add racing Wait is a misuse.
		cfg.LLMBatchSize = 1
		cfg.SamplerCapacity = 1000
		cfg.SamplerPerMin = 60000
		env := newPipeEnv(t, cfg)
		env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
		env.llm.verdicts = []int{0}

		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for i := 0; i < perWorker; i++ {
					env.process(t, testMessage(fmt.Sprintf("w%d-m%d", w, i), "late message"))
				}
			}(w)
		}
		close(start)
		// Overlap the drain with live traffic.
		env.pipe.Shutdown(context.Background())
		wg.Wait()
		env.pipe.Shutdown(context.Background()) // drains any straggler

		const total = workers * perWorker
		sum := env.outcome(t, "clean") + env.outcome(t, "flagged") + env.outcome(t, "skipped")
		if sum != total {
			t.Fatalf("round %d: outcomes sum to %v over %d messages: a batch was lost in the drain",
				round, sum, total)
		}
	}
}

// usageQuantity sums the quantities of the usage.v1 events produced so far.
func usageQuantity(t *testing.T, env *pipeEnv) int64 {
	t.Helper()
	var n int64
	for _, rec := range env.prod.byTopic(contracts.TopicUsage) {
		var u contracts.Usage
		if err := json.Unmarshal(rec.Value, &u); err != nil {
			t.Fatal(err)
		}
		n += u.Quantity
	}
	return n
}

// A guard against the sampler quietly capping the concurrency tests: with
// these settings every message must reach stage 9.
func TestConcurrencyFixturesAdmitEverything(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SamplerCapacity = 1000
	cfg.SamplerPerMin = 60000
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
	for i := 0; i < 100; i++ {
		env.process(t, testMessage(fmt.Sprintf("m%d", i), "text"))
	}
	if got := testutil.ToFloat64(env.met.SamplerSkipped); got != 0 {
		t.Fatalf("sampler_skipped = %v, want 0: the fixture must not cap the concurrency tests", got)
	}
}
