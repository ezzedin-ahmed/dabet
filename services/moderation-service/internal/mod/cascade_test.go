package mod

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"dabet/pkg/contracts"
	"dabet/pkg/policyapi"
	"dabet/pkg/rediskeys"
)

// This file covers the merge of stages 4, 5 and 6 into one Lua script.
//
// The regression the merge could most easily introduce is not a wrong
// verdict for the message in hand — it is a wrong verdict for a LATER
// message, because a script that updated all three structures
// unconditionally would let a stopped message pollute the windows the next
// one is judged against. §7.3's "first hit wins, ordered strictly by cost"
// is therefore a property of the STATE, and that is what these tests
// assert: first directly on the stored keys, then through the pipeline as
// the verdict the pollution would change.

// ---------------------------------------------------------------------------
// The short-circuit property, on the keys themselves.
// ---------------------------------------------------------------------------

func TestRateLimitHitTouchesNothingBelowIt(t *testing.T) {
	s, mr := newTestState(t)
	ctx := context.Background()
	dupKey := rediskeys.Dup("ct", "au")
	embKey := rediskeys.Emb("ct", "au")

	full := func(hash string) CascadeParams {
		return CascadeParams{
			// Capacity 1 with no refill inside the test: the second call
			// finds the bucket empty.
			Rate:     &RateParams{Capacity: 1, RefillPerSec: 0, Now: t0, TTL: time.Minute},
			Dup:      &DupParams{Hash: hash, Depth: 4, TTL: 5 * time.Minute},
			EmbDepth: 4,
		}
	}

	// One message gets through and populates both windows.
	if res, err := s.Cascade(ctx, "ct", "au", full("h1")); err != nil || res.Hit != CascadeNone {
		t.Fatalf("first call = (%v, %v), want a clean pass", res.Hit, err)
	}
	if err := s.EmbAppend(ctx, "ct", "au", []float32{1, 0, 0}, 4, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	dupBefore, embBefore := listOf(t, mr, dupKey), listOf(t, mr, embKey)
	dupTTLBefore, embTTLBefore := mr.TTL(dupKey), mr.TTL(embKey)

	// The next one is rate limited. §7.3: the duplicate stage never runs,
	// so its hash is never pushed; the semantic stage never runs, so the
	// comparison window is neither read nor written.
	res, err := s.Cascade(ctx, "ct", "au", full("h2"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Hit != CascadeRateLimited {
		t.Fatalf("hit = %v, want CascadeRateLimited", res.Hit)
	}
	if len(res.Vectors) != 0 {
		t.Fatalf("a rate-limited message read %d vectors; the semantic stage must not run", len(res.Vectors))
	}
	if got := listOf(t, mr, dupKey); !equalStrings(got, dupBefore) {
		t.Fatalf("duplicate window = %v, want %v unchanged: a rate-limited message must not enter it "+
			"(§7.3 first-hit-wins) or it will make the NEXT message look like a duplicate", got, dupBefore)
	}
	if got := listOf(t, mr, embKey); !equalStrings(got, embBefore) {
		t.Fatalf("embedding window = %v, want %v unchanged", got, embBefore)
	}
	if mr.TTL(dupKey) != dupTTLBefore || mr.TTL(embKey) != embTTLBefore {
		t.Fatal("a rate-limited message refreshed a TTL below its stage")
	}
}

func TestDuplicateHitTouchesNothingBelowIt(t *testing.T) {
	s, mr := newTestState(t)
	ctx := context.Background()
	dupKey := rediskeys.Dup("ct", "au")
	embKey := rediskeys.Emb("ct", "au")

	params := func(hash string) CascadeParams {
		return CascadeParams{
			Dup:      &DupParams{Hash: hash, Depth: 4, TTL: 5 * time.Minute},
			EmbDepth: 4,
		}
	}

	if res, err := s.Cascade(ctx, "ct", "au", params("h1")); err != nil || res.Hit != CascadeNone {
		t.Fatalf("first call = (%v, %v), want a clean pass", res.Hit, err)
	}
	if err := s.EmbAppend(ctx, "ct", "au", []float32{1, 0, 0}, 4, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	embBefore := listOf(t, mr, embKey)
	embTTLBefore := mr.TTL(embKey)

	res, err := s.Cascade(ctx, "ct", "au", params("h1"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Hit != CascadeDuplicate {
		t.Fatalf("hit = %v, want CascadeDuplicate", res.Hit)
	}
	if len(res.Vectors) != 0 {
		t.Fatalf("a duplicate read %d vectors; the semantic stage must not run", len(res.Vectors))
	}
	if got := listOf(t, mr, embKey); !equalStrings(got, embBefore) {
		t.Fatalf("embedding window = %v, want %v unchanged: a duplicate must not append a vector, "+
			"or it becomes the thing later messages are compared against", got, embBefore)
	}
	if mr.TTL(embKey) != embTTLBefore {
		t.Fatal("a duplicate refreshed the embedding window's TTL")
	}
	// The duplicate window itself IS still updated on a hit — the separate
	// script pushed unconditionally and the eviction order depends on it
	// (see TestDupCheckMembershipAndDepth). Pinned so a later "tidy-up"
	// cannot quietly change the window's depth semantics.
	if got := listOf(t, mr, dupKey); len(got) != 2 || got[0] != "h1" || got[1] != "h1" {
		t.Fatalf("duplicate window = %v, want the hash pushed again on a hit", got)
	}
}

// A stage the policy leaves off must not touch its key at all — the merged
// script may not "helpfully" maintain state nobody asked for.
func TestDisabledMergedStagesTouchNoKeys(t *testing.T) {
	s, mr := newTestState(t)
	ctx := context.Background()

	if _, err := s.Cascade(ctx, "ct", "au", CascadeParams{}); err != nil {
		t.Fatal(err)
	}
	if keys := mr.Keys(); len(keys) != 0 {
		t.Fatalf("a cascade with every stage disabled created %v", keys)
	}
	// Rate only: no dup or emb key.
	if _, err := s.Cascade(ctx, "ct", "au", CascadeParams{
		Rate: &RateParams{Capacity: 5, RefillPerSec: 1, Now: t0, TTL: time.Minute},
	}); err != nil {
		t.Fatal(err)
	}
	if keys := mr.Keys(); len(keys) != 1 || keys[0] != rediskeys.Rate("ct", "au") {
		t.Fatalf("rate-only cascade touched %v", keys)
	}
	// Duplicate only: still no emb key.
	if _, err := s.Cascade(ctx, "ct", "au", CascadeParams{
		Dup: &DupParams{Hash: "h1", Depth: 4, TTL: time.Minute},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := mr.List(rediskeys.Emb("ct", "au")); err == nil {
		t.Fatal("the semantic stage was disabled but the embedding key exists")
	}
}

// ---------------------------------------------------------------------------
// The same property, as the verdict it would change.
// ---------------------------------------------------------------------------

// With a one-deep duplicate window the pollution is immediately visible: a
// naive merge would push the rate-limited message's hash, evict the
// legitimate one, and let the next repeat through as clean.
func TestRateLimitedMessageDoesNotPolluteTheDuplicateWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DupDepth = 1
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.RateLimitMessages = i32(1)
		p.RateLimitSeconds = i32(10)
		p.Spam = policyapi.SpamMode_SPAM_MODE_IDENTICAL
	}))

	env.process(t, testMessage("m1", "alpha")) // spends the only token
	env.process(t, testMessage("m2", "beta"))  // rate limited

	if f := onlyFlag(t, env); f.Detector != contracts.DetectorRateLimit || f.MessageID != "m2" {
		t.Fatalf("flag = %+v, want rate_limit on m2", f)
	}
	if got := listOf(t, env.mr, rediskeys.Dup("ct_9f2a", "sd_3b71")); len(got) != 1 ||
		got[0] != HashText(Normalize("alpha")) {
		t.Fatalf("duplicate window after a rate-limited message = %v, want only alpha's hash", got)
	}

	// And the consequence: once the bucket refills, "alpha" is still a
	// duplicate. If m2 had been pushed it would have evicted alpha and
	// this message would come back clean.
	env.clock.Advance(10 * time.Second)
	env.process(t, testMessage("m3", "alpha"))
	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 2 {
		t.Fatalf("flagged %d, want 2", len(flagged))
	}
	if f := unmarshalFlagged(t, flagged[1]); f.Detector != contracts.DetectorDuplicate {
		t.Fatalf("detector = %s, want duplicate: the rate-limited message evicted alpha from the window",
			f.Detector)
	}
}

// The same, one stage down: a duplicate must not append its embedding, or
// it becomes the vector later messages are compared against.
func TestDuplicateDoesNotPolluteTheEmbeddingWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.EmbDepth = 1
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
	}))

	env.embed.vec = []float32{1, 0, 0}
	env.process(t, testMessage("m1", "buy my merch"))

	embBefore := listOf(t, env.mr, rediskeys.Emb("ct_9f2a", "sd_3b71"))
	// Identical text: the duplicate stage stops it. The embedder is armed
	// with an orthogonal vector, so if the stage below it ran anyway the
	// window would be overwritten with something unrelated.
	env.embed.vec = []float32{0, 1, 0}
	env.process(t, testMessage("m2", "buy my merch"))

	if f := onlyFlag(t, env); f.Detector != contracts.DetectorDuplicate || f.MessageID != "m2" {
		t.Fatalf("flag = %+v, want duplicate on m2", f)
	}
	if got := listOf(t, env.mr, rediskeys.Emb("ct_9f2a", "sd_3b71")); !equalStrings(got, embBefore) {
		t.Fatal("a duplicate appended its embedding; the semantic stage below it must not have run")
	}

	// The consequence: a reworded repeat still matches m1's vector.
	env.embed.vec = []float32{1, 0, 0}
	env.process(t, testMessage("m3", "purchase my merchandise"))
	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 2 {
		t.Fatalf("flagged %d, want 2", len(flagged))
	}
	if f := unmarshalFlagged(t, flagged[1]); f.Detector != contracts.DetectorSemanticSpam {
		t.Fatalf("detector = %s, want semantic_spam: m2's vector displaced m1's", f.Detector)
	}
}

// The embedding is the expensive half of stage 6 (§7.4: the only pre-LLM
// detector that costs a network round trip). Merging the Redis calls must
// not turn it into something every message pays for.
func TestStoppedMessagesNeverReachTheEmbedder(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.RateLimitMessages = i32(1)
		p.RateLimitSeconds = i32(10)
		p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
	}))
	counting := &countingEmbedder{inner: env.embed}
	env.pipe.embed = counting

	env.process(t, testMessage("m1", "alpha"))        // passes: one embed
	env.process(t, testMessage("m2", "alpha"))        // rate limited
	env.clock.Advance(10 * time.Second)               //
	env.process(t, testMessage("m3", "alpha"))        // duplicate
	env.clock.Advance(10 * time.Second)               //
	env.process(t, testMessage("m4", "alpha indeed")) // passes: one embed

	if counting.count() != 2 {
		t.Fatalf("embedder called %d times over 4 messages, want 2: a message stopped by the rate "+
			"limiter or the duplicate detector must never be embedded", counting.count())
	}
}

// ---------------------------------------------------------------------------
// Every enabled/disabled combination of the three merged stages.
// ---------------------------------------------------------------------------

// The sequence is the same for each shape: a first message, a verbatim
// repeat, then a reworded repeat that embeds identically to the first. The
// expected detectors are the pre-merge behaviour, stage by stage.
func TestEveryMergedStageCombination(t *testing.T) {
	const none = contracts.Detector("")
	cases := []struct {
		name string
		rate bool
		spam policyapi.SpamMode
		want []contracts.Detector // one per message
	}{
		{"all off", false, policyapi.SpamMode_SPAM_MODE_NONE,
			[]contracts.Detector{none, none, none}},
		{"identical only", false, policyapi.SpamMode_SPAM_MODE_IDENTICAL,
			[]contracts.Detector{none, contracts.DetectorDuplicate, none}},
		{"semantic only", false, policyapi.SpamMode_SPAM_MODE_SEMANTIC,
			[]contracts.Detector{none, contracts.DetectorDuplicate, contracts.DetectorSemanticSpam}},
		{"rate only", true, policyapi.SpamMode_SPAM_MODE_NONE,
			[]contracts.Detector{none, none, contracts.DetectorRateLimit}},
		// The rate limiter takes a token from the repeat too — it runs
		// before the duplicate stage and does not know it will be stopped
		// — so the third message finds the bucket empty either way.
		{"rate + identical", true, policyapi.SpamMode_SPAM_MODE_IDENTICAL,
			[]contracts.Detector{none, contracts.DetectorDuplicate, contracts.DetectorRateLimit}},
		{"rate + semantic", true, policyapi.SpamMode_SPAM_MODE_SEMANTIC,
			[]contracts.Detector{none, contracts.DetectorDuplicate, contracts.DetectorRateLimit}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newPipeEnv(t, DefaultConfig())
			env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
				if tc.rate {
					p.RateLimitMessages = i32(2) // spent by the first two
					p.RateLimitSeconds = i32(10)
				}
				p.Spam = tc.spam
			}))
			env.embed.vec = []float32{1, 0, 0} // every text embeds alike

			texts := []string{"buy my merch", "buy my merch", "purchase my merchandise"}
			for i, text := range texts {
				before := len(env.prod.byTopic(contracts.TopicFlagged))
				env.process(t, testMessage(fmt.Sprintf("m%d", i+1), text))
				flagged := env.prod.byTopic(contracts.TopicFlagged)
				got := none
				if len(flagged) > before {
					got = unmarshalFlagged(t, flagged[len(flagged)-1]).Detector
				}
				if got != tc.want[i] {
					t.Fatalf("message %d (%q): detector = %q, want %q", i+1, text, got, tc.want[i])
				}
			}
			// Nothing but the expected flags, and every message accounted.
			if sum := env.outcome(t, "clean") + env.outcome(t, "flagged"); sum != 3 {
				t.Fatalf("outcomes sum to %v, want 3", sum)
			}
			if got := env.failOpen(t, "redis", ""); got != 0 {
				t.Fatalf("fail_open{redis} = %v with a healthy Redis, want 0", got)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// What the merge actually bought, per policy shape.
// ---------------------------------------------------------------------------

// Round trips reaching the Redis client for ONE message, in steady state
// (the script is already cached, so no EVALSHA/EVAL fallback is counted).
//
// The "before" column was measured on the pre-merge code with the same
// probe; it is recorded here so the claim stays checkable rather than
// asserted from memory:
//
//	policy shape                  before  after
//	nothing enabled                    1      1   seen only
//	rate limit only                    2      2   seen + bucket
//	spam = identical                   2      2   seen + dup
//	rate + identical                   3      2   the merge starts paying
//	spam = semantic                    4      3   dup + LRANGE + append -> merged + append
//	rate + semantic                    5      3
//	rate + semantic + restricted      (6)     4   the sampler is its own slot, +1
//
// The last "before" is in brackets because it is the row above plus the
// sampler's one call rather than a direct reading: the pre-merge fixture
// flagged that message as semantic spam before it reached the sampler.
// The +1 itself is measured — this test's own last two rows differ by
// exactly the sampler.
//
// The sampler (samp:{content_id}) and the redelivery guard
// (seen:<message_id>) are in other slot families and MUST stay separate
// calls, which is why the floor is not lower.
func TestRoundTripsPerPolicyShape(t *testing.T) {
	cases := []struct {
		name      string
		mut       func(*policyapi.ResolvedPolicy)
		want      int
		wasBefore int
	}{
		{"nothing enabled", func(p *policyapi.ResolvedPolicy) {}, 1, 1},
		{"rate limit only", func(p *policyapi.ResolvedPolicy) {
			p.RateLimitMessages = i32(100)
			p.RateLimitSeconds = i32(10)
		}, 2, 2},
		{"spam identical", func(p *policyapi.ResolvedPolicy) {
			p.Spam = policyapi.SpamMode_SPAM_MODE_IDENTICAL
		}, 2, 2},
		{"rate + identical", func(p *policyapi.ResolvedPolicy) {
			p.RateLimitMessages = i32(100)
			p.RateLimitSeconds = i32(10)
			p.Spam = policyapi.SpamMode_SPAM_MODE_IDENTICAL
		}, 2, 3},
		{"spam semantic", func(p *policyapi.ResolvedPolicy) {
			p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
		}, 3, 4},
		{"rate + semantic", func(p *policyapi.ResolvedPolicy) {
			p.RateLimitMessages = i32(100)
			p.RateLimitSeconds = i32(10)
			p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
		}, 3, 5},
		{"rate + semantic + restricted content", func(p *policyapi.ResolvedPolicy) {
			p.RateLimitMessages = i32(100)
			p.RateLimitSeconds = i32(10)
			p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
			p.RestrictedContent = rcPolicy("x", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO).RestrictedContent
		}, 4, 6},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newPipeEnv(t, DefaultConfig())
			env.policy.val = cachedPolicy(testPolicy(tc.mut))
			env.warmRedis(t)

			// The first message loads the script and populates the
			// windows; the second is the one that is measured.
			env.embed.vec = []float32{1, 0, 0}
			env.process(t, testMessage("m1", "first text"))
			base := env.probe.count()
			env.embed.vec = []float32{0, 1, 0} // orthogonal: no semantic hit
			env.process(t, testMessage("m2", "second text"))

			if got := env.probe.count() - base; got != tc.want {
				t.Fatalf("%d Redis round trips, want %d (was %d before the merge)",
					got, tc.want, tc.wasBefore)
			}
			if len(env.prod.records) != 0 {
				t.Fatalf("the measured message was flagged: %d records", len(env.prod.records))
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Concurrency: several partition workers driving the merged script.
// ---------------------------------------------------------------------------

func TestConcurrentMergedCascade(t *testing.T) {
	const workers, perWorker = 8, 40
	cfg := DefaultConfig()
	cfg.SamplerCapacity = 10000
	cfg.SamplerPerMin = 60000
	env := newPipeEnv(t, cfg)
	pol := rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)
	// Every merged stage on, with a bucket wide enough that the rate
	// limiter does not swallow the traffic before the rest runs.
	pol.RateLimitMessages = i32(100000)
	pol.RateLimitSeconds = i32(10)
	pol.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
	env.policy.val = cachedPolicy(pol)

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
	sum := env.outcome(t, "clean") + env.outcome(t, "flagged") + env.outcome(t, "skipped")
	if sum != total {
		t.Fatalf("outcomes sum to %v over %d messages: counts were lost or double-counted", sum, total)
	}
	if got := env.failOpen(t, "redis", ""); got != 0 {
		t.Fatalf("fail_open{redis} = %v with a healthy Redis, want 0", got)
	}
	// The windows stayed within their configured depth despite the
	// concurrency: LTRIM runs inside the script, not as a follow-up.
	if got := listOf(t, env.mr, rediskeys.Dup("ct_9f2a", "sd_3b71")); len(got) > cfg.DupDepth {
		t.Fatalf("duplicate window holds %d entries, want <= %d", len(got), cfg.DupDepth)
	}
	if got := listOf(t, env.mr, rediskeys.Emb("ct_9f2a", "sd_3b71")); len(got) > cfg.EmbDepth {
		t.Fatalf("embedding window holds %d entries, want <= %d", len(got), cfg.EmbDepth)
	}
}

// ---------------------------------------------------------------------------
// Fail-open (§4.7) across the merged call.
// ---------------------------------------------------------------------------

func TestMergedCascadeFailsOpenOnceAndSkipsTheRest(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(fullCascadePolicy())
	env.warmRedis(t)
	env.mr.SetError("redis is down")

	env.process(t, testMessage("m1", "a badword appears"))

	// One message, one fail-open, however many stages it lost (§4.7).
	if got := env.failOpen(t, "redis", ""); got != 1 {
		t.Fatalf("fail_open{redis} = %v, want exactly 1", got)
	}
	// The embedding was not paid for: with the comparison window
	// unreadable there is nothing to compare against.
	if got := env.failOpen(t, "embedding", ""); got != 0 {
		t.Fatalf("fail_open{embedding} = %v, want 0: the semantic stage is a Redis skip here", got)
	}
	// And the cascade still continues to the stages that do not need
	// Redis.
	if f := onlyFlag(t, env); f.Detector != contracts.DetectorRestrictedWord {
		t.Fatalf("detector = %s, want restricted_word", f.Detector)
	}
}

// A reply that does not match the script's contract is an infrastructure
// failure, not a verdict: the message fails open rather than being flagged
// on a number nobody can explain.
func TestUnknownCascadeReplyIsAnError(t *testing.T) {
	if _, err := decodeCascade(nil); err == nil {
		t.Fatal("an empty reply must be an error")
	}
	if _, err := decodeCascade([]any{"not a number"}); err == nil {
		t.Fatal("a non-integer stage must be an error")
	}
	if _, err := decodeCascade([]any{int64(7)}); err == nil {
		t.Fatal("an unknown stage code must be an error")
	}
	// A corrupt vector is skipped, not fatal: one bad entry must not fail
	// the whole message open.
	res, err := decodeCascade([]any{int64(0), string(packVector([]float32{1, 0})), "xyz"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Vectors) != 1 {
		t.Fatalf("decoded %d vectors, want 1 (the well-formed one)", len(res.Vectors))
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

type miniredisLister interface {
	List(string) ([]string, error)
}

// listOf returns a list key's contents, treating "no such key" as empty.
func listOf(t *testing.T, mr miniredisLister, key string) []string {
	t.Helper()
	vals, err := mr.List(key)
	if err != nil {
		return nil
	}
	return vals
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// onlyFlag asserts exactly one flagged.v1 record and returns it.
func onlyFlag(t *testing.T, env *pipeEnv) contracts.Flagged {
	t.Helper()
	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 1 {
		t.Fatalf("flagged %d records, want 1", len(flagged))
	}
	return unmarshalFlagged(t, flagged[0])
}

// countingEmbedder counts calls to the embedding service (§8.4) so a test
// can assert that a stopped message never paid for one.
type countingEmbedder struct {
	inner Embedder
	mu    sync.Mutex
	n     int
}

func (c *countingEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
	return c.inner.Embed(ctx, texts)
}

func (c *countingEmbedder) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}
