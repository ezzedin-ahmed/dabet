package mod

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/contracts"
	"dabet/pkg/policyapi"
)

func (e *pipeEnv) usageTotal() int64 {
	e.usage.mu.Lock()
	defer e.usage.mu.Unlock()
	var n int64
	for _, v := range e.usage.counts {
		n += v
	}
	return n
}

func (e *pipeEnv) failOpen(t *testing.T, component, reason string) float64 {
	t.Helper()
	return testutil.ToFloat64(e.met.FailOpen.WithLabelValues(component, reason))
}

func (e *pipeEnv) outcome(t *testing.T, outcome string) float64 {
	t.Helper()
	return testutil.ToFloat64(e.met.MessagesTotal.WithLabelValues(outcome))
}

func TestCleanMessageProducesNothing(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.RestrictedWords = []string{"badword"}
	}))

	env.process(t, testMessage("m1", "a perfectly fine message"))

	if len(env.prod.records) != 0 {
		t.Fatalf("clean message produced %d records", len(env.prod.records))
	}
	if env.outcome(t, "clean") != 1 {
		t.Fatal("outcome clean not counted")
	}
	if env.usageTotal() != 1 {
		t.Fatalf("usage = %d, want 1 (clean messages are billed)", env.usageTotal())
	}
}

func TestRedeliveryGuardDrops(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy())

	env.process(t, testMessage("m1", "hello"))
	env.process(t, testMessage("m1", "hello")) // Kafka redelivery

	if env.outcome(t, "skipped") != 1 {
		t.Fatal("redelivered message must be dropped as skipped")
	}
	if env.usageTotal() != 1 {
		t.Fatalf("usage = %d, want 1: redeliveries are not billed twice", env.usageTotal())
	}
	if env.policy.calls != 1 {
		t.Fatalf("policy resolved %d times, want 1: guard fires before policy", env.policy.calls)
	}
}

func TestNoPolicyPassesAndBills(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = CachedPolicy{} // negative: no policy at any scope

	env.process(t, testMessage("m1", "whatever FLAGME badword"))

	if len(env.prod.records) != 0 {
		t.Fatal("unmoderated creator must produce nothing")
	}
	if env.outcome(t, "clean") != 1 || env.usageTotal() != 1 {
		t.Fatal("no-policy message is clean and billed (§7.3 step 2)")
	}
}

func TestPolicyFailOpen(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.err = errors.New("policy-service down, cache cold")

	env.process(t, testMessage("m1", "badword"))

	if env.failOpen(t, "policy", "") != 1 {
		t.Fatal("fail_open_total{component=policy} must count")
	}
	if len(env.prod.records) != 0 {
		t.Fatal("message must pass unmoderated")
	}
	if env.outcome(t, "skipped") != 1 {
		t.Fatal("outcome skipped expected")
	}
}

func TestZeroCreditsPassUnbilled(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.RestrictedWords = []string{"badword"}
	}))
	env.credits.ok = false

	env.process(t, testMessage("m1", "badword"))

	if env.failOpen(t, "credits", "no_credits") != 1 {
		t.Fatal("fail_open_total{reason=no_credits} must count")
	}
	if len(env.prod.records) != 0 {
		t.Fatal("zero-credit message passes unmoderated even with a word hit")
	}
	if env.usageTotal() != 0 {
		t.Fatal("zero-credit messages are NOT billed (§5.8)")
	}
}

// First hit wins, in cost order: the rate limiter fires before the
// duplicate detector even when both would match.
func TestFirstHitWinsRateBeforeDuplicate(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.RateLimitMessages = i32(1)
		p.RateLimitSeconds = i32(10)
		p.Spam = policyapi.SpamMode_SPAM_MODE_IDENTICAL
	}))

	env.process(t, testMessage("m1", "same text"))
	env.process(t, testMessage("m2", "same text")) // rate AND duplicate would hit

	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 1 {
		t.Fatalf("flagged %d, want 1", len(flagged))
	}
	f := unmarshalFlagged(t, flagged[0])
	if f.Detector != contracts.DetectorRateLimit {
		t.Fatalf("detector = %s, want rate_limit (cheaper stage wins)", f.Detector)
	}
	if f.Action != contracts.ActionAutoDelete || f.PolicyID != "pol_7a13" || f.Text != "same text" {
		t.Fatalf("flagged payload wrong: %+v", f)
	}
	if f.FlaggedAt.IsZero() {
		t.Fatal("flagged_at must be set")
	}
	// auto_delete: deletions.v1 too, keyed by content_id, no text.
	dels := env.prod.byTopic(contracts.TopicDeletions)
	if len(dels) != 1 || dels[0].Key != "ct_9f2a" {
		t.Fatalf("deletions = %+v", dels)
	}
	if strings.Contains(string(dels[0].Value), "same text") {
		t.Fatal("deletions.v1 must never carry text")
	}
	var del contracts.Deletion
	if err := json.Unmarshal(dels[0].Value, &del); err != nil {
		t.Fatal(err)
	}
	if del.Reason != contracts.DetectorRateLimit || del.MessageID != "m2" {
		t.Fatalf("deletion payload wrong: %+v", del)
	}
}

// Duplicate fires before restricted words; the first delivery of the same
// text falls through duplicate and is caught by words.
func TestFirstHitWinsDuplicateBeforeWords(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.Spam = policyapi.SpamMode_SPAM_MODE_IDENTICAL
		p.RestrictedWords = []string{"badword"}
	}))

	env.process(t, testMessage("m1", "badword here"))
	env.process(t, testMessage("m2", "BADWORD   here")) // identical after normalisation

	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 2 {
		t.Fatalf("flagged %d, want 2", len(flagged))
	}
	if f := unmarshalFlagged(t, flagged[0]); f.Detector != contracts.DetectorRestrictedWord {
		t.Fatalf("first delivery detector = %s, want restricted_word", f.Detector)
	}
	if f := unmarshalFlagged(t, flagged[1]); f.Detector != contracts.DetectorDuplicate {
		t.Fatalf("second delivery detector = %s, want duplicate (fires before words)", f.Detector)
	}
}

func TestDisabledStagesAreSkipped(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	// Policy exists but enables nothing: no rate limit, spam none, no
	// words, no restricted content.
	env.policy.val = cachedPolicy(testPolicy())

	env.process(t, testMessage("m1", "text"))
	env.process(t, testMessage("m2", "text")) // identical, but dup is disabled

	if len(env.prod.records) != 0 {
		t.Fatal("everything disabled: nothing may be flagged")
	}
	for _, k := range env.mr.Keys() {
		if !strings.HasPrefix(k, "seen:") {
			t.Fatalf("disabled stage touched Redis: key %q", k)
		}
	}
	if env.llm.calls != 0 {
		t.Fatal("no restricted_content: LLM must not be called")
	}
	if got := testutil.ToFloat64(env.met.SamplerSkipped); got != 0 {
		t.Fatal("sampler does not run when the LLM stage is disabled")
	}
}

func TestSemanticSpamThreshold(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
	}))
	env.embed.vec = []float32{1, 0, 0} // every text embeds identically

	env.process(t, testMessage("m1", "buy my merch"))       // history empty
	env.process(t, testMessage("m2", "purchase my merch!")) // reworded: dup misses, cosine 1.0

	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 1 {
		t.Fatalf("flagged %d, want 1", len(flagged))
	}
	if f := unmarshalFlagged(t, flagged[0]); f.Detector != contracts.DetectorSemanticSpam || f.MessageID != "m2" {
		t.Fatalf("flag = %+v, want semantic_spam on m2", f)
	}
}

func TestSemanticBelowThresholdPasses(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
	}))

	env.embed.vec = []float32{1, 0, 0}
	env.process(t, testMessage("m1", "first"))
	env.embed.vec = []float32{0.9, 0.44, 0} // cosine ~0.9 < 0.95
	env.process(t, testMessage("m2", "second"))

	if len(env.prod.records) != 0 {
		t.Fatal("below-threshold similarity must pass")
	}
}

func TestEmbeddingDownSkipsSemanticStage(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
		p.RestrictedWords = []string{"badword"}
	}))
	env.embed.err = errors.New("embedding service down")

	env.process(t, testMessage("m1", "clean message"))
	if env.failOpen(t, "embedding", "") != 1 {
		t.Fatal("embedding fail-open must count")
	}
	// The cascade continues: words still fire.
	env.process(t, testMessage("m2", "badword"))
	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 1 {
		t.Fatalf("flagged %d, want 1", len(flagged))
	}
	if f := unmarshalFlagged(t, flagged[0]); f.Detector != contracts.DetectorRestrictedWord {
		t.Fatalf("detector = %s, want restricted_word after embedding skip", f.Detector)
	}
}

func TestRedisDownContinuesToWordsAndCountsOnce(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.RateLimitMessages = i32(1)
		p.RateLimitSeconds = i32(10)
		p.Spam = policyapi.SpamMode_SPAM_MODE_SEMANTIC
		p.RestrictedWords = []string{"badword"}
	}))
	env.mr.Close() // Redis dies entirely

	env.process(t, testMessage("m1", "a badword appears"))

	// §4.7: skip rate/dup/semantic, continue to word + LLM stages.
	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 1 {
		t.Fatalf("flagged %d, want 1: words run in-memory", len(flagged))
	}
	if f := unmarshalFlagged(t, flagged[0]); f.Detector != contracts.DetectorRestrictedWord {
		t.Fatalf("detector = %s, want restricted_word", f.Detector)
	}
	if got := env.failOpen(t, "redis", ""); got != 1 {
		t.Fatalf("fail_open{redis} = %v, want exactly 1 per message despite multiple failed stages", got)
	}
	if env.embed.err == nil && env.failOpen(t, "embedding", "") != 0 {
		t.Fatal("embedding stage is skipped with Redis down, not failed")
	}
}

func TestRedisDownSamplerFallsBackToMemory(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SamplerCapacity = 1 // one LLM slot, no refill within the test
	cfg.SamplerPerMin = 60
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
	env.mr.Close()

	env.process(t, testMessage("m1", "first"))
	env.process(t, testMessage("m2", "second"))

	if got := testutil.ToFloat64(env.met.SamplerSkipped); got != 1 {
		t.Fatalf("sampler_skipped = %v, want 1: in-memory fallback bucket enforces the ceiling", got)
	}
	env.pipe.batcher.mu.Lock()
	pending := 0
	for _, p := range env.pipe.batcher.pending {
		pending += len(p.messages)
	}
	env.pipe.batcher.mu.Unlock()
	if pending != 1 {
		t.Fatalf("LLM pending = %d, want 1 (the sampled message)", pending)
	}
}

func TestSamplerCeilingSkipsLLM(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SamplerCapacity = 2
	cfg.SamplerPerMin = 60
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))

	for _, id := range []string{"m1", "m2", "m3", "m4"} {
		env.process(t, testMessage(id, "message "+id))
	}
	if got := testutil.ToFloat64(env.met.SamplerSkipped); got != 2 {
		t.Fatalf("sampler_skipped = %v, want 2 of 4", got)
	}
	// Skipped messages are treated clean.
	if env.outcome(t, "clean") != 2 {
		t.Fatalf("clean = %v, want 2 (unsampled)", env.outcome(t, "clean"))
	}
}

func TestLLMBatchDispatchAndActionRouting(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LLMBatchSize = 2
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
	env.llm.verdicts = []int{1, 0} // first violates rule 1

	env.process(t, testMessage("m1", "FLAGME text"))
	env.process(t, testMessage("m2", "clean text")) // completes the batch
	env.pipe.wg.Wait()

	if env.llm.calls != 1 || len(env.llm.batches[0]) != 2 {
		t.Fatalf("llm calls = %d, batches = %v", env.llm.calls, env.llm.batches)
	}
	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 1 {
		t.Fatalf("flagged %d, want 1", len(flagged))
	}
	f := unmarshalFlagged(t, flagged[0])
	if f.Detector != contracts.DetectorRestrictedContent || f.Action != contracts.ActionAutoDelete || f.MessageID != "m1" {
		t.Fatalf("flag = %+v", f)
	}
	if dels := env.prod.byTopic(contracts.TopicDeletions); len(dels) != 1 {
		t.Fatalf("auto action must also delete, got %d", len(dels))
	}
	if env.outcome(t, "clean") != 1 || env.outcome(t, "flagged") != 1 {
		t.Fatal("batch outcomes miscounted")
	}
}

func TestLLMReviewActionSkipsDeletion(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	pol := rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_REVIEW)
	env.llm.verdicts = []int{1}

	env.pipe.dispatch(context.Background(), &LLMBatch{Policy: pol, Messages: []contracts.Message{testMessage("m1", "borderline")}})

	flagged := env.prod.byTopic(contracts.TopicFlagged)
	if len(flagged) != 1 {
		t.Fatalf("flagged %d, want 1", len(flagged))
	}
	if f := unmarshalFlagged(t, flagged[0]); f.Action != contracts.ActionReview {
		t.Fatalf("action = %s, want review", f.Action)
	}
	if dels := env.prod.byTopic(contracts.TopicDeletions); len(dels) != 0 {
		t.Fatal("review action must NOT publish a deletion")
	}
	if flagged[0].Key != "9d4e-creator" {
		t.Fatalf("flagged.v1 key = %q, want creator_id", flagged[0].Key)
	}
}

func TestLLMErrorFailsWholeBatchOpen(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	pol := rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO)
	env.llm.err = errors.New("llm timeout")

	env.pipe.dispatch(context.Background(), &LLMBatch{Policy: pol, Messages: []contracts.Message{
		testMessage("m1", "a"), testMessage("m2", "b"), testMessage("m3", "c"),
	}})

	if got := env.failOpen(t, "llm", ""); got != 3 {
		t.Fatalf("fail_open{llm} = %v, want 3 (whole batch)", got)
	}
	if len(env.prod.records) != 0 {
		t.Fatal("failed batch publishes nothing")
	}
	if env.llm.calls != 1 {
		t.Fatalf("llm calls = %d, want 1: no retry (§7.9)", env.llm.calls)
	}
	if env.outcome(t, "skipped") != 3 {
		t.Fatal("batch messages counted as skipped")
	}
}

func TestLLMLingerDispatchViaBatcher(t *testing.T) {
	cfg := DefaultConfig()
	env := newPipeEnv(t, cfg)
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
	env.llm.verdicts = []int{0}

	env.process(t, testMessage("m1", "waiting"))
	if env.llm.calls != 0 {
		t.Fatal("under batch size: nothing dispatched yet")
	}
	env.clock.Advance(cfg.LLMLinger)
	for _, b := range env.pipe.batcher.Due(env.clock.Now()) {
		env.pipe.dispatch(context.Background(), b)
	}
	if env.llm.calls != 1 {
		t.Fatal("linger-due batch must dispatch")
	}
}

func TestProducerFailureCountsKafkaFailOpen(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(testPolicy(func(p *policyapi.ResolvedPolicy) {
		p.RestrictedWords = []string{"badword"}
	}))
	env.prod.fail = true

	env.process(t, testMessage("m1", "badword"))

	// Both the flagged and the deletion publish drop.
	if got := env.failOpen(t, "kafka", ""); got != 2 {
		t.Fatalf("fail_open{kafka} = %v, want 2", got)
	}
	if env.outcome(t, "flagged") != 1 {
		t.Fatal("the message is still accounted as flagged")
	}
}

func TestMalformedMessageSkipped(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.pipe.Process(context.Background(), []byte("{not json"))
	if env.outcome(t, "skipped") != 1 {
		t.Fatal("malformed payload must be skipped")
	}
	if env.policy.calls != 0 {
		t.Fatal("malformed payload must not resolve policy")
	}
}

func TestShutdownFlushesPendingBatchesAndUsage(t *testing.T) {
	env := newPipeEnv(t, DefaultConfig())
	env.policy.val = cachedPolicy(rcPolicy("pol_7a13", policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_AUTO))
	env.llm.verdicts = []int{0}

	env.process(t, testMessage("m1", "pending message"))
	env.pipe.Shutdown(context.Background())

	if env.llm.calls != 1 {
		t.Fatal("shutdown must dispatch pending LLM batches")
	}
	usage := env.prod.byTopic(contracts.TopicUsage)
	if len(usage) != 1 {
		t.Fatalf("shutdown flushed %d usage events, want 1", len(usage))
	}
	var u contracts.Usage
	if err := json.Unmarshal(usage[0].Value, &u); err != nil {
		t.Fatal(err)
	}
	if u.Quantity != 1 || u.IdempotencyKey != "mod:inst-1:2026-08-19T14:02:9d4e-creator" {
		t.Fatalf("usage event = %+v", u)
	}
}
