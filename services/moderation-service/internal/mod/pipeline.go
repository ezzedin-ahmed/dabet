package mod

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"

	"dabet/pkg/contracts"
	"dabet/pkg/kafkax"
	"dabet/pkg/policyapi"
	"dabet/pkg/rediskeys"
	"dabet/pkg/tracing"
)

// CreditsChecker is the advisory credits_ok flag (§5.8); implemented by
// pkg/credits.Client (60 s TTL, fails open to true on transport errors).
type CreditsChecker interface {
	OK(ctx context.Context, creatorID string) bool
}

// Embedder is the embedding service (§8.4); implemented by
// pkg/embeddings.Client.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Config carries the tunables of the cascade. Every documented number is
// a default, not a constant (§4.4); main overrides from env.
type Config struct {
	SeenTTL           time.Duration // redelivery guard, §7.4
	DupDepth          int           // last-N hashes, A15
	DupTTL            time.Duration
	EmbDepth          int // last-N vectors (mirrors A15)
	EmbTTL            time.Duration
	SemanticThreshold float64 // cosine, A16
	SamplerCapacity   float64 // tokens, A17
	SamplerPerMin     float64 // refill, A17
	SamplerTTL        time.Duration
	LLMBatchSize      int           // A18
	LLMLinger         time.Duration // A18
}

// DefaultConfig returns the documented defaults.
func DefaultConfig() Config {
	return Config{
		SeenTTL:           5 * time.Minute,
		DupDepth:          20,
		DupTTL:            5 * time.Minute,
		EmbDepth:          20,
		EmbTTL:            5 * time.Minute,
		SemanticThreshold: 0.95,
		SamplerCapacity:   30,
		SamplerPerMin:     30,
		SamplerTTL:        5 * time.Minute,
		LLMBatchSize:      32,
		LLMLinger:         50 * time.Millisecond,
	}
}

// Pipeline is the §7.3 cascade. One instance serves the whole consumer;
// Process is invoked per record by the (single-goroutine) consumer loop,
// while LLM dispatch and background flushing run on their own goroutines.
type Pipeline struct {
	cfg      Config
	policies PolicyGetter
	credits  CreditsChecker
	state    *RedisState
	embed    Embedder
	llm      Classifier
	batcher  *LLMBatcher
	memSamp  *MemSampler
	pub      *Publisher
	usage    *UsageAggregator
	met      *Metrics
	now      func() time.Time

	wg sync.WaitGroup // in-flight LLM dispatches
}

// NewPipeline wires the cascade. state may be nil only in tests; a broken
// Redis is handled per message, not at construction.
func NewPipeline(cfg Config, policies PolicyGetter, credits CreditsChecker, state *RedisState, embed Embedder, llm Classifier, pub *Publisher, usage *UsageAggregator, met *Metrics, now func() time.Time) *Pipeline {
	return &Pipeline{
		cfg:      cfg,
		policies: policies,
		credits:  credits,
		state:    state,
		embed:    embed,
		llm:      llm,
		batcher:  NewLLMBatcher(cfg.LLMBatchSize, cfg.LLMLinger),
		memSamp:  NewMemSampler(cfg.SamplerCapacity, cfg.SamplerPerMin, cfg.SamplerTTL),
		pub:      pub,
		usage:    usage,
		met:      met,
		now:      now,
	}
}

// Handler adapts the pipeline to the kafkax consumer. It never returns an
// error: every failure mode inside Process is a fail-open, and failing the
// batch would stall consumption, which §4.7 forbids ("accept the lag" is
// about backlog, not about refusing to move on).
func (p *Pipeline) Handler(group string) kafkax.Handler {
	return func(ctx context.Context, rec *kgo.Record) error {
		p.met.KafkaConsumed.WithLabelValues(rec.Topic, group, "ok").Inc()
		// One span for the whole cascade, hanging off the consumer span
		// kafkax opened from the record's traceparent — so this is the
		// same trace as the adapter ingest that produced the record.
		// Per-stage timings deliberately stay in the histograms
		// (observeStage): a span per stage at 500 000 msg/s would cost
		// more than the stages do.
		ctx, span := tracing.Tracer().Start(ctx, "moderation.cascade")
		defer span.End()
		p.Process(ctx, rec.Value)
		return nil
	}
}

// Process runs one message through the cascade. First hit wins; stages
// disabled by policy are skipped; every dependency failure fails open per
// the normative table in §4.7.
func (p *Pipeline) Process(ctx context.Context, value []byte) {
	var msg contracts.Message
	if err := json.Unmarshal(value, &msg); err != nil {
		p.met.MessagesTotal.WithLabelValues("skipped").Inc()
		return
	}
	// P4: identifiers only. msg.Text never reaches a span, an event, or an
	// error — and author_id is deliberately absent (see pkg/tracing).
	trace.SpanFromContext(ctx).SetAttributes(
		tracing.MessageID(msg.MessageID),
		tracing.ContentID(msg.ContentID),
		tracing.CreatorID(msg.CreatorID),
	)

	// Redis availability is decided per message: the first failing Redis
	// operation marks it down for the remaining stages and counts ONE
	// fail_open_total{component="redis"} for this message.
	redisDown := p.state == nil
	redisCounted := false
	redisFail := func() {
		redisDown = true
		if !redisCounted {
			redisCounted = true
			p.met.FailOpen.WithLabelValues("redis", "").Inc()
			p.met.DependencyUp.WithLabelValues("redis").Set(0)
		}
	}

	// Stage 1 — redelivery guard (§7.4). Redis down: skip, fail open.
	if !redisDown {
		t0 := p.now()
		already, err := p.state.Seen(ctx, msg.MessageID, p.cfg.SeenTTL)
		p.observeStage("seen", t0)
		if err != nil {
			redisFail()
		} else {
			p.met.DependencyUp.WithLabelValues("redis").Set(1)
			if already {
				p.met.MessagesTotal.WithLabelValues("skipped").Inc()
				return
			}
		}
	}

	// Stage 2 — policy. Error with a cold cache = pass unmoderated. A
	// fail-open pass is still work performed, so it is billed; only the
	// explicit zero-credit case below is exempt (§5.8).
	t0 := p.now()
	cp, err := p.policies.Get(ctx, msg.CreatorID, msg.ContentID)
	p.observeStage("policy", t0)
	if err != nil {
		p.met.FailOpen.WithLabelValues("policy", "").Inc()
		p.met.DependencyUp.WithLabelValues("policy-service").Set(0)
		p.usage.Inc(msg.CreatorID)
		p.met.MessagesTotal.WithLabelValues("skipped").Inc()
		return
	}
	p.met.DependencyUp.WithLabelValues("policy-service").Set(1)
	if cp.Policy == nil {
		// No policy at any scope: not moderated, but processed (§7.3).
		p.usage.Inc(msg.CreatorID)
		p.met.MessagesTotal.WithLabelValues("clean").Inc()
		return
	}
	pol := cp.Policy

	// Stage 3 — credits (§5.8). ok=false: pass unmoderated and do NOT
	// bill — zero-credit messages are free by design, since charging for
	// explicitly unmoderated throughput would bill for nothing.
	t0 = p.now()
	creditsOK := p.credits.OK(ctx, msg.CreatorID)
	p.observeStage("credits", t0)
	if !creditsOK {
		p.met.FailOpen.WithLabelValues("credits", "no_credits").Inc()
		p.met.MessagesTotal.WithLabelValues("skipped").Inc()
		return
	}

	// From here the message is processed (clean or flagged): bill it.
	p.usage.Inc(msg.CreatorID)

	norm := Normalize(msg.Text)

	// Stage 4 — rate limit, only when the policy sets one.
	if pol.RateLimitMessages != nil && pol.RateLimitSeconds != nil && !redisDown {
		capacity := float64(pol.GetRateLimitMessages())
		window := time.Duration(pol.GetRateLimitSeconds()) * time.Second
		t0 = p.now()
		allowed, err := p.state.TakeToken(ctx, rediskeys.Rate(msg.ContentID, msg.AuthorID),
			capacity, capacity/window.Seconds(), p.now(), 2*window)
		p.observeStage("rate_limit", t0)
		if err != nil {
			redisFail()
		} else if !allowed {
			p.flag(ctx, msg, contracts.DetectorRateLimit, contracts.ActionAutoDelete, pol.GetPolicyId())
			return
		}
	}

	// Stage 5 — duplicate, for spam = identical OR semantic (semantic
	// implies the identical check as its cheap first line).
	spam := pol.GetSpam()
	spamOn := spam == policyapi.SpamMode_SPAM_MODE_IDENTICAL || spam == policyapi.SpamMode_SPAM_MODE_SEMANTIC
	if spamOn && !redisDown {
		t0 = p.now()
		hit, err := p.state.DupCheck(ctx, rediskeys.Dup(msg.ContentID, msg.AuthorID),
			HashText(norm), p.cfg.DupDepth, p.cfg.DupTTL)
		p.observeStage("duplicate", t0)
		if err != nil {
			redisFail()
		} else if hit {
			p.flag(ctx, msg, contracts.DetectorDuplicate, contracts.ActionAutoDelete, pol.GetPolicyId())
			return
		}
	}

	// Stage 6 — semantic spam. Embedding down: skip the stage, continue.
	if spam == policyapi.SpamMode_SPAM_MODE_SEMANTIC && !redisDown {
		t0 = p.now()
		vecs, err := p.embed.Embed(ctx, []string{norm})
		if err != nil || len(vecs) != 1 {
			p.met.FailOpen.WithLabelValues("embedding", "").Inc()
			p.met.DependencyUp.WithLabelValues("embedding").Set(0)
			p.observeStage("semantic", t0)
		} else {
			p.met.DependencyUp.WithLabelValues("embedding").Set(1)
			sim, err := p.state.EmbMaxSimilarity(ctx, rediskeys.Emb(msg.ContentID, msg.AuthorID),
				vecs[0], p.cfg.EmbDepth, p.cfg.EmbTTL)
			p.observeStage("semantic", t0)
			if err != nil {
				redisFail()
			} else if sim >= p.cfg.SemanticThreshold {
				p.flag(ctx, msg, contracts.DetectorSemanticSpam, contracts.ActionAutoDelete, pol.GetPolicyId())
				return
			}
		}
	}

	// Stage 7 — restricted words: in-memory, works with every dependency
	// dead. Matcher was compiled when the policy entered the cache.
	if cp.Matcher != nil {
		t0 = p.now()
		hit := cp.Matcher.Match(norm)
		p.observeStage("restricted_words", t0)
		if hit {
			p.flag(ctx, msg, contracts.DetectorRestrictedWord, contracts.ActionAutoDelete, pol.GetPolicyId())
			return
		}
	}

	// Stages 8–9 exist only when the policy defines restricted content.
	if len(pol.GetRestrictedContent()) == 0 {
		p.met.MessagesTotal.WithLabelValues("clean").Inc()
		return
	}

	// Stage 8 — sampler. Redis down: per-instance in-memory fallback
	// bucket (see MemSampler for the documented deviation).
	t0 = p.now()
	sampled := false
	if !redisDown {
		a, err := p.state.TakeToken(ctx, rediskeys.Samp(msg.ContentID),
			p.cfg.SamplerCapacity, p.cfg.SamplerPerMin/60, p.now(), p.cfg.SamplerTTL)
		if err != nil {
			redisFail()
		} else {
			sampled = a
		}
	}
	if redisDown {
		sampled = p.memSamp.Allow(msg.ContentID, p.now())
	}
	p.observeStage("sampler", t0)
	if !sampled {
		p.met.SamplerSkipped.Inc()
		p.met.MessagesTotal.WithLabelValues("clean").Inc()
		return
	}

	// Stage 9 — LLM, batched per policy. The outcome metric for this
	// message is recorded when its batch resolves. Dispatch must survive
	// the per-record context.
	if batch := p.batcher.Add(msg, pol, p.now()); batch != nil {
		p.dispatchAsync(context.WithoutCancel(ctx), batch)
	}
}

// dispatchAsync runs one LLM batch on its own goroutine so a full batch
// never stalls the consumer loop for the LLM timeout.
func (p *Pipeline) dispatchAsync(ctx context.Context, batch *LLMBatch) {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.dispatch(ctx, batch)
	}()
}

// dispatch sends one batch to the LLM and publishes verdicts. On timeout,
// transport error, or an unparseable/incomplete response the WHOLE batch
// fails open — no retry (§7.9).
func (p *Pipeline) dispatch(ctx context.Context, batch *LLMBatch) {
	texts := make([]string, len(batch.Messages))
	for i, m := range batch.Messages {
		texts[i] = m.Text
	}
	p.met.LLMBatchSize.Observe(float64(len(texts)))

	start := p.now()
	verdicts, err := p.llm.Classify(ctx, batch.Policy, texts)
	p.met.LLMLatency.Observe(p.now().Sub(start).Seconds())
	p.observeStage("llm", start)
	if err != nil {
		p.met.LLMRequests.WithLabelValues("error").Inc()
		p.met.DependencyUp.WithLabelValues("llm").Set(0)
		p.met.FailOpen.WithLabelValues("llm", "").Add(float64(len(texts)))
		p.met.MessagesTotal.WithLabelValues("skipped").Add(float64(len(texts)))
		return
	}
	p.met.LLMRequests.WithLabelValues("ok").Inc()
	p.met.DependencyUp.WithLabelValues("llm").Set(1)

	action := contracts.ActionAutoDelete
	if batch.Policy.GetRestrictedContentAction() == policyapi.RestrictedContentAction_RESTRICTED_CONTENT_ACTION_REVIEW {
		action = contracts.ActionReview
	}
	for i, msg := range batch.Messages {
		if verdicts[i] > 0 {
			p.flag(ctx, msg, contracts.DetectorRestrictedContent, action, batch.Policy.GetPolicyId())
		} else {
			p.met.MessagesTotal.WithLabelValues("clean").Inc()
		}
	}
}

// flag publishes the verdict: every flag goes to flagged.v1 (full payload
// incl. text); action=auto_delete additionally goes to deletions.v1 (no
// text). The e2e latency SLI is observed for flagged messages (§4.6).
func (p *Pipeline) flag(ctx context.Context, msg contracts.Message, det contracts.Detector, action contracts.Action, policyID string) {
	now := p.now()
	p.met.DetectorHits.WithLabelValues(string(det), string(action)).Inc()
	// The detector NAME, not what it matched (P4).
	trace.SpanFromContext(ctx).SetAttributes(
		tracing.Outcome("flagged"), tracing.Detector(string(det)))

	flagged := contracts.Flagged{
		MessageID: msg.MessageID,
		ContentID: msg.ContentID,
		AuthorID:  msg.AuthorID,
		CreatorID: msg.CreatorID,
		Text:      msg.Text,
		Detector:  det,
		Action:    action,
		PolicyID:  policyID,
		FlaggedAt: now,
	}
	val, err := json.Marshal(flagged)
	if err == nil {
		t0 := p.now()
		ok := p.pub.Publish(ctx, contracts.TopicFlagged, contracts.FlaggedKey(msg.CreatorID), val)
		p.observeStage("publish", t0)
		if ok {
			p.met.E2ELatency.Observe(now.Sub(msg.IngestedAt).Seconds())
		}
	}

	if action == contracts.ActionAutoDelete {
		del := contracts.Deletion{
			MessageID: msg.MessageID,
			ContentID: msg.ContentID,
			CreatorID: msg.CreatorID,
			Reason:    det,
			IssuedAt:  p.now(),
		}
		if dv, err := json.Marshal(del); err == nil {
			p.pub.Publish(ctx, contracts.TopicDeletions, contracts.DeletionsKey(msg.ContentID), dv)
		}
	}
	p.met.MessagesTotal.WithLabelValues("flagged").Inc()
}

// RunBackground drives the linger trigger of the LLM batcher and the
// minute flush of the usage aggregator until ctx is cancelled. Dispatches
// deliberately outlive ctx (they carry their own timeout).
func (p *Pipeline) RunBackground(ctx context.Context) {
	interval := p.cfg.LLMLinger / 4
	if interval < 5*time.Millisecond {
		interval = 5 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastUsage := p.now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, b := range p.batcher.Due(p.now()) {
				p.dispatchAsync(context.WithoutCancel(ctx), b)
			}
			if now := p.now(); now.Sub(lastUsage) >= time.Second {
				lastUsage = now
				p.usage.FlushDue(context.WithoutCancel(ctx))
			}
		}
	}
}

// Shutdown drains pending LLM batches, waits for in-flight dispatches,
// and flushes the remaining usage windows (§7.10).
func (p *Pipeline) Shutdown(ctx context.Context) {
	for _, b := range p.batcher.FlushAll() {
		p.dispatchAsync(ctx, b)
	}
	p.wg.Wait()
	p.usage.FlushAll(ctx)
}

func (p *Pipeline) observeStage(stage string, since time.Time) {
	p.met.StageDuration.WithLabelValues(stage).Observe(p.now().Sub(since).Seconds())
}
