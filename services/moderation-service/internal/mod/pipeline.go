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
	LLMBatchSize      int           // A18 — size trigger; see LLMBatcher
	LLMLinger         time.Duration // A18 — linger trigger; see LLMBatcher

	// Redis circuit breaker (§4.7 "skip", not "try and fail"). Not a
	// number the spec assigns, so these are ours; all three are env
	// overridable per §4.4. See Breaker for the rationale.
	RedisBreakerThreshold   int           // consecutive failures that trip it
	RedisBreakerCooldown    time.Duration // open window before the first probe
	RedisBreakerMaxCooldown time.Duration // cap on the backed-off open window
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

		// Five consecutive failures is short enough that an outage costs
		// well under a second of wasted work per instance, and long enough
		// that a single dropped connection does not disable the Redis
		// stages. The 500 ms floor bounds the probe cost to one failed
		// call per half second; the 5 s cap keeps a long outage at roughly
		// one wasted call per five seconds per instance.
		RedisBreakerThreshold:   5,
		RedisBreakerCooldown:    500 * time.Millisecond,
		RedisBreakerMaxCooldown: 5 * time.Second,
	}
}

// Pipeline is the §7.3 cascade. One instance serves the whole consumer.
//
// CONCURRENCY. Process is safe to call from several goroutines at once —
// kafkax may drive one pipeline from several partition workers — and LLM
// dispatch and background flushing already run on their own goroutines.
// Every piece of shared state is either immutable after construction
// (cfg, matchers, the clock) or internally locked (batcher, memSamp,
// usage, policies, breaker); Prometheus collectors are safe by contract.
// Per-message state lives in locals only.
type Pipeline struct {
	cfg      Config
	policies PolicyGetter
	credits  CreditsChecker
	state    *RedisState
	breaker  *Breaker
	embed    Embedder
	llm      Classifier
	batcher  *LLMBatcher
	memSamp  *MemSampler
	pub      *Publisher
	usage    *UsageAggregator
	met      *Metrics
	now      func() time.Time

	mu     sync.Mutex     // guards closed; serialises wg.Add against Shutdown
	closed bool           // Shutdown has begun: no new tracked dispatches
	wg     sync.WaitGroup // in-flight LLM dispatches
}

// NewPipeline wires the cascade. state may be nil only in tests; a broken
// Redis is handled by the shared breaker, not at construction.
func NewPipeline(cfg Config, policies PolicyGetter, credits CreditsChecker, state *RedisState, embed Embedder, llm Classifier, pub *Publisher, usage *UsageAggregator, met *Metrics, now func() time.Time) *Pipeline {
	return &Pipeline{
		cfg:      cfg,
		policies: policies,
		credits:  credits,
		state:    state,
		breaker:  NewBreaker(cfg.RedisBreakerThreshold, cfg.RedisBreakerCooldown, cfg.RedisBreakerMaxCooldown),
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

// redisGate is one message's view of the shared Redis breaker.
//
// It decides ONCE per message whether Redis may be touched at all. When
// the breaker is open no call is attempted — that, and not the counting,
// is the fix for the measured collapse (see Breaker): the previous code
// marked Redis down only for the current message, so every subsequent
// message re-paid the client's failure latency on the consumer goroutine.
//
// Accounting is honest in both directions: a message that skipped or
// failed any Redis-backed stage counts exactly one
// fail_open_total{component="redis"}, no matter how many of stages
// 1/4/5/6/8 it went on to skip, and a message that never reached a
// Redis-backed stage counts none.
type redisGate struct {
	p       *Pipeline
	decided bool // the breaker has been consulted for this message
	allowed bool // calls may still be attempted
	probe   bool // this message holds the half-open probe token
	counted bool // the fail-open for this message has been counted
}

// use reports whether the Redis-backed stage about to run may issue a
// call. It is deliberately lazy: a message whose policy enables no
// Redis-backed stage never consults the breaker and never counts.
func (g *redisGate) use() bool {
	if g.p.state == nil {
		g.skipped()
		return false
	}
	if !g.decided {
		g.decided = true
		g.allowed, g.probe = g.p.breaker.Allow(g.p.now())
		if !g.allowed {
			g.p.met.DependencyUp.WithLabelValues("redis").Set(0)
		}
	}
	if !g.allowed {
		g.skipped()
		return false
	}
	return true
}

// ok reports a successful Redis call, closing the breaker when this
// message carried the probe.
func (g *redisGate) ok() {
	g.p.breaker.Succeed(g.p.now(), g.probe)
	g.probe = false
	g.p.met.DependencyUp.WithLabelValues("redis").Set(1)
}

// fail reports a failed Redis call. The remaining stages of this message
// are skipped without a further attempt.
func (g *redisGate) fail() {
	g.p.breaker.Fail(g.p.now(), g.probe)
	g.probe = false
	g.allowed = false
	g.p.met.DependencyUp.WithLabelValues("redis").Set(0)
	g.skipped()
}

// skipped counts this message's single Redis fail-open, once.
func (g *redisGate) skipped() {
	if g.counted {
		return
	}
	g.counted = true
	g.p.met.FailOpen.WithLabelValues("redis", "").Inc()
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

	// Redis availability comes from the SHARED breaker, not from this
	// message: while it is open the Redis-backed stages are skipped
	// outright, with no call attempted (§4.7 says "skip"). The gate counts
	// one fail_open_total{component="redis"} per affected message.
	rg := redisGate{p: p}

	// Stage 1 — redelivery guard (§7.4). Redis down: skip, fail open.
	if rg.use() {
		t0 := p.now()
		already, err := p.state.Seen(ctx, msg.MessageID, p.cfg.SeenTTL)
		p.observeStage("seen", t0)
		if err != nil {
			rg.fail()
		} else {
			rg.ok()
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

	// Stages 4–6 — rate limit, duplicate, semantic spam. All three read and
	// write keys of one (content, author) pair, which §4.3 gives the same
	// hash tag, so the whole group is one Lua script and one round trip
	// (cascadeLua). §7.3's ordering is unchanged and still observable in
	// the state: the script returns on the first hit and never touches the
	// structures of a later stage, so a rate-limited message still does not
	// enter the duplicate window.
	rateOn := pol.RateLimitMessages != nil && pol.RateLimitSeconds != nil
	spam := pol.GetSpam()
	spamOn := spam == policyapi.SpamMode_SPAM_MODE_IDENTICAL || spam == policyapi.SpamMode_SPAM_MODE_SEMANTIC
	semanticOn := spam == policyapi.SpamMode_SPAM_MODE_SEMANTIC

	// windowRead is set once the merged call has returned a comparison
	// window, i.e. once stages 4 and 5 have passed with Redis healthy.
	// Without it there is nothing for the semantic stage to compare
	// against and it is skipped, exactly as it was when Redis was the
	// stage's own dependency.
	var cascade CascadeResult
	windowRead := false

	if (rateOn || spamOn) && rg.use() {
		params := CascadeParams{}
		if rateOn {
			capacity := float64(pol.GetRateLimitMessages())
			window := time.Duration(pol.GetRateLimitSeconds()) * time.Second
			params.Rate = &RateParams{
				Capacity:     capacity,
				RefillPerSec: capacity / window.Seconds(),
				Now:          p.now(),
				TTL:          2 * window,
			}
		}
		// The duplicate stage runs for spam = identical AND for semantic,
		// which keeps the identical check as its cheap first line.
		if spamOn {
			params.Dup = &DupParams{Hash: HashText(norm), Depth: p.cfg.DupDepth, TTL: p.cfg.DupTTL}
		}
		if semanticOn {
			params.EmbDepth = p.cfg.EmbDepth
		}

		t0 = p.now()
		res, err := p.state.Cascade(ctx, msg.ContentID, msg.AuthorID, params)
		// One call, so one duration — attributed to each stage that took
		// part in it, which keeps every stage histogram's COUNT meaning
		// "messages that ran this stage" (what §4.6's budget line and the
		// e2e assertions read). The sum across the three now double-counts
		// the shared round trip, deliberately: §4.6 budgets the Redis
		// cascade as one line, not three.
		if rateOn {
			p.observeStage("rate_limit", t0)
		}
		if spamOn && res.Hit != CascadeRateLimited {
			p.observeStage("duplicate", t0)
		}
		if err != nil {
			rg.fail()
		} else {
			rg.ok()
			switch res.Hit {
			case CascadeRateLimited:
				p.flag(ctx, msg, contracts.DetectorRateLimit, contracts.ActionAutoDelete, pol.GetPolicyId())
				return
			case CascadeDuplicate:
				p.flag(ctx, msg, contracts.DetectorDuplicate, contracts.ActionAutoDelete, pol.GetPolicyId())
				return
			}
			cascade, windowRead = res, semanticOn
		}
	}

	// Stage 6, second half — the embedding is only paid for once the two
	// cheaper stages have passed, which is the whole point of ordering by
	// cost (§7.4: this is the only pre-LLM detector that costs a network
	// round trip). The comparison window arrived with the merged call
	// above, so what remains is embed → cosine in Go → append.
	// Embedding down: skip the stage, continue.
	if windowRead && rg.use() {
		t0 = p.now()
		vecs, err := p.embed.Embed(ctx, []string{norm})
		if err != nil || len(vecs) != 1 {
			p.met.FailOpen.WithLabelValues("embedding", "").Inc()
			p.met.DependencyUp.WithLabelValues("embedding").Set(0)
			p.observeStage("semantic", t0)
		} else {
			p.met.DependencyUp.WithLabelValues("embedding").Set(1)
			sim := maxSimilarity(vecs[0], cascade.Vectors)
			// The verdict is decided before the append, but an append
			// failure still suppresses it: a flag the window did not
			// record would make the next repeat look like a first
			// sighting, which is what the pre-merge read-then-write did.
			appendErr := p.state.EmbAppend(ctx, msg.ContentID, msg.AuthorID, vecs[0], p.cfg.EmbDepth, p.cfg.EmbTTL)
			p.observeStage("semantic", t0)
			if appendErr != nil {
				rg.fail()
			} else {
				rg.ok()
				if sim >= p.cfg.SemanticThreshold {
					p.flag(ctx, msg, contracts.DetectorSemanticSpam, contracts.ActionAutoDelete, pol.GetPolicyId())
					return
				}
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
	// bucket (see MemSampler for the documented deviation). "Down" now
	// includes "breaker open", which is the same situation reached without
	// paying for the call.
	t0 = p.now()
	sampled, decided := false, false
	if rg.use() {
		a, err := p.state.TakeToken(ctx, rediskeys.Samp(msg.ContentID),
			p.cfg.SamplerCapacity, p.cfg.SamplerPerMin/60, p.now(), p.cfg.SamplerTTL)
		if err != nil {
			rg.fail()
		} else {
			rg.ok()
			sampled, decided = a, true
		}
	}
	if !decided {
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
//
// The closed flag exists because Process may run on several partition
// workers: without it a straggling worker could call wg.Add concurrently
// with Shutdown's wg.Wait, which is documented WaitGroup misuse and is
// reported by -race. After Shutdown has begun, a late batch is dispatched
// inline instead — it still reaches the LLM and is still accounted, it
// just blocks its own caller rather than escaping the drain.
func (p *Pipeline) dispatchAsync(ctx context.Context, batch *LLMBatch) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.dispatch(ctx, batch)
		return
	}
	p.wg.Add(1)
	p.mu.Unlock()
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
	msgChars := 0
	for i, m := range batch.Messages {
		texts[i] = m.Text
		msgChars += len(m.Text)
	}
	p.met.LLMBatchSize.Observe(float64(len(texts)))
	p.met.LLMBatchTrigger.WithLabelValues(string(batch.Trigger)).Inc()
	// A18's cost model, made visible (see LLMBatcher): the rubric is sent
	// once per BATCH and the messages once each, so
	// llm_prompt_chars_total{part="rubric"} divided by the message part is
	// the share of prompt spend that batching is supposed to amortise, and
	// the rubric part divided by llm_requests_total is what one more
	// message per batch would save.
	p.met.LLMPromptChars.WithLabelValues("rubric").Add(float64(rubricChars(batch.Policy)))
	p.met.LLMPromptChars.WithLabelValues("messages").Add(float64(msgChars))

	// The model deadline starts HERE, at dispatch, not when the first
	// message of the batch was enqueued: §7.9's 1 000 ms is the LLM's own
	// share of the §4.6 budget, and charging the linger wait against it
	// would make the linger trigger self-defeating. Linger time is still
	// inside the SLI, which is where it belongs. No retry either way.
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

	// Policy vocabulary → wire vocabulary. The two names differ on
	// purpose and neither is wrong (F7): the policy document says what the
	// creator asked for, restricted_content_action = "auto" | "review"
	// (§6.4, policy.RCActionAuto), while flagged.v1 says what downstream
	// must DO, action = "auto_delete" | "review" (§4.2, frozen). The
	// mapping is auto → auto_delete, review → review; only "auto_delete"
	// also produces a deletions.v1 record. Do not unify the spellings —
	// §4.2 is a frozen contract.
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
// and flushes the remaining usage windows (§7.10). The final flush runs
// on its own WaitGroup so nothing Adds to p.wg while it is being waited
// on (see dispatchAsync).
func (p *Pipeline) Shutdown(ctx context.Context) {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()

	var last sync.WaitGroup
	for _, b := range p.batcher.FlushAll() {
		last.Add(1)
		go func() {
			defer last.Done()
			p.dispatch(ctx, b)
		}()
	}
	last.Wait()
	p.wg.Wait()
	p.usage.FlushAll(ctx)
}

func (p *Pipeline) observeStage(stage string, since time.Time) {
	p.met.StageDuration.WithLabelValues(stage).Observe(p.now().Sub(since).Seconds())
}
