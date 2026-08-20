package mod

import (
	"github.com/prometheus/client_golang/prometheus"

	"dabet/pkg/obs"
)

// Metrics is the Area C metric set (§7.11) plus the shared fail-open /
// dependency / kafka vectors from pkg/obs. Cardinality rule (§4.5): no
// message_id, author_id, content_id, creator_id, or text ever appears in a
// label.
type Metrics struct {
	MessagesTotal  *prometheus.CounterVec // outcome: clean|flagged|skipped
	DetectorHits   *prometheus.CounterVec // detector, action
	E2ELatency     prometheus.Histogram   // flagged_at - ingested_at, flagged only (§4.6)
	StageDuration  *prometheus.HistogramVec
	SamplerSkipped prometheus.Counter
	LLMBatchSize   prometheus.Histogram
	LLMRequests    *prometheus.CounterVec // outcome
	LLMLatency     prometheus.Histogram

	// A18 cost visibility, beyond §7.11's table. Both are closed, tiny
	// label sets, so the §4.5 cardinality rule is untouched.
	LLMBatchTrigger *prometheus.CounterVec // trigger: size|linger|shutdown
	LLMPromptChars  *prometheus.CounterVec // part: rubric|messages

	// Shared (§4.5), registered by pkg/obs against the same registry.
	FailOpen      *prometheus.CounterVec // component, reason
	DependencyUp  *prometheus.GaugeVec
	KafkaConsumed *prometheus.CounterVec // topic, group, outcome
}

// NewMetrics registers the Area C metrics on reg and borrows the shared
// vectors from shared.
func NewMetrics(reg prometheus.Registerer, shared *obs.Metrics) *Metrics {
	m := &Metrics{
		MessagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moderation_messages_total",
			Help: "Messages consumed from messages.v1 by outcome.",
		}, []string{"outcome"}),
		DetectorHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "moderation_detector_hits_total",
			Help: "Cascade detector hits.",
		}, []string{"detector", "action"}),
		E2ELatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "moderation_e2e_latency_seconds",
			Help:    "flagged_at - ingested_at for flagged messages (the SLI, §4.6).",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
		}),
		StageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "moderation_stage_duration_seconds",
			Help:    "Wall time spent per cascade stage.",
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
		}, []string{"stage"}),
		SamplerSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sampler_skipped_total",
			Help: "Messages that skipped the LLM stage for lack of a sampler token.",
		}),
		LLMBatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "llm_batch_size",
			Help:    "Messages per LLM batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 24, 32, 48, 64},
		}),
		LLMRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_requests_total",
			Help: "LLM batch requests by outcome.",
		}, []string{"outcome"}),
		LLMLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "llm_latency_seconds",
			Help:    "LLM batch request latency, measured from dispatch (§7.9's timeout excludes the linger wait).",
			Buckets: prometheus.DefBuckets,
		}),
		LLMBatchTrigger: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_batch_trigger_total",
			Help: "LLM batches by which A18 trigger released them. A size:linger ratio near zero means the 32-message trigger never fires and the rubric is being re-sent per handful of messages.",
		}, []string{"trigger"}),
		LLMPromptChars: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "llm_prompt_chars_total",
			Help: "Prompt characters sent to the LLM by part (roughly 4 chars per token). part=\"rubric\" is charged once per batch, part=\"messages\" once per message, so their ratio is what batching is failing to amortise.",
		}, []string{"part"}),
		FailOpen:      shared.FailOpenTotal,
		DependencyUp:  shared.DependencyUp,
		KafkaConsumed: shared.KafkaConsumedTotal,
	}
	reg.MustRegister(
		m.MessagesTotal, m.DetectorHits, m.E2ELatency, m.StageDuration,
		m.SamplerSkipped, m.LLMBatchSize, m.LLMRequests, m.LLMLatency,
		m.LLMBatchTrigger, m.LLMPromptChars,
	)
	return m
}
