package ingest

import "github.com/prometheus/client_golang/prometheus"

// Drop reasons for insights_messages_dropped_total (docs §8.9).
const (
	DropReasonFlagged = "flagged"
	DropReasonSampled = "sampled"
	DropReasonRestart = "restart"
)

// Metrics is the Area D metric set for the ingestion pipeline (docs §8.9).
// The standard per-service metrics (kafka_*, fail_open_total, ...) come from
// pkg/obs and are registered by the service runner; only the area-specific
// series live here.
//
// Cardinality rule (§4.5, P4): no metric here is ever labelled with
// message_id, author_id, content_id, creator_id, or text.
type Metrics struct {
	Buffered           prometheus.Gauge
	DroppedTotal       *prometheus.CounterVec // reason
	ContaminationTotal prometheus.Counter
	EmbedRequestsTotal *prometheus.CounterVec // outcome
	EmbedLatency       prometheus.Histogram
	S3BytesWritten     prometheus.Counter
}

// NewMetrics constructs and registers the Area D metric set.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Buffered: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "insights_messages_buffered",
			Help: "Messages currently held in the exclusion buffer.",
		}),
		DroppedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "insights_messages_dropped_total",
			Help: "Messages dropped before embedding, by reason.",
		}, []string{"reason"}),
		ContaminationTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "insights_contamination_estimate_total",
			Help: "Flags that arrived after their message had already left the exclusion buffer.",
		}),
		EmbedRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "embedding_requests_total",
			Help: "Embedding service requests, by outcome.",
		}, []string{"outcome"}),
		EmbedLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "embedding_latency_seconds",
			Help:    "Embedding request latency.",
			Buckets: prometheus.DefBuckets,
		}),
		S3BytesWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "s3_embedding_bytes_written_total",
			Help: "Parquet bytes written to the embeddings bucket.",
		}),
	}
	reg.MustRegister(
		m.Buffered,
		m.DroppedTotal,
		m.ContaminationTotal,
		m.EmbedRequestsTotal,
		m.EmbedLatency,
		m.S3BytesWritten,
	)
	return m
}
