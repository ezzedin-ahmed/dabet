package job

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the §8.9 clusters-job metrics, plus the counts-backfill set
// that extends them. Per the §4.5 cardinality rule none of these carries
// creator_id, content_id, or any identifier — only trigger, outcome, and
// the row operation.
type Metrics struct {
	Runs     *prometheus.CounterVec   // trigger, outcome
	Duration *prometheus.HistogramVec // trigger

	// Backfills counts topic_counts rewrites by outcome: ok, error, or
	// skipped (window newer than the backfill lag).
	Backfills *prometheus.CounterVec // trigger, outcome
	// BackfillRows counts rows moved by the rewrite, op = deleted|written.
	BackfillRows *prometheus.CounterVec // trigger, op
	// BackfillDuration times the delete-then-insert pair.
	BackfillDuration *prometheus.HistogramVec // trigger
}

// NewMetrics registers the metric set.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clusters_job_runs_total",
			Help: "Clustering runs by trigger and outcome.",
		}, []string{"trigger", "outcome"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clusters_job_duration_seconds",
			Help:    "Clustering run duration.",
			Buckets: []float64{0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}, []string{"trigger"}),
		Backfills: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clusters_job_counts_backfill_total",
			Help: "topic_counts backfills by trigger and outcome.",
		}, []string{"trigger", "outcome"}),
		BackfillRows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clusters_job_counts_backfill_rows_total",
			Help: "topic_counts rows deleted and written by the backfill.",
		}, []string{"trigger", "op"}),
		BackfillDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "clusters_job_counts_backfill_duration_seconds",
			Help:    "topic_counts backfill duration (delete plus insert).",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"trigger"}),
	}
	reg.MustRegister(m.Runs, m.Duration, m.Backfills, m.BackfillRows, m.BackfillDuration)
	return m
}
