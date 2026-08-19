package job

import "github.com/prometheus/client_golang/prometheus"

// Metrics are the §8.9 clusters-job metrics.
type Metrics struct {
	Runs     *prometheus.CounterVec   // trigger, outcome
	Duration *prometheus.HistogramVec // trigger
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
	}
	reg.MustRegister(m.Runs, m.Duration)
	return m
}
