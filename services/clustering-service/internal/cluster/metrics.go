package cluster

import "github.com/prometheus/client_golang/prometheus"

// Assignment results for clustering_assignments_total (docs §8.9).
const (
	ResultTopic      = "topic"
	ResultTheme      = "theme"
	ResultUnassigned = "unassigned"
)

// Metrics is the Area D metric set for live classification (docs §8.9).
// The standard per-service metrics (dependency_up, fail_open_total, ...)
// come from pkg/obs and are registered by the service runner.
//
// Cardinality rule (§4.5, P4): never labelled with creator_id, content_id,
// topic ids, or anything derived from message text.
type Metrics struct {
	AssignmentsTotal *prometheus.CounterVec // result
}

// NewMetrics constructs and registers the clustering metric set.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		AssignmentsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "clustering_assignments_total",
			Help: "Live centroid assignments, by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(m.AssignmentsTotal)
	return m
}
