// Package metrics registers the Area B metrics of docs §6.9 against the
// shared service registry. The standard §4.5 set (http_*, dependency_up,
// fail_open_total) lives in dabet/pkg/obs.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics is the Area B metric set.
type Metrics struct {
	// ResolveTotal counts resolutions by result: content, platform,
	// creator, or none.
	ResolveTotal *prometheus.CounterVec
	// CacheHits counts cache lookups by layer (memcached) and hit
	// ("true"/"false"). The "local" layer belongs to moderation-service.
	CacheHits *prometheus.CounterVec
	// ResolveDuration times resolution by the layer that answered.
	ResolveDuration *prometheus.HistogramVec
	// WritesTotal counts CRUD writes by operation and scope.
	WritesTotal *prometheus.CounterVec
}

// New registers and returns the Area B metrics.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ResolveTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "policy_resolve_total",
			Help: "Policy resolutions by winning scope, or none.",
		}, []string{"result"}),
		CacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "policy_cache_hits_total",
			Help: "Policy cache lookups by layer and hit.",
		}, []string{"layer", "hit"}),
		ResolveDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "policy_resolve_duration_seconds",
			Help:    "Policy resolution latency by answering layer.",
			Buckets: prometheus.DefBuckets,
		}, []string{"layer"}),
		WritesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "policy_writes_total",
			Help: "Policy CRUD writes by operation and scope.",
		}, []string{"operation", "scope"}),
	}
	reg.MustRegister(m.ResolveTotal, m.CacheHits, m.ResolveDuration, m.WritesTotal)
	return m
}
