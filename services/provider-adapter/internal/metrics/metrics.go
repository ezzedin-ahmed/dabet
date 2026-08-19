// Package metrics registers the adapter-specific metrics of docs §7.11.
// The shared HTTP/Kafka/fail-open metrics come from dabet/pkg/obs on the
// same registry.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics is the Area C adapter metric set (§7.11).
type Metrics struct {
	// ConnectionsActive is adapter_connections_active{platform}: watch
	// loops currently running.
	ConnectionsActive *prometheus.GaugeVec
	// IngestTotal is adapter_ingest_total{platform}: messages produced to
	// messages.v1.
	IngestTotal *prometheus.CounterVec
	// DeletionsTotal is deletions_total{platform,outcome}: outcomes per the
	// §7.2 response table (ok, not_found, gone, auth_failed, dropped, ...).
	DeletionsTotal *prometheus.CounterVec
	// DeletionLatency is deletion_latency_seconds{platform}: time from
	// picking a deletions.v1 record up to its terminal outcome, retries
	// included.
	DeletionLatency *prometheus.HistogramVec
}

// New constructs and registers the adapter metric set.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ConnectionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "adapter_connections_active",
			Help: "Connections with a running watch loop.",
		}, []string{"platform"}),
		IngestTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "adapter_ingest_total",
			Help: "Messages ingested and produced to messages.v1.",
		}, []string{"platform"}),
		DeletionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "deletions_total",
			Help: "Deletion attempts by terminal outcome.",
		}, []string{"platform", "outcome"}),
		DeletionLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "deletion_latency_seconds",
			Help:    "Latency from deletion record receipt to terminal outcome.",
			Buckets: prometheus.DefBuckets,
		}, []string{"platform"}),
	}
	reg.MustRegister(m.ConnectionsActive, m.IngestTotal, m.DeletionsTotal, m.DeletionLatency)
	return m
}
