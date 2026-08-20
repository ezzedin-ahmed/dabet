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
	// ConnectionRefreshTotal is connection_refresh_total{platform,outcome}
	// (§5.9). It lives here rather than in user-service because §5.6 makes
	// the adapter the component that performs refreshes.
	ConnectionRefreshTotal *prometheus.CounterVec

	// The A13 sharding set (§7.2). All five are unlabelled on purpose:
	// the natural label would be connection_id or instance_id, and §4.5
	// forbids the first outright and the second is unbounded across
	// deploys. Everything these answer — who owns how much, is the fleet
	// balanced — is a per-target series already, because Prometheus adds
	// the instance label itself.

	// ShardConnectionsOwned is adapter_shard_connections_owned: the
	// connections this instance's ring segment claims and it is actually
	// watching. Summed across targets it should equal the fleet's active
	// connection count; a shortfall means capacity refusals.
	ShardConnectionsOwned prometheus.Gauge
	// ShardConnectionsRefused is adapter_shard_connections_refused:
	// connections in this instance's segment that the per-instance cap
	// turned away. Nobody else watches them. Non-zero is a capacity alarm
	// — the fix is more instances.
	ShardConnectionsRefused prometheus.Gauge
	// ShardRefusedTotal is adapter_shard_refused_total: connections newly
	// refused at the cap, counted once per connection per refusal so the
	// rate is meaningful rather than a re-count of a standing condition.
	ShardRefusedTotal prometheus.Counter
	// ShardRebalancesTotal is adapter_shard_rebalances_total: membership
	// views applied. Its rate is the deploy/flap signal.
	ShardRebalancesTotal prometheus.Counter
	// ShardMembers is adapter_shard_members: instances in this instance's
	// membership view. Targets disagreeing on this value means a split
	// view, which is exactly when double-ownership happens.
	ShardMembers prometheus.Gauge
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
		ConnectionRefreshTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "connection_refresh_total",
			Help: "Lazy token refresh attempts by outcome (§5.6).",
		}, []string{"platform", "outcome"}),
		ShardConnectionsOwned: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "adapter_shard_connections_owned",
			Help: "Connections in this instance's ring segment that it is watching (§7.2 A13).",
		}),
		ShardConnectionsRefused: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "adapter_shard_connections_refused",
			Help: "Connections in this instance's ring segment left unwatched because the per-instance cap is full. Non-zero means the fleet is under-provisioned.",
		}),
		ShardRefusedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "adapter_shard_refused_total",
			Help: "Connections newly refused at the per-instance cap.",
		}),
		ShardRebalancesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "adapter_shard_rebalances_total",
			Help: "Membership changes applied to the connection assignment.",
		}),
		ShardMembers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "adapter_shard_members",
			Help: "Adapter instances in this instance's membership view.",
		}),
	}
	reg.MustRegister(
		m.ConnectionsActive, m.IngestTotal, m.DeletionsTotal, m.DeletionLatency, m.ConnectionRefreshTotal,
		m.ShardConnectionsOwned, m.ShardConnectionsRefused, m.ShardRefusedTotal,
		m.ShardRebalancesTotal, m.ShardMembers,
	)
	return m
}
