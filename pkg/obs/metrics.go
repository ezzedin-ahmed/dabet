package obs

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// defaultKafkaLag holds the kafka_consumer_lag_messages gauge of the most
// recently constructed Metrics. §4.5 mandates the metric on every service
// and §4.7 makes it the primary overload signal, but the value can only be
// known by whatever is consuming — so the consumer needs a way to find the
// gauge without every main() remembering to hand it over. A service builds
// exactly one Metrics (pkg/service.New), so "most recent" is "the
// process's". Tests that build several registries get the last one; they
// can pass a gauge explicitly instead.
var defaultKafkaLag atomic.Pointer[prometheus.GaugeVec]

// DefaultKafkaConsumerLag returns the process's kafka_consumer_lag_messages
// gauge, or nil if no Metrics has been constructed. Labels are
// (topic, partition, group) — never an id, per the cardinality rule above.
func DefaultKafkaConsumerLag() *prometheus.GaugeVec { return defaultKafkaLag.Load() }

// SetDefaultKafkaConsumerLag overrides the process default. Intended for
// tests and for services that register their own gauge.
func SetDefaultKafkaConsumerLag(vec *prometheus.GaugeVec) { defaultKafkaLag.Store(vec) }

// Metrics holds the standard metrics from docs §4.5 that every service
// exposes. Area-specific metrics are registered by the service itself
// against the same registry.
type Metrics struct {
	HTTPRequestsTotal   *prometheus.CounterVec   // route, method, status
	HTTPRequestDuration *prometheus.HistogramVec // route, method
	KafkaConsumerLag    *prometheus.GaugeVec     // topic, partition, group
	KafkaConsumedTotal  *prometheus.CounterVec   // topic, group, outcome
	DependencyUp        *prometheus.GaugeVec     // dependency
	FailOpenTotal       *prometheus.CounterVec   // component, reason
}

// NewMetrics constructs and registers the standard metric set.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		HTTPRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests served.",
		}, []string{"route", "method", "status"}),
		HTTPRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
		KafkaConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_lag_messages",
			Help: "Consumer group lag in messages.",
		}, []string{"topic", "partition", "group"}),
		KafkaConsumedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kafka_messages_consumed_total",
			Help: "Kafka messages consumed.",
		}, []string{"topic", "group", "outcome"}),
		DependencyUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dependency_up",
			Help: "Whether a dependency is reachable (1) or not (0).",
		}, []string{"dependency"}),
		FailOpenTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fail_open_total",
			Help: "Messages that went unmoderated because something was broken. Must be zero in steady state.",
		}, []string{"component", "reason"}),
	}
	reg.MustRegister(
		m.HTTPRequestsTotal,
		m.HTTPRequestDuration,
		m.KafkaConsumerLag,
		m.KafkaConsumedTotal,
		m.DependencyUp,
		m.FailOpenTotal,
	)
	defaultKafkaLag.Store(m.KafkaConsumerLag)
	return m
}
