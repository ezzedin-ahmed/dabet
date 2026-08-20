package kafkax

import (
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/config"
)

// Canonical environment variable names for the consumer's tunables. Names
// are prefixed by concern, not by service (docs §4.4), so every service
// reads the same strings. Every documented number below is a default, not
// a constant in code.
const (
	// EnvConsumerPartitionConcurrency caps how many records the instance
	// handles at once across all of its partitions. Ordering within a
	// partition is unaffected by this number: it is a ceiling on parallel
	// handler invocations, never a reordering. 1 restores the old
	// serial-per-instance behaviour; 0 means unlimited.
	EnvConsumerPartitionConcurrency = "KAFKA_CONSUMER_PARTITION_CONCURRENCY"
	// EnvConsumerKeyConcurrency is how many records of one partition may be
	// in flight at once, fanned out by record key: a record is routed to
	// worker stableHash(key) % N, so one key is always handled by one
	// worker, in offset order, while unrelated keys proceed in parallel.
	// 1 — the default — is the historical one-goroutine-per-partition
	// consumer and changes nothing at all.
	//
	// This is the throughput knob. Ordering is preserved per record key,
	// which for messages.v1 is exactly the (sender, content) pair §7.3
	// names; see keyed.go for why that keeps the guarantee verbatim.
	EnvConsumerKeyConcurrency = "KAFKA_CONSUMER_KEY_CONCURRENCY"
	// EnvConsumerKeyQueueDepth is how many records may queue in front of
	// one per-key worker. It also sets the partition's in-flight ceiling
	// (KeyConcurrency × KeyQueueDepth records), which is what stops a slow
	// key growing the uncommitted window without limit.
	EnvConsumerKeyQueueDepth = "KAFKA_CONSUMER_KEY_QUEUE_DEPTH"
	// EnvConsumerQueueDepth is how many polled batches may sit queued in
	// front of one partition worker before the poll loop waits for it.
	// Bounds memory; a partition can never queue more than this.
	EnvConsumerQueueDepth = "KAFKA_CONSUMER_QUEUE_DEPTH"
	// EnvConsumerMaxPollRecords caps records per poll, which bounds how
	// long a rebalance can be blocked behind dispatch.
	EnvConsumerMaxPollRecords = "KAFKA_CONSUMER_MAX_POLL_RECORDS"
	// EnvConsumerDrainTimeout bounds how long a revoke waits for in-flight
	// handlers on the revoked partitions before giving up on them.
	EnvConsumerDrainTimeout = "KAFKA_CONSUMER_DRAIN_TIMEOUT"
	// EnvCommitInterval is how often processed offsets are committed.
	EnvCommitInterval = "KAFKA_COMMIT_INTERVAL"
	// EnvCommitRecords is how many processed records trigger a commit
	// ahead of the interval. 0 disables the record trigger.
	EnvCommitRecords = "KAFKA_COMMIT_RECORDS"
	// EnvLagInterval is how often kafka_consumer_lag_messages is sampled
	// from the broker. 0 disables lag sampling entirely.
	EnvLagInterval = "KAFKA_LAG_INTERVAL"
	// EnvLagTimeout bounds one lag sample's broker call.
	EnvLagTimeout = "KAFKA_LAG_TIMEOUT"
)

// Documented defaults (docs §4.4: the number here is the default, not a
// constant). They are chosen to be safe for every Dabet consumer as it
// stands today — all four handlers are goroutine-safe, and per-partition
// ordering, which is what §7.3 actually depends on, is preserved
// regardless of these values.
const (
	DefaultPartitionConcurrency = 64
	// DefaultKeyConcurrency is 1 on purpose: per-key concurrency is opt-in,
	// so an unconfigured deployment behaves exactly as it did before this
	// feature existed. Set KAFKA_CONSUMER_KEY_CONCURRENCY to turn it on.
	DefaultKeyConcurrency = 1
	DefaultKeyQueueDepth  = 64
	DefaultQueueDepth     = 2
	DefaultMaxPollRecords = 1000
	DefaultDrainTimeout   = 30 * time.Second
	DefaultCommitInterval = time.Second
	DefaultCommitRecords  = 1000
	DefaultLagInterval    = 15 * time.Second
	DefaultLagTimeout     = 5 * time.Second
)

// MinCommitInterval is the shortest commit interval franz-go accepts;
// anything below it is clamped. Finer granularity than this comes from
// CommitRecords, which costs no extra timer.
const MinCommitInterval = 100 * time.Millisecond

// MaxKeyConcurrency caps the per-partition fan-out. A member owning many
// partitions multiplies this number by its assignment, so a fat-fingered
// value is clamped rather than turned into a few hundred thousand
// goroutines and their queues.
const MaxKeyConcurrency = 1024

// ConsumerConfig carries the consumer's tunables. Build one with
// DefaultConsumerConfig (which reads the environment) and adjust it with
// Options passed to NewConsumer.
type ConsumerConfig struct {
	// PartitionConcurrency caps concurrent handler invocations across all
	// partitions this instance owns. 0 is unlimited, 1 is serial.
	PartitionConcurrency int
	// KeyConcurrency is how many per-key workers each partition fans out
	// to. 1 (the default) is the historical serial-per-partition
	// consumer. Above 1, records are routed by stableHash(record.Key), so
	// ordering is preserved per key rather than per partition and offsets
	// are committed at the contiguous completed prefix.
	//
	// It composes with PartitionConcurrency rather than replacing it:
	// KeyConcurrency is the per-partition fan-out width, PartitionConcurrency
	// is the instance-wide ceiling on concurrent handler invocations, and
	// the effective parallelism is the smaller of the two. Setting
	// PartitionConcurrency to 1 forces KeyConcurrency back to 1, since a
	// strictly serial instance cannot use a fan-out and would pay for the
	// out-of-order bookkeeping for nothing.
	KeyConcurrency int
	// KeyQueueDepth bounds one per-key worker's queue, and with
	// KeyConcurrency bounds a partition's in-flight (uncommitted) window at
	// KeyConcurrency × KeyQueueDepth records. Minimum 1.
	KeyQueueDepth int
	// QueueDepth is the per-partition batch queue depth (minimum 1).
	QueueDepth int
	// MaxPollRecords caps records returned by one poll (minimum 1).
	MaxPollRecords int
	// DrainTimeout bounds the wait for in-flight handlers on revoke.
	DrainTimeout time.Duration
	// CommitInterval is how often processed offsets are committed.
	CommitInterval time.Duration
	// CommitRecords triggers a commit once this many records have been
	// processed since the last one. 0 disables the trigger.
	CommitRecords int
	// LagInterval is the lag sampling period. 0 disables sampling.
	LagInterval time.Duration
	// LagTimeout bounds one lag sample.
	LagTimeout time.Duration
	// Security is the broker transport security (TLS/SASL) of §3's
	// managed-cloud row. Its zero value is plaintext with no SASL, which
	// is what the Compose profile uses and what DefaultConsumerConfig
	// returns when no KAFKA_TLS_*/KAFKA_SASL_* variable is set.
	Security SecurityConfig
	// Logger receives the consumer's own warnings. Never nil after
	// DefaultConsumerConfig; per P4 nothing here logs message text or ids.
	Logger *slog.Logger
	// LagGauge receives lag samples. Defaults to the process's
	// kafka_consumer_lag_messages gauge (see obs.NewMetrics); nil disables
	// the gauge without disabling sampling bookkeeping.
	LagGauge LagGauge
	// KgoOpts are appended to the franz-go client options, after the ones
	// this package requires. Escape hatch; unused by Dabet services.
	KgoOpts []kgo.Opt

	// lagGaugeSet records that a caller chose a gauge (possibly nil, to
	// mean "none"), so the process default is not substituted later.
	lagGaugeSet bool
}

// Option adjusts a ConsumerConfig. Options are applied after the
// environment, so an explicit option wins over an env var.
type Option func(*ConsumerConfig)

// DefaultConsumerConfig returns the documented defaults with any
// KAFKA_* environment overrides applied. An unparseable value is an
// error rather than a silent fallback.
func DefaultConsumerConfig() (ConsumerConfig, error) {
	cfg := ConsumerConfig{
		PartitionConcurrency: DefaultPartitionConcurrency,
		KeyConcurrency:       DefaultKeyConcurrency,
		KeyQueueDepth:        DefaultKeyQueueDepth,
		QueueDepth:           DefaultQueueDepth,
		MaxPollRecords:       DefaultMaxPollRecords,
		DrainTimeout:         DefaultDrainTimeout,
		CommitInterval:       DefaultCommitInterval,
		CommitRecords:        DefaultCommitRecords,
		LagInterval:          DefaultLagInterval,
		LagTimeout:           DefaultLagTimeout,
		Logger:               slog.Default(),
	}
	var err error
	if cfg.PartitionConcurrency, err = config.GetInt(EnvConsumerPartitionConcurrency, cfg.PartitionConcurrency); err != nil {
		return cfg, err
	}
	if cfg.KeyConcurrency, err = config.GetInt(EnvConsumerKeyConcurrency, cfg.KeyConcurrency); err != nil {
		return cfg, err
	}
	if cfg.KeyQueueDepth, err = config.GetInt(EnvConsumerKeyQueueDepth, cfg.KeyQueueDepth); err != nil {
		return cfg, err
	}
	if cfg.QueueDepth, err = config.GetInt(EnvConsumerQueueDepth, cfg.QueueDepth); err != nil {
		return cfg, err
	}
	if cfg.MaxPollRecords, err = config.GetInt(EnvConsumerMaxPollRecords, cfg.MaxPollRecords); err != nil {
		return cfg, err
	}
	if cfg.CommitRecords, err = config.GetInt(EnvCommitRecords, cfg.CommitRecords); err != nil {
		return cfg, err
	}
	if cfg.DrainTimeout, err = config.GetDuration(EnvConsumerDrainTimeout, cfg.DrainTimeout); err != nil {
		return cfg, err
	}
	if cfg.CommitInterval, err = config.GetDuration(EnvCommitInterval, cfg.CommitInterval); err != nil {
		return cfg, err
	}
	if cfg.LagInterval, err = config.GetDuration(EnvLagInterval, cfg.LagInterval); err != nil {
		return cfg, err
	}
	if cfg.LagTimeout, err = config.GetDuration(EnvLagTimeout, cfg.LagTimeout); err != nil {
		return cfg, err
	}
	if cfg.Security, err = DefaultSecurityConfig(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// normalise clamps values that would otherwise deadlock or panic.
func (c *ConsumerConfig) normalise() {
	if c.PartitionConcurrency < 0 {
		c.PartitionConcurrency = 0
	}
	if c.KeyConcurrency < 1 {
		c.KeyConcurrency = 1
	}
	if c.KeyConcurrency > MaxKeyConcurrency {
		c.KeyConcurrency = MaxKeyConcurrency
	}
	if c.PartitionConcurrency == 1 {
		// A fan-out behind a ceiling of one handler is strictly serial
		// anyway: it would buy no throughput and would only trade
		// per-record commits for low-water-mark commits. The explicit
		// "make this instance serial" knob wins.
		c.KeyConcurrency = 1
	}
	if c.KeyQueueDepth < 1 {
		c.KeyQueueDepth = 1
	}
	if c.QueueDepth < 1 {
		c.QueueDepth = 1
	}
	if c.MaxPollRecords < 1 {
		c.MaxPollRecords = 1
	}
	if c.CommitInterval <= 0 {
		c.CommitInterval = DefaultCommitInterval
	}
	if c.CommitInterval < MinCommitInterval {
		// franz-go rejects a shorter autocommit interval outright, and a
		// commit RPC per few milliseconds would cost more than the
		// granularity buys. Use CommitRecords for finer granularity.
		c.CommitInterval = MinCommitInterval
	}
	if c.CommitRecords < 0 {
		c.CommitRecords = 0
	}
	if c.DrainTimeout <= 0 {
		c.DrainTimeout = DefaultDrainTimeout
	}
	if c.LagInterval < 0 {
		c.LagInterval = 0
	}
	if c.LagTimeout <= 0 {
		c.LagTimeout = DefaultLagTimeout
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// WithPartitionConcurrency caps concurrent handler invocations. 1 restores
// the pre-concurrency serial behaviour; 0 is unlimited. It never affects
// ordering within a partition.
func WithPartitionConcurrency(n int) Option {
	return func(c *ConsumerConfig) { c.PartitionConcurrency = n }
}

// WithKeyConcurrency sets the per-partition per-key fan-out. 1 (the
// default) is the historical serial-per-partition consumer; above 1,
// records are routed by record key so unrelated keys run in parallel while
// each key stays strictly in offset order. Clamped to MaxKeyConcurrency,
// and forced back to 1 if PartitionConcurrency is 1.
func WithKeyConcurrency(n int) Option { return func(c *ConsumerConfig) { c.KeyConcurrency = n } }

// WithKeyQueueDepth bounds one per-key worker's queue, and with it the
// partition's in-flight window.
func WithKeyQueueDepth(n int) Option { return func(c *ConsumerConfig) { c.KeyQueueDepth = n } }

// WithQueueDepth sets how many polled batches may queue per partition.
func WithQueueDepth(n int) Option { return func(c *ConsumerConfig) { c.QueueDepth = n } }

// WithMaxPollRecords caps records returned by one poll.
func WithMaxPollRecords(n int) Option { return func(c *ConsumerConfig) { c.MaxPollRecords = n } }

// WithDrainTimeout bounds the wait for in-flight handlers on revoke.
func WithDrainTimeout(d time.Duration) Option { return func(c *ConsumerConfig) { c.DrainTimeout = d } }

// WithCommitInterval sets how often processed offsets are committed.
func WithCommitInterval(d time.Duration) Option {
	return func(c *ConsumerConfig) { c.CommitInterval = d }
}

// WithCommitRecords sets the record count that triggers a commit ahead of
// the interval. 0 disables the trigger.
func WithCommitRecords(n int) Option { return func(c *ConsumerConfig) { c.CommitRecords = n } }

// WithLagSampling sets the lag sampling interval. 0 disables sampling.
func WithLagSampling(d time.Duration) Option { return func(c *ConsumerConfig) { c.LagInterval = d } }

// WithLagGauge routes lag samples to g instead of the process default.
// A nil g leaves the gauge unpopulated.
func WithLagGauge(g LagGauge) Option {
	return func(c *ConsumerConfig) {
		c.LagGauge = g
		c.lagGaugeSet = true
	}
}

// WithPrometheusLagGauge routes lag samples to a Prometheus gauge vector
// labelled (topic, partition, group) — i.e. obs.Metrics.KafkaConsumerLag.
func WithPrometheusLagGauge(vec *prometheus.GaugeVec) Option {
	return WithLagGauge(PrometheusLagGauge(vec))
}

// WithSecurity sets the broker transport security explicitly, overriding
// whatever the KAFKA_TLS_*/KAFKA_SASL_* variables said.
func WithSecurity(s SecurityConfig) Option { return func(c *ConsumerConfig) { c.Security = s } }

// WithLogger sets the logger for the consumer's own warnings.
func WithLogger(l *slog.Logger) Option { return func(c *ConsumerConfig) { c.Logger = l } }

// WithKgoOptions appends franz-go options after the ones this package
// requires, so a caller can override client-level settings.
func WithKgoOptions(opts ...kgo.Opt) Option {
	return func(c *ConsumerConfig) { c.KgoOpts = append(c.KgoOpts, opts...) }
}
