// Package kafkax wraps franz-go with the producer and consumer settings
// mandated by docs §4.2: zstd compression, acks=all, idempotent producer,
// and at-least-once consumption.
//
// # Delivery guarantee
//
// At-least-once, unchanged from the first version of this package: an
// offset is committed only after the handler for that record has returned
// nil. A handler error never advances the offset past the record that
// failed, so that record is redelivered. Effects must therefore be
// idempotent (P3, docs §7.8); Dabet's are, via the seen:{message_id} guard
// and deterministic usage idempotency keys.
//
// # Ordering guarantee
//
// Each assigned partition is processed by exactly one goroutine, one
// record at a time, in the order the broker returned them. Two records
// from the same partition are never in flight at once, and a partition is
// never processed by this member while it is being revoked. Records from
// different partitions are processed concurrently — that is the point.
//
// This is precisely the property docs §7.3 depends on: because
// messages.v1 is keyed by hash(author_id, content_id), all state for one
// (sender, content) pair lives on one partition and is therefore mutated
// by one goroutine of one consumer, in order, with no distributed locking
// anywhere in the hot path. Concurrency here is across partitions only and
// cannot break it.
//
// # Commit granularity
//
// Offsets are committed per partition as that partition makes progress:
// on a configurable interval (KAFKA_COMMIT_INTERVAL, default 1s) and after
// a configurable number of processed records (KAFKA_COMMIT_RECORDS,
// default 1000). A whole polled fetch is no longer the unit, so a crash
// re-delivers only the uncommitted tail rather than the whole fetch, and
// kafka_consumer_lag_messages moves smoothly instead of in fetch-sized
// jumps. What has never changed is the invariant underneath: a commit can
// only ever include records whose handler returned nil.
//
// # Lag
//
// The consumer samples high watermarks on an interval (KAFKA_LAG_INTERVAL,
// default 15s) and publishes kafka_consumer_lag_messages per topic,
// partition and group — §4.5's mandated metric and §4.7's primary overload
// signal. Sampling is entirely off the per-message path and, per P2, a
// broker failure while sampling is logged and counted, never propagated.
//
// # Transport security
//
// Producers and consumers can speak TLS and SASL (SCRAM-SHA-512,
// SCRAM-SHA-256, PLAIN, AWS_MSK_IAM) to a managed broker — §3's
// three-broker MSK row. It is entirely opt-in through the KAFKA_TLS_* and
// KAFKA_SASL_* variables in security.go; with none of them set the clients
// built here are byte-identical to the plaintext ones, so the Compose
// profile is untouched and the topology difference really is configuration
// only.
//
// # Configuration
//
// Every number above is an environment-overridable default in the §4.4
// style; see options.go for the full list. Explicit Options passed to
// NewConsumer take precedence over the environment.
package kafkax

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is a thin synchronous producer. franz-go enables idempotence by
// default and acks=all is required for it; zstd batch compression is set
// explicitly.
type Producer struct {
	cl *kgo.Client
}

// NewProducer connects a producer to brokers.
//
// Transport security comes from the environment (see security.go). With
// none of the KAFKA_TLS_*/KAFKA_SASL_* variables set — the Compose profile
// — no extra options are added and the client is byte-identical to the
// plaintext one this function has always built.
func NewProducer(brokers []string) (*Producer, error) {
	sec, err := DefaultSecurityConfig()
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return NewProducerWithSecurity(brokers, sec)
}

// NewProducerWithSecurity is NewProducer with an explicit SecurityConfig,
// bypassing the environment. Used by tests and by callers that resolve
// their credentials themselves.
func NewProducerWithSecurity(brokers []string, sec SecurityConfig) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	}
	secOpts, err := sec.Options()
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	opts = append(opts, secOpts...)

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Produce writes one record and waits for the broker ack. W3C trace
// context is injected into the record headers (see trace.go), so a
// consumer of this record continues the producer's trace; with tracing
// off no headers are added at all.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	ctx, span := StartProduceSpan(ctx, rec)
	defer span.End()
	err := p.cl.ProduceSync(ctx, rec).FirstErr()
	recordError(span, err)
	return err
}

// Close flushes and closes the client.
func (p *Producer) Close() { p.cl.Close() }
