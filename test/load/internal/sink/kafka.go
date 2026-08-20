package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
	"dabet/test/load/internal/gen"
)

// KafkaConfig configures the direct-to-Kafka sink.
type KafkaConfig struct {
	Brokers []string `json:"brokers"`
	Topic   string   `json:"topic"`

	// Faithful mirrors the §4.2 producer contract: acks=all,
	// idempotence on, zstd. That is what production does, so it is the
	// default — but it caps in-flight requests per partition, so the
	// self-benchmark can turn it off to find the generator's true
	// ceiling and report both numbers.
	Faithful bool `json:"faithful"`

	// MaxBuffered bounds the async produce queue. When it fills,
	// Produce blocks — which is real backpressure and shows up as send
	// lag rather than being hidden.
	MaxBuffered int `json:"max_buffered"`

	// LingerMicros lets franz-go accumulate a batch before sending.
	// Zero means "send as soon as a batch can be formed".
	LingerMicros int `json:"linger_micros"`
}

// DefaultKafkaConfig is the production-faithful setting.
func DefaultKafkaConfig(brokers []string) KafkaConfig {
	return KafkaConfig{
		Brokers:      brokers,
		Topic:        contracts.TopicMessages,
		Faithful:     true,
		MaxBuffered:  200_000,
		LingerMicros: 1000,
	}
}

// Kafka produces messages.v1 records directly, bypassing
// provider-adapter so that what is being measured is moderation
// throughput and not one HTTP request per message.
type Kafka struct {
	Counters
	cl    *kgo.Client
	topic string
}

// NewKafka connects a producer.
func NewKafka(cfg KafkaConfig) (*Kafka, error) {
	if cfg.Topic == "" {
		cfg.Topic = contracts.TopicMessages
	}
	if cfg.MaxBuffered <= 0 {
		cfg.MaxBuffered = 200_000
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.MaxBufferedRecords(cfg.MaxBuffered),
		kgo.ProducerLinger(microseconds(cfg.LingerMicros)),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}
	if cfg.Faithful {
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	} else {
		opts = append(opts,
			kgo.RequiredAcks(kgo.LeaderAck()),
			kgo.DisableIdempotentWrite(),
		)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("load producer: %w", err)
	}
	return &Kafka{cl: cl, topic: cfg.Topic}, nil
}

// Send enqueues one record. The partition key comes from
// pkg/contracts.MessagesKey via the generator, so records land exactly
// where production would put them — which is what makes the hot-spot
// scenario's partition imbalance real rather than an artefact.
func (k *Kafka) Send(ctx context.Context, rec gen.Record) error {
	val, err := json.Marshal(rec.Msg)
	if err != nil {
		k.failed.Add(1)
		return err
	}
	k.accepted.Add(1)
	k.bytes.Add(int64(len(val) + len(rec.Key)))
	k.cl.Produce(ctx, &kgo.Record{Topic: k.topic, Key: rec.Key, Value: val},
		func(_ *kgo.Record, err error) {
			if err != nil {
				k.failed.Add(1)
				return
			}
			k.acked.Add(1)
		})
	return nil
}

// Flush waits for every buffered record.
func (k *Kafka) Flush(ctx context.Context) error { return k.cl.Flush(ctx) }

// Close closes the client.
func (k *Kafka) Close() { k.cl.Close() }

func microseconds(n int) time.Duration { return time.Duration(n) * time.Microsecond }
