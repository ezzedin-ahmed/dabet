package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/kafkax"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Kafka is the franz-go Reader. A single long-lived client serves
// metadata and offset queries; each Scan uses a short-lived direct
// consumer pinned to one partition at one offset, so concurrent HTTP
// requests never share seek state.
type Kafka struct {
	brokers []string
	topic   string
	cl      *kgo.Client

	metaTTL   time.Duration
	mu        sync.Mutex
	partsN    int32
	partsGood time.Time
}

// NewKafka connects the metadata client. metaTTL bounds how long a
// discovered partition count is trusted before re-asking the broker.
func NewKafka(brokers []string, topic string, metaTTL time.Duration) (*Kafka, error) {
	// Direct kgo clients must still carry the shared transport security, or
	// they silently stay plaintext while this service's kafkax clients
	// authenticate fine — which only shows up against a managed broker.
	sec, err := kafkax.DefaultSecurityConfig()
	if err != nil {
		return nil, fmt.Errorf("kafka reader: %w", err)
	}
	secOpts, err := sec.Options()
	if err != nil {
		return nil, fmt.Errorf("kafka reader: %w", err)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, secOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka reader: %w", err)
	}
	return &Kafka{brokers: brokers, topic: topic, cl: cl, metaTTL: metaTTL}, nil
}

// securityOptions resolves the shared KAFKA_TLS_*/KAFKA_SASL_* transport
// settings for the short-lived clients this reader creates per scan.
func (k *Kafka) securityOptions() ([]kgo.Opt, error) {
	sec, err := kafkax.DefaultSecurityConfig()
	if err != nil {
		return nil, err
	}
	return sec.Options()
}

// Close closes the metadata client.
func (k *Kafka) Close() { k.cl.Close() }

// Partitions returns the topic's partition count from broker metadata,
// cached for metaTTL.
func (k *Kafka) Partitions(ctx context.Context) (int32, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.partsN > 0 && time.Since(k.partsGood) < k.metaTTL {
		return k.partsN, nil
	}

	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(k.topic)
	req.Topics = append(req.Topics, rt)

	resp, err := k.cl.Request(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("metadata request: %w", err)
	}
	md := resp.(*kmsg.MetadataResponse)
	if len(md.Topics) != 1 {
		return 0, fmt.Errorf("metadata: expected 1 topic, got %d", len(md.Topics))
	}
	t := md.Topics[0]
	if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
		return 0, fmt.Errorf("metadata for %s: %w", k.topic, err)
	}
	n := int32(len(t.Partitions))
	if n == 0 {
		return 0, fmt.Errorf("metadata for %s: no partitions", k.topic)
	}
	k.partsN, k.partsGood = n, time.Now()
	return n, nil
}

// Offsets returns the earliest retained offset and high watermark of one
// partition via ListOffsets (timestamps -2 and -1).
func (k *Kafka) Offsets(ctx context.Context, partition int32) (earliest, high int64, err error) {
	earliest, err = k.listOffset(ctx, partition, -2)
	if err != nil {
		return 0, 0, err
	}
	high, err = k.listOffset(ctx, partition, -1)
	if err != nil {
		return 0, 0, err
	}
	return earliest, high, nil
}

func (k *Kafka) listOffset(ctx context.Context, partition int32, timestamp int64) (int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = k.topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = partition
	rp.Timestamp = timestamp
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := k.cl.Request(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("list offsets: %w", err)
	}
	lo := resp.(*kmsg.ListOffsetsResponse)
	if len(lo.Topics) != 1 || len(lo.Topics[0].Partitions) != 1 {
		return 0, fmt.Errorf("list offsets: unexpected response shape")
	}
	p := lo.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
		return 0, fmt.Errorf("list offsets %s/%d: %w", k.topic, partition, err)
	}
	return p.Offset, nil
}

// Scan reads records with offset >= from until fn returns false or the
// high watermark observed at scan start is reached.
func (k *Kafka) Scan(ctx context.Context, partition int32, from int64, fn func(Record) bool) error {
	_, high, err := k.Offsets(ctx, partition)
	if err != nil {
		return err
	}
	if from >= high {
		return nil
	}

	secOpts, err := k.securityOptions()
	if err != nil {
		return fmt.Errorf("kafka scan client: %w", err)
	}
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(k.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			k.topic: {partition: kgo.NewOffset().At(from)},
		}),
	}, secOpts...)...)
	if err != nil {
		return fmt.Errorf("kafka scan client: %w", err)
	}
	defer cl.Close()

	for {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, fe := range fetches.Errors() {
			return fmt.Errorf("kafka fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
		iter := fetches.RecordIter()
		for !iter.Done() {
			r := iter.Next()
			if r.Partition != partition || r.Offset < from {
				continue
			}
			if !fn(Record{Offset: r.Offset, Key: r.Key, Value: r.Value}) {
				return nil
			}
			if r.Offset >= high-1 {
				return nil
			}
		}
	}
}
