// Package kafkax wraps franz-go with the producer and consumer settings
// mandated by docs §4.2: zstd compression, acks=all, idempotent producer,
// and at-least-once consumption with commits only after handler success.
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
func NewProducer(brokers []string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Produce writes one record and waits for the broker ack.
func (p *Producer) Produce(ctx context.Context, topic string, key, value []byte) error {
	return p.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
}

// Close flushes and closes the client.
func (p *Producer) Close() { p.cl.Close() }

// Handler processes one record. Returning an error stops the consumer
// without committing, so the batch is redelivered (at-least-once);
// handlers must be idempotent (P3).
type Handler func(ctx context.Context, rec *kgo.Record) error

// Consumer is a consumer-group member that commits offsets only after the
// handler has succeeded for every polled record.
type Consumer struct {
	cl      *kgo.Client
	handler Handler
}

// NewConsumer joins group on topics.
func NewConsumer(brokers []string, group string, topics []string, h Handler) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{cl: cl, handler: h}, nil
}

// Run polls until ctx is cancelled or the handler fails.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		fetches := c.cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, fe := range fetches.Errors() {
			return fmt.Errorf("kafka fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
		var handlerErr error
		fetches.EachRecord(func(rec *kgo.Record) {
			if handlerErr != nil {
				return
			}
			handlerErr = c.handler(ctx, rec)
		})
		if handlerErr != nil {
			return handlerErr
		}
		if err := c.cl.CommitUncommittedOffsets(ctx); err != nil {
			return fmt.Errorf("kafka commit: %w", err)
		}
	}
}

// Close leaves the group and closes the client.
func (c *Consumer) Close() { c.cl.Close() }
