package mod

import (
	"context"
	"time"
)

// KafkaProducer is the producing side the publisher retries over;
// implemented by kafkax.Producer.
type KafkaProducer interface {
	Produce(ctx context.Context, topic string, key, value []byte) error
}

// Publisher writes verdict and usage events with the §4.7 producer
// policy: retry with exponential backoff for up to maxElapsed (30 s), then
// drop the event and count fail_open_total{component="kafka"}.
type Publisher struct {
	prod       KafkaProducer
	maxElapsed time.Duration
	baseDelay  time.Duration
	maxDelay   time.Duration
	onFailOpen func()
	sleep      func(time.Duration)
	now        func() time.Time
}

// NewPublisher builds a publisher. onFailOpen is invoked once per dropped
// event.
func NewPublisher(prod KafkaProducer, maxElapsed time.Duration, onFailOpen func()) *Publisher {
	return &Publisher{
		prod:       prod,
		maxElapsed: maxElapsed,
		baseDelay:  100 * time.Millisecond,
		maxDelay:   5 * time.Second,
		onFailOpen: onFailOpen,
		sleep:      time.Sleep,
		now:        time.Now,
	}
}

// Publish produces one record, retrying on failure. Returns true when the
// broker acked, false when the event was dropped after the retry budget.
func (p *Publisher) Publish(ctx context.Context, topic string, key, value []byte) bool {
	deadline := p.now().Add(p.maxElapsed)
	delay := p.baseDelay
	for {
		if err := p.prod.Produce(ctx, topic, key, value); err == nil {
			return true
		}
		if ctx.Err() != nil || p.now().Add(delay).After(deadline) {
			p.onFailOpen()
			return false
		}
		p.sleep(delay)
		delay *= 2
		if delay > p.maxDelay {
			delay = p.maxDelay
		}
	}
}
