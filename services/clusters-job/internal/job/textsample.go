package job

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
)

// TextSource supplies a bounded sample of the creator's recent message
// text. Text is not stored anywhere in the system, so the only source is
// messages.v1 within Kafka retention (§8.6, §10 known gap 6): for windows
// older than retention this legitimately returns nothing and labels
// degrade to prior/generic. Returned text is radioactive (P4): in-memory
// only, never logged or persisted.
type TextSource interface {
	Sample(ctx context.Context, creatorID string, max int) ([]string, error)
}

// KafkaTextSource scans messages.v1 from a lookback offset without a
// consumer group (no commits — this is a read-only sample). The scan is
// bounded three ways: at most max texts collected, at most maxScan
// records inspected, and at most budget wall time. At production volume a
// full-topic scan is a firehose, so the bounds are the contract: this is
// a best-effort sample, not a complete read.
type KafkaTextSource struct {
	brokers  []string
	lookback time.Duration
	maxScan  int
	budget   time.Duration
	now      func() time.Time
}

// NewKafkaTextSource builds a sampler over brokers.
func NewKafkaTextSource(brokers []string, lookback time.Duration, maxScan int, budget time.Duration) *KafkaTextSource {
	return &KafkaTextSource{brokers: brokers, lookback: lookback, maxScan: maxScan, budget: budget, now: time.Now}
}

// Sample implements TextSource.
func (s *KafkaTextSource) Sample(ctx context.Context, creatorID string, max int) ([]string, error) {
	if max <= 0 {
		return nil, nil
	}
	since := s.now().Add(-s.lookback)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(s.brokers...),
		kgo.ConsumeTopics(contracts.TopicMessages),
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(since.UnixMilli())),
	)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(ctx, s.budget)
	defer cancel()

	var texts []string
	scanned := 0
	for scanned < s.maxScan && len(texts) < max {
		fetches := cl.PollFetches(ctx)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			break // budget spent: return what we have
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for _, fe := range fetches.Errors() {
			return nil, fe.Err
		}
		done := false
		fetches.EachRecord(func(rec *kgo.Record) {
			if done {
				return
			}
			scanned++
			var m contracts.Message
			if json.Unmarshal(rec.Value, &m) == nil && m.CreatorID == creatorID && m.Text != "" {
				texts = append(texts, m.Text)
			}
			if scanned >= s.maxScan || len(texts) >= max {
				done = true
			}
		})
	}
	return texts, nil
}
