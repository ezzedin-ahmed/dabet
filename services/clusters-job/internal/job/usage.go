package job

import (
	"context"
	"encoding/json"

	"dabet/pkg/contracts"
	"dabet/pkg/kafkax"
)

// UsagePublisher emits usage.v1 events. Reclustering is a real compute
// cost (§8.6); only credits-service knows what it costs.
type UsagePublisher interface {
	PublishUsage(ctx context.Context, u contracts.Usage) error
}

// KafkaUsage publishes usage.v1 keyed by creator_id via the shared
// producer settings (§4.2).
type KafkaUsage struct {
	p *kafkax.Producer
}

// NewKafkaUsage wraps an existing producer.
func NewKafkaUsage(p *kafkax.Producer) *KafkaUsage { return &KafkaUsage{p: p} }

// PublishUsage implements UsagePublisher.
func (k *KafkaUsage) PublishUsage(ctx context.Context, u contracts.Usage) error {
	b, err := json.Marshal(u)
	if err != nil {
		return err
	}
	return k.p.Produce(ctx, contracts.TopicUsage, contracts.UsageKey(u.CreatorID), b)
}
