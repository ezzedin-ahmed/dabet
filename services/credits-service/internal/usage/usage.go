// Package usage consumes usage.v1 (docs §7.10 producers, §5.7 pricing)
// and converts quantities of work into negative ledger entries. Only this
// package knows what an event costs; producers emit quantities, never
// money.
//
// Pricing (ASSUMPTION — §5.7 fixes where the rate lives, not its value):
// rates are credits per unit of work, environment-overridable per §4.4:
//
//	CREDITS_COST_MESSAGES_PROCESSED   default 0.001  (1 credit / 1000 messages)
//	CREDITS_COST_MESSAGES_RECLUSTERED default 0.0001 (1 credit / 10000 messages)
//
// The charge for an event is ceil(quantity x rate) credits, so no
// non-zero quantity is ever free.
package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
	"dabet/pkg/obs"

	"dabet/services/credits-service/internal/ledger"
	"dabet/services/credits-service/internal/metrics"
)

// Environment variable names for the conversion rates.
const (
	EnvCostMessagesProcessed   = "CREDITS_COST_MESSAGES_PROCESSED"
	EnvCostMessagesReclustered = "CREDITS_COST_MESSAGES_RECLUSTERED"

	defaultCostMessagesProcessed   = 0.001  // 1 credit per 1000 messages
	defaultCostMessagesReclustered = 0.0001 // 1 credit per 10000 messages
)

// Rates holds credits-per-unit conversion rates.
type Rates struct {
	MessagesProcessed   float64
	MessagesReclustered float64
}

// LoadRates reads the rates from the environment via getenv (os.Getenv in
// production), applying the documented defaults.
func LoadRates(getenv func(string) string) (Rates, error) {
	r := Rates{
		MessagesProcessed:   defaultCostMessagesProcessed,
		MessagesReclustered: defaultCostMessagesReclustered,
	}
	if err := loadRate(getenv, EnvCostMessagesProcessed, &r.MessagesProcessed); err != nil {
		return Rates{}, err
	}
	if err := loadRate(getenv, EnvCostMessagesReclustered, &r.MessagesReclustered); err != nil {
		return Rates{}, err
	}
	return r, nil
}

func loadRate(getenv func(string) string, name string, dst *float64) error {
	v := getenv(name)
	if v == "" {
		return nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 || math.IsInf(f, 0) {
		return fmt.Errorf("environment variable %s: must be a positive number, got %q", name, v)
	}
	*dst = f
	return nil
}

// Delta converts an event's quantity into a ledger delta:
// -ceil(quantity x rate). ok is false for an unknown event type. The tiny
// epsilon absorbs binary-float noise so that e.g. 1000 x 0.001 charges 1
// credit, not 2.
func (r Rates) Delta(eventType contracts.EventType, quantity int64) (int64, bool) {
	var rate float64
	switch eventType {
	case contracts.EventMessagesProcessed:
		rate = r.MessagesProcessed
	case contracts.EventMessagesReclustered:
		rate = r.MessagesReclustered
	default:
		return 0, false
	}
	const eps = 1e-9
	credits := int64(math.Ceil(float64(quantity)*rate - eps))
	if credits < 1 {
		credits = 1 // any positive quantity costs at least one credit
	}
	return -credits, true
}

// Consumer converts usage.v1 records into ledger entries.
type Consumer struct {
	repo     ledger.Repository
	rates    Rates
	met      *metrics.Credits
	obsMet   *obs.Metrics
	group    string
	notifier *notify
	logger   *slog.Logger
}

// notify decouples the Consumer from the notify package for tests.
type notify struct {
	fn func(ctx context.Context, creatorID string, before, after int64)
}

// NewConsumer builds the record handler's receiver. balanceChanged is
// invoked best-effort (in a goroutine) after a non-replayed application;
// pass nil to disable notifications.
func NewConsumer(repo ledger.Repository, rates Rates, met *metrics.Credits, obsMet *obs.Metrics, group string, balanceChanged func(ctx context.Context, creatorID string, before, after int64), logger *slog.Logger) *Consumer {
	c := &Consumer{repo: repo, rates: rates, met: met, obsMet: obsMet, group: group, logger: logger}
	if balanceChanged != nil {
		c.notifier = &notify{fn: balanceChanged}
	}
	return c
}

// Handle processes one usage.v1 record. Malformed events are counted and
// skipped — they must never wedge the partition — while storage errors
// are returned so the batch is redelivered (at-least-once; the
// idempotency key makes the retry safe).
func (c *Consumer) Handle(ctx context.Context, rec *kgo.Record) error {
	var ev contracts.Usage
	if err := json.Unmarshal(rec.Value, &ev); err != nil {
		c.skipMalformed(rec, "undecodable usage event")
		return nil
	}
	if ev.CreatorID == "" || ev.IdempotencyKey == "" || ev.Quantity <= 0 {
		c.skipMalformed(rec, "usage event missing creator_id, idempotency_key, or positive quantity")
		return nil
	}
	delta, ok := c.rates.Delta(ev.EventType, ev.Quantity)
	if !ok {
		c.skipMalformed(rec, "usage event has unknown event_type")
		return nil
	}

	res, err := c.repo.Apply(ctx, ev.CreatorID, delta, string(ev.EventType), ev.IdempotencyKey, map[string]any{
		"quantity":     ev.Quantity,
		"window_start": ev.WindowStart,
		"window_end":   ev.WindowEnd,
	})
	if err != nil {
		return fmt.Errorf("apply usage event: %w", err)
	}

	outcome := metrics.OutcomeApplied
	if res.Replayed {
		outcome = metrics.OutcomeReplayed
	}
	c.met.UsageEvents.WithLabelValues(string(ev.EventType), outcome).Inc()
	c.obsMet.KafkaConsumedTotal.WithLabelValues(rec.Topic, c.group, "ok").Inc()

	if !res.Replayed && c.notifier != nil {
		// Best-effort, off the consume path: a notification must never
		// block or fail the ledger (A8).
		go c.notifier.fn(context.WithoutCancel(ctx), ev.CreatorID, res.Balance-delta, res.Balance)
	}
	return nil
}

// skipMalformed counts a bad record and moves on. Only the partition and
// offset are logged — never the payload (P4).
func (c *Consumer) skipMalformed(rec *kgo.Record, msg string) {
	c.obsMet.KafkaConsumedTotal.WithLabelValues(rec.Topic, c.group, "malformed").Inc()
	c.logger.Warn(msg, "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
}
