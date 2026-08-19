// Package metrics registers the credits-specific metrics of docs §5.9.
//
// Cardinality: §4.5 permits a creator_id label on credits_* metrics, but
// we deliberately keep it off — at 10M creators a per-creator label on
// counters would explode series cardinality for no operational gain, and
// §5.9's own label table lists none. Per-creator state lives in Postgres,
// not Prometheus.
package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Outcome label values for credits_usage_events_total.
const (
	OutcomeApplied  = "applied"
	OutcomeReplayed = "replayed"
)

// Credits holds the §5.9 credits metrics.
type Credits struct {
	// TopupCents is credits_topup_cents_total: cents successfully
	// captured via Stripe (incremented on payment_intent.succeeded).
	TopupCents prometheus.Counter
	// UsageEvents is credits_usage_events_total{event_type,outcome}.
	UsageEvents *prometheus.CounterVec
	// BalanceNegative is credits_balance_negative: the count of creators
	// with a negative balance, refreshed periodically.
	BalanceNegative prometheus.Gauge
}

// New constructs and registers the credits metric set.
func New(reg prometheus.Registerer) *Credits {
	c := &Credits{
		TopupCents: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "credits_topup_cents_total",
			Help: "Cents captured through Stripe top-ups.",
		}),
		UsageEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "credits_usage_events_total",
			Help: "usage.v1 events applied to the ledger.",
		}, []string{"event_type", "outcome"}),
		BalanceNegative: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "credits_balance_negative",
			Help: "Creators whose credit balance is below zero.",
		}),
	}
	reg.MustRegister(c.TopupCents, c.UsageEvents, c.BalanceNegative)
	return c
}

// negativeCounter is the one repository method the gauge needs.
type negativeCounter interface {
	NegativeBalances(ctx context.Context) (int64, error)
}

// RunNegativeGauge refreshes credits_balance_negative every interval
// until ctx is cancelled. Failures are logged and retried next tick.
func (c *Credits) RunNegativeGauge(ctx context.Context, repo negativeCounter, interval time.Duration, logger *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		n, err := repo.NegativeBalances(ctx)
		if err != nil {
			if ctx.Err() == nil {
				logger.Warn("negative-balance gauge refresh failed", "error", err.Error())
			}
		} else {
			c.BalanceNegative.Set(float64(n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
