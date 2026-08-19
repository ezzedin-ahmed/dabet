// Package deletion consumes deletions.v1 and turns each record back into a
// platform delete call, implementing the §7.2 response table. Routing is
// lookup-free: the platform tag embedded in the opaque content_id selects
// the driver. Deletion is best-effort by design (P2): every failure class
// ends in a counted terminal outcome, and the handler never wedges the
// partition.
package deletion

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
	"dabet/pkg/obs"

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/metrics"
)

// Group is the adapter's consumer group on deletions.v1.
const Group = "provider-adapter"

// TokenRefresher is the §5.6 lazy-refresh hook: on a 401 the processor
// asks for fresh credentials and retries once. The real implementation
// (advisory lock, re-read, refresh-token exchange against Area A) lands
// with the connections phase; v1 wires StubRefresher.
type TokenRefresher interface {
	// Refresh returns the connection with a fresh access token, or an
	// error if the token could not be refreshed (the connection is then
	// treated as expired and the deletion dropped as auth_failed).
	Refresh(ctx context.Context, conn driver.Connection) (driver.Connection, error)
}

// StubRefresher always fails: until the connections phase, a 401 is
// terminal after the single refresh attempt.
type StubRefresher struct{}

// Refresh implements TokenRefresher.
func (StubRefresher) Refresh(_ context.Context, conn driver.Connection) (driver.Connection, error) {
	return conn, errors.New("deletion: token refresh not implemented yet (§5.6, connections phase)")
}

// Terminal outcomes for deletions_total{outcome}.
const (
	OutcomeOK           = "ok"            // provider confirmed the delete
	OutcomeNotFound     = "not_found"     // already gone — success (§7.2)
	OutcomeGone         = "gone"          // stream ended / content gone
	OutcomeAuthFailed   = "auth_failed"   // 401 persisted after one refresh
	OutcomeDropped      = "dropped"       // retries exhausted (429/5xx)
	OutcomeNoConnection = "no_connection" // no active connection for creator+platform
	OutcomeUnroutable   = "unroutable"    // content_id platform tag unknown
	OutcomeInvalid      = "invalid"       // record did not decode
)

// PlatformResolver resolves the platform embedded in an opaque id;
// satisfied by opaque.Platform.
type PlatformResolver func(id string) (string, error)

// Processor handles one deletions.v1 record at a time.
type Processor struct {
	registry  *driver.Registry
	source    connsource.Source
	refresher TokenRefresher
	platform  PlatformResolver
	metrics   *metrics.Metrics
	obs       *obs.Metrics
	log       *slog.Logger

	// MaxAttempts caps delete attempts per record. §7.2 mandates the cap
	// for 5xx; 429 shares it so a hard-throttling provider cannot stall
	// the partition (P2).
	MaxAttempts int
	// BaseBackoff seeds the exponential backoff; MaxBackoff caps it.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// Sleep waits between retries; injectable for tests.
	Sleep func(ctx context.Context, d time.Duration) error
	// Jitter returns a uniform float in [0,1); injectable for tests.
	Jitter func() float64
}

// NewProcessor wires a Processor with the documented defaults.
func NewProcessor(reg *driver.Registry, src connsource.Source, refresher TokenRefresher, platform PlatformResolver, m *metrics.Metrics, om *obs.Metrics, log *slog.Logger) *Processor {
	return &Processor{
		registry:    reg,
		source:      src,
		refresher:   refresher,
		platform:    platform,
		metrics:     m,
		obs:         om,
		log:         log,
		MaxAttempts: 5,
		BaseBackoff: 200 * time.Millisecond,
		MaxBackoff:  5 * time.Second,
		Sleep:       sleepCtx,
		Jitter:      rand.Float64,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Handle is the kafkax.Handler for deletions.v1. It only returns an error
// when ctx is cancelled — every provider failure is absorbed into a
// counted outcome so the consumer keeps committing (best-effort, P2).
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	start := time.Now()
	var del contracts.Deletion
	if err := json.Unmarshal(rec.Value, &del); err != nil {
		p.log.Error("undecodable deletion record", "error", err.Error())
		p.finish("unknown", OutcomeInvalid, start)
		p.obs.KafkaConsumedTotal.WithLabelValues(contracts.TopicDeletions, Group, "error").Inc()
		return nil
	}
	platform, outcome := p.process(ctx, del)
	if err := ctx.Err(); err != nil {
		return err // redelivered after restart; drivers treat repeats as already-gone
	}
	p.finish(platform, outcome, start)
	p.obs.KafkaConsumedTotal.WithLabelValues(contracts.TopicDeletions, Group, "ok").Inc()
	return nil
}

func (p *Processor) finish(platform, outcome string, start time.Time) {
	p.metrics.DeletionsTotal.WithLabelValues(platform, outcome).Inc()
	p.metrics.DeletionLatency.WithLabelValues(platform).Observe(time.Since(start).Seconds())
	if outcome == OutcomeDropped || outcome == OutcomeNoConnection {
		p.obs.FailOpenTotal.WithLabelValues("provider-adapter", "deletion_"+outcome).Inc()
	}
}

// process routes and executes one deletion, returning (platform, outcome).
func (p *Processor) process(ctx context.Context, del contracts.Deletion) (string, string) {
	platform, err := p.platform(del.ContentID)
	if err != nil {
		p.log.Error("deletion unroutable", "creator_id", del.CreatorID, "error", err.Error())
		return "unknown", OutcomeUnroutable
	}
	drv, ok := p.registry.Get(platform)
	if !ok {
		p.log.Error("no driver registered", "platform", platform)
		return platform, OutcomeUnroutable
	}
	conn, ok := p.source.Lookup(del.CreatorID, platform)
	if !ok {
		p.log.Warn("no active connection for deletion", "platform", platform, "creator_id", del.CreatorID)
		return platform, OutcomeNoConnection
	}
	return platform, p.deleteWithRetry(ctx, drv, conn, del)
}

// deleteWithRetry implements the §7.2 response table.
func (p *Processor) deleteWithRetry(ctx context.Context, drv driver.Driver, conn driver.Connection, del contracts.Deletion) string {
	refreshed := false
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return OutcomeDropped
		}
		err := drv.Delete(ctx, conn, del.ContentID, del.MessageID)
		switch {
		case err == nil:
			return OutcomeOK
		case errors.Is(err, driver.ErrNotFound):
			// Treated as success — the viewer or another mod got there first.
			return OutcomeNotFound
		case errors.Is(err, driver.ErrGone):
			return OutcomeGone
		case errors.Is(err, driver.ErrUnauthorized):
			if refreshed {
				return OutcomeAuthFailed
			}
			fresh, rerr := p.refresher.Refresh(ctx, conn)
			if rerr != nil {
				p.log.Warn("token refresh failed", "platform", conn.Platform, "connection_id", conn.ID, "error", rerr.Error())
				return OutcomeAuthFailed
			}
			conn = fresh
			refreshed = true
			// Retry the original call once, immediately (§5.6).
		case errors.Is(err, driver.ErrRateLimited):
			if attempt >= p.MaxAttempts {
				return OutcomeDropped
			}
			// Backoff with jitter (0.5x–1.5x of the exponential step).
			d := p.backoff(attempt)
			d = d/2 + time.Duration(p.Jitter()*float64(d))
			if p.Sleep(ctx, d) != nil {
				return OutcomeDropped
			}
		default:
			// 5xx / transport errors: exponential backoff, up to
			// MaxAttempts total attempts, then drop.
			if attempt >= p.MaxAttempts {
				p.log.Warn("deletion dropped after retries", "platform", conn.Platform, "attempts", attempt, "error", err.Error())
				return OutcomeDropped
			}
			if p.Sleep(ctx, p.backoff(attempt)) != nil {
				return OutcomeDropped
			}
		}
	}
}

// backoff returns BaseBackoff * 2^(attempt-1), capped at MaxBackoff.
func (p *Processor) backoff(attempt int) time.Duration {
	d := p.BaseBackoff
	for i := 1; i < attempt && d < p.MaxBackoff; i++ {
		d *= 2
	}
	return min(d, p.MaxBackoff)
}
