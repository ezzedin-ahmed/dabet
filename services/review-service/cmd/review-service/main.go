// Command review-service serves the §7.6 review queue on the shared
// service runner: GET/POST /v1/reviews over a direct franz-go partition
// reader (no message database — the queue is a position in the creator's
// flagged.v1 partition), pgx-backed review cursors, and a kafkax producer
// for deletions.v1. It applies the embedded review-schema migrations at
// startup.
package main

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/pkg/config"
	"dabet/pkg/contracts"
	"dabet/pkg/httpx"
	"dabet/pkg/kafkax"
	"dabet/pkg/service"

	"dabet/services/review-service/internal/api"
	"dabet/services/review-service/internal/metrics"
	"dabet/services/review-service/internal/migrate"
	"dabet/services/review-service/internal/partition"
	"dabet/services/review-service/internal/queue"
	"dabet/services/review-service/internal/store"
)

// Service-local tunables (documented defaults, env-overridable per §4.4).
const (
	// EnvMetadataTTL bounds how long the discovered flagged.v1 partition
	// count is cached before re-asking the broker.
	EnvMetadataTTL = "REVIEW_PARTITION_METADATA_TTL"
	// EnvLagSampleInterval is how often the lag sampler re-ages
	// review_queue_lag_seconds for recently-active creators.
	EnvLagSampleInterval = "REVIEW_LAG_SAMPLE_INTERVAL"
	// EnvLagActiveTTL is how long a creator counts as recently active
	// before their gauge series is dropped.
	EnvLagActiveTTL = "REVIEW_LAG_ACTIVE_TTL"
)

func main() {
	svc := service.New("review-service")
	if err := run(context.Background(), svc); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, svc *service.Service) error {
	dsn, err := config.Get(config.EnvPostgresDSN)
	if err != nil {
		return err
	}
	// §5.4 access tokens: HS256 (JWT_SECRET) by default, RS256
	// (JWT_PUBLIC_KEY / JWT_PUBLIC_KEY_FILE) when JWT_ALG says so.
	verifier, err := httpx.VerifierFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	brokersRaw, err := config.Get(config.EnvKafkaBrokers)
	if err != nil {
		return err
	}
	brokers := strings.Split(brokersRaw, ",")
	metaTTL, err := config.GetDuration(EnvMetadataTTL, time.Minute)
	if err != nil {
		return err
	}
	sampleEvery, err := config.GetDuration(EnvLagSampleInterval, time.Minute)
	if err != nil {
		return err
	}
	activeTTL, err := config.GetDuration(EnvLagActiveTTL, time.Hour)
	if err != nil {
		return err
	}

	pool, err := connectWithRetry(ctx, svc.Logger, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := migrate.Run(ctx, pool); err != nil {
		return err
	}
	svc.Logger.Info("review migrations applied")

	reader, err := queue.NewKafka(brokers, contracts.TopicFlagged, metaTTL)
	if err != nil {
		return err
	}
	defer reader.Close()

	producer, err := kafkax.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	m := metrics.New(svc.Registry)
	tracker := metrics.NewLagTracker(m, nil)

	h := api.New(reader, store.NewPostgres(pool), producer,
		partition.NewMapper(contracts.TopicFlagged), m, tracker, svc.Logger, nil)
	h.Register(svc.Mux, verifier)

	samplerCtx, cancelSampler := context.WithCancel(ctx)
	defer cancelSampler()
	go tracker.Run(samplerCtx, sampleEvery, activeTTL)

	return svc.Run(ctx)
}

// connectWithRetry tolerates Compose start ordering: Postgres may accept
// connections a few seconds after review-service starts.
func connectWithRetry(ctx context.Context, logger *slog.Logger, dsn string) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 1; attempt <= 10; attempt++ {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		logger.Warn("postgres not ready, retrying", "attempt", attempt, "error", err.Error())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return nil, lastErr
}
