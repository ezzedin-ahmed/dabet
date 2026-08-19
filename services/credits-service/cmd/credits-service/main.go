// Command credits-service runs the §5.7 credits ledger: the billing
// schema migrations, the usage.v1 consumer that converts quantities of
// work into charges, the balance/entries/topup/webhook HTTP API, and the
// internal §5.8 credits-ok flag — all on the shared service skeleton
// (config from env, JSON logs, /healthz, /readyz, Prometheus /metrics,
// graceful shutdown).
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

	"dabet/services/credits-service/internal/api"
	"dabet/services/credits-service/internal/metrics"
	"dabet/services/credits-service/internal/migrate"
	"dabet/services/credits-service/internal/notify"
	"dabet/services/credits-service/internal/repo"
	"dabet/services/credits-service/internal/stripe"
	"dabet/services/credits-service/internal/usage"
)

// Service-specific environment variables; shared names live in pkg/config
// and the pricing rates in internal/usage.
const (
	envStripeSecretKey     = "STRIPE_SECRET_KEY"
	envStripeWebhookSecret = "STRIPE_WEBHOOK_SECRET"
	envStripeAPIBase       = "STRIPE_API_BASE" // overridable for the e2e stub
	envConsumerGroup       = "CREDITS_CONSUMER_GROUP"
	envNegativeGaugeEvery  = "CREDITS_NEGATIVE_GAUGE_INTERVAL"
)

func main() {
	svc := service.New("credits-service")
	if err := run(context.Background(), svc); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}

func run(ctx context.Context, svc *service.Service) error {
	log := svc.Logger

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
	stripeKey, err := config.Get(envStripeSecretKey)
	if err != nil {
		return err
	}
	webhookSecret, err := config.Get(envStripeWebhookSecret)
	if err != nil {
		return err
	}
	rates, err := usage.LoadRates(os.Getenv)
	if err != nil {
		return err
	}
	creditsPerCent, err := config.GetInt(api.EnvCreditsPerCent, api.DefaultCreditsPerCent)
	if err != nil {
		return err
	}
	gaugeInterval, err := config.GetDuration(envNegativeGaugeEvery, 60*time.Second)
	if err != nil {
		return err
	}
	brokers := strings.Split(config.GetDefault(config.EnvKafkaBrokers, "localhost:9092"), ",")
	group := config.GetDefault(envConsumerGroup, "credits-service")

	pool, err := connectWithRetry(ctx, log, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := migrate.Run(ctx, pool); err != nil {
		return err
	}
	log.Info("billing migrations applied")

	met := metrics.New(svc.Registry)
	ledgerRepo := repo.NewPostgres(pool)
	notifier := notify.New(ledgerRepo, notify.LogMailer{Logger: log}, log)

	stripeClient := stripe.NewClient(
		config.GetDefault(envStripeAPIBase, "https://api.stripe.com"), stripeKey)

	h := api.NewHandler(ledgerRepo, stripeClient, met, []byte(webhookSecret), int64(creditsPerCent), log)
	h.Notify = notifier
	h.Routes(svc.Mux, verifier)

	consumer, err := kafkax.NewConsumer(brokers, group, []string{contracts.TopicUsage},
		usage.NewConsumer(ledgerRepo, rates, met, svc.Metrics, group, notifier.BalanceChanged, log).Handle)
	if err != nil {
		return err
	}
	defer consumer.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// credits_balance_negative (§5.9), refreshed periodically.
	go met.RunNegativeGauge(runCtx, ledgerRepo, gaugeInterval, log)

	// The consumer restarts on transient errors (Kafka or Postgres down):
	// losing a dependency must not kill the process, and the idempotent
	// ledger makes redelivery safe.
	go func() {
		for runCtx.Err() == nil {
			if err := consumer.Run(runCtx); err != nil && runCtx.Err() == nil {
				log.Error("usage consumer error, restarting", "error", err.Error())
				select {
				case <-time.After(time.Second):
				case <-runCtx.Done():
				}
			}
		}
	}()

	return svc.Run(runCtx)
}

// connectWithRetry tolerates Compose start ordering: Postgres may accept
// connections a few seconds after credits-service starts.
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
