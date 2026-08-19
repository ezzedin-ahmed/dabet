// Command provider-adapter is the only component that knows a platform
// exists (docs §7.2): it turns platform chat streams into messages.v1 and
// deletions.v1 back into platform delete calls. Config from env, JSON
// logs, /healthz, /readyz, Prometheus /metrics, graceful shutdown — all
// from the shared runner.
package main

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/pkg/config"
	"dabet/pkg/contracts"
	"dabet/pkg/kafkax"
	"dabet/pkg/service"

	"dabet/services/provider-adapter/internal/connpg"
	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/deletion"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/drivers/discord"
	"dabet/services/provider-adapter/internal/drivers/twitch"
	"dabet/services/provider-adapter/internal/drivers/youtube"
	"dabet/services/provider-adapter/internal/ingest"
	"dabet/services/provider-adapter/internal/metrics"
	"dabet/services/provider-adapter/internal/mockdriver"
	"dabet/services/provider-adapter/internal/opaque"
	"dabet/services/provider-adapter/internal/refresh"
)

func main() {
	svc := service.New("provider-adapter")
	if err := run(svc); err != nil {
		svc.Logger.Error("service exited", "error", err.Error())
		os.Exit(1)
	}
}

func run(svc *service.Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	brokersEnv, err := config.Get(config.EnvKafkaBrokers)
	if err != nil {
		return err
	}
	brokers := splitCSV(brokersEnv)

	buffer, err := config.GetInt("ADAPTER_INGEST_BUFFER", 256)
	if err != nil {
		return err
	}
	watchRetry, err := config.GetDuration("ADAPTER_WATCH_RETRY", 2*time.Second)
	if err != nil {
		return err
	}
	deleteBackoff, err := config.GetDuration("ADAPTER_DELETE_BACKOFF_BASE", 200*time.Millisecond)
	if err != nil {
		return err
	}

	m := metrics.New(svc.Registry)
	minter := opaque.NewMinter()

	// Drivers. Adding a platform is one implementation plus one Register
	// call and one opaque tag (N3).
	mock := mockdriver.New(nil)
	// Like the real drivers, the mock resolves opaque ids back to native
	// ones before "calling the platform" — which is what makes an
	// injected message correlatable with its deletion.
	mock.Resolver = minter
	registry := driver.NewRegistry()
	registry.Register(mock)
	registry.Register(youtube.New(minter))
	registry.Register(twitch.New(minter, config.GetDefault("TWITCH_CLIENT_ID", "")))
	registry.Register(discord.New(minter))

	// Connection assignment: env-seeded Static source (also the mock
	// injection endpoint's registry), plus — when POSTGRES_DSN is set —
	// the Postgres-backed poller over Area A's identity.connections
	// (§5.5/§5.6). The consistent-hash coordinator (A13) slots in behind
	// the same interface.
	seed, err := connsource.ParseEnv(config.GetDefault("ADAPTER_CONNECTIONS", ""))
	if err != nil {
		return err
	}
	static := connsource.NewStatic(seed...)

	// Mock platform HTTP surface for local e2e.
	mockdriver.NewHandlers(mock, static).Register(svc.Mux)

	var source connsource.Source = static
	var refresher deletion.TokenRefresher = deletion.StubRefresher{}
	if dsn := config.GetDefault(config.EnvPostgresDSN, ""); dsn != "" {
		pollInterval, err := config.GetDuration("ADAPTER_CONNSOURCE_POLL", 15*time.Second)
		if err != nil {
			return err
		}
		pool, err := connectWithRetry(ctx, svc, dsn)
		if err != nil {
			return err
		}
		defer pool.Close()

		store := connpg.New(pool)
		poller := connsource.NewPoller(store, pollInterval, svc.Logger)
		if err := poller.Load(ctx); err != nil {
			svc.Logger.Warn("initial connection load failed; will retry on poll", "error", err.Error())
		}
		go func() {
			if err := poller.Run(ctx); err != nil && ctx.Err() == nil {
				svc.Logger.Error("connection poller exited", "error", err.Error())
			}
		}()

		multi := connsource.NewMulti(poller, static)
		multi.Forward(ctx)
		source = multi

		// §5.6 lazy refresh, in place of the stub: advisory-locked
		// re-read + refresh-token exchange, expired-on-auth-error.
		refresher = refresh.New(store, refresh.EndpointsFromEnv(config.GetDefault),
			m.ConnectionRefreshTotal, svc.Logger, poller.Evict, poller.Update)
	}

	producer, err := kafkax.NewProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	manager := ingest.NewManager(registry, source, producer, minter, m, svc.Logger)
	manager.Buffer = buffer
	manager.WatchRetry = watchRetry

	processor := deletion.NewProcessor(registry, source, refresher, opaque.Platform, m, svc.Metrics, svc.Logger)
	processor.BaseBackoff = deleteBackoff

	consumer, err := kafkax.NewConsumer(brokers, deletion.Group, []string{contracts.TopicDeletions}, processor.Handle)
	if err != nil {
		return err
	}
	defer consumer.Close()

	go func() {
		if err := manager.Run(ctx); err != nil && ctx.Err() == nil {
			svc.Logger.Error("ingest manager exited", "error", err.Error())
		}
	}()
	go func() {
		// Deletion is best-effort (P2): a broken broker must not kill the
		// process, so the consumer restarts with backoff until shutdown.
		for ctx.Err() == nil {
			if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
				svc.Logger.Error("deletion consumer failed; restarting", "error", err.Error())
				select {
				case <-ctx.Done():
				case <-time.After(2 * time.Second):
				}
			}
		}
	}()

	err = svc.Run(ctx)
	cancel()
	return err
}

// connectWithRetry tolerates Compose start ordering: Postgres may accept
// connections a few seconds after provider-adapter starts.
func connectWithRetry(ctx context.Context, svc *service.Service, dsn string) (*pgxpool.Pool, error) {
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
		svc.Logger.Warn("postgres not ready, retrying", "attempt", attempt, "error", err.Error())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return nil, lastErr
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
