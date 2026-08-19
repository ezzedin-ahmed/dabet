// Command user-service serves the §5.4 auth endpoints: register, verify,
// login, refresh, logout, and /v1/me. It applies the embedded identity
// migrations at startup and runs on the shared service skeleton (config
// from env, JSON logs, /healthz, /readyz, Prometheus /metrics, graceful
// shutdown).
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dabet/pkg/config"
	"dabet/pkg/service"

	"dabet/services/user-service/internal/api"
	"dabet/services/user-service/internal/migrate"
	"dabet/services/user-service/internal/repo"
)

func main() {
	svc := service.New("user-service")
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
	jwtSecret, err := config.Get(config.EnvJWTSecret)
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
	svc.Logger.Info("identity migrations applied")

	logins := api.NewLoginsCounter()
	svc.Registry.MustRegister(logins)

	h, err := api.NewHandler(repo.NewPostgres(pool), []byte(jwtSecret), authLogger(svc.Logger), logins)
	if err != nil {
		return err
	}
	h.Routes(svc.Mux)

	return svc.Run(ctx)
}

// connectWithRetry tolerates Compose start ordering: Postgres may accept
// connections a few seconds after user-service starts.
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

// authLogger returns the handler logger. The shared skeleton logger is
// info-level; when LOG_LEVEL=debug a debug-level logger is substituted so
// the dev-mode email-verification delivery channel (documented deviation
// in internal/api) is visible.
func authLogger(base *slog.Logger) *slog.Logger {
	if config.GetDefault("LOG_LEVEL", "info") != "debug" {
		return base
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h).With("service", "user-service")
}
