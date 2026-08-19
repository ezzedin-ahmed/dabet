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
	"dabet/services/user-service/internal/oauth"
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

	r := repo.NewPostgres(pool)
	h, err := api.NewHandler(r, []byte(jwtSecret), authLogger(svc.Logger), logins)
	if err != nil {
		return err
	}

	// §5.5 OAuth provider set. OAUTH_MOCK_ENABLED gates the mock platform
	// (documented deviation, for e2e against a stub provider).
	mockEnabled := isTrue(config.GetDefault("OAUTH_MOCK_ENABLED", ""))
	h.Providers = oauth.LoadProviders(config.GetDefault, mockEnabled)
	h.AppRedirectURL = config.GetDefault("APP_REDIRECT_URL", h.AppRedirectURL)
	// Test-only: return the email-verification token in the registration
	// response so an automated e2e run can verify an address without a
	// mailer. Defaults off; local Compose only (see api.Handler).
	h.EchoVerificationToken = isTrue(config.GetDefault("DEV_EXPOSE_VERIFICATION_TOKEN", ""))
	if h.EchoVerificationToken {
		svc.Logger.Warn("DEV_EXPOSE_VERIFICATION_TOKEN is set: registration responses carry the raw email-verification token; never enable this outside local development")
	}
	h.Routes(svc.Mux)

	// connections_active{platform} (§5.9), refreshed periodically from
	// the database.
	gaugeInterval, err := config.GetDuration("CONNECTIONS_GAUGE_INTERVAL", 30*time.Second)
	if err != nil {
		return err
	}
	gauge := api.NewConnectionsGauge()
	svc.Registry.MustRegister(gauge)
	gaugeCtx, stopGauge := context.WithCancel(ctx)
	defer stopGauge()
	go api.RunConnectionsGauge(gaugeCtx, r, gauge, platformNames(h.Providers), gaugeInterval)

	return svc.Run(ctx)
}

func isTrue(v string) bool { return v == "1" || v == "true" || v == "yes" }

func platformNames(providers map[string]*oauth.Provider) []string {
	out := make([]string, 0, len(providers))
	for p := range providers {
		out = append(out, p)
	}
	return out
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
