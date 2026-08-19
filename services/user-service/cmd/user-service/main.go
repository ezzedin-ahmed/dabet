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
	"dabet/services/user-service/internal/auth"
	"dabet/services/user-service/internal/expiry"
	"dabet/services/user-service/internal/mail"
	"dabet/services/user-service/internal/migrate"
	"dabet/services/user-service/internal/oauth"
	"dabet/services/user-service/internal/repo"
)

// envExpirySweep tunes the A6 notification sweep (§5.5).
const envExpirySweep = "CONNECTION_EXPIRY_SWEEP_INTERVAL"

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
	// §5.4: HS256 by default (JWT_SECRET), RS256 when the deployment sets
	// JWT_ALG=RS256 and a private key. A broken key is a startup failure,
	// not a per-request 401.
	keys, err := auth.KeyringFromEnv(os.Getenv)
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
	h, err := api.NewHandler(r, keys, authLogger(svc.Logger), logins)
	if err != nil {
		return err
	}

	// Outbound email (§5.4, §5.5/A6). Off unless MAIL_SMTP_ADDR is set,
	// in which case the mailer logs the verification token at debug
	// exactly as v1 did. A broken mail configuration is a startup error;
	// a broken mail *server* is not, and never fails a request (§4.7).
	mailCfg, err := mail.ConfigFromEnv(os.Getenv)
	if err != nil {
		return err
	}
	mailer, err := mail.New(mailCfg, svc.Registry, authLogger(svc.Logger))
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mailer.Close(shutdownCtx); err != nil {
			svc.Logger.Warn("mail queue did not drain before shutdown", "error", err.Error())
		}
	}()
	h.Mail = mailer
	svc.Logger.Info("outbound email configured",
		"enabled", mailCfg.Enabled(), "tls", string(mailCfg.TLS))
	svc.Logger.Info("access tokens configured", "alg", keys.Signer.Alg(), "kid", keys.Signer.Kid())

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

	// A6: provider-adapter marks a connection 'expired' when a refresh
	// fails with an auth error (§5.6); this sweep is what tells the
	// creator, since v1 has no in-app notifications.
	sweepInterval, err := config.GetDuration(envExpirySweep, expiry.DefaultInterval)
	if err != nil {
		return err
	}
	go expiry.New(r, mailer, svc.Logger, sweepInterval, expiry.DefaultBatch).Run(gaugeCtx)

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
