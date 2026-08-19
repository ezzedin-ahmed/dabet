package mail

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestDisabledMailerLogsAndDoesNotSend(t *testing.T) {
	const token = "tok_secret_value"

	// Info level: the default. The token must not appear anywhere.
	var info bytes.Buffer
	reg := prometheus.NewRegistry()
	m, err := New(Config{}, reg, slog.New(slog.NewJSONHandler(&info, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if m.Enabled() {
		t.Fatal("a mailer with no MAIL_SMTP_ADDR must be disabled")
	}
	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", token); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	if strings.Contains(info.String(), token) {
		t.Errorf("verification token leaked into logs at info+: %s", info.String())
	}
	if n := testutil.ToFloat64(m.met.sent.WithLabelValues(TemplateVerification, OutcomeLogged)); n != 1 {
		t.Errorf("emails_sent_total{outcome=logged} = %v, want 1", n)
	}

	// Debug level: v1's dev-mode delivery channel still works, so local
	// development and anything reading the log for a token is unaffected.
	var debug bytes.Buffer
	m2, err := New(Config{}, nil, slog.New(slog.NewJSONHandler(&debug, &slog.HandlerOptions{Level: slog.LevelDebug})))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := m2.SendVerification(t.Context(), "creator@example.test", "Ada", token); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	if !strings.Contains(debug.String(), token) {
		t.Errorf("debug-level dev delivery channel lost the token: %s", debug.String())
	}
}

// blockingMailer returns a mailer whose transport blocks until release is
// closed, so the queue can be filled deterministically.
func blockingMailer(t *testing.T, queueSize int) (*Mailer, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	m, _ := testMailer(t, Config{
		Addr: "127.0.0.1:1", TLS: TLSNone, QueueSize: queueSize, Workers: 1,
	})
	m.send = func(ctx context.Context, _, _ string, _ []byte) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return m, func() { once.Do(func() { close(release) }) }
}

func TestEnqueueNeverBlocksAndOverflowDrops(t *testing.T) {
	m, release := blockingMailer(t, 2)
	defer release()

	// 1 in flight on the worker, 2 queued, the rest must be dropped
	// rather than block the caller (§4.7).
	const attempts = 12
	var dropped int
	start := time.Now()
	for i := 0; i < attempts; i++ {
		err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok")
		if errors.Is(err, ErrQueueFull) {
			dropped++
			continue
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("enqueueing %d messages took %v — the caller was blocked", attempts, elapsed)
	}
	if dropped == 0 {
		t.Fatal("a saturated queue must drop, not grow")
	}
	if n := testutil.ToFloat64(m.met.sent.WithLabelValues(TemplateVerification, OutcomeDropped)); int(n) != dropped {
		t.Errorf("emails_sent_total{outcome=dropped} = %v, want %d", n, dropped)
	}
}

func TestQueueDepthGauge(t *testing.T) {
	m, release := blockingMailer(t, 4)
	defer release()

	for i := 0; i < 3; i++ {
		if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok"); err != nil {
			t.Fatalf("SendVerification: %v", err)
		}
	}
	// One message is being sent, so the queue holds the remainder.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if v := testutil.ToFloat64(m.met.depth); v == 2 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Errorf("email_queue_depth = %v, want 2", testutil.ToFloat64(m.met.depth))
}

func TestCloseRejectsFurtherSends(t *testing.T) {
	srv := newFakeSMTP(t)
	m, _ := testMailer(t, Config{Addr: srv.addr(), TLS: TLSNone})
	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok"); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := m.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	srv.wait(t, 1) // the queued message drained before shutdown
	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok"); !errors.Is(err, ErrClosed) {
		t.Errorf("send after Close = %v, want ErrClosed", err)
	}
}

func TestUnknownTemplateIsAnError(t *testing.T) {
	m, _ := testMailer(t, Config{Addr: "127.0.0.1:1", TLS: TLSNone})
	if err := m.enqueue(job{template: "nope", resolve: static(Recipient{Email: "a@b.test"})}); err == nil {
		t.Fatal("enqueueing an unknown template must fail loudly")
	}
}

func TestConfigFromEnv(t *testing.T) {
	env := func(kv map[string]string) func(string) string {
		return func(k string) string { return kv[k] }
	}

	t.Run("absent configuration disables the mailer", func(t *testing.T) {
		cfg, err := ConfigFromEnv(env(nil))
		if err != nil {
			t.Fatalf("an unset mailer must not be an error: %v", err)
		}
		if cfg.Enabled() {
			t.Error("mailer enabled without MAIL_SMTP_ADDR")
		}
	})

	t.Run("defaults", func(t *testing.T) {
		cfg, err := ConfigFromEnv(env(map[string]string{
			EnvSMTPAddr: "mail:1025",
			EnvFrom:     "Dabet <no-reply@dabet.test>",
		}))
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.TLS != TLSStartTLS || cfg.Workers != DefaultWorkers ||
			cfg.QueueSize != DefaultQueueSize || cfg.MaxAttempts != DefaultMaxAttempts ||
			cfg.Timeout != DefaultTimeout || cfg.VerifyURL != DefaultVerifyURL {
			t.Errorf("unexpected defaults: %+v", cfg)
		}
	})

	t.Run("overrides", func(t *testing.T) {
		cfg, err := ConfigFromEnv(env(map[string]string{
			EnvSMTPAddr:    "mail:1025",
			EnvFrom:        "no-reply@dabet.test",
			EnvTLS:         "NONE",
			EnvTimeout:     "3s",
			EnvQueueSize:   "16",
			EnvWorkers:     "4",
			EnvMaxAttempts: "5",
			EnvRetryBase:   "250ms",
			EnvVerifyURL:   "https://app.dabet.test/verify",
		}))
		if err != nil {
			t.Fatalf("ConfigFromEnv: %v", err)
		}
		if cfg.TLS != TLSNone || cfg.Timeout != 3*time.Second || cfg.QueueSize != 16 ||
			cfg.Workers != 4 || cfg.MaxAttempts != 5 || cfg.RetryBase != 250*time.Millisecond {
			t.Errorf("overrides not applied: %+v", cfg)
		}
	})

	for name, kv := range map[string]map[string]string{
		"missing from":        {EnvSMTPAddr: "mail:1025"},
		"bad from":            {EnvSMTPAddr: "mail:1025", EnvFrom: "not-an-address"},
		"bad addr":            {EnvSMTPAddr: "mail", EnvFrom: "a@b.test"},
		"unknown tls mode":    {EnvSMTPAddr: "mail:1025", EnvFrom: "a@b.test", EnvTLS: "ssl"},
		"password no user":    {EnvSMTPAddr: "mail:1025", EnvFrom: "a@b.test", EnvPassword: "p"},
		"plain without tls":   {EnvSMTPAddr: "mail:1025", EnvFrom: "a@b.test", EnvTLS: "none", EnvUsername: "u", EnvPassword: "p"},
		"unparsable duration": {EnvSMTPAddr: "mail:1025", EnvFrom: "a@b.test", EnvTimeout: "soon"},
		"unparsable int":      {EnvSMTPAddr: "mail:1025", EnvFrom: "a@b.test", EnvWorkers: "many"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ConfigFromEnv(env(kv)); err == nil {
				t.Fatal("want a startup error")
			}
		})
	}
}

func TestBackoffDoubles(t *testing.T) {
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second} {
		if got := backoff(time.Second, attempt); got != want {
			t.Errorf("backoff(attempt=%d) = %v, want %v", attempt, got, want)
		}
	}
	if got := backoff(time.Hour, 4); got != time.Minute {
		t.Errorf("backoff must be capped, got %v", got)
	}
}
