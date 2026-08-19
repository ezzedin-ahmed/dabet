package mail

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	netmail "net/mail"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/services/credits-service/internal/notify"
)

// fixedRecipients is the identity lookup double. The real one reads
// identity.creators; what matters here is that the lookup happens on a
// worker and that its failures are contained.
type fixedRecipients struct {
	email string
	name  string
	err   error
	calls atomic.Int32
	block chan struct{}
}

func (f *fixedRecipients) Recipient(ctx context.Context, _ string) (string, string, error) {
	f.calls.Add(1)
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return "", "", ctx.Err()
		}
	}
	return f.email, f.name, f.err
}

func testMailer(t *testing.T, cfg Config, rcpt Recipients) (*Mailer, *prometheus.Registry) {
	t.Helper()
	if cfg.From == "" {
		cfg.From = "Dabet <no-reply@dabet.test>"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 8
	}
	if cfg.Workers == 0 {
		cfg.Workers = 1
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 1
	}
	if cfg.BillingURL == "" {
		cfg.BillingURL = "https://app.dabet.test/billing"
	}
	reg := prometheus.NewRegistry()
	m, err := New(cfg, rcpt, reg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.Close(ctx) //nolint:errcheck
	})
	return m, reg
}

type parsed struct {
	header netmail.Header
	text   string
	html   string
}

func parseDelivery(t *testing.T, raw string) parsed {
	t.Helper()
	msg, err := netmail.ReadMessage(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("parse message: %v", err)
	}
	mediaType, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
	if err != nil {
		t.Fatalf("parse content type: %v", err)
	}
	if mediaType != "multipart/alternative" {
		t.Fatalf("Content-Type = %q, want multipart/alternative", mediaType)
	}
	if n := strings.Count(raw, "Content-Transfer-Encoding: quoted-printable"); n != 2 {
		t.Fatalf("quoted-printable parts = %d, want 2:\n%s", n, raw)
	}
	out := parsed{header: msg.Header}
	mr := multipart.NewReader(msg.Body, params["boundary"])
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("next part: %v", err)
		}
		body, err := io.ReadAll(p)
		if err != nil {
			t.Fatalf("read part: %v", err)
		}
		switch ct := p.Header.Get("Content-Type"); {
		case strings.HasPrefix(ct, "text/plain"):
			out.text = string(body)
		case strings.HasPrefix(ct, "text/html"):
			out.html = string(body)
		default:
			t.Fatalf("unexpected part type %q", ct)
		}
	}
	if out.text == "" || out.html == "" {
		t.Fatal("message must carry both a text and an HTML part")
	}
	return out
}

// The template names the notifier picks must exist in the mailer, or A8
// silently sends nothing.
func TestNotifyTemplateNamesExist(t *testing.T) {
	if notify.TemplateCreditsLow != TemplateCreditsLow ||
		notify.TemplateCreditsExhausted != TemplateCreditsExhausted {
		t.Fatal("notify and mail disagree on the A8 template names")
	}
	tmpl, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	for _, name := range []string{notify.TemplateCreditsLow, notify.TemplateCreditsExhausted} {
		if _, ok := tmpl[name]; !ok {
			t.Errorf("no template registered for %q", name)
		}
	}
}

func TestSendBalanceDeliversBothTemplates(t *testing.T) {
	for _, tc := range []struct {
		template string
		balance  int64
		topup    int64
		wants    []string
	}{
		{TemplateCreditsLow, 180, 1000, []string{"180", "1000", "20%", "https://app.dabet.test/billing"}},
		{TemplateCreditsExhausted, 0, 0, []string{"zero", "unmoderated", "https://app.dabet.test/billing"}},
	} {
		t.Run(tc.template, func(t *testing.T) {
			srv := newFakeSMTP(t)
			rcpt := &fixedRecipients{email: "creator@example.test", name: "Ada Lovelace"}
			m, _ := testMailer(t, Config{Addr: srv.addr(), TLS: TLSNone}, rcpt)

			if err := m.SendBalance(t.Context(), "creator-1", tc.template, tc.balance, tc.topup); err != nil {
				t.Fatalf("SendBalance: %v", err)
			}
			got := srv.wait(t, 1)[0]
			if got.from != "no-reply@dabet.test" || len(got.to) != 1 || got.to[0] != "creator@example.test" {
				t.Errorf("envelope = %s -> %v", got.from, got.to)
			}
			msg := parseDelivery(t, got.data)
			if h := msg.header.Get("Subject"); h != subjects[tc.template] {
				t.Errorf("Subject = %q", h)
			}
			if !strings.Contains(msg.text, "Ada Lovelace") {
				t.Errorf("greeting missing; the recipient name is resolved on the worker:\n%s", msg.text)
			}
			for _, want := range tc.wants {
				if !strings.Contains(msg.text, want) {
					t.Errorf("text part missing %q:\n%s", want, msg.text)
				}
			}
			// P4 and §5.7: no chat text, no payment details, no creator id.
			for _, forbidden := range []string{"creator-1", "pi_", "card"} {
				if strings.Contains(msg.text, forbidden) || strings.Contains(msg.html, forbidden) {
					t.Errorf("message leaks %q", forbidden)
				}
			}
			if rcpt.calls.Load() != 1 {
				t.Errorf("recipient lookups = %d, want 1", rcpt.calls.Load())
			}
		})
	}
}

// The address lookup is a database read; it must happen on the worker, so
// a slow or broken identity read cannot reach the ledger path.
func TestRecipientLookupHappensOnTheWorker(t *testing.T) {
	srv := newFakeSMTP(t)
	block := make(chan struct{})
	rcpt := &fixedRecipients{email: "creator@example.test", name: "Ada", block: block}
	m, _ := testMailer(t, Config{Addr: srv.addr(), TLS: TLSNone}, rcpt)

	start := time.Now()
	if err := m.SendBalance(t.Context(), "creator-1", TemplateCreditsLow, 1, 10); err != nil {
		t.Fatalf("SendBalance: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("SendBalance blocked for %v on the recipient lookup", elapsed)
	}
	close(block)
	srv.wait(t, 1)
}

func TestRecipientLookupFailureIsContained(t *testing.T) {
	srv := newFakeSMTP(t)
	rcpt := &fixedRecipients{err: errors.New("creator not found")}
	m, _ := testMailer(t, Config{Addr: srv.addr(), TLS: TLSNone}, rcpt)

	if err := m.SendBalance(t.Context(), "creator-1", TemplateCreditsLow, 1, 10); err != nil {
		t.Fatalf("SendBalance must not surface a lookup failure: %v", err)
	}
	waitForCounter(t, m, TemplateCreditsLow, OutcomeFailed, 1)
}

func TestDisabledMailerLogsOnly(t *testing.T) {
	var logs bytes.Buffer
	m, err := New(Config{}, nil, nil,
		slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if m.Enabled() {
		t.Fatal("a mailer with no MAIL_SMTP_ADDR must be disabled")
	}
	if err := m.SendBalance(t.Context(), "creator-1", TemplateCreditsExhausted, 0, 0); err != nil {
		t.Fatalf("SendBalance: %v", err)
	}
	if n := testutil.ToFloat64(m.met.sent.WithLabelValues(TemplateCreditsExhausted, OutcomeLogged)); n != 1 {
		t.Errorf("emails_sent_total{outcome=logged} = %v, want 1", n)
	}
	if !strings.Contains(logs.String(), TemplateCreditsExhausted) {
		t.Errorf("disabled mailer logged nothing useful: %s", logs.String())
	}
}

func TestStartTLSAndAuth(t *testing.T) {
	serverTLS, clientTLS := testCerts(t)
	srv := newFakeSMTP(t, withTLS(serverTLS, false))
	rcpt := &fixedRecipients{email: "creator@example.test", name: "Ada"}
	m, _ := testMailer(t, Config{
		Addr: srv.addr(), TLS: TLSStartTLS, TLSConfig: clientTLS,
		Username: "dabet", Password: "s3cret",
	}, rcpt)

	if err := m.SendBalance(t.Context(), "creator-1", TemplateCreditsLow, 1, 10); err != nil {
		t.Fatalf("SendBalance: %v", err)
	}
	got := srv.wait(t, 1)[0]
	if !got.overTLS {
		t.Error("credentials were sent over a plaintext connection")
	}
	if got.authPlain == "" {
		t.Error("AUTH PLAIN was not attempted")
	}
}

func TestImplicitTLS(t *testing.T) {
	serverTLS, clientTLS := testCerts(t)
	srv := newFakeSMTP(t, withTLS(serverTLS, true))
	m, _ := testMailer(t, Config{Addr: srv.addr(), TLS: TLSImplicit, TLSConfig: clientTLS},
		&fixedRecipients{email: "creator@example.test", name: "Ada"})

	if err := m.SendBalance(t.Context(), "creator-1", TemplateCreditsLow, 1, 10); err != nil {
		t.Fatalf("SendBalance: %v", err)
	}
	if !srv.wait(t, 1)[0].overTLS {
		t.Error("implicit TLS delivery was not encrypted")
	}
}

func TestRetryThenSuccess(t *testing.T) {
	srv := newFakeSMTP(t, failFirst(2))
	m, _ := testMailer(t, Config{
		Addr: srv.addr(), TLS: TLSNone, MaxAttempts: 3, RetryBase: time.Millisecond,
	}, &fixedRecipients{email: "creator@example.test", name: "Ada"})

	if err := m.SendBalance(t.Context(), "creator-1", TemplateCreditsLow, 1, 10); err != nil {
		t.Fatalf("SendBalance: %v", err)
	}
	srv.wait(t, 1)
	waitForCounter(t, m, TemplateCreditsLow, OutcomeSent, 1)
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	srv := newFakeSMTP(t, failFirst(100))
	m, _ := testMailer(t, Config{
		Addr: srv.addr(), TLS: TLSNone, MaxAttempts: 2, RetryBase: time.Millisecond,
	}, &fixedRecipients{email: "creator@example.test", name: "Ada"})

	if err := m.SendBalance(t.Context(), "creator-1", TemplateCreditsLow, 1, 10); err != nil {
		t.Fatalf("SendBalance must not surface a delivery failure: %v", err)
	}
	waitForCounter(t, m, TemplateCreditsLow, OutcomeFailed, 1)
}

// A saturated queue drops and counts rather than blocking the caller —
// which, for A8, is a goroutine spawned off the webhook or the usage
// consumer (§4.7).
func TestQueueOverflowDropsAndCounts(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	defer func() { once.Do(func() { close(release) }) }()

	m, _ := testMailer(t, Config{Addr: "127.0.0.1:1", TLS: TLSNone, QueueSize: 2, Workers: 1},
		&fixedRecipients{email: "creator@example.test", name: "Ada"})
	m.send = func(ctx context.Context, _, _ string, _ []byte) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var dropped int
	start := time.Now()
	for i := 0; i < 12; i++ {
		if err := m.SendBalance(t.Context(), "creator-1", TemplateCreditsLow, 1, 10); errors.Is(err, ErrQueueFull) {
			dropped++
		} else if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("enqueue blocked for %v", elapsed)
	}
	if dropped == 0 {
		t.Fatal("a saturated queue must drop, not grow")
	}
	if n := testutil.ToFloat64(m.met.sent.WithLabelValues(TemplateCreditsLow, OutcomeDropped)); int(n) != dropped {
		t.Errorf("emails_sent_total{outcome=dropped} = %v, want %d", n, dropped)
	}
}

func TestHTMLTemplateEscapesTheName(t *testing.T) {
	m, err := New(Config{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, text, html, err := m.render(TemplateCreditsLow, balanceData{
		Name: `<script>alert("x")</script>`, Balance: 1, LastTopup: 10,
		BillingURL: "https://app.dabet.test/billing",
	})
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(html, "<script>") || !strings.Contains(html, "&lt;script&gt;") {
		t.Errorf("html part is not escaped:\n%s", html)
	}
	if !strings.Contains(text, "<script>") {
		t.Errorf("text part should carry the literal value:\n%s", text)
	}
}

func TestConfigFromEnv(t *testing.T) {
	env := func(kv map[string]string) func(string) string {
		return func(k string) string { return kv[k] }
	}
	cfg, err := ConfigFromEnv(env(nil))
	if err != nil {
		t.Fatalf("an unset mailer must not be an error: %v", err)
	}
	if cfg.Enabled() {
		t.Error("mailer enabled without MAIL_SMTP_ADDR")
	}

	cfg, err = ConfigFromEnv(env(map[string]string{
		EnvSMTPAddr: "mail:1025", EnvFrom: "no-reply@dabet.test",
		EnvTLS: "none", EnvBillingURL: "https://app.dabet.test/billing",
	}))
	if err != nil {
		t.Fatalf("ConfigFromEnv: %v", err)
	}
	if !cfg.Enabled() || cfg.TLS != TLSNone || cfg.BillingURL != "https://app.dabet.test/billing" {
		t.Errorf("unexpected config: %+v", cfg)
	}

	for name, kv := range map[string]map[string]string{
		"missing from":      {EnvSMTPAddr: "mail:1025"},
		"plain without tls": {EnvSMTPAddr: "mail:1025", EnvFrom: "a@b.test", EnvTLS: "none", EnvUsername: "u", EnvPassword: "p"},
		"unknown tls mode":  {EnvSMTPAddr: "mail:1025", EnvFrom: "a@b.test", EnvTLS: "ssl"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ConfigFromEnv(env(kv)); err == nil {
				t.Fatal("want a startup error")
			}
		})
	}
}

func waitForCounter(t *testing.T, m *Mailer, template, outcome string, want float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := testutil.ToFloat64(m.met.sent.WithLabelValues(template, outcome)); got >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("emails_sent_total{template=%q,outcome=%q} did not reach %v", template, outcome, want)
}
