package mail

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	netmail "net/mail"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// testMailer builds an enabled mailer with test-sized timeouts against
// addr, plus the registry its metrics are on.
func testMailer(t *testing.T, cfg Config) (*Mailer, *prometheus.Registry) {
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
	if cfg.VerifyURL == "" {
		cfg.VerifyURL = "https://app.dabet.test/verify"
	}
	if cfg.AppConnectionsURL == "" {
		cfg.AppConnectionsURL = "https://app.dabet.test/connections"
	}
	reg := prometheus.NewRegistry()
	m, err := New(cfg, reg, slog.New(slog.DiscardHandler))
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

// parsed is a received message split into headers and decoded parts.
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
	// Both parts must be quoted-printable. mime/multipart decodes (and
	// strips the header of) such parts transparently, so the assertion is
	// made against the bytes that crossed the wire.
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
		ct := p.Header.Get("Content-Type")
		switch {
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

func TestSendVerificationDeliversMultipart(t *testing.T) {
	srv := newFakeSMTP(t)
	m, reg := testMailer(t, Config{Addr: srv.addr(), TLS: TLSNone})

	const token = "tok_9f3c1a2b"
	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada Lovelace", token); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}

	got := srv.wait(t, 1)[0]
	if got.from != "no-reply@dabet.test" {
		t.Errorf("envelope MAIL FROM = %q", got.from)
	}
	if len(got.to) != 1 || got.to[0] != "creator@example.test" {
		t.Errorf("envelope RCPT TO = %v", got.to)
	}

	msg := parseDelivery(t, got.data)
	if h := msg.header.Get("From"); h != "Dabet <no-reply@dabet.test>" {
		t.Errorf("From = %q", h)
	}
	if h := msg.header.Get("To"); !strings.Contains(h, "creator@example.test") || !strings.Contains(h, "Ada Lovelace") {
		t.Errorf("To = %q", h)
	}
	if h := msg.header.Get("Subject"); h != subjects[TemplateVerification] {
		t.Errorf("Subject = %q", h)
	}
	for _, h := range []string{"Date", "Message-ID", "MIME-Version"} {
		if msg.header.Get(h) == "" {
			t.Errorf("missing %s header", h)
		}
	}

	link := "https://app.dabet.test/verify?token=" + token
	if !strings.Contains(msg.text, link) {
		t.Errorf("text part missing the verification link:\n%s", msg.text)
	}
	if !strings.Contains(msg.html, "href=\""+link+"\"") {
		t.Errorf("html part missing the verification link:\n%s", msg.html)
	}
	// The token is a credential: it appears once per body, in the link,
	// and nowhere else.
	if n := strings.Count(msg.text, token); n != 1 {
		t.Errorf("token appears %d times in the text part, want 1", n)
	}
	if n := strings.Count(msg.html, token); n != 1 {
		t.Errorf("token appears %d times in the html part, want 1", n)
	}
	if !strings.Contains(msg.text, "Ada Lovelace") {
		t.Error("text part should greet the creator by name")
	}

	if n := testutil.ToFloat64(m.met.sent.WithLabelValues(TemplateVerification, OutcomeSent)); n != 1 {
		t.Errorf("emails_sent_total{sent} = %v, want 1", n)
	}
	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP email_queue_depth Emails waiting in the bounded send queue.
# TYPE email_queue_depth gauge
email_queue_depth 0
`), "email_queue_depth"); err != nil {
		t.Error(err)
	}
}

func TestSendConnectionExpired(t *testing.T) {
	srv := newFakeSMTP(t)
	m, _ := testMailer(t, Config{Addr: srv.addr(), TLS: TLSNone})

	if err := m.SendConnectionExpired(t.Context(), "creator@example.test", "Ada", "twitch", "somechannel"); err != nil {
		t.Fatalf("SendConnectionExpired: %v", err)
	}
	msg := parseDelivery(t, srv.wait(t, 1)[0].data)
	for _, want := range []string{"twitch", "somechannel", "https://app.dabet.test/connections"} {
		if !strings.Contains(msg.text, want) {
			t.Errorf("text part missing %q:\n%s", want, msg.text)
		}
	}
	if strings.Contains(msg.text, "token") || strings.Contains(msg.html, "token") {
		t.Error("the A6 mail must not carry any token")
	}
}

func TestStartTLSDelivery(t *testing.T) {
	serverTLS, clientTLS := testCerts(t)
	srv := newFakeSMTP(t, withTLS(serverTLS, false))
	m, _ := testMailer(t, Config{Addr: srv.addr(), TLS: TLSStartTLS, TLSConfig: clientTLS})

	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok"); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	if got := srv.wait(t, 1)[0]; !got.overTLS {
		t.Error("message was accepted over a plaintext connection despite MAIL_TLS=starttls")
	}
}

func TestImplicitTLSDelivery(t *testing.T) {
	serverTLS, clientTLS := testCerts(t)
	srv := newFakeSMTP(t, withTLS(serverTLS, true))
	m, _ := testMailer(t, Config{Addr: srv.addr(), TLS: TLSImplicit, TLSConfig: clientTLS})

	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok"); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	if got := srv.wait(t, 1)[0]; !got.overTLS {
		t.Error("implicit TLS delivery was not encrypted")
	}
}

func TestAuthPlainOnlyOverTLS(t *testing.T) {
	serverTLS, clientTLS := testCerts(t)
	srv := newFakeSMTP(t, withTLS(serverTLS, false))
	m, _ := testMailer(t, Config{
		Addr: srv.addr(), TLS: TLSStartTLS, TLSConfig: clientTLS,
		Username: "dabet", Password: "s3cret",
	})

	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok"); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	got := srv.wait(t, 1)[0]
	if !got.overTLS {
		t.Fatal("credentials were sent over a plaintext connection")
	}
	raw, err := base64.StdEncoding.DecodeString(got.authPlain)
	if err != nil {
		t.Fatalf("decode AUTH PLAIN payload: %v", err)
	}
	if want := "\x00dabet\x00s3cret"; string(raw) != want {
		t.Errorf("AUTH PLAIN payload = %q, want %q", raw, want)
	}
	// The password must not have leaked into the message itself.
	if strings.Contains(got.data, "s3cret") {
		t.Error("password present in the message body")
	}
}

func TestRetryThenSuccess(t *testing.T) {
	srv := newFakeSMTP(t, failFirst(2))
	m, _ := testMailer(t, Config{
		Addr: srv.addr(), TLS: TLSNone,
		MaxAttempts: 3, RetryBase: time.Millisecond,
	})

	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok"); err != nil {
		t.Fatalf("SendVerification: %v", err)
	}
	srv.wait(t, 1)
	waitForCounter(t, m, TemplateVerification, OutcomeSent, 1)
	if n := testutil.ToFloat64(m.met.sent.WithLabelValues(TemplateVerification, OutcomeFailed)); n != 0 {
		t.Errorf("emails_sent_total{failed} = %v, want 0 — the send eventually succeeded", n)
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	srv := newFakeSMTP(t, failFirst(100))
	m, _ := testMailer(t, Config{
		Addr: srv.addr(), TLS: TLSNone,
		MaxAttempts: 2, RetryBase: time.Millisecond,
	})

	if err := m.SendVerification(t.Context(), "creator@example.test", "Ada", "tok"); err != nil {
		t.Fatalf("SendVerification must not surface a delivery failure: %v", err)
	}
	waitForCounter(t, m, TemplateVerification, OutcomeFailed, 1)
}

// waitForCounter polls a counter until it reaches want.
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
