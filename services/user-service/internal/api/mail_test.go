package api

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/services/user-service/internal/auth"
	mailer "dabet/services/user-service/internal/mail"
	"dabet/services/user-service/internal/repo"
)

// recordingMailer is the api-level seam double. The real SMTP protocol is
// exercised in internal/mail against an in-process server; here we care
// only about what the handler hands to the mailer, and when.
type recordingMailer struct {
	mu   sync.Mutex
	sent []sentMail
	err  error
}

type sentMail struct{ email, fullname, token string }

func (r *recordingMailer) SendVerification(_ context.Context, email, fullname, token string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, sentMail{email, fullname, token})
	return r.err
}

func (r *recordingMailer) messages() []sentMail {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]sentMail(nil), r.sent...)
}

// mailFixture is newFixture with the mailer installed before the server
// starts, so nothing writes to the handler once it is serving.
func mailFixture(t *testing.T, m Mailer, logger *slog.Logger, echo bool) *fixture {
	t.Helper()
	fake := repo.NewFake()
	h, err := NewHandler(fake, auth.HMACKeyring([]byte(testSecret)), logger, NewLoginsCounter())
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	seq := &tokenSeq{}
	h.NewToken = seq.next
	h.Mail = m
	h.EchoVerificationToken = echo
	mux := http.NewServeMux()
	h.Routes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return &fixture{t: t, h: h, fake: fake, seq: seq, server: server}
}

const mailRegisterBody = `{"email":"creator@example.test","fullname":"Ada Lovelace","password":"a perfectly fine passphrase"}`

func TestRegisterQueuesVerificationEmail(t *testing.T) {
	rec := &recordingMailer{}
	f := mailFixture(t, rec, slog.New(slog.DiscardHandler), true)

	status, body := f.post("/v1/auth/register", mailRegisterBody)
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %v)", status, body)
	}
	sent := rec.messages()
	if len(sent) != 1 {
		t.Fatalf("sent %d verification emails, want 1", len(sent))
	}
	if sent[0].email != "creator@example.test" || sent[0].fullname != "Ada Lovelace" {
		t.Errorf("recipient = %+v", sent[0])
	}
	// The mailed token is the one the creator can actually redeem.
	if got, want := sent[0].token, body["verification_token"]; got != want {
		t.Errorf("mailed token = %q, echoed token = %v", got, want)
	}
	if status, _ := f.post("/v1/auth/verify", fmt.Sprintf(`{"token":%q}`, sent[0].token)); status != http.StatusNoContent {
		t.Errorf("the mailed token did not verify the address: status %d", status)
	}
}

// §4.7: email is best effort. A mailer that cannot accept the message
// must not change the status code the creator sees.
func TestRegisterSucceedsWhenTheMailerRefuses(t *testing.T) {
	rec := &recordingMailer{err: mailer.ErrQueueFull}
	f := mailFixture(t, rec, slog.New(slog.DiscardHandler), false)

	if status, body := f.post("/v1/auth/register", mailRegisterBody); status != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (body %v)", status, body)
	}
}

// The real mailer, pointed at a port nothing is listening on: registration
// must still be a fast 201 and the failure must surface only as a metric.
func TestRegisterSucceedsWithSMTPDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	dead := ln.Addr().String()
	ln.Close() // nothing answers here now

	reg := prometheus.NewRegistry()
	m, err := mailer.New(mailer.Config{
		Addr: dead, From: "Dabet <no-reply@dabet.test>", TLS: mailer.TLSNone,
		Timeout: 250 * time.Millisecond, QueueSize: 4, Workers: 1,
		MaxAttempts: 2, RetryBase: time.Millisecond,
		VerifyURL: "https://app.dabet.test/verify", AppConnectionsURL: "https://app.dabet.test/connections",
	}, reg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("mail.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		m.Close(ctx) //nolint:errcheck
	})

	f := mailFixture(t, m, slog.New(slog.DiscardHandler), true)

	start := time.Now()
	status, body := f.post("/v1/auth/register", mailRegisterBody)
	elapsed := time.Since(start)
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want 201 with SMTP down (body %v)", status, body)
	}
	if elapsed > 2*time.Second {
		t.Errorf("registration took %v with SMTP down: the request path is blocking on email", elapsed)
	}
	if body["verification_token"] == "" {
		t.Error("the dev-mode token echo must keep working when delivery fails")
	}

	// The failure is observable where §4.5 says it should be.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n := testutil.CollectAndCount(reg, "emails_sent_total"); n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("emails_sent_total never recorded the failed delivery")
}

// The default deployment (no MAIL_SMTP_ADDR) keeps v1 behaviour: the
// token reaches nobody but the debug log and, when explicitly enabled,
// the registration response that test/e2e reads.
func TestRegisterWithMailerDisabled(t *testing.T) {
	var logs bytes.Buffer
	m, err := mailer.New(mailer.Config{}, nil,
		slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if err != nil {
		t.Fatalf("mail.New: %v", err)
	}
	if m.Enabled() {
		t.Fatal("mailer must default to disabled")
	}
	f := mailFixture(t, m, slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelInfo})), true)

	status, body := f.post("/v1/auth/register", mailRegisterBody)
	if status != http.StatusCreated {
		t.Fatalf("status = %d, want 201", status)
	}
	token, _ := body["verification_token"].(string)
	if token == "" {
		t.Fatal("DEV_EXPOSE_VERIFICATION_TOKEN path lost the token")
	}
	if strings.Contains(logs.String(), token) {
		t.Errorf("verification token present in logs at info+: %s", logs.String())
	}
	if status, _ := f.post("/v1/auth/verify", fmt.Sprintf(`{"token":%q}`, token)); status != http.StatusNoContent {
		t.Errorf("echoed token did not verify: status %d", status)
	}
}

// A registration that never reaches the mailer (nil seam) still works —
// unit tests and any deployment that wires no mailer at all.
func TestRegisterWithoutMailer(t *testing.T) {
	f := mailFixture(t, nil, slog.New(slog.DiscardHandler), false)
	if status, _ := f.post("/v1/auth/register", mailRegisterBody); status != http.StatusCreated {
		t.Fatalf("status = %d, want 201", status)
	}
}
