// Package mail is credits-service's outbound email path: an SMTP client,
// a bounded asynchronous send queue, and the two §5.8/A8 balance
// templates — 20 % of the last top-up, and zero.
//
// It is a deliberate copy of user-service's package of the same name
// rather than a shared library: pkg/ carries the cross-service contracts
// of §4, and a mail transport is not a contract. The copies differ in
// their templates, their configuration surface, and the seam they plug
// into.
//
// Two rules shape the design:
//
//   - §4.7, fail open. Email must never block or fail a request. Callers
//     enqueue and return; sending, retrying and giving up all happen on
//     worker goroutines. A full queue drops the message and counts it
//     rather than applying backpressure to an HTTP handler.
//   - Off by default. With no MAIL_SMTP_ADDR the mailer keeps v1's
//     log-only behaviour, so local development, Compose and test/e2e are
//     unaffected by this package existing.
//
// P4: no chat message text ever reaches an email. These messages carry
// no secrets at all — a balance, a threshold, and a link to the billing
// page.
package mail

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	netmail "net/mail"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Environment variables (§4.4: prefixed by concern, not by service).
const (
	EnvSMTPAddr    = "MAIL_SMTP_ADDR" // host:port; empty disables sending
	EnvFrom        = "MAIL_FROM"      // RFC 5322 address, required when enabled
	EnvUsername    = "MAIL_SMTP_USERNAME"
	EnvPassword    = "MAIL_SMTP_PASSWORD"
	EnvTLS         = "MAIL_TLS"          // starttls (default) | tls | none
	EnvTimeout     = "MAIL_TIMEOUT"      // per-attempt SMTP deadline
	EnvQueueSize   = "MAIL_QUEUE_SIZE"   // bounded queue depth
	EnvWorkers     = "MAIL_WORKERS"      // concurrent senders
	EnvMaxAttempts = "MAIL_MAX_ATTEMPTS" // attempts per message
	EnvRetryBase   = "MAIL_RETRY_BASE"   // first backoff, doubled per attempt
	EnvHelo        = "MAIL_HELO"         // EHLO name
	EnvBillingURL  = "APP_BILLING_URL"   // top-up page, linked from the A8 mail
)

// TLSMode selects how the SMTP connection is protected.
type TLSMode string

// Supported TLS modes.
const (
	// TLSStartTLS upgrades a plaintext connection with STARTTLS. Default.
	TLSStartTLS TLSMode = "starttls"
	// TLSImplicit dials TLS directly (SMTPS, usually port 465).
	TLSImplicit TLSMode = "tls"
	// TLSNone is plaintext. Local capture (Mailpit/MailHog) only; the
	// mailer refuses to authenticate over it.
	TLSNone TLSMode = "none"
)

// Errors returned by the enqueue path. Both mean "not sent"; neither is
// ever fatal to the caller (§4.7).
var (
	// ErrQueueFull is returned when the bounded queue is saturated. The
	// message is dropped and counted.
	ErrQueueFull = errors.New("mail: send queue full")
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("mail: mailer closed")
)

// Defaults for the tunables of §4.4 ("the documented number is the
// default, not a constant in code").
const (
	DefaultTimeout     = 10 * time.Second
	DefaultQueueSize   = 256
	DefaultWorkers     = 2
	DefaultMaxAttempts = 3
	DefaultRetryBase   = time.Second
	DefaultHelo        = "localhost"
	DefaultBillingURL  = "http://localhost:3000/billing"
)

// Config is the mailer's configuration. The zero value is a valid,
// disabled mailer.
type Config struct {
	// Addr is the SMTP server, host:port. Empty means disabled.
	Addr string
	// From is the envelope and header sender.
	From string
	// Username and Password authenticate with AUTH PLAIN. Permitted only
	// with TLSStartTLS or TLSImplicit — credentials are never sent over a
	// plaintext connection.
	Username string
	Password string
	TLS      TLSMode
	// Timeout bounds one send attempt end to end.
	Timeout time.Duration
	// QueueSize bounds the outstanding queue; overflow drops.
	QueueSize int
	// Workers is the number of concurrent senders.
	Workers int
	// MaxAttempts counts the first attempt, so 1 disables retries.
	MaxAttempts int
	// RetryBase is the first backoff; it doubles per attempt.
	RetryBase time.Duration
	// Helo is the EHLO name.
	Helo string

	// BillingURL is where the A8 mail sends the creator to top up.
	BillingURL string

	// TLSConfig overrides the derived client TLS configuration. Tests set
	// it to trust a throwaway CA; production leaves it nil.
	TLSConfig *tls.Config
}

// Enabled reports whether the mailer sends rather than logs.
func (c Config) Enabled() bool { return c.Addr != "" }

// ConfigFromEnv reads the mailer configuration. get is os.Getenv in
// production. A configuration that cannot possibly work is an error at
// startup rather than a silent per-message failure — but an *absent*
// configuration is not an error, it is the log-only default.
func ConfigFromEnv(get func(string) string) (Config, error) {
	cfg := Config{
		Addr:        strings.TrimSpace(get(EnvSMTPAddr)),
		From:        strings.TrimSpace(get(EnvFrom)),
		Username:    get(EnvUsername),
		Password:    get(EnvPassword),
		TLS:         TLSMode(strings.ToLower(strings.TrimSpace(get(EnvTLS)))),
		Timeout:     DefaultTimeout,
		QueueSize:   DefaultQueueSize,
		Workers:     DefaultWorkers,
		MaxAttempts: DefaultMaxAttempts,
		RetryBase:   DefaultRetryBase,
		Helo:        orDefault(get(EnvHelo), DefaultHelo),
		BillingURL:  orDefault(get(EnvBillingURL), DefaultBillingURL),
	}
	if cfg.TLS == "" {
		cfg.TLS = TLSStartTLS
	}

	var err error
	if cfg.Timeout, err = durationEnv(get, EnvTimeout, DefaultTimeout); err != nil {
		return Config{}, err
	}
	if cfg.RetryBase, err = durationEnv(get, EnvRetryBase, DefaultRetryBase); err != nil {
		return Config{}, err
	}
	if cfg.QueueSize, err = intEnv(get, EnvQueueSize, DefaultQueueSize); err != nil {
		return Config{}, err
	}
	if cfg.Workers, err = intEnv(get, EnvWorkers, DefaultWorkers); err != nil {
		return Config{}, err
	}
	if cfg.MaxAttempts, err = intEnv(get, EnvMaxAttempts, DefaultMaxAttempts); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if !c.Enabled() {
		// Disabled: nothing downstream is used, so nothing else matters.
		return nil
	}
	if !strings.Contains(c.Addr, ":") {
		return fmt.Errorf("%s must be host:port", EnvSMTPAddr)
	}
	if c.From == "" {
		return fmt.Errorf("%s is required when %s is set", EnvFrom, EnvSMTPAddr)
	}
	if _, err := netmail.ParseAddress(c.From); err != nil {
		return fmt.Errorf("%s is not a valid address: %w", EnvFrom, err)
	}
	switch c.TLS {
	case TLSStartTLS, TLSImplicit, TLSNone:
	default:
		return fmt.Errorf("%s must be one of %q, %q, %q", EnvTLS, TLSStartTLS, TLSImplicit, TLSNone)
	}
	if c.Password != "" && c.Username == "" {
		return fmt.Errorf("%s is set without %s", EnvPassword, EnvUsername)
	}
	if c.Username != "" && c.TLS == TLSNone {
		// AUTH PLAIN over a plaintext connection hands the password to
		// anyone on the path. net/smtp refuses it too; failing at startup
		// makes the misconfiguration obvious instead of silent.
		return fmt.Errorf("%s requires %s=%q or %q: PLAIN authentication is only sent over TLS",
			EnvUsername, EnvTLS, TLSStartTLS, TLSImplicit)
	}
	if c.QueueSize < 1 || c.Workers < 1 || c.MaxAttempts < 1 {
		return fmt.Errorf("%s, %s and %s must be >= 1", EnvQueueSize, EnvWorkers, EnvMaxAttempts)
	}
	if c.Timeout <= 0 || c.RetryBase < 0 {
		return fmt.Errorf("%s must be > 0 and %s must be >= 0", EnvTimeout, EnvRetryBase)
	}
	return nil
}

func orDefault(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return strings.TrimSpace(v)
}

func intEnv(get func(string) string, name string, def int) (int, error) {
	v := strings.TrimSpace(get(name))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s: %w", name, err)
	}
	return n, nil
}

func durationEnv(get func(string) string, name string, def time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(get(name))
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("environment variable %s: %w", name, err)
	}
	return d, nil
}

// Recipient is one addressee.
type Recipient struct {
	Email string
	Name  string
}

// resolver produces a recipient at send time. Constant recipients use
// static(); credits-style lookups (creator id -> address) can do their
// database read on the worker rather than on the caller's goroutine.
type resolver func(ctx context.Context) (Recipient, error)

func static(r Recipient) resolver {
	return func(context.Context) (Recipient, error) { return r, nil }
}

type job struct {
	template string
	resolve  resolver
	data     any
}

// Mailer renders templates and delivers them over SMTP, asynchronously.
// The zero value is not usable; call New.
type Mailer struct {
	cfg  Config
	tmpl map[string]templateSet
	met  *metrics
	log  *slog.Logger
	// recipients resolves creator ids to addresses, on the worker.
	recipients Recipients

	queue chan job
	wg    sync.WaitGroup

	mu     sync.RWMutex
	closed bool
	once   sync.Once

	// send is the transport, swapped in tests. Nil means the real SMTP
	// client.
	send func(ctx context.Context, from string, to string, msg []byte) error
	now  func() time.Time
}

// New builds a Mailer and, when enabled, starts its workers. Templates
// are parsed once here so a broken template is a startup failure.
// recipients resolves creator ids to addresses and may be nil when the
// mailer is disabled. Metrics register on reg (§4.5); reg may be nil in
// tests.
func New(cfg Config, recipients Recipients, reg prometheus.Registerer, logger *slog.Logger) (*Mailer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	tmpl, err := loadTemplates()
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	m := &Mailer{
		cfg:        cfg,
		tmpl:       tmpl,
		log:        logger,
		recipients: recipients,
		now:        func() time.Time { return time.Now().UTC() },
	}
	if cfg.Enabled() {
		m.queue = make(chan job, cfg.QueueSize)
	}
	m.met = newMetrics(reg, func() float64 { return float64(len(m.queue)) })
	if cfg.Enabled() {
		for i := 0; i < cfg.Workers; i++ {
			m.wg.Add(1)
			go m.worker()
		}
	}
	return m, nil
}

// Enabled reports whether messages are sent rather than logged.
func (m *Mailer) Enabled() bool { return m.cfg.Enabled() }

// Close stops accepting messages and waits for the queue to drain, or
// for ctx to expire. Draining is best effort: undelivered mail is lost on
// shutdown, which is the correct trade for a notification.
func (m *Mailer) Close(ctx context.Context) error {
	if !m.cfg.Enabled() {
		return nil
	}
	m.once.Do(func() {
		m.mu.Lock()
		m.closed = true
		close(m.queue)
		m.mu.Unlock()
	})
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// enqueue is the whole non-blocking contract: it either hands the job to
// a worker immediately or drops it. It never waits.
func (m *Mailer) enqueue(j job) error {
	if _, ok := m.tmpl[j.template]; !ok {
		return fmt.Errorf("mail: unknown template %q", j.template)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return ErrClosed
	}
	select {
	case m.queue <- j:
		return nil
	default:
		m.met.sent.WithLabelValues(j.template, OutcomeDropped).Inc()
		m.log.Warn("email dropped: send queue full",
			"template", j.template, "queue_size", cap(m.queue))
		return ErrQueueFull
	}
}

func (m *Mailer) worker() {
	defer m.wg.Done()
	for j := range m.queue {
		m.process(j)
	}
}

// process renders and delivers one job, retrying with exponential
// backoff. Every exit path increments emails_sent_total exactly once.
func (m *Mailer) process(j job) {
	ctx := context.Background()

	resolveCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	to, err := j.resolve(resolveCtx)
	cancel()
	if err != nil {
		m.fail(j.template, "recipient lookup failed", err)
		return
	}
	if to.Email == "" {
		m.fail(j.template, "no recipient address", errors.New("empty address"))
		return
	}

	// The greeting is only knowable once the recipient is resolved, which
	// deliberately happens here rather than on the caller's goroutine.
	subject, text, html, err := m.render(j.template, applyRecipient(j.data, to))
	if err != nil {
		m.fail(j.template, "render failed", err)
		return
	}
	raw, err := message{
		From:    m.cfg.From,
		To:      to,
		Subject: subject,
		Text:    text,
		HTML:    html,
		Date:    m.now(),
	}.bytes()
	if err != nil {
		m.fail(j.template, "encode failed", err)
		return
	}
	from, err := addressOnly(m.cfg.From)
	if err != nil {
		m.fail(j.template, "invalid sender", err)
		return
	}

	send := m.send
	if send == nil {
		send = m.smtpSend
	}
	for attempt := 1; attempt <= m.cfg.MaxAttempts; attempt++ {
		sendCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
		err = send(sendCtx, from, to.Email, raw)
		cancel()
		if err == nil {
			m.met.sent.WithLabelValues(j.template, OutcomeSent).Inc()
			m.log.Debug("email sent", "template", j.template, "attempts", attempt)
			return
		}
		if attempt < m.cfg.MaxAttempts {
			m.log.Warn("email send failed, retrying",
				"template", j.template, "attempt", attempt, "error", err.Error())
			time.Sleep(backoff(m.cfg.RetryBase, attempt))
		}
	}
	m.fail(j.template, "send failed after retries", err)
}

// backoff doubles per attempt: base, 2*base, 4*base…
func backoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d > time.Minute {
			return time.Minute
		}
	}
	return d
}

func (m *Mailer) fail(template, msg string, err error) {
	m.met.sent.WithLabelValues(template, OutcomeFailed).Inc()
	m.log.Warn("email not delivered: "+msg, "template", template, "error", err.Error())
}

// logOnly records the disabled-mailer outcome for a template.
func (m *Mailer) logOnly(template string) {
	m.met.sent.WithLabelValues(template, OutcomeLogged).Inc()
}
