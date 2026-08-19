// Package notify implements the A8 balance-threshold notifications: the
// creator is emailed when their balance crosses 20% of their last top-up
// and when it crosses zero. Per the assumptions register this is
// deliberately minimal — no preferences, no digesting, no templates.
//
// Delivery is best-effort and must never block or fail the ledger:
// callers invoke BalanceChanged after their transaction has committed
// (typically in a goroutine), and every failure is logged and dropped.
//
// There are two delivery paths. Without MAIL_SMTP_ADDR the Mailer is
// LogMailer, which writes the notification to the service log — v1's
// behaviour, unchanged. With a mail server configured, main builds the
// Notifier with NewTemplated instead, and internal/mail renders the two
// A8 templates and queues them for asynchronous delivery.
package notify

import (
	"context"
	"log/slog"
)

// Template names of the two A8 messages. They are the values of
// emails_sent_total{template} and must match internal/mail's registry.
const (
	// TemplateCreditsLow is the 20 %-of-last-top-up warning.
	TemplateCreditsLow = "credits_low"
	// TemplateCreditsExhausted is the zero-balance notice.
	TemplateCreditsExhausted = "credits_exhausted"
)

// Mailer delivers one notification to a creator. Implementations must not
// include balance amounts in transport metadata or logs beyond what the
// caller passes.
type Mailer interface {
	Send(ctx context.Context, creatorID, subject, body string) error
}

// TemplatedMailer is the mail-sending alternative to Mailer: it takes
// the template name and the numbers rather than a rendered body, so the
// mailer owns the wording and the metric label. internal/mail satisfies
// it. Implementations must not block — the ledger calls this path.
//
// It is a separate interface rather than a widened Mailer so that the
// log-only default keeps its exact v1 shape.
type TemplatedMailer interface {
	SendBalance(ctx context.Context, creatorID, template string, balance, lastTopup int64) error
}

// LogMailer is the v1 Mailer: it writes the notification to the service
// log instead of sending email.
type LogMailer struct {
	Logger *slog.Logger
}

// Send implements Mailer.
func (l LogMailer) Send(_ context.Context, creatorID, subject, _ string) error {
	l.Logger.Info("credits notification", "creator_id", creatorID, "subject", subject)
	return nil
}

// lastTopupReader is the one repository method the notifier needs.
type lastTopupReader interface {
	LastTopup(ctx context.Context, creatorID string) (int64, bool, error)
}

// Notifier fires threshold notifications on balance transitions.
type Notifier struct {
	repo      lastTopupReader
	mailer    Mailer
	templated TemplatedMailer
	logger    *slog.Logger
}

// New builds a Notifier over a plain Mailer (the log-only default).
func New(repo lastTopupReader, mailer Mailer, logger *slog.Logger) *Notifier {
	return &Notifier{repo: repo, mailer: mailer, logger: logger}
}

// NewTemplated builds a Notifier that sends the A8 templates through a
// real mailer. Used when MAIL_SMTP_ADDR is configured.
func NewTemplated(repo lastTopupReader, mailer TemplatedMailer, logger *slog.Logger) *Notifier {
	return &Notifier{repo: repo, templated: mailer, logger: logger}
}

// BalanceChanged inspects a committed balance transition and sends at
// most one notification:
//
//   - crossing to <= 0 sends "credits exhausted";
//   - otherwise, crossing to <= 20% of the creator's last top-up sends
//     "credits low" (skipped when the creator has never topped up).
//
// Only downward transitions are considered. Errors are logged, never
// returned — this is advisory, not part of the ledger.
func (n *Notifier) BalanceChanged(ctx context.Context, creatorID string, before, after int64) {
	if after >= before {
		return
	}
	if before > 0 && after <= 0 {
		// The zero notice does not need the last top-up, and must go out
		// even for a creator who has never topped up (a refund can drive
		// a balance negative), so it is sent before the lookup.
		n.send(ctx, creatorID, TemplateCreditsExhausted,
			"Your Dabet credits have run out",
			"Your credit balance has reached zero; messages are passing through unmoderated. Top up to resume moderation.",
			after, 0)
		return
	}
	lastTopup, found, err := n.repo.LastTopup(ctx, creatorID)
	if err != nil {
		n.logger.Warn("notification threshold lookup failed", "creator_id", creatorID, "error", err.Error())
		return
	}
	if !found || lastTopup <= 0 {
		return
	}
	threshold := lastTopup / 5 // 20% of last top-up
	if before > threshold && after <= threshold && after > 0 {
		n.send(ctx, creatorID, TemplateCreditsLow,
			"Your Dabet credits are running low",
			"Your credit balance is below 20% of your last top-up. Top up to avoid an interruption in moderation.",
			after, lastTopup)
	}
}

// send routes one notification down whichever delivery path is
// configured. Failures are logged and dropped: A8 is advisory, and §4.7
// does not let a mail problem become a ledger problem.
func (n *Notifier) send(ctx context.Context, creatorID, template, subject, body string, balance, lastTopup int64) {
	var err error
	switch {
	case n.templated != nil:
		err = n.templated.SendBalance(ctx, creatorID, template, balance, lastTopup)
	case n.mailer != nil:
		err = n.mailer.Send(ctx, creatorID, subject, body)
	default:
		return
	}
	if err != nil {
		n.logger.Warn("notification send failed",
			"creator_id", creatorID, "template", template, "error", err.Error())
	}
}
