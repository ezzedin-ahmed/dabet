// Package notify implements the A8 balance-threshold notifications: the
// creator is emailed when their balance crosses 20% of their last top-up
// and when it crosses zero. Per the assumptions register this is
// deliberately minimal — no preferences, no digesting, no templates.
//
// Delivery is best-effort and must never block or fail the ledger:
// callers invoke BalanceChanged after their transaction has committed
// (typically in a goroutine), and every failure is logged and dropped.
// No mailer exists in v1, so the default Mailer implementation logs the
// notification instead of sending it (documented deviation, same approach
// as user-service's verification emails).
package notify

import (
	"context"
	"log/slog"
)

// Mailer delivers one notification to a creator. Implementations must not
// include balance amounts in transport metadata or logs beyond what the
// caller passes.
type Mailer interface {
	Send(ctx context.Context, creatorID, subject, body string) error
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
	repo   lastTopupReader
	mailer Mailer
	logger *slog.Logger
}

// New builds a Notifier.
func New(repo lastTopupReader, mailer Mailer, logger *slog.Logger) *Notifier {
	return &Notifier{repo: repo, mailer: mailer, logger: logger}
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
		n.send(ctx, creatorID, "Your Dabet credits have run out",
			"Your credit balance has reached zero; messages are passing through unmoderated. Top up to resume moderation.")
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
		n.send(ctx, creatorID, "Your Dabet credits are running low",
			"Your credit balance is below 20% of your last top-up. Top up to avoid an interruption in moderation.")
	}
}

func (n *Notifier) send(ctx context.Context, creatorID, subject, body string) {
	if err := n.mailer.Send(ctx, creatorID, subject, body); err != nil {
		n.logger.Warn("notification send failed", "creator_id", creatorID, "error", err.Error())
	}
}
