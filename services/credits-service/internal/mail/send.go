package mail

import (
	"context"
	"fmt"
	"strings"
)

// Recipients resolves a creator id to the address the mail goes to.
// credits-service does not own identity.creators, so this is a seam: the
// Postgres implementation in internal/identity reads the address, and a
// future internal user-service API would drop in behind the same
// interface.
//
// The lookup runs on a mail worker, never on the caller's goroutine, so
// a slow or broken identity read cannot reach the ledger path.
type Recipients interface {
	Recipient(ctx context.Context, creatorID string) (email, fullname string, err error)
}

// balanceData is the data of both A8 templates. Numbers and a link — no
// message text (P4), no payment details, no tokens.
type balanceData struct {
	Name       string
	Balance    int64
	LastTopup  int64
	BillingURL string
}

// SendBalance queues one A8 threshold notification. It satisfies
// notify.TemplatedMailer, and like every other path in this package it
// returns as soon as the message is queued: the §5.7 ledger must never
// wait on a mail server.
func (m *Mailer) SendBalance(_ context.Context, creatorID, template string, balance, lastTopup int64) error {
	if !m.Enabled() {
		m.log.Info("credits notification not sent: mailer disabled", "template", template)
		m.logOnly(template)
		return nil
	}
	if m.recipients == nil {
		return fmt.Errorf("mail: no recipient resolver configured")
	}
	resolve := func(ctx context.Context) (Recipient, error) {
		email, fullname, err := m.recipients.Recipient(ctx, creatorID)
		if err != nil {
			return Recipient{}, err
		}
		return Recipient{Email: email, Name: fullname}, nil
	}
	return m.enqueue(job{
		template: template,
		resolve:  resolve,
		data: balanceData{
			// Name is filled by applyRecipient on the worker, once the
			// address lookup has produced one.
			Balance:    balance,
			LastTopup:  lastTopup,
			BillingURL: m.cfg.BillingURL,
		},
	})
}

// applyRecipient fills the greeting once the recipient is known: the
// address (and the name with it) is resolved on the worker, not when the
// message is queued.
func applyRecipient(data any, to Recipient) any {
	if b, ok := data.(balanceData); ok {
		b.Name = greeting(to.Name)
		return b
	}
	return data
}

// greeting keeps the salutation sensible when a name is missing.
func greeting(name string) string {
	if strings.TrimSpace(name) == "" {
		return "there"
	}
	return name
}
