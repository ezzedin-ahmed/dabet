package notify

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"dabet/services/credits-service/internal/ledger"
)

type templatedRecorder struct {
	sent []templatedCall
	err  error
}

type templatedCall struct {
	creatorID string
	template  string
	balance   int64
	lastTopup int64
}

func (r *templatedRecorder) SendBalance(_ context.Context, creatorID, template string, balance, lastTopup int64) error {
	r.sent = append(r.sent, templatedCall{creatorID, template, balance, lastTopup})
	return r.err
}

func templatedSetup(t *testing.T) (*ledger.Memory, *templatedRecorder, *Notifier) {
	t.Helper()
	mem := ledger.NewMemory()
	rec := &templatedRecorder{}
	return mem, rec, NewTemplated(mem, rec, slog.New(slog.DiscardHandler))
}

func TestTemplatedZeroCrossing(t *testing.T) {
	mem, rec, n := templatedSetup(t)
	mem.Apply(t.Context(), "c1", 100, ledger.ReasonTopup, "t1", nil)

	n.BalanceChanged(t.Context(), "c1", 5, -2)
	if len(rec.sent) != 1 {
		t.Fatalf("sent %d notifications, want 1", len(rec.sent))
	}
	got := rec.sent[0]
	if got.template != TemplateCreditsExhausted || got.creatorID != "c1" || got.balance != -2 {
		t.Errorf("unexpected call %+v", got)
	}
}

func TestTemplatedLowCrossingCarriesTheThresholdInputs(t *testing.T) {
	mem, rec, n := templatedSetup(t)
	mem.Apply(t.Context(), "c1", 1000, ledger.ReasonTopup, "t1", nil)

	n.BalanceChanged(t.Context(), "c1", 250, 180) // crosses 20% of 1000
	if len(rec.sent) != 1 {
		t.Fatalf("sent %d notifications, want 1", len(rec.sent))
	}
	got := rec.sent[0]
	if got.template != TemplateCreditsLow || got.balance != 180 || got.lastTopup != 1000 {
		t.Errorf("unexpected call %+v — the template needs both numbers", got)
	}
}

// §4.7: a mailer that cannot take the message is logged and dropped. The
// ledger has already committed; nothing about it may depend on this.
func TestTemplatedSendFailureIsSwallowed(t *testing.T) {
	mem, rec, n := templatedSetup(t)
	rec.err = errors.New("queue full")
	mem.Apply(t.Context(), "c1", 100, ledger.ReasonTopup, "t1", nil)

	n.BalanceChanged(t.Context(), "c1", 5, 0) // must not panic or block
	if len(rec.sent) != 1 {
		t.Fatalf("attempted %d sends, want 1", len(rec.sent))
	}
}

// Upward and non-crossing transitions behave identically on both paths.
func TestTemplatedIgnoresNonCrossings(t *testing.T) {
	mem, rec, n := templatedSetup(t)
	mem.Apply(t.Context(), "c1", 1000, ledger.ReasonTopup, "t1", nil)

	n.BalanceChanged(t.Context(), "c1", 100, 900) // upward
	n.BalanceChanged(t.Context(), "c1", 900, 800) // still above 20%
	n.BalanceChanged(t.Context(), "c1", 180, 150) // already below
	if len(rec.sent) != 0 {
		t.Errorf("unexpected notifications: %+v", rec.sent)
	}
}
