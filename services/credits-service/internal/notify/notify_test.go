package notify

import (
	"context"
	"log/slog"
	"testing"

	"dabet/services/credits-service/internal/ledger"
)

type recordingMailer struct {
	sent []string // subjects
}

func (r *recordingMailer) Send(_ context.Context, _, subject, _ string) error {
	r.sent = append(r.sent, subject)
	return nil
}

func setup(t *testing.T) (*ledger.Memory, *recordingMailer, *Notifier) {
	t.Helper()
	mem := ledger.NewMemory()
	m := &recordingMailer{}
	return mem, m, New(mem, m, slog.New(slog.DiscardHandler))
}

func TestZeroCrossingNotifies(t *testing.T) {
	mem, m, n := setup(t)
	mem.Apply(t.Context(), "c1", 100, ledger.ReasonTopup, "t1", nil)

	n.BalanceChanged(t.Context(), "c1", 5, -2)
	if len(m.sent) != 1 {
		t.Fatalf("sent %d notifications, want 1", len(m.sent))
	}
	// A further drop below zero is not a crossing.
	n.BalanceChanged(t.Context(), "c1", -2, -10)
	if len(m.sent) != 1 {
		t.Fatalf("repeated zero notifications: %v", m.sent)
	}
}

func TestTwentyPercentCrossingNotifies(t *testing.T) {
	mem, m, n := setup(t)
	mem.Apply(t.Context(), "c1", 100, ledger.ReasonTopup, "t1", nil)

	// 100-credit top-up: threshold 20.
	n.BalanceChanged(t.Context(), "c1", 25, 21) // above threshold, no send
	if len(m.sent) != 0 {
		t.Fatalf("premature notification: %v", m.sent)
	}
	n.BalanceChanged(t.Context(), "c1", 21, 19) // crossing
	if len(m.sent) != 1 {
		t.Fatalf("sent %d, want 1", len(m.sent))
	}
	n.BalanceChanged(t.Context(), "c1", 19, 15) // already below, no repeat
	if len(m.sent) != 1 {
		t.Fatalf("repeated low notifications: %v", m.sent)
	}
}

func TestNoTopupNoLowNotification(t *testing.T) {
	_, m, n := setup(t)
	n.BalanceChanged(t.Context(), "c1", 5, 3)
	if len(m.sent) != 0 {
		t.Fatalf("creator without top-ups must not get low warnings: %v", m.sent)
	}
}

func TestUpwardChangesIgnored(t *testing.T) {
	mem, m, n := setup(t)
	mem.Apply(t.Context(), "c1", 100, ledger.ReasonTopup, "t1", nil)
	n.BalanceChanged(t.Context(), "c1", -5, 100)
	n.BalanceChanged(t.Context(), "c1", 10, 10)
	if len(m.sent) != 0 {
		t.Fatalf("upward or flat changes must not notify: %v", m.sent)
	}
}
