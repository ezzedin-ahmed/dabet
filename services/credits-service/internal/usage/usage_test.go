package usage

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
	"dabet/pkg/obs"

	"dabet/services/credits-service/internal/ledger"
	"dabet/services/credits-service/internal/metrics"
)

func TestLoadRatesDefaults(t *testing.T) {
	r, err := LoadRates(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if r.MessagesProcessed != 0.001 || r.MessagesReclustered != 0.0001 {
		t.Fatalf("defaults wrong: %+v", r)
	}
}

func TestLoadRatesFromEnv(t *testing.T) {
	env := map[string]string{
		EnvCostMessagesProcessed:   "0.01",
		EnvCostMessagesReclustered: "0.002",
	}
	r, err := LoadRates(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if r.MessagesProcessed != 0.01 || r.MessagesReclustered != 0.002 {
		t.Fatalf("env rates wrong: %+v", r)
	}
}

func TestLoadRatesRejectsBadValues(t *testing.T) {
	for _, bad := range []string{"zero", "0", "-1", "nope", "+Inf"} {
		env := map[string]string{EnvCostMessagesProcessed: bad}
		if _, err := LoadRates(func(k string) string { return env[k] }); err == nil {
			t.Errorf("value %q: expected error", bad)
		}
	}
}

func TestDeltaMath(t *testing.T) {
	r := Rates{MessagesProcessed: 0.001, MessagesReclustered: 0.0001}
	cases := []struct {
		et   contracts.EventType
		qty  int64
		want int64
	}{
		{contracts.EventMessagesProcessed, 1000, -1}, // exactly one credit, no float noise
		{contracts.EventMessagesProcessed, 1, -1},    // minimum charge
		{contracts.EventMessagesProcessed, 1001, -2}, // ceil
		{contracts.EventMessagesProcessed, 2000, -2}, //
		{contracts.EventMessagesReclustered, 10000, -1},
		{contracts.EventMessagesReclustered, 10001, -2},
	}
	for _, c := range cases {
		got, ok := r.Delta(c.et, c.qty)
		if !ok || got != c.want {
			t.Errorf("Delta(%s, %d) = %d ok=%v, want %d", c.et, c.qty, got, ok, c.want)
		}
	}
	if _, ok := r.Delta("bogus", 10); ok {
		t.Error("unknown event type must not convert")
	}
}

func testConsumer(t *testing.T, repo ledger.Repository) (*Consumer, *metrics.Credits, *obs.Metrics) {
	t.Helper()
	reg := prometheus.NewRegistry()
	met := metrics.New(reg)
	obsMet := obs.NewMetrics(prometheus.NewRegistry())
	rates := Rates{MessagesProcessed: 0.001, MessagesReclustered: 0.0001}
	c := NewConsumer(repo, rates, met, obsMet, "credits-service", nil, slog.New(slog.DiscardHandler))
	return c, met, obsMet
}

func usageRecord(t *testing.T, ev contracts.Usage) *kgo.Record {
	t.Helper()
	b, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: contracts.TopicUsage, Value: b}
}

func TestHandleAppliesAndReplays(t *testing.T) {
	mem := ledger.NewMemory()
	c, met, _ := testConsumer(t, mem)
	ev := contracts.Usage{
		CreatorID:      "c1",
		EventType:      contracts.EventMessagesProcessed,
		Quantity:       2000,
		WindowStart:    time.Now().UTC(),
		WindowEnd:      time.Now().UTC(),
		IdempotencyKey: "mod:i-1:14:00:c1",
	}
	rec := usageRecord(t, ev)

	if err := c.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	balance, _, _, _ := mem.Balance(context.Background(), "c1")
	if balance != -2 {
		t.Fatalf("balance = %d, want -2", balance)
	}
	if got := testutil.ToFloat64(met.UsageEvents.WithLabelValues("messages_processed", "applied")); got != 1 {
		t.Fatalf("applied counter = %v, want 1", got)
	}

	// Redelivery: replayed outcome, balance unchanged.
	if err := c.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	balance, _, _, _ = mem.Balance(context.Background(), "c1")
	if balance != -2 {
		t.Fatalf("balance after replay = %d, want -2", balance)
	}
	if got := testutil.ToFloat64(met.UsageEvents.WithLabelValues("messages_processed", "replayed")); got != 1 {
		t.Fatalf("replayed counter = %v, want 1", got)
	}
	entries, _ := mem.Entries(context.Background(), "c1", 0, 10)
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	if entries[0].Reason != "messages_processed" || entries[0].IdempotencyKey != ev.IdempotencyKey {
		t.Fatalf("entry reason/key wrong: %+v", entries[0])
	}
}

func TestHandleSkipsMalformed(t *testing.T) {
	mem := ledger.NewMemory()
	c, _, obsMet := testConsumer(t, mem)

	bad := []*kgo.Record{
		{Topic: contracts.TopicUsage, Value: []byte("not json")},
		usageRecord(t, contracts.Usage{EventType: contracts.EventMessagesProcessed, Quantity: 10, IdempotencyKey: "k"}), // no creator
		usageRecord(t, contracts.Usage{CreatorID: "c1", EventType: contracts.EventMessagesProcessed, Quantity: 10}),     // no key
		usageRecord(t, contracts.Usage{CreatorID: "c1", EventType: contracts.EventMessagesProcessed, Quantity: 0, IdempotencyKey: "k"}),
		usageRecord(t, contracts.Usage{CreatorID: "c1", EventType: contracts.EventMessagesProcessed, Quantity: -5, IdempotencyKey: "k"}),
		usageRecord(t, contracts.Usage{CreatorID: "c1", EventType: "mystery", Quantity: 10, IdempotencyKey: "k"}),
	}
	for i, rec := range bad {
		if err := c.Handle(context.Background(), rec); err != nil {
			t.Fatalf("record %d: malformed events must never wedge the partition: %v", i, err)
		}
	}
	if got := testutil.ToFloat64(obsMet.KafkaConsumedTotal.WithLabelValues(contracts.TopicUsage, "credits-service", "malformed")); got != float64(len(bad)) {
		t.Fatalf("malformed counter = %v, want %d", got, len(bad))
	}
	if entries, _ := mem.Entries(context.Background(), "c1", 0, 10); len(entries) != 0 {
		t.Fatalf("malformed events must not write entries, got %d", len(entries))
	}
}

type failingRepo struct{ ledger.Repository }

func (failingRepo) Apply(context.Context, string, int64, string, string, map[string]any) (ledger.ApplyResult, error) {
	return ledger.ApplyResult{}, errors.New("db down")
}

func TestHandleReturnsStorageErrors(t *testing.T) {
	c, _, _ := testConsumer(t, failingRepo{})
	rec := usageRecord(t, contracts.Usage{
		CreatorID: "c1", EventType: contracts.EventMessagesProcessed,
		Quantity: 10, IdempotencyKey: "k",
	})
	if err := c.Handle(context.Background(), rec); err == nil {
		t.Fatal("storage errors must be returned so the batch is redelivered")
	}
}
