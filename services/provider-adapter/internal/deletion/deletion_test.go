package deletion

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

	"dabet/services/provider-adapter/internal/connsource"
	"dabet/services/provider-adapter/internal/driver"
	"dabet/services/provider-adapter/internal/metrics"
	"dabet/services/provider-adapter/internal/opaque"
)

// fakeDriver returns scripted errors, one per Delete call; when the script
// is exhausted, Delete succeeds.
type fakeDriver struct {
	script   []error
	calls    int
	lastConn driver.Connection
}

func (f *fakeDriver) Platform() string { return "mock" }
func (f *fakeDriver) Watch(context.Context, driver.Connection, chan<- driver.Message) error {
	return driver.ErrNotImplemented
}
func (f *fakeDriver) DiscoverLive(context.Context, driver.Connection) ([]driver.ContentRef, error) {
	return nil, driver.ErrNotImplemented
}
func (f *fakeDriver) Delete(_ context.Context, conn driver.Connection, _, _ string) error {
	f.lastConn = conn
	f.calls++
	if f.calls <= len(f.script) {
		return f.script[f.calls-1]
	}
	return nil
}

type fakeRefresher struct {
	calls int
	err   error
	token string
}

func (f *fakeRefresher) Refresh(_ context.Context, conn driver.Connection) (driver.Connection, error) {
	f.calls++
	if f.err != nil {
		return conn, f.err
	}
	conn.AccessToken = f.token
	return conn, nil
}

type fixture struct {
	proc   *Processor
	drv    *fakeDriver
	ref    *fakeRefresher
	m      *metrics.Metrics
	om     *obs.Metrics
	sleeps *[]time.Duration
}

func newFixture(t *testing.T, script ...error) *fixture {
	t.Helper()
	drv := &fakeDriver{script: script}
	reg := driver.NewRegistry()
	reg.Register(drv)
	src := connsource.NewStatic(driver.Connection{
		ID: "conn-1", CreatorID: "creator-1", Platform: "mock", AccessToken: "old-token",
	})
	promReg := prometheus.NewRegistry()
	m := metrics.New(promReg)
	om := obs.NewMetrics(prometheus.NewRegistry())
	ref := &fakeRefresher{token: "new-token"}
	log := slog.New(slog.DiscardHandler)
	p := NewProcessor(reg, src, ref, opaque.Platform, m, om, log)
	sleeps := &[]time.Duration{}
	p.Sleep = func(_ context.Context, d time.Duration) error {
		*sleeps = append(*sleeps, d)
		return nil
	}
	p.Jitter = func() float64 { return 0.5 } // deterministic: jittered delay == base delay
	return &fixture{proc: p, drv: drv, ref: ref, m: m, om: om, sleeps: sleeps}
}

func record(t *testing.T, del contracts.Deletion) *kgo.Record {
	t.Helper()
	b, err := json.Marshal(del)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: contracts.TopicDeletions, Key: contracts.DeletionsKey(del.ContentID), Value: b}
}

func mockDeletion(t *testing.T) contracts.Deletion {
	t.Helper()
	contentID, err := opaque.MintContentID("mock", "channel-1")
	if err != nil {
		t.Fatal(err)
	}
	messageID, _, err := opaque.MintMessageID("mock", "native-msg-1")
	if err != nil {
		t.Fatal(err)
	}
	return contracts.Deletion{
		MessageID: messageID,
		ContentID: contentID,
		CreatorID: "creator-1",
		Reason:    contracts.DetectorRestrictedWord,
		IssuedAt:  time.Now().UTC(),
	}
}

func (f *fixture) outcome(t *testing.T, platform, outcome string) float64 {
	t.Helper()
	return testutil.ToFloat64(f.m.DeletionsTotal.WithLabelValues(platform, outcome))
}

func handle(t *testing.T, f *fixture) {
	t.Helper()
	if err := f.proc.Handle(context.Background(), record(t, mockDeletion(t))); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

func TestDeleteSuccess(t *testing.T) {
	f := newFixture(t)
	handle(t, f)
	if f.drv.calls != 1 {
		t.Errorf("calls = %d, want 1", f.drv.calls)
	}
	if got := f.outcome(t, "mock", OutcomeOK); got != 1 {
		t.Errorf("outcome ok = %v, want 1", got)
	}
}

func TestDeleteNotFoundIsSuccess(t *testing.T) {
	f := newFixture(t, driver.ErrNotFound)
	handle(t, f)
	if f.drv.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on not-found)", f.drv.calls)
	}
	if got := f.outcome(t, "mock", OutcomeNotFound); got != 1 {
		t.Errorf("outcome not_found = %v, want 1", got)
	}
}

func TestDeleteGoneIsTerminal(t *testing.T) {
	f := newFixture(t, driver.ErrGone)
	handle(t, f)
	if f.drv.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on gone)", f.drv.calls)
	}
	if got := f.outcome(t, "mock", OutcomeGone); got != 1 {
		t.Errorf("outcome gone = %v, want 1", got)
	}
}

func TestRateLimitedRetriesWithJitterThenSucceeds(t *testing.T) {
	f := newFixture(t, driver.ErrRateLimited, driver.ErrRateLimited)
	handle(t, f)
	if f.drv.calls != 3 {
		t.Errorf("calls = %d, want 3", f.drv.calls)
	}
	if got := f.outcome(t, "mock", OutcomeOK); got != 1 {
		t.Errorf("outcome ok = %v, want 1", got)
	}
	// With Jitter=0.5 the jittered delay equals the exponential step.
	want := []time.Duration{f.proc.BaseBackoff, 2 * f.proc.BaseBackoff}
	if len(*f.sleeps) != len(want) {
		t.Fatalf("sleeps = %v, want %v", *f.sleeps, want)
	}
	for i, d := range want {
		if (*f.sleeps)[i] != d {
			t.Errorf("sleep[%d] = %v, want %v", i, (*f.sleeps)[i], d)
		}
	}
}

func TestRateLimitedExhaustsAttempts(t *testing.T) {
	f := newFixture(t,
		driver.ErrRateLimited, driver.ErrRateLimited, driver.ErrRateLimited,
		driver.ErrRateLimited, driver.ErrRateLimited, driver.ErrRateLimited)
	handle(t, f)
	if f.drv.calls != f.proc.MaxAttempts {
		t.Errorf("calls = %d, want %d", f.drv.calls, f.proc.MaxAttempts)
	}
	if got := f.outcome(t, "mock", OutcomeDropped); got != 1 {
		t.Errorf("outcome dropped = %v, want 1", got)
	}
}

func TestUnauthorizedRefreshesAndRetriesOnce(t *testing.T) {
	f := newFixture(t, driver.ErrUnauthorized)
	handle(t, f)
	if f.ref.calls != 1 {
		t.Errorf("refresher calls = %d, want 1", f.ref.calls)
	}
	if f.drv.calls != 2 {
		t.Errorf("calls = %d, want 2 (retry once after refresh)", f.drv.calls)
	}
	if f.drv.lastConn.AccessToken != "new-token" {
		t.Errorf("retry used token %q, want refreshed token", f.drv.lastConn.AccessToken)
	}
	if got := f.outcome(t, "mock", OutcomeOK); got != 1 {
		t.Errorf("outcome ok = %v, want 1", got)
	}
	if len(*f.sleeps) != 0 {
		t.Errorf("refresh retry should be immediate, slept %v", *f.sleeps)
	}
}

func TestUnauthorizedTwiceIsAuthFailed(t *testing.T) {
	f := newFixture(t, driver.ErrUnauthorized, driver.ErrUnauthorized)
	handle(t, f)
	if f.ref.calls != 1 {
		t.Errorf("refresher calls = %d, want exactly 1", f.ref.calls)
	}
	if f.drv.calls != 2 {
		t.Errorf("calls = %d, want 2", f.drv.calls)
	}
	if got := f.outcome(t, "mock", OutcomeAuthFailed); got != 1 {
		t.Errorf("outcome auth_failed = %v, want 1", got)
	}
}

func TestRefreshFailureIsAuthFailed(t *testing.T) {
	f := newFixture(t, driver.ErrUnauthorized)
	f.ref.err = errors.New("refresh token revoked")
	handle(t, f)
	if f.drv.calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry without fresh token)", f.drv.calls)
	}
	if got := f.outcome(t, "mock", OutcomeAuthFailed); got != 1 {
		t.Errorf("outcome auth_failed = %v, want 1", got)
	}
}

func TestTransientErrorsBackOffExponentiallyThenDrop(t *testing.T) {
	boom := errors.New("provider returned status 503")
	f := newFixture(t, boom, boom, boom, boom, boom, boom)
	handle(t, f)
	if f.drv.calls != f.proc.MaxAttempts {
		t.Errorf("calls = %d, want %d", f.drv.calls, f.proc.MaxAttempts)
	}
	if got := f.outcome(t, "mock", OutcomeDropped); got != 1 {
		t.Errorf("outcome dropped = %v, want 1", got)
	}
	base := f.proc.BaseBackoff
	want := []time.Duration{base, 2 * base, 4 * base, 8 * base}
	if len(*f.sleeps) != len(want) {
		t.Fatalf("sleeps = %v, want %v", *f.sleeps, want)
	}
	for i, d := range want {
		if (*f.sleeps)[i] != d {
			t.Errorf("sleep[%d] = %v, want %v", i, (*f.sleeps)[i], d)
		}
	}
	if got := testutil.ToFloat64(f.om.FailOpenTotal.WithLabelValues("provider-adapter", "deletion_dropped")); got != 1 {
		t.Errorf("fail_open_total = %v, want 1", got)
	}
}

func TestTransientErrorThenSuccess(t *testing.T) {
	f := newFixture(t, errors.New("connection reset"))
	handle(t, f)
	if f.drv.calls != 2 {
		t.Errorf("calls = %d, want 2", f.drv.calls)
	}
	if got := f.outcome(t, "mock", OutcomeOK); got != 1 {
		t.Errorf("outcome ok = %v, want 1", got)
	}
}

func TestBackoffCapsAtMax(t *testing.T) {
	f := newFixture(t)
	f.proc.BaseBackoff = time.Second
	f.proc.MaxBackoff = 3 * time.Second
	if d := f.proc.backoff(4); d != 3*time.Second {
		t.Errorf("backoff(4) = %v, want cap %v", d, 3*time.Second)
	}
}

func TestUnroutableContentID(t *testing.T) {
	f := newFixture(t)
	del := mockDeletion(t)
	del.ContentID = "9d4e-not-an-opaque-id"
	if err := f.proc.Handle(context.Background(), record(t, del)); err != nil {
		t.Fatal(err)
	}
	if f.drv.calls != 0 {
		t.Errorf("driver called for unroutable deletion")
	}
	if got := f.outcome(t, "unknown", OutcomeUnroutable); got != 1 {
		t.Errorf("outcome unroutable = %v, want 1", got)
	}
}

func TestNoConnectionForCreator(t *testing.T) {
	f := newFixture(t)
	del := mockDeletion(t)
	del.CreatorID = "creator-without-connection"
	if err := f.proc.Handle(context.Background(), record(t, del)); err != nil {
		t.Fatal(err)
	}
	if got := f.outcome(t, "mock", OutcomeNoConnection); got != 1 {
		t.Errorf("outcome no_connection = %v, want 1", got)
	}
}

func TestInvalidRecordIsCountedAndSkipped(t *testing.T) {
	f := newFixture(t)
	rec := &kgo.Record{Topic: contracts.TopicDeletions, Value: []byte("not json")}
	if err := f.proc.Handle(context.Background(), rec); err != nil {
		t.Fatal(err)
	}
	if got := f.outcome(t, "unknown", OutcomeInvalid); got != 1 {
		t.Errorf("outcome invalid = %v, want 1", got)
	}
}

func TestCancelledContextPropagates(t *testing.T) {
	f := newFixture(t, driver.ErrRateLimited, driver.ErrRateLimited)
	ctx, cancel := context.WithCancel(context.Background())
	f.proc.Sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}
	err := f.proc.Handle(ctx, record(t, mockDeletion(t)))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Handle = %v, want context.Canceled so the record is not committed", err)
	}
}
