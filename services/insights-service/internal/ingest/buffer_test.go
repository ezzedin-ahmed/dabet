package ingest

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Tests drive every stage with explicit times — there is no wall clock and
// no sleeping anywhere in this package's tests.

func newTestMetrics() *Metrics {
	return NewMetrics(prometheus.NewRegistry())
}

func testMsg(id string) BufferedMessage {
	return BufferedMessage{MessageID: id, CreatorID: "cr-1", ContentID: "ct-1", Text: "hello world"}
}

var t0 = time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)

func TestBufferReleasesCleanMessageAtDeadline(t *testing.T) {
	m := newTestMetrics()
	b := NewBuffer(2*time.Second, 10, m)
	b.Add(testMsg("m1"), t0)

	if got := b.PopDue(t0.Add(1999 * time.Millisecond)); len(got) != 0 {
		t.Fatalf("released %d messages before the window elapsed", len(got))
	}
	got := b.PopDue(t0.Add(2 * time.Second))
	if len(got) != 1 || got[0].MessageID != "m1" {
		t.Fatalf("expected m1 released at deadline, got %v", got)
	}
	if n := b.Len(); n != 0 {
		t.Fatalf("buffer should be empty, has %d", n)
	}
}

func TestBufferFlagBeforeDeadlineDropsAsFlagged(t *testing.T) {
	m := newTestMetrics()
	b := NewBuffer(2*time.Second, 10, m)
	b.Add(testMsg("m1"), t0)

	if !b.Flag("m1") {
		t.Fatal("flag within the window should drop the buffered message")
	}
	if got := b.PopDue(t0.Add(time.Minute)); len(got) != 0 {
		t.Fatalf("flagged message must never be released, got %v", got)
	}
	if v := testutil.ToFloat64(m.DroppedTotal.WithLabelValues(DropReasonFlagged)); v != 1 {
		t.Fatalf("dropped{flagged} = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.ContaminationTotal); v != 0 {
		t.Fatalf("contamination = %v, want 0", v)
	}
}

func TestBufferFlagAfterReleaseCountsContamination(t *testing.T) {
	m := newTestMetrics()
	b := NewBuffer(2*time.Second, 10, m)
	b.Add(testMsg("m1"), t0)
	b.PopDue(t0.Add(2 * time.Second)) // m1 released (embedded)

	if b.Flag("m1") {
		t.Fatal("flag after release should not report a drop")
	}
	if v := testutil.ToFloat64(m.ContaminationTotal); v != 1 {
		t.Fatalf("contamination = %v, want 1", v)
	}
	if v := testutil.ToFloat64(m.DroppedTotal.WithLabelValues(DropReasonFlagged)); v != 0 {
		t.Fatalf("dropped{flagged} = %v, want 0", v)
	}
}

func TestBufferRedeliveryIgnored(t *testing.T) {
	m := newTestMetrics()
	b := NewBuffer(2*time.Second, 10, m)
	b.Add(testMsg("m1"), t0)
	b.Add(testMsg("m1"), t0.Add(time.Second)) // Kafka redelivery

	if got := b.PopDue(t0.Add(2 * time.Second)); len(got) != 1 {
		t.Fatalf("redelivered message must be buffered once, released %d", len(got))
	}
}

func TestBufferOverflowReleasesOldestEarly(t *testing.T) {
	m := newTestMetrics()
	b := NewBuffer(2*time.Second, 2, m)
	b.Add(testMsg("m1"), t0)
	b.Add(testMsg("m2"), t0)
	b.Add(testMsg("m3"), t0) // overflow: m1 evicted, queued for release

	if n := b.Len(); n != 2 {
		t.Fatalf("buffer size %d exceeds bound 2", n)
	}
	got := b.PopDue(t0) // nothing due yet, but the evicted m1 is ready
	if len(got) != 1 || got[0].MessageID != "m1" {
		t.Fatalf("expected evicted m1 released early, got %v", got)
	}
	if got := b.PopDue(t0.Add(2 * time.Second)); len(got) != 2 {
		t.Fatalf("expected m2,m3 at deadline, got %v", got)
	}
}

func TestBufferDrainCountsRestartDrops(t *testing.T) {
	m := newTestMetrics()
	b := NewBuffer(2*time.Second, 10, m)
	b.Add(testMsg("m1"), t0)
	b.Add(testMsg("m2"), t0)

	if n := b.Drain(); n != 2 {
		t.Fatalf("drained %d, want 2", n)
	}
	if v := testutil.ToFloat64(m.DroppedTotal.WithLabelValues(DropReasonRestart)); v != 2 {
		t.Fatalf("dropped{restart} = %v, want 2", v)
	}
	if v := testutil.ToFloat64(m.Buffered); v != 0 {
		t.Fatalf("buffered gauge = %v, want 0", v)
	}
}

func TestBufferedGaugeTracksSize(t *testing.T) {
	m := newTestMetrics()
	b := NewBuffer(2*time.Second, 10, m)
	b.Add(testMsg("m1"), t0)
	b.Add(testMsg("m2"), t0)
	if v := testutil.ToFloat64(m.Buffered); v != 2 {
		t.Fatalf("buffered gauge = %v, want 2", v)
	}
	b.Flag("m1")
	if v := testutil.ToFloat64(m.Buffered); v != 1 {
		t.Fatalf("buffered gauge = %v, want 1", v)
	}
}
