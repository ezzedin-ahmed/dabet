package cluster

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func newCountHarness(sink CountSink, maxRows int) (*CountBuffer, *prometheus.CounterVec, *prometheus.GaugeVec) {
	failOpen := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fail_open_total"}, []string{"component", "reason"})
	depUp := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "dependency_up"}, []string{"dependency"})
	return NewCountBuffer(sink, maxRows, time.Minute, time.Second, failOpen, depUp), failOpen, depUp
}

func TestBucketHourTruncation(t *testing.T) {
	in := time.Date(2026, 8, 19, 13, 59, 59, 999_000_000, time.FixedZone("UTC+2", 2*3600))
	got := BucketHour(in)
	want := time.Date(2026, 8, 19, 11, 0, 0, 0, time.UTC)
	if !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("BucketHour(%v) = %v, want %v", in, got, want)
	}
}

func TestCountBufferMergesIncrements(t *testing.T) {
	sink := &fakeSink{}
	b, _, _ := newCountHarness(sink, 1000)

	// Same key three times, one different theme, one different hour.
	b.Add("cr-1", "ct-1", "topic-1", ZeroUUID, t0)
	b.Add("cr-1", "ct-1", "topic-1", ZeroUUID, t0.Add(time.Minute))
	b.Add("cr-1", "ct-1", "topic-1", ZeroUUID, t0.Add(2*time.Minute))
	b.Add("cr-1", "ct-1", "topic-1", "theme-1", t0)
	b.Add("cr-1", "ct-1", "topic-1", ZeroUUID, t0.Add(time.Hour))

	if b.Len() != 3 {
		t.Fatalf("distinct rows = %d, want 3", b.Len())
	}
	b.Flush(context.Background())
	if len(sink.batches) != 1 {
		t.Fatalf("expected one batched insert, got %d", len(sink.batches))
	}
	rows := sink.batches[0]
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].BucketHour.Equal(rows[j].BucketHour) {
			return rows[i].BucketHour.Before(rows[j].BucketHour)
		}
		return rows[i].ThemeID < rows[j].ThemeID
	})
	if rows[0].ThemeID != ZeroUUID || rows[0].Count != 3 {
		t.Fatalf("merged row wrong: %+v", rows[0])
	}
	if rows[1].ThemeID != "theme-1" || rows[1].Count != 1 {
		t.Fatalf("theme row wrong: %+v", rows[1])
	}
	if rows[2].Count != 1 || !rows[2].BucketHour.Equal(time.Date(2026, 8, 19, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("next-hour row wrong: %+v", rows[2])
	}
	// Flush drained the buffer; a second flush inserts nothing.
	b.Flush(context.Background())
	if len(sink.batches) != 1 {
		t.Fatalf("empty flush must not insert, got %d batches", len(sink.batches))
	}
}

func TestCountBufferFlushesEarlyWhenFull(t *testing.T) {
	sink := &fakeSink{}
	b, _, _ := newCountHarness(sink, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()

	b.Add("cr-1", "ct-1", "topic-1", ZeroUUID, t0)
	b.Add("cr-1", "ct-1", "topic-2", ZeroUUID, t0)

	deadline := time.After(2 * time.Second)
	for b.Len() != 0 {
		select {
		case <-deadline:
			t.Fatal("size-triggered flush never happened")
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
	if len(sink.batches) == 0 || len(sink.batches[0]) != 2 {
		t.Fatalf("expected an early batched insert of 2 rows, got %+v", sink.batches)
	}
}

func TestCountBufferDrainsOnShutdown(t *testing.T) {
	sink := &fakeSink{}
	b, _, _ := newCountHarness(sink, 1000)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.Run(ctx); close(done) }()

	b.Add("cr-1", "ct-1", "topic-1", ZeroUUID, t0)
	cancel()
	<-done
	if len(sink.batches) != 1 || len(sink.batches[0]) != 1 {
		t.Fatalf("expected the final drain flush, got %+v", sink.batches)
	}
}

func TestCountBufferClickhouseDownDropsBatch(t *testing.T) {
	sink := &fakeSink{err: errors.New("clickhouse down")}
	b, failOpen, depUp := newCountHarness(sink, 1000)
	b.Add("cr-1", "ct-1", "topic-1", ZeroUUID, t0)
	b.Add("cr-1", "ct-2", "topic-1", ZeroUUID, t0)
	b.Flush(context.Background())

	if got := testutil.ToFloat64(failOpen.WithLabelValues("clickhouse", "insert_failed")); got != 2 {
		t.Fatalf("fail_open clickhouse = %v, want 2", got)
	}
	if got := testutil.ToFloat64(depUp.WithLabelValues("clickhouse")); got != 0 {
		t.Fatalf("dependency_up clickhouse = %v, want 0", got)
	}
	// The failed batch is dropped, not retried.
	if b.Len() != 0 {
		t.Fatalf("failed rows must be dropped, %d still buffered", b.Len())
	}
	sink.err = nil
	b.Add("cr-1", "ct-1", "topic-1", ZeroUUID, t0)
	b.Flush(context.Background())
	if got := testutil.ToFloat64(depUp.WithLabelValues("clickhouse")); got != 1 {
		t.Fatalf("dependency_up clickhouse = %v, want 1 after recovery", got)
	}
}
