package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func newAssignVecs() (*prometheus.CounterVec, *prometheus.GaugeVec) {
	failOpen := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fail_open_total"}, []string{"component", "reason"})
	depUp := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "dependency_up"}, []string{"dependency"})
	return failOpen, depUp
}

func assignRecs(n int) []EmbeddingRecord {
	recs := make([]EmbeddingRecord, n)
	for i := range recs {
		recs[i] = EmbeddingRecord{CreatorID: "cr-1", ContentID: "ct-1", EmbeddedAt: t0, Vector: []float32{1, 2, 3}}
	}
	return recs
}

// drain runs the worker until the queue is empty, then cancels it.
func drain(t *testing.T, a *AsyncAssigner) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Run(ctx); close(done) }()
	deadline := time.After(2 * time.Second)
	for len(a.queue) > 0 {
		select {
		case <-deadline:
			t.Fatal("assign queue never drained")
		case <-time.After(5 * time.Millisecond):
		}
	}
	time.Sleep(20 * time.Millisecond) // let the in-flight post finish
	cancel()
	<-done
}

func TestAssignerPostsBatch(t *testing.T) {
	var got []byte
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		got, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	failOpen, depUp := newAssignVecs()
	a := NewAsyncAssigner(srv.URL, time.Second, 8, failOpen, depUp)
	a.Send(assignRecs(2))
	drain(t, a)

	if path != "/internal/v1/assign" {
		t.Fatalf("posted to %q", path)
	}
	var req struct {
		Records []struct {
			CreatorID  string    `json:"creator_id"`
			ContentID  string    `json:"content_id"`
			EmbeddedAt time.Time `json:"embedded_at"`
			Vector     []float32 `json:"vector"`
		} `json:"records"`
	}
	if err := json.Unmarshal(got, &req); err != nil {
		t.Fatalf("bad body %s: %v", got, err)
	}
	if len(req.Records) != 2 || req.Records[0].CreatorID != "cr-1" || len(req.Records[0].Vector) != 3 {
		t.Fatalf("body = %s", got)
	}
	// P4: the wire carries creator/content/timestamp/vector only.
	for _, needle := range [][]byte{[]byte("author"), []byte("text"), []byte("message_id")} {
		if bytes.Contains(got, needle) {
			t.Fatalf("radioactive field %q on the assign wire", needle)
		}
	}
	if v := testutil.ToFloat64(depUp.WithLabelValues("clustering")); v != 1 {
		t.Fatalf("dependency_up clustering = %v, want 1", v)
	}
}

func TestAssignerClusteringDownFailsOpen(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	failOpen, depUp := newAssignVecs()
	a := NewAsyncAssigner(srv.URL, time.Second, 8, failOpen, depUp)
	a.Send(assignRecs(3))
	drain(t, a)

	if v := testutil.ToFloat64(failOpen.WithLabelValues("clustering", "request_failed")); v != 3 {
		t.Fatalf("fail_open clustering = %v, want 3", v)
	}
	if v := testutil.ToFloat64(depUp.WithLabelValues("clustering")); v != 0 {
		t.Fatalf("dependency_up clustering = %v, want 0", v)
	}
}

func TestAssignerFullQueueDropsWithoutBlocking(t *testing.T) {
	failOpen, _ := newAssignVecs()
	a := NewAsyncAssigner("http://127.0.0.1:0", time.Second, 1, failOpen, prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "d"}, []string{"dependency"}))
	// No worker running: first Send fills the queue, second must drop
	// immediately rather than block the pipeline goroutine.
	a.Send(assignRecs(1))
	donec := make(chan struct{})
	go func() {
		a.Send(assignRecs(2))
		close(donec)
	}()
	select {
	case <-donec:
	case <-time.After(time.Second):
		t.Fatal("Send blocked on a full queue")
	}
	if v := testutil.ToFloat64(failOpen.WithLabelValues("clustering", "queue_full")); v != 2 {
		t.Fatalf("fail_open queue_full = %v, want 2", v)
	}
}

// TestPipelineParquetPathUnaffectedByClusteringDown drives the full
// pipeline with the assign hook pointed at a dead endpoint and asserts the
// parquet path still lands the records in S3 — clustering being down is
// counted, never propagated (§4.7).
func TestPipelineParquetPathUnaffectedByClusteringDown(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	store := newMemStore()
	m := newTestMetrics()
	std := newTestObs()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	assigner := NewAsyncAssigner(srv.URL, time.Second, 8, std.FailOpenTotal, std.DependencyUp)
	p := NewPipeline(
		NewBuffer(2*time.Second, 1000, m),
		NewSampler(60, 60, 1000),
		NewBatcher(64, 250*time.Millisecond),
		NewEmbedder(&fakeEmbed{}, m, std.FailOpenTotal),
		NewRoller(store, 8<<20, time.Minute, m, std.FailOpenTotal),
		assigner,
		m, std, logger, 50*time.Millisecond,
	)
	ctx := context.Background()
	if err := p.HandleMessage(ctx, messageRecord(t, "m-1", "hello there")); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(10 * time.Second)
	p.step(ctx, later)
	p.step(ctx, later.Add(time.Second))
	p.step(ctx, later.Add(2*time.Minute))

	if len(store.keys()) != 1 {
		t.Fatalf("parquet path affected by clustering outage: objects %v", store.keys())
	}
	drain(t, assigner)
	if v := testutil.ToFloat64(std.FailOpenTotal.WithLabelValues("clustering", "request_failed")); v != 1 {
		t.Fatalf("fail_open clustering = %v, want 1", v)
	}
}
