package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"dabet/pkg/embeddings"
	"dabet/pkg/obs"
)

// fakeEmbed is a canned EmbedClient.
type fakeEmbed struct {
	err   error
	dims  int
	calls [][]string
}

func (f *fakeEmbed) Embed(_ context.Context, texts []string) ([][]float32, error) {
	f.calls = append(f.calls, texts)
	if f.err != nil {
		return nil, f.err
	}
	dims := f.dims
	if dims == 0 {
		dims = embeddings.Dimensions
	}
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = make([]float32, dims)
		out[i][0] = float32(i + 1)
	}
	return out, nil
}

func newTestObs() *obs.Metrics { return obs.NewMetrics(prometheus.NewRegistry()) }

func TestEmbedBatchSuccess(t *testing.T) {
	m := newTestMetrics()
	std := newTestObs()
	fe := &fakeEmbed{}
	e := NewEmbedder(fe, m, std.FailOpenTotal)

	msgs := []BufferedMessage{
		{MessageID: "m1", CreatorID: "cr-1", ContentID: "ct-1", Text: "one"},
		{MessageID: "m2", CreatorID: "cr-2", ContentID: "ct-2", Text: "two"},
	}
	recs := e.EmbedBatch(context.Background(), msgs, t0)
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	for i, r := range recs {
		if r.CreatorID != msgs[i].CreatorID || r.ContentID != msgs[i].ContentID || !r.EmbeddedAt.Equal(t0) {
			t.Fatalf("record %d mismatch: %+v", i, r)
		}
		if len(r.Vector) != embeddings.Dimensions {
			t.Fatalf("record %d has %d dims", i, len(r.Vector))
		}
	}
	if len(fe.calls) != 1 || len(fe.calls[0]) != 2 || fe.calls[0][0] != "one" {
		t.Fatalf("embed called with %v", fe.calls)
	}
	if v := testutil.ToFloat64(m.EmbedRequestsTotal.WithLabelValues("ok")); v != 1 {
		t.Fatalf("embedding_requests_total{ok} = %v, want 1", v)
	}
}

// TestEmbedBatchFailureFailsOpen: an embedding-service failure drops the
// batch — no blocking, no crash, no retry — and counts it (§4.7).
func TestEmbedBatchFailureFailsOpen(t *testing.T) {
	m := newTestMetrics()
	std := newTestObs()
	e := NewEmbedder(&fakeEmbed{err: errors.New("boom")}, m, std.FailOpenTotal)

	recs := e.EmbedBatch(context.Background(), []BufferedMessage{testMsg("m1"), testMsg("m2"), testMsg("m3")}, t0)
	if recs != nil {
		t.Fatalf("failed batch must yield no records, got %v", recs)
	}
	if v := testutil.ToFloat64(m.EmbedRequestsTotal.WithLabelValues("error")); v != 1 {
		t.Fatalf("embedding_requests_total{error} = %v, want 1", v)
	}
	if v := testutil.ToFloat64(std.FailOpenTotal.WithLabelValues("embedding", "request_failed")); v != 3 {
		t.Fatalf("fail_open_total{embedding} = %v, want 3 (one per dropped message)", v)
	}
}

func TestEmbedBatchWrongDimensionsFailsOpen(t *testing.T) {
	m := newTestMetrics()
	std := newTestObs()
	e := NewEmbedder(&fakeEmbed{dims: 8}, m, std.FailOpenTotal)

	if recs := e.EmbedBatch(context.Background(), []BufferedMessage{testMsg("m1")}, t0); recs != nil {
		t.Fatalf("wrong-dimension batch must be dropped, got %v", recs)
	}
	if v := testutil.ToFloat64(m.EmbedRequestsTotal.WithLabelValues("error")); v != 1 {
		t.Fatalf("embedding_requests_total{error} = %v, want 1", v)
	}
}

func TestEmbedBatchEmptyIsNoop(t *testing.T) {
	m := newTestMetrics()
	std := newTestObs()
	fe := &fakeEmbed{}
	e := NewEmbedder(fe, m, std.FailOpenTotal)
	if recs := e.EmbedBatch(context.Background(), nil, t0); recs != nil {
		t.Fatal("empty batch must be a no-op")
	}
	if len(fe.calls) != 0 {
		t.Fatal("empty batch must not call the embedding service")
	}
}
