package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"dabet/pkg/contracts"
)

func newTestPipeline(t *testing.T, fe EmbedClient, store ObjectStore) (*Pipeline, *Metrics) {
	t.Helper()
	m := newTestMetrics()
	std := newTestObs()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	p := NewPipeline(
		NewBuffer(2*time.Second, 1000, m),
		NewSampler(60, 60, 1000),
		NewBatcher(64, 250*time.Millisecond),
		NewEmbedder(fe, m, std.FailOpenTotal),
		NewRoller(store, 8<<20, time.Minute, m, std.FailOpenTotal),
		m, std, logger, 50*time.Millisecond,
	)
	return p, m
}

func messageRecord(t *testing.T, id, text string) *kgo.Record {
	t.Helper()
	v, err := json.Marshal(contracts.Message{
		MessageID:  id,
		ContentID:  "ct-1",
		AuthorID:   "sd-author-radioactive",
		CreatorID:  "cr-1",
		Text:       text,
		IngestedAt: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: contracts.TopicMessages, Value: v}
}

func flaggedRecord(t *testing.T, id string) *kgo.Record {
	t.Helper()
	v, err := json.Marshal(contracts.Flagged{
		MessageID: id,
		ContentID: "ct-1",
		AuthorID:  "sd-author-radioactive",
		CreatorID: "cr-1",
		Text:      "the flagged text",
		Detector:  contracts.DetectorRestrictedContent,
		Action:    contracts.ActionAutoDelete,
		FlaggedAt: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: contracts.TopicFlagged, Value: v}
}

// TestPipelineEndToEnd drives a clean message and a flagged message through
// handler → buffer → sampler → batcher → embedder → roller and asserts the
// parquet on the fake store holds exactly the clean message's record — with
// creator/content/timestamp/vector and nothing radioactive.
func TestPipelineEndToEnd(t *testing.T) {
	store := newMemStore()
	p, _ := newTestPipeline(t, &fakeEmbed{}, store)
	ctx := context.Background()

	if err := p.HandleMessage(ctx, messageRecord(t, "m-clean", "what a great goal")); err != nil {
		t.Fatal(err)
	}
	if err := p.HandleMessage(ctx, messageRecord(t, "m-bad", "spam spam spam")); err != nil {
		t.Fatal(err)
	}
	if err := p.HandleFlagged(ctx, flaggedRecord(t, "m-bad")); err != nil {
		t.Fatal(err)
	}

	// The handlers stamp wall-clock arrival; drive the loop far in the
	// future so the window, linger, and roll age have all elapsed.
	later := time.Now().Add(10 * time.Second)
	p.step(ctx, later)                    // releases m-clean, batches it
	p.step(ctx, later.Add(time.Second))   // linger expires → embed → roller
	p.step(ctx, later.Add(2*time.Minute)) // roll age expires → S3

	keys := store.keys()
	if len(keys) != 1 {
		t.Fatalf("expected one parquet object, got %v", keys)
	}
	recs := decodeRecords(t, store.objects[keys[0]])
	if len(recs) != 1 {
		t.Fatalf("expected exactly the clean record, got %d", len(recs))
	}
	if recs[0].CreatorID != "cr-1" || recs[0].ContentID != "ct-1" {
		t.Fatalf("unexpected record: %+v", recs[0])
	}
	// P4: the object must not contain the message text or the author id
	// anywhere, not even outside the schema.
	raw := store.objects[keys[0]]
	for _, needle := range [][]byte{[]byte("what a great goal"), []byte("spam spam"), []byte("sd-author-radioactive"), []byte("m-clean")} {
		if bytes.Contains(raw, needle) {
			t.Fatalf("radioactive bytes %q found in parquet object", needle)
		}
	}
}

func TestPipelineSamplerDropsBeyondCeiling(t *testing.T) {
	store := newMemStore()
	p, m := newTestPipeline(t, &fakeEmbed{}, store)
	ctx := context.Background()

	for i := 0; i < 100; i++ { // capacity is 60
		if err := p.HandleMessage(ctx, messageRecord(t, fmt.Sprintf("m%d", i), "hi")); err != nil {
			t.Fatal(err)
		}
	}
	later := time.Now().Add(10 * time.Second)
	p.step(ctx, later)
	p.step(ctx, later.Add(time.Second))

	if v := testutil.ToFloat64(m.DroppedTotal.WithLabelValues(DropReasonSampled)); v != 40 {
		t.Fatalf("dropped{sampled} = %v, want 40", v)
	}
	p.step(ctx, later.Add(2*time.Minute))
	total := 0
	for _, k := range store.keys() {
		total += len(decodeRecords(t, store.objects[k]))
	}
	if total != 60 {
		t.Fatalf("embedded %d records, want the 60 sampled ones", total)
	}
}

// TestPipelineEmbedFailureDropsBatch: the pipeline survives an embedding
// outage — the batch is dropped, nothing reaches S3, nothing crashes.
func TestPipelineEmbedFailureDropsBatch(t *testing.T) {
	store := newMemStore()
	p, m := newTestPipeline(t, &fakeEmbed{err: fmt.Errorf("embedding down")}, store)
	ctx := context.Background()

	if err := p.HandleMessage(ctx, messageRecord(t, "m1", "hello")); err != nil {
		t.Fatal(err)
	}
	later := time.Now().Add(10 * time.Second)
	p.step(ctx, later)
	p.step(ctx, later.Add(time.Second))
	p.step(ctx, later.Add(2*time.Minute))

	if len(store.keys()) != 0 {
		t.Fatalf("failed batch must not reach S3, got %v", store.keys())
	}
	if v := testutil.ToFloat64(m.EmbedRequestsTotal.WithLabelValues("error")); v != 1 {
		t.Fatalf("embedding_requests_total{error} = %v, want 1", v)
	}
}

func TestPipelineMalformedRecordsAreSkippedNotFatal(t *testing.T) {
	store := newMemStore()
	p, _ := newTestPipeline(t, &fakeEmbed{}, store)
	ctx := context.Background()

	if err := p.HandleMessage(ctx, &kgo.Record{Topic: contracts.TopicMessages, Value: []byte("{not json")}); err != nil {
		t.Fatalf("malformed message must not error (would block the partition): %v", err)
	}
	if err := p.HandleFlagged(ctx, &kgo.Record{Topic: contracts.TopicFlagged, Value: []byte("{not json")}); err != nil {
		t.Fatalf("malformed flag must not error: %v", err)
	}
}

// TestPipelineFlagForUnseenMessageCountsContamination covers both the
// late-flag case and the cross-instance case (§8.3): the flag's message is
// not in this buffer, so the contamination estimate moves.
func TestPipelineFlagForUnseenMessageCountsContamination(t *testing.T) {
	store := newMemStore()
	p, m := newTestPipeline(t, &fakeEmbed{}, store)

	if err := p.HandleFlagged(context.Background(), flaggedRecord(t, "m-elsewhere")); err != nil {
		t.Fatal(err)
	}
	if v := testutil.ToFloat64(m.ContaminationTotal); v != 1 {
		t.Fatalf("contamination = %v, want 1", v)
	}
}
