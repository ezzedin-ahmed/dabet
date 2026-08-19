package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// memStore is an in-memory ObjectStore.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	err     error
}

func newMemStore() *memStore { return &memStore{objects: make(map[string][]byte)} }

func (s *memStore) Put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	s.objects[key] = append([]byte(nil), data...)
	return nil
}

func (s *memStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.objects))
	for k := range s.objects {
		out = append(out, k)
	}
	return out
}

func testRecs(creator string, n int, at time.Time) []EmbeddingRecord {
	out := make([]EmbeddingRecord, n)
	for i := range out {
		out[i] = EmbeddingRecord{CreatorID: creator, ContentID: "ct-1", EmbeddedAt: at, Vector: testVector(int64(i))}
	}
	return out
}

func decodeRecords(t *testing.T, data []byte) []EmbeddingRecord {
	t.Helper()
	r := parquet.NewGenericReader[EmbeddingRecord](bytes.NewReader(data))
	defer r.Close()
	out := make([]EmbeddingRecord, r.NumRows())
	if _, err := r.Read(out); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return out
}

var keyRe = regexp.MustCompile(`^creator_id=cr-1/date=2026-08-19/embeddings-\d+-\d+\.parquet$`)

func TestRollerRollsBySize(t *testing.T) {
	m := newTestMetrics()
	std := newTestObs()
	store := newMemStore()
	// One 384-dim fp32 record is ~1.5KB; 8KB rolls after ~6 records.
	r := NewRoller(store, 8<<10, time.Minute, m, std.FailOpenTotal)

	r.Add(context.Background(), testRecs("cr-1", 20, t0), t0)
	keys := store.keys()
	if len(keys) < 2 {
		t.Fatalf("expected multiple size-rolled files, got %v", keys)
	}
	total := 0
	for _, k := range keys {
		if !keyRe.MatchString(k) {
			t.Fatalf("object key %q not partitioned creator_id/date", k)
		}
		total += len(decodeRecords(t, store.objects[k]))
	}
	// Remaining records may still be open (below both thresholds).
	r.FlushAll(context.Background())
	total = 0
	for _, k := range store.keys() {
		total += len(decodeRecords(t, store.objects[k]))
	}
	if total != 20 {
		t.Fatalf("wrote %d records across files, want 20", total)
	}
	if v := testutil.ToFloat64(m.S3BytesWritten); v <= 0 {
		t.Fatal("s3_embedding_bytes_written_total not incremented")
	}
}

func TestRollerRollsByAge(t *testing.T) {
	m := newTestMetrics()
	std := newTestObs()
	store := newMemStore()
	r := NewRoller(store, 8<<20, time.Minute, m, std.FailOpenTotal)

	r.Add(context.Background(), testRecs("cr-1", 3, t0), t0)
	r.FlushDue(context.Background(), t0.Add(59*time.Second))
	if len(store.keys()) != 0 {
		t.Fatal("file rolled before its age threshold")
	}
	r.FlushDue(context.Background(), t0.Add(time.Minute))
	keys := store.keys()
	if len(keys) != 1 {
		t.Fatalf("expected one age-rolled file, got %v", keys)
	}
	if got := decodeRecords(t, store.objects[keys[0]]); len(got) != 3 {
		t.Fatalf("rolled file has %d records, want 3", len(got))
	}
}

func TestRollerPartitionsByCreatorAndDate(t *testing.T) {
	m := newTestMetrics()
	std := newTestObs()
	store := newMemStore()
	r := NewRoller(store, 8<<20, time.Minute, m, std.FailOpenTotal)

	day2 := time.Date(2026, 8, 20, 0, 0, 1, 0, time.UTC)
	r.Add(context.Background(), testRecs("cr-1", 1, t0), t0)
	r.Add(context.Background(), testRecs("cr-2", 1, t0), t0)
	r.Add(context.Background(), testRecs("cr-1", 1, day2), day2)
	r.FlushAll(context.Background())

	keys := store.keys()
	if len(keys) != 3 {
		t.Fatalf("expected 3 partitioned files, got %v", keys)
	}
	want := []*regexp.Regexp{
		regexp.MustCompile(`^creator_id=cr-1/date=2026-08-19/`),
		regexp.MustCompile(`^creator_id=cr-2/date=2026-08-19/`),
		regexp.MustCompile(`^creator_id=cr-1/date=2026-08-20/`),
	}
	for _, re := range want {
		found := false
		for _, k := range keys {
			if re.MatchString(k) {
				found = true
			}
		}
		if !found {
			t.Fatalf("no object matching %v in %v", re, keys)
		}
	}
}

// TestRollerPutFailureFailsOpen: an S3 failure drops the file's records and
// counts them; it never blocks the pipeline (§4.7).
func TestRollerPutFailureFailsOpen(t *testing.T) {
	m := newTestMetrics()
	std := newTestObs()
	store := newMemStore()
	store.err = errors.New("s3 down")
	r := NewRoller(store, 8<<20, time.Minute, m, std.FailOpenTotal)

	r.Add(context.Background(), testRecs("cr-1", 5, t0), t0)
	r.FlushAll(context.Background())
	if v := testutil.ToFloat64(std.FailOpenTotal.WithLabelValues("s3", "put_failed")); v != 5 {
		t.Fatalf("fail_open_total{s3} = %v, want 5", v)
	}
	if v := testutil.ToFloat64(m.S3BytesWritten); v != 0 {
		t.Fatalf("bytes counter moved on failure: %v", v)
	}
}
