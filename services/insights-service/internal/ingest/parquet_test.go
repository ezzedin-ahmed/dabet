package ingest

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"dabet/pkg/embeddings"
)

func testVector(seed int64) []float32 {
	rng := rand.New(rand.NewSource(seed))
	v := make([]float32, embeddings.Dimensions)
	for i := range v {
		v[i] = rng.Float32()
	}
	return v
}

// TestParquetRoundTrip encodes records to an in-memory buffer and reads them
// back, asserting the exact record schema of docs §8.4: field names, 384
// dimensions, and — the whole privacy story — the absence of any author_id
// or text field.
func TestParquetRoundTrip(t *testing.T) {
	at := time.Date(2026, 8, 19, 14, 2, 11, 412_000_000, time.UTC)
	in := []EmbeddingRecord{
		{CreatorID: "cr-1", ContentID: "ct-1", EmbeddedAt: at, Vector: testVector(1)},
		{CreatorID: "cr-1", ContentID: "ct-2", EmbeddedAt: at.Add(time.Second), Vector: testVector(2)},
	}
	var buf bytes.Buffer
	if err := WriteParquet(&buf, in); err != nil {
		t.Fatal(err)
	}

	// Assert the file's own schema, not the Go struct's.
	pf, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	fields := pf.Schema().Fields()
	want := []string{"creator_id", "content_id", "embedded_at", "vector"}
	if len(fields) != len(want) {
		t.Fatalf("schema has %d fields, want %d", len(fields), len(want))
	}
	for i, f := range fields {
		if f.Name() != want[i] {
			t.Fatalf("field %d = %q, want %q", i, f.Name(), want[i])
		}
		switch f.Name() {
		case "author_id", "text":
			t.Fatalf("radioactive field %q present in parquet schema", f.Name())
		}
	}

	r := parquet.NewGenericReader[EmbeddingRecord](bytes.NewReader(buf.Bytes()))
	defer r.Close()
	out := make([]EmbeddingRecord, len(in)+1)
	n, _ := r.Read(out)
	if n != len(in) {
		t.Fatalf("read %d records, want %d", n, len(in))
	}
	for i := range in {
		got, want := out[i], in[i]
		if got.CreatorID != want.CreatorID || got.ContentID != want.ContentID {
			t.Fatalf("record %d ids mismatch: %+v", i, got)
		}
		if !got.EmbeddedAt.Equal(want.EmbeddedAt.Truncate(time.Millisecond)) {
			t.Fatalf("record %d embedded_at = %v, want %v", i, got.EmbeddedAt, want.EmbeddedAt)
		}
		if len(got.Vector) != embeddings.Dimensions {
			t.Fatalf("record %d vector has %d dims, want %d", i, len(got.Vector), embeddings.Dimensions)
		}
		for j := range got.Vector {
			if got.Vector[j] != want.Vector[j] {
				t.Fatalf("record %d vector[%d] = %v, want %v", i, j, got.Vector[j], want.Vector[j])
			}
		}
	}
}
