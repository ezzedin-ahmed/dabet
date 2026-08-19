package ingest

import (
	"io"

	"time"

	"github.com/parquet-go/parquet-go"
)

// EmbeddingRecord is the S3 parquet record of docs §8.4:
//
//	creator_id, content_id, embedded_at, vector[384]
//
// This struct is the whole privacy story of indefinite retention (§4.8): it
// carries NO author_id and NO text, so the corpus is not attributable to an
// individual sender and cannot be reversed into messages. Do not add fields
// without re-reading §4.8.
//
// Vectors are stored as float32 — parquet has no widely-supported half
// float, and fp32 keeps the file readable by every parquet consumer.
type EmbeddingRecord struct {
	CreatorID  string    `parquet:"creator_id"`
	ContentID  string    `parquet:"content_id"`
	EmbeddedAt time.Time `parquet:"embedded_at,timestamp(millisecond)"`
	Vector     []float32 `parquet:"vector"`
}

// approxEncodedSize estimates the on-file footprint of a record, used to
// roll files by size without flushing the parquet writer.
func (r EmbeddingRecord) approxEncodedSize() int {
	return len(r.CreatorID) + len(r.ContentID) + 8 + 4*len(r.Vector)
}

// WriteParquet encodes recs as one parquet file to w.
func WriteParquet(w io.Writer, recs []EmbeddingRecord) error {
	pw := parquet.NewGenericWriter[EmbeddingRecord](w)
	if _, err := pw.Write(recs); err != nil {
		return err
	}
	return pw.Close()
}
