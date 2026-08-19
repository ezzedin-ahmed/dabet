package job

import (
	"bytes"
	"time"

	"github.com/parquet-go/parquet-go"
)

// EmbeddingRecord mirrors the parquet schema insights-service writes to S3
// (services/insights-service/internal/ingest/parquet.go, docs §8.4):
//
//	creator_id, content_id, embedded_at, vector[384]
//
// No author_id, no text — the corpus is not attributable to individuals
// and cannot be reversed into messages (§4.8).
type EmbeddingRecord struct {
	CreatorID  string    `parquet:"creator_id"`
	ContentID  string    `parquet:"content_id"`
	EmbeddedAt time.Time `parquet:"embedded_at,timestamp(millisecond)"`
	Vector     []float32 `parquet:"vector"`
}

// ReadEmbeddings decodes one parquet object.
func ReadEmbeddings(data []byte) ([]EmbeddingRecord, error) {
	return parquet.Read[EmbeddingRecord](bytes.NewReader(data), int64(len(data)))
}
