// Package queue abstracts the direct (non-group) partition read that backs
// the review queue of docs §7.6. The real implementation is a franz-go
// client; handler tests use the in-memory FakeLog.
package queue

import "context"

// Record is one raw record in a partition. Value is the flagged.v1 JSON
// (P4: it carries message text — it must reach API responses only, never
// logs or metrics).
type Record struct {
	Offset int64
	Key    []byte
	Value  []byte
}

// Reader reads one topic's partitions directly, outside any consumer
// group: per-creator progress lives in review.review_cursors, not in
// Kafka group offsets (§7.6).
type Reader interface {
	// Partitions returns the topic's partition count, discovered from
	// broker metadata and cached.
	Partitions(ctx context.Context) (int32, error)

	// Offsets returns the earliest retained offset and the high
	// watermark (offset one past the last record) of a partition.
	Offsets(ctx context.Context, partition int32) (earliest, high int64, err error)

	// Scan reads records with offset >= from, in order, invoking fn for
	// each until fn returns false, the high watermark as of the start of
	// the scan is reached, or ctx ends. Records below the earliest
	// retained offset are unreadable; scanning from below it starts at
	// the earliest instead (mirroring broker auto-reset semantics).
	Scan(ctx context.Context, partition int32, from int64, fn func(Record) bool) error
}
