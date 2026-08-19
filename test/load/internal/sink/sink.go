// Package sink is where a generated record goes: straight to Kafka
// (the default, which isolates moderation throughput from the adapter),
// through provider-adapter's HTTP ingress (to measure that hop), or
// nowhere at all (the generator self-benchmark).
package sink

import (
	"context"
	"sync/atomic"

	"dabet/test/load/internal/gen"
)

// Sink accepts records. Send must be safe for concurrent use and should
// not block beyond genuine backpressure — blocking is how the harness
// learns that the sink is the bottleneck, but silently buffering
// forever would hide it.
type Sink interface {
	Send(ctx context.Context, rec gen.Record) error
	// Flush blocks until everything accepted so far has been
	// acknowledged (or failed).
	Flush(ctx context.Context) error
	Close()
	// Stats reports what the sink did.
	Stats() Stats
}

// Stats counts what a sink accepted and what went wrong.
type Stats struct {
	Accepted int64 `json:"accepted"`
	Acked    int64 `json:"acked"`
	Failed   int64 `json:"failed"`
	Bytes    int64 `json:"bytes"`
}

// Counters is the embeddable atomic tally every sink keeps.
type Counters struct {
	accepted atomic.Int64
	acked    atomic.Int64
	failed   atomic.Int64
	bytes    atomic.Int64
}

func (c *Counters) Stats() Stats {
	return Stats{
		Accepted: c.accepted.Load(),
		Acked:    c.acked.Load(),
		Failed:   c.failed.Load(),
		Bytes:    c.bytes.Load(),
	}
}
