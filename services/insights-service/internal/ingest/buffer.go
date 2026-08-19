// Package ingest implements the insights ingestion pipeline (docs §8.2–8.4):
// exclusion buffer → per-content sampler → embedding batches → parquet on S3.
//
// P4 applies throughout: message text lives only in process memory on its way
// to the embedding service. It is never logged, never labelled on a metric,
// and never written to storage — the parquet record carries creator_id,
// content_id, embedded_at, and the vector, nothing else.
package ingest

import (
	"container/list"
	"sync"
	"time"
)

// BufferedMessage is the slice of a messages.v1 event the pipeline needs.
// author_id is deliberately not carried past the Kafka handler: nothing
// downstream may use it (§4.8), so it is shed at the earliest opportunity.
type BufferedMessage struct {
	MessageID string
	CreatorID string
	ContentID string
	Text      string
}

type bufferEntry struct {
	msg      BufferedMessage
	deadline time.Time
}

// Buffer is the exclusion buffer of docs §8.3. Every message is held for a
// fixed window before it may be embedded; a flag arriving for its message_id
// inside the window removes it. Flags whose message is not in the buffer
// (already released, sampled away, or buffered on another instance — the two
// topics are partitioned differently, so an instance may see a flag for a
// message it never consumed) increment the contamination estimate.
//
// The buffer is in-memory and lost on restart, dropping up to one window of
// messages per instance. Accepted per §8.3 — Insights is an aggregate product
// and does not require completeness.
//
// Size is bounded by maxSize. On overflow the oldest entry is released early
// (queued for the next PopDue) rather than discarded: an early release only
// shortens the exclusion window for that one message — the same accepted
// contamination tail as a slow verdict — while discarding it would silently
// bias the corpus under load.
//
// Buffer is safe for concurrent use: Kafka handlers Add/Flag while the
// pipeline loop calls PopDue.
type Buffer struct {
	mu      sync.Mutex
	window  time.Duration
	maxSize int
	entries map[string]*list.Element // message_id → element in order
	order   *list.List               // *bufferEntry, front = oldest
	ready   []BufferedMessage        // overflow-evicted, awaiting PopDue
	metrics *Metrics
}

// NewBuffer builds an exclusion buffer holding messages for window, bounded
// at maxSize entries.
func NewBuffer(window time.Duration, maxSize int, m *Metrics) *Buffer {
	return &Buffer{
		window:  window,
		maxSize: maxSize,
		entries: make(map[string]*list.Element),
		order:   list.New(),
		metrics: m,
	}
}

// Add holds msg until now+window. A message_id already buffered (Kafka
// redelivery, P3) is ignored.
func (b *Buffer) Add(msg BufferedMessage, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.entries[msg.MessageID]; ok {
		return
	}
	el := b.order.PushBack(&bufferEntry{msg: msg, deadline: now.Add(b.window)})
	b.entries[msg.MessageID] = el
	if b.order.Len() > b.maxSize {
		oldest := b.order.Front()
		e := oldest.Value.(*bufferEntry)
		b.order.Remove(oldest)
		delete(b.entries, e.msg.MessageID)
		b.ready = append(b.ready, e.msg)
	}
	b.metrics.Buffered.Set(float64(b.order.Len()))
}

// Flag processes a flagged.v1 event for messageID. If the message is still
// buffered it is dropped (reason "flagged"); otherwise the contamination
// estimate is incremented. Returns whether the message was dropped.
func (b *Buffer) Flag(messageID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	el, ok := b.entries[messageID]
	if !ok {
		b.metrics.ContaminationTotal.Inc()
		return false
	}
	b.order.Remove(el)
	delete(b.entries, messageID)
	b.metrics.DroppedTotal.WithLabelValues(DropReasonFlagged).Inc()
	b.metrics.Buffered.Set(float64(b.order.Len()))
	return true
}

// PopDue releases every message whose window has elapsed at now, plus any
// overflow-evicted messages, in arrival order.
func (b *Buffer) PopDue(now time.Time) []BufferedMessage {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.ready
	b.ready = nil
	for {
		front := b.order.Front()
		if front == nil {
			break
		}
		e := front.Value.(*bufferEntry)
		if e.deadline.After(now) {
			break
		}
		b.order.Remove(front)
		delete(b.entries, e.msg.MessageID)
		out = append(out, e.msg)
	}
	b.metrics.Buffered.Set(float64(b.order.Len()))
	return out
}

// Len reports the number of buffered messages.
func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.order.Len()
}

// Drain empties the buffer, counting every remaining message as dropped with
// reason "restart" (§8.3: buffer contents are lost on shutdown). Best-effort:
// an unclean death cannot count anything.
func (b *Buffer) Drain() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := b.order.Len()
	if n > 0 {
		b.metrics.DroppedTotal.WithLabelValues(DropReasonRestart).Add(float64(n))
	}
	b.order.Init()
	clear(b.entries)
	b.ready = nil
	b.metrics.Buffered.Set(0)
	return n
}
