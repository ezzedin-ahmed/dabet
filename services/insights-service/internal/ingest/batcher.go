package ingest

import "time"

// Batcher accumulates sampled messages into embedding batches, dispatching
// when either maxSize messages or the linger deadline is reached (docs §8.4;
// the same size-or-time shape as the LLM batcher, §7.9).
//
// Batcher is not safe for concurrent use; it is owned by the pipeline loop.
type Batcher struct {
	maxSize int
	linger  time.Duration
	pending []BufferedMessage
	oldest  time.Time // arrival of pending[0]
}

// NewBatcher builds a batcher flushing at maxSize messages or after linger.
func NewBatcher(maxSize int, linger time.Duration) *Batcher {
	return &Batcher{maxSize: maxSize, linger: linger}
}

// Add appends msg and returns a full batch when maxSize is reached, nil
// otherwise.
func (b *Batcher) Add(msg BufferedMessage, now time.Time) []BufferedMessage {
	if len(b.pending) == 0 {
		b.oldest = now
	}
	b.pending = append(b.pending, msg)
	if len(b.pending) >= b.maxSize {
		return b.take()
	}
	return nil
}

// Due returns the pending batch when the oldest pending message has waited
// at least linger, nil otherwise.
func (b *Batcher) Due(now time.Time) []BufferedMessage {
	if len(b.pending) == 0 || now.Sub(b.oldest) < b.linger {
		return nil
	}
	return b.take()
}

// Flush returns whatever is pending, regardless of size or age.
func (b *Batcher) Flush() []BufferedMessage { return b.take() }

// Len reports the number of pending messages.
func (b *Batcher) Len() int { return len(b.pending) }

func (b *Batcher) take() []BufferedMessage {
	out := b.pending
	b.pending = nil
	return out
}
