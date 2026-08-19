package ingest

import (
	"fmt"
	"testing"
	"time"
)

func TestBatcherFlushesAtSize(t *testing.T) {
	b := NewBatcher(3, 250*time.Millisecond)
	if got := b.Add(testMsg("m1"), t0); got != nil {
		t.Fatalf("premature batch: %v", got)
	}
	if got := b.Add(testMsg("m2"), t0); got != nil {
		t.Fatalf("premature batch: %v", got)
	}
	got := b.Add(testMsg("m3"), t0)
	if len(got) != 3 {
		t.Fatalf("batch size %d, want 3", len(got))
	}
	if b.Len() != 0 {
		t.Fatal("batcher should be empty after flush")
	}
}

func TestBatcherFlushesAtLinger(t *testing.T) {
	b := NewBatcher(64, 250*time.Millisecond)
	b.Add(testMsg("m1"), t0)
	b.Add(testMsg("m2"), t0.Add(100*time.Millisecond))

	if got := b.Due(t0.Add(249 * time.Millisecond)); got != nil {
		t.Fatalf("flushed before linger: %v", got)
	}
	got := b.Due(t0.Add(250 * time.Millisecond)) // measured from the OLDEST message
	if len(got) != 2 {
		t.Fatalf("linger flush size %d, want 2", len(got))
	}
	if got := b.Due(t0.Add(time.Hour)); got != nil {
		t.Fatal("empty batcher must not flush")
	}
}

func TestBatcherLingerRestartsAfterFlush(t *testing.T) {
	b := NewBatcher(64, 250*time.Millisecond)
	b.Add(testMsg("m1"), t0)
	b.Due(t0.Add(time.Second)) // flush
	b.Add(testMsg("m2"), t0.Add(2*time.Second))
	if got := b.Due(t0.Add(2*time.Second + 100*time.Millisecond)); got != nil {
		t.Fatal("linger must be measured from the new batch's first message")
	}
	if got := b.Due(t0.Add(2*time.Second + 250*time.Millisecond)); len(got) != 1 {
		t.Fatalf("expected m2 flushed, got %v", got)
	}
}

func TestBatcherSizeFlushOrderPreserved(t *testing.T) {
	b := NewBatcher(4, time.Second)
	var last []BufferedMessage
	for i := 0; i < 4; i++ {
		last = b.Add(testMsg(fmt.Sprintf("m%d", i)), t0)
	}
	for i, m := range last {
		if m.MessageID != fmt.Sprintf("m%d", i) {
			t.Fatalf("batch order broken at %d: %s", i, m.MessageID)
		}
	}
}
