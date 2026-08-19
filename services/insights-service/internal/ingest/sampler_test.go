package ingest

import (
	"fmt"
	"testing"
	"time"
)

func TestSamplerBurstBeyondCapacityIsRejected(t *testing.T) {
	s := NewSampler(60, 60, 1000) // 60/min, capacity 60
	allowed := 0
	for i := 0; i < 200; i++ {
		if s.Allow("ct-1", t0) {
			allowed++
		}
	}
	if allowed != 60 {
		t.Fatalf("burst of 200 allowed %d, want exactly capacity 60", allowed)
	}
}

func TestSamplerRefillsOverTime(t *testing.T) {
	s := NewSampler(60, 60, 1000) // 1 token/s
	for i := 0; i < 60; i++ {
		s.Allow("ct-1", t0)
	}
	if s.Allow("ct-1", t0) {
		t.Fatal("bucket should be empty")
	}
	if !s.Allow("ct-1", t0.Add(time.Second)) {
		t.Fatal("one second should refill one token")
	}
	if s.Allow("ct-1", t0.Add(time.Second)) {
		t.Fatal("that token was already spent")
	}
}

func TestSamplerRefillCapsAtCapacity(t *testing.T) {
	s := NewSampler(60, 60, 1000)
	for i := 0; i < 60; i++ {
		s.Allow("ct-1", t0)
	}
	later := t0.Add(time.Hour) // would refill 3600 tokens uncapped
	allowed := 0
	for i := 0; i < 200; i++ {
		if s.Allow("ct-1", later) {
			allowed++
		}
	}
	if allowed != 60 {
		t.Fatalf("after long idle allowed %d, want capacity 60", allowed)
	}
}

func TestSamplerContentsAreIndependent(t *testing.T) {
	s := NewSampler(60, 1, 1000)
	if !s.Allow("ct-1", t0) || !s.Allow("ct-2", t0) {
		t.Fatal("each content gets its own bucket")
	}
	if s.Allow("ct-1", t0) {
		t.Fatal("ct-1 exhausted")
	}
}

func TestSamplerBoundsTrackedContents(t *testing.T) {
	s := NewSampler(60, 60, 100)
	for i := 0; i < 1000; i++ {
		s.Allow(fmt.Sprintf("ct-%d", i), t0.Add(time.Duration(i)*time.Millisecond))
	}
	if n := len(s.buckets); n > 100 {
		t.Fatalf("sampler tracks %d contents, bound is 100", n)
	}
}
