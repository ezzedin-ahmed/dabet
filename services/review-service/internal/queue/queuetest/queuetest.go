// Package queuetest provides an in-memory fake partition log implementing
// queue.Reader for handler tests: appendable, truncatable (to simulate
// retention expiry), no network.
package queuetest

import (
	"context"
	"fmt"
	"sync"

	"dabet/services/review-service/internal/queue"
)

// FakeLog is an in-memory topic: a fixed partition count, each partition
// an append-only record log with an earliest retained offset.
type FakeLog struct {
	mu       sync.Mutex
	parts    int32
	logs     map[int32][]queue.Record
	earliest map[int32]int64
	next     map[int32]int64
}

// NewFakeLog builds a FakeLog with partitions empty partitions.
func NewFakeLog(partitions int32) *FakeLog {
	return &FakeLog{
		parts:    partitions,
		logs:     make(map[int32][]queue.Record),
		earliest: make(map[int32]int64),
		next:     make(map[int32]int64),
	}
}

// Append appends one record to a partition and returns its offset.
func (f *FakeLog) Append(partition int32, key, value []byte) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	off := f.next[partition]
	f.logs[partition] = append(f.logs[partition], queue.Record{Offset: off, Key: key, Value: value})
	f.next[partition] = off + 1
	return off
}

// Truncate simulates retention expiry: records below newEarliest become
// unreadable.
func (f *FakeLog) Truncate(partition int32, newEarliest int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if newEarliest > f.next[partition] {
		newEarliest = f.next[partition]
	}
	f.earliest[partition] = newEarliest
	kept := f.logs[partition][:0]
	for _, r := range f.logs[partition] {
		if r.Offset >= newEarliest {
			kept = append(kept, r)
		}
	}
	f.logs[partition] = kept
}

// Partitions implements queue.Reader.
func (f *FakeLog) Partitions(context.Context) (int32, error) {
	return f.parts, nil
}

// Offsets implements queue.Reader.
func (f *FakeLog) Offsets(_ context.Context, partition int32) (int64, int64, error) {
	if partition < 0 || partition >= f.parts {
		return 0, 0, fmt.Errorf("fake log: no partition %d", partition)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.earliest[partition], f.next[partition], nil
}

// Scan implements queue.Reader: records with offset >= from (or the
// earliest retained offset if from is below it), up to the high watermark
// at scan start.
func (f *FakeLog) Scan(_ context.Context, partition int32, from int64, fn func(queue.Record) bool) error {
	if partition < 0 || partition >= f.parts {
		return fmt.Errorf("fake log: no partition %d", partition)
	}
	f.mu.Lock()
	recs := make([]queue.Record, len(f.logs[partition]))
	copy(recs, f.logs[partition])
	high := f.next[partition]
	f.mu.Unlock()

	for _, r := range recs {
		if r.Offset < from || r.Offset >= high {
			continue
		}
		if !fn(r) {
			return nil
		}
	}
	return nil
}
