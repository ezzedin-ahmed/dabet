package mod

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"dabet/pkg/contracts"
)

// UsageAggregator implements §7.10: an in-process counter per
// (creator_id, minute) counting every message PROCESSED — clean or
// flagged — but not redelivery drops and not zero-credit passes (a
// zero-credit message is passed unmoderated and is not billed, §5.8).
// Windows strictly before the current minute are flushed to usage.v1 with
// the deterministic idempotency key "mod:{instance_id}:{minute}:{creator_id}"
// so a redelivered flush is discarded by the credits ledger.
type UsageAggregator struct {
	instanceID string
	pub        *Publisher
	now        func() time.Time

	mu     sync.Mutex
	counts map[usageWindow]int64
}

type usageWindow struct {
	minute  int64 // unix time / 60
	creator string
}

// NewUsageAggregator builds the aggregator; instanceID is the hostname or
// INSTANCE_ID env.
func NewUsageAggregator(instanceID string, pub *Publisher, now func() time.Time) *UsageAggregator {
	return &UsageAggregator{
		instanceID: instanceID,
		pub:        pub,
		now:        now,
		counts:     make(map[usageWindow]int64),
	}
}

// Inc counts one processed message for creatorID in the current minute.
func (u *UsageAggregator) Inc(creatorID string) {
	w := usageWindow{minute: u.now().Unix() / 60, creator: creatorID}
	u.mu.Lock()
	u.counts[w]++
	u.mu.Unlock()
}

// FlushDue emits every window older than the current minute.
func (u *UsageAggregator) FlushDue(ctx context.Context) {
	current := u.now().Unix() / 60
	u.flush(ctx, func(w usageWindow) bool { return w.minute < current })
}

// FlushAll emits everything, including the open window (shutdown path).
func (u *UsageAggregator) FlushAll(ctx context.Context) {
	u.flush(ctx, func(usageWindow) bool { return true })
}

func (u *UsageAggregator) flush(ctx context.Context, due func(usageWindow) bool) {
	u.mu.Lock()
	ready := make(map[usageWindow]int64)
	for w, n := range u.counts {
		if due(w) {
			ready[w] = n
			delete(u.counts, w)
		}
	}
	u.mu.Unlock()

	for w, n := range ready {
		start := time.Unix(w.minute*60, 0).UTC()
		ev := contracts.Usage{
			CreatorID:      w.creator,
			EventType:      contracts.EventMessagesProcessed,
			Quantity:       n,
			WindowStart:    start,
			WindowEnd:      start.Add(time.Minute),
			IdempotencyKey: UsageIdempotencyKey(u.instanceID, start, w.creator),
		}
		val, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		// Publish handles retry/backoff and counts the fail-open on drop.
		u.pub.Publish(ctx, contracts.TopicUsage, contracts.UsageKey(w.creator), val)
	}
}

// UsageIdempotencyKey is "mod:{instance_id}:{minute}:{creator_id}" with
// the minute rendered as the UTC window start "2006-01-02T15:04" —
// deterministic per producer identity, window, and creator (§7.10).
func UsageIdempotencyKey(instanceID string, windowStart time.Time, creatorID string) string {
	return fmt.Sprintf("mod:%s:%s:%s", instanceID, windowStart.UTC().Format("2006-01-02T15:04"), creatorID)
}
