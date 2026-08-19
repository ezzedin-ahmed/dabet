package job

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"
)

// CountRow is one topic_counts row (§8.7). It is byte-for-byte the shape
// clustering-service's live path writes (cluster.CountRow): one row per
// (creator, content, topic, theme, hour), with ThemeID the zero UUID when
// the assignment is topic-level only. The backfill must produce rows a
// live writer could have produced, because the two land in the same
// SummingMergeTree and the §8.8 API cannot tell them apart.
type CountRow struct {
	CreatorID  string
	ContentID  string
	TopicID    string
	ThemeID    string
	BucketHour time.Time
	Count      uint64
}

// CountStore is the topic_counts surface used by the backfill (chstore.Store).
type CountStore interface {
	// DeleteCounts removes the creator's rows whose bucket_hour is in
	// [from, to) and reports how many rows it removed. Scoped to one
	// creator: another creator's rows are never touched (§8.6).
	DeleteCounts(ctx context.Context, creatorID string, from, to time.Time) (int64, error)
	// InsertCounts appends rows as one batch, exactly as the live writer does.
	InsertCounts(ctx context.Context, rows []CountRow) error
}

// Backfill modes for Config.BackfillCounts (env CLUSTERS_BACKFILL_COUNTS).
const (
	// BackfillOff never rewrites topic_counts. Reclustering then relabels
	// and re-centroids without repairing the dashboard — the pre-existing
	// behaviour, kept as an escape hatch.
	BackfillOff = "off"
	// BackfillOnDemand (the default) rewrites counts for creator-requested
	// reclusters and operator RUN_ONCE runs only.
	BackfillOnDemand = "on_demand"
	// BackfillAlways additionally rewrites counts on every periodic
	// trigger run. See ValidBackfillMode for why this is not the default.
	BackfillAlways = "always"
)

// ValidBackfillMode reports whether s names a backfill mode.
//
// The default is BackfillOnDemand rather than BackfillAlways for three
// reasons:
//
//   - §8.6 scopes history rewriting to the creator who asked for it. A
//     periodic sweep rewriting every creator's last 30 days is a much
//     larger promise than the spec makes.
//   - Every backfill is a ClickHouse mutation (lightweight DELETE) over a
//     creator's partitions. Periodic triggers fire per creator as often as
//     the cooldown allows (30m), so `always` multiplies mutation load by
//     the creator count for a result that mostly rewrites rows to the same
//     ids.
//   - Periodic windows end at now, which is exactly where the
//     concurrent-live-writer race lives (see backfillCounts). On-demand
//     windows are historical (§8.6 — "older than 7 days"), so the lag
//     cutoff never bites them.
func ValidBackfillMode(s string) bool {
	switch s {
	case BackfillOff, BackfillOnDemand, BackfillAlways:
		return true
	}
	return false
}

// backfillEnabled decides whether this run rewrites counts.
func (r *Runner) backfillEnabled(trigger string) bool {
	switch r.Cfg.BackfillCounts {
	case BackfillOff:
		return false
	case BackfillAlways:
		return true
	default: // BackfillOnDemand, and the zero value
		return trigger == TriggerOnDemand || trigger == TriggerManual
	}
}

// BucketHour truncates t to its UTC hour — the base granularity of
// topic_counts (§8.7). Identical to clustering-service's BucketHour; the
// two must agree or backfilled and live rows land in different buckets.
func BucketHour(t time.Time) time.Time { return t.UTC().Truncate(time.Hour) }

// ceilHour rounds t up to an hour boundary. Used on the exclusive end of a
// run window so the bucket containing To-ε is covered.
func ceilHour(t time.Time) time.Time {
	b := BucketHour(t)
	if t.UTC().After(b) {
		return b.Add(time.Hour)
	}
	return b
}

type countKey struct {
	contentID  string
	topicID    string
	themeID    string
	bucketHour time.Time
}

// countRowsFor recomputes the creator's topic_counts rows for one run from
// the points the run actually clustered.
//
// The §8.4 S3 record carries creator_id, content_id, embedded_at and the
// vector — exactly the four fields a topic_counts row needs once HDBSCAN
// has said which cluster the point belongs to. So the counts are not
// re-derived from anything approximate: they are the same points, bucketed
// by their own embedded_at, attributed to the ids this run just wrote.
//
// HDBSCAN noise (label < 0) is a member of no cluster, so a noise point is
// never visited here and contributes to no topic. That is deliberate and
// mirrors §8.5: a live record below the 0.75 threshold is "unassigned" and
// increments nothing. Noise is the batch equivalent, and the system stores
// no unassigned counter anywhere, so there is nothing else it could
// become.
//
// Points whose bucket is at or after before are skipped — see
// backfillCounts for why the recent tail is left alone.
func countRowsFor(creatorID string, points []Point, topics, themes []clusterOut,
	topicIDs, themeIDs []string, before time.Time) []CountRow {

	// A point belongs to at most one theme, and that theme is inside its
	// own topic (the fine pass runs within a topic's members).
	themeOf := make(map[int]string, len(points))
	for i, th := range themes {
		for _, m := range th.members {
			themeOf[m] = themeIDs[i]
		}
	}

	agg := make(map[countKey]uint64)
	for ti, tc := range topics {
		for _, m := range tc.members {
			bucket := BucketHour(points[m].EmbeddedAt)
			if !bucket.Before(before) {
				continue
			}
			themeID := ZeroUUID
			if id, ok := themeOf[m]; ok {
				themeID = id
			}
			agg[countKey{
				contentID:  points[m].ContentID,
				topicID:    topicIDs[ti],
				themeID:    themeID,
				bucketHour: bucket,
			}]++
		}
	}

	out := make([]CountRow, 0, len(agg))
	for k, n := range agg {
		out = append(out, CountRow{
			CreatorID:  creatorID,
			ContentID:  k.contentID,
			TopicID:    k.topicID,
			ThemeID:    k.themeID,
			BucketHour: k.bucketHour,
			Count:      n,
		})
	}
	// Deterministic order: makes the insert batch stable across identical
	// re-runs and lets tests compare batches directly.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case !a.BucketHour.Equal(b.BucketHour):
			return a.BucketHour.Before(b.BucketHour)
		case a.TopicID != b.TopicID:
			return a.TopicID < b.TopicID
		case a.ThemeID != b.ThemeID:
			return a.ThemeID < b.ThemeID
		default:
			return a.ContentID < b.ContentID
		}
	})
	return out
}

// backfillCounts rewrites the creator's topic_counts for the run's window
// so the dashboard attributes history to the ids this run just wrote
// (§8.6 — "on-demand reclustering rewrites history for the requesting
// creator only… the dashboard is not immutable").
//
// # Why delete-then-insert, and what that costs
//
// topic_counts is a SummingMergeTree (§8.7): it sums rows with equal
// ordering keys, it never replaces them. Inserting recomputed rows on top
// of the live ones would double every count. There is no upsert. So the
// rewrite is a lightweight DELETE scoped to
// (creator_id, bucket_hour in [lo, hi)) — the same shape as the existing
// stale-topics delete in chstore — followed by one batched insert. The
// DELETE is issued with lightweight_deletes_sync = 2, so it has taken
// effect before the insert is sent; a reordering there would be the one
// way to double-count.
//
// # The concurrent live writer
//
// clustering-service keeps writing topic_counts for the same creator
// throughout the run. A fully race-free rewrite is not achievable from
// this service alone: it would need clustering-service to fence writes for
// a creator (a lock, or a run epoch stamped on every row) and that service
// is out of scope here. Instead the race is bounded rather than removed:
//
//   - The rewrite covers only buckets that closed at least BackfillLag ago
//     (default 2h — orders of magnitude above clustering-service's count
//     buffer flush interval, which is seconds). Anything the live writer
//     is plausibly still flushing is outside the delete range, so it is
//     neither deleted nor recomputed.
//   - What remains inconsistent: for a window that runs up to now, the
//     trailing <= BackfillLag hours keep their live attribution. If the
//     recluster changed topic identity, those buckets can hold counts for
//     topic ids that DeleteTopicsExcept has just removed from `topics`,
//     which the §8.8 API renders as an unlabelled entry. It self-heals at
//     the next run that covers those hours; on-demand runs target
//     historical windows (§8.6) where the cutoff never bites at all.
//   - A live insert landing inside [lo, hi) during the mutation — only
//     possible if a buffer stalled for hours — is lost, not doubled. Under
//     a SummingMergeTree, losing an increment is the recoverable failure
//     and doubling one is not.
//
// # Failure mid-way
//
// If the DELETE succeeds and the INSERT fails, the window reads as zero
// until the next run rewrites it: an under-count, never a double-count.
// The run is reported as failed so the periodic path retries it.
//
// # Idempotency
//
// Re-running the same window deletes everything the previous run wrote
// before writing again, and the row set is a pure function of the points
// and the (deterministic) cluster ids. Two identical runs converge on
// identical state instead of accumulating.
func (r *Runner) backfillCounts(ctx context.Context, d Decision, points []Point,
	topics, themes []clusterOut, topicIDs, themeIDs []string, log *slog.Logger) (deleted, written int64, err error) {

	if r.Counts == nil || !r.backfillEnabled(d.Trigger) {
		return 0, 0, nil
	}

	lo := BucketHour(d.From)
	hi := ceilHour(d.To)
	if cutoff := BucketHour(r.now().Add(-r.Cfg.BackfillLag)); cutoff.Before(hi) {
		hi = cutoff
	}
	if !lo.Before(hi) {
		// The whole window is inside the live writer's lag. Rewriting it
		// would race a writer we do not control, so we decline.
		log.Info("counts backfill skipped: window is newer than the backfill lag",
			"lag", r.Cfg.BackfillLag.String())
		r.observeBackfill(d.Trigger, "skipped", 0, 0, 0)
		return 0, 0, nil
	}

	rows := countRowsFor(d.CreatorID, points, topics, themes, topicIDs, themeIDs, hi)
	start := r.now()

	deleted, err = r.Counts.DeleteCounts(ctx, d.CreatorID, lo, hi)
	if err != nil {
		r.observeBackfill(d.Trigger, "error", 0, 0, r.now().Sub(start).Seconds())
		return 0, 0, fmt.Errorf("delete counts: %w", err)
	}
	if len(rows) > 0 {
		if err := r.Counts.InsertCounts(ctx, rows); err != nil {
			// Deleted but not reinserted: the window under-counts until the
			// next run. Loud, because it is creator-visible.
			log.Error("counts backfill deleted rows but failed to reinsert; window under-counts until the next run",
				"deleted", deleted, "pending", len(rows), "error", err.Error())
			r.observeBackfill(d.Trigger, "error", deleted, 0, r.now().Sub(start).Seconds())
			return deleted, 0, fmt.Errorf("insert counts: %w", err)
		}
	}
	written = int64(len(rows))
	r.observeBackfill(d.Trigger, "ok", deleted, written, r.now().Sub(start).Seconds())
	log.Info("counts backfill complete", "rows_deleted", deleted, "rows_written", written,
		"from", lo.Format(time.RFC3339), "to", hi.Format(time.RFC3339))
	return deleted, written, nil
}

// observeBackfill moves the §8.9 backfill metrics. Per §4.5 nothing here is
// labelled by creator_id, content_id, or any identifier — only trigger,
// outcome, and the row operation.
func (r *Runner) observeBackfill(trigger, outcome string, deleted, written int64, seconds float64) {
	if r.Metrics == nil {
		return
	}
	r.Metrics.Backfills.WithLabelValues(trigger, outcome).Inc()
	if deleted > 0 {
		r.Metrics.BackfillRows.WithLabelValues(trigger, "deleted").Add(float64(deleted))
	}
	if written > 0 {
		r.Metrics.BackfillRows.WithLabelValues(trigger, "written").Add(float64(written))
	}
	if outcome != "skipped" {
		r.Metrics.BackfillDuration.WithLabelValues(trigger).Observe(seconds)
	}
}
