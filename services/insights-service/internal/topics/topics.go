// Package topics serves the creator-facing topics API (docs §8.8) over the
// ClickHouse tables of §8.7. Topics and themes are machine-discovered by
// clustering; nothing here creates them, and there are no sample or message
// endpoints — the messages behind a topic do not exist anywhere after
// retention (§8.8). A topic is a shape and a count, not a drill-down.
package topics

import (
	"context"
	"time"
)

// ZeroUUID is the sentinel of §8.7: "no theme" in topic_counts.theme_id and
// "is a topic" in topics.parent_id.
const ZeroUUID = "00000000-0000-0000-0000-000000000000"

// Granularities for the series rollup. Hourly buckets are the base
// granularity in topic_counts; days and months are rollups over the same
// table (§8.7).
const (
	GranularityHour  = "hour"
	GranularityDay   = "day"
	GranularityMonth = "month"
)

// DefaultWindow is the from/to default: the last 24 hours (§8.8).
const DefaultWindow = 24 * time.Hour

// SeriesQuery selects rolled-up counts from topic_counts.
type SeriesQuery struct {
	CreatorID string
	// ContentID filters to one content when non-empty.
	ContentID string
	// TopicID restricts to one topic when non-empty.
	TopicID string
	// ByTheme groups by theme_id within TopicID (excluding the zero UUID)
	// instead of by topic_id.
	ByTheme     bool
	From, To    time.Time
	Granularity string
}

// SeriesRow is one (topic-or-theme, bucket) count. Rows arrive grouped by
// ID with buckets ascending.
type SeriesRow struct {
	ID     string
	Bucket time.Time
	Count  uint64
}

// Meta is a labelled topic or theme from the topics table.
type Meta struct {
	ID          string
	Label       string
	Description string
}

// Store reads the §8.7 tables. Faked in tests; backed by ClickHouse in
// production.
type Store interface {
	// Series returns rolled-up counts for the query window.
	Series(ctx context.Context, q SeriesQuery) ([]SeriesRow, error)
	// Meta lists labelled entries whose parent_id is parentID — ZeroUUID
	// for a creator's topics, a topic id for that topic's themes.
	Meta(ctx context.Context, creatorID, parentID string) ([]Meta, error)
	// Get fetches one topic (parent_id = ZeroUUID) by id, creator-scoped:
	// ok=false when it does not exist or belongs to another creator.
	Get(ctx context.Context, creatorID, topicID string) (Meta, bool, error)
}

// Bucket is one series point of the response JSON (§8.8).
type Bucket struct {
	Bucket time.Time `json:"bucket"`
	Count  uint64    `json:"count"`
}

// Topic is the response shape of §8.8, shared by topics and themes.
type Topic struct {
	ID           string   `json:"id"`
	Label        string   `json:"label"`
	Description  string   `json:"description"`
	MessageCount uint64   `json:"message_count"`
	Series       []Bucket `json:"series"`
}
