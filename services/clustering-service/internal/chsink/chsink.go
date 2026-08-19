// Package chsink is the ClickHouse-backed CountSink: it owns the §8.7
// schema (created verbatim on startup, made idempotent with IF NOT EXISTS)
// and writes topic_counts increments as batched inserts.
package chsink

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"dabet/services/clustering-service/internal/cluster"
)

// DDL of docs §8.7, verbatim apart from IF NOT EXISTS so startup is
// idempotent. SummingMergeTree collapses the live increments;
// ReplacingMergeTree on version lets a recluster overwrite labels in place.
// The ordering key leads with creator_id because every query is
// creator-scoped.
const (
	ddlTopicCounts = `CREATE TABLE IF NOT EXISTS topic_counts (
    creator_id   UUID,
    content_id   String,
    topic_id     UUID,
    theme_id     UUID,          -- zero UUID when the assignment is topic-level only
    bucket_hour  DateTime,
    count        UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(bucket_hour)
ORDER BY (creator_id, bucket_hour, topic_id, theme_id, content_id)`

	ddlTopics = `CREATE TABLE IF NOT EXISTS topics (
    creator_id  UUID,
    topic_id    UUID,
    parent_id   UUID,           -- zero UUID for a topic; topic_id for a theme
    label       String,
    description String,
    version     UInt32,
    updated_at  DateTime
) ENGINE = ReplacingMergeTree(version)
ORDER BY (creator_id, topic_id)`
)

// Sink implements cluster.CountSink on a ClickHouse connection.
type Sink struct {
	conn driver.Conn
}

// Open builds a Sink from a ClickHouse DSN (e.g.
// clickhouse://localhost:9002/dabet). The connection is lazy: Open does not
// require ClickHouse to be up.
func Open(dsn string) (*Sink, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	return &Sink{conn: conn}, nil
}

// EnsureSchema creates the §8.7 tables if absent. It fails when ClickHouse
// is unreachable, so the caller retries in the background rather than
// crashing (§4.7).
func (s *Sink) EnsureSchema(ctx context.Context) error {
	for _, ddl := range []string{ddlTopicCounts, ddlTopics} {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("clickhouse ensure schema: %w", err)
		}
	}
	return nil
}

// InsertCounts writes rows as one batched insert.
func (s *Sink) InsertCounts(ctx context.Context, rows []cluster.CountRow) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO topic_counts (creator_id, content_id, topic_id, theme_id, bucket_hour, count)")
	if err != nil {
		return fmt.Errorf("clickhouse prepare: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(r.CreatorID, r.ContentID, r.TopicID, r.ThemeID, r.BucketHour, r.Count); err != nil {
			return fmt.Errorf("clickhouse append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse send: %w", err)
	}
	return nil
}

// Close releases the connection pool.
func (s *Sink) Close() error { return s.conn.Close() }
