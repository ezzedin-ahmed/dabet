// Package chstore is the ClickHouse side of clusters-job: the §8.7 topics
// and topic_counts tables (same DDL as clustering-service, created
// idempotently so either service may bootstrap first), the documented
// clusters_job_state addition, versioned topic upserts, and the trigger
// queries.
package chstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"dabet/services/clusters-job/internal/job"
)

// DDL of docs §8.7, verbatim apart from IF NOT EXISTS — byte-identical to
// services/clustering-service/internal/chsink so that whichever service
// starts first creates the same tables.
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

	// clusters_job_state is a documented clusters-job addition (not in
	// §8.7): one row per creator recording the last run, the corpus size
	// it saw, and the topics version it wrote, so the §8.6 triggers
	// (doubling, cooldowns) survive restarts. ReplacingMergeTree on
	// updated_at keeps the latest row.
	ddlJobState = `CREATE TABLE IF NOT EXISTS clusters_job_state (
    creator_id       UUID,
    last_run_at      DateTime,
    last_point_count UInt64,
    topics_version   UInt32,
    updated_at       DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (creator_id)`
)

// Store implements job.TopicStore, the trigger state store, and the
// assigned-count query on one ClickHouse connection.
type Store struct {
	conn driver.Conn
}

// Open builds a Store from a ClickHouse DSN. The connection is lazy: Open
// does not require ClickHouse to be up.
func Open(dsn string) (*Store, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	return &Store{conn: conn}, nil
}

// EnsureSchema creates the tables if absent. It fails when ClickHouse is
// unreachable, so the caller retries in the background (§4.7).
func (s *Store) EnsureSchema(ctx context.Context) error {
	for _, ddl := range []string{ddlTopicCounts, ddlTopics, ddlJobState} {
		if err := s.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("clickhouse ensure schema: %w", err)
		}
	}
	return nil
}

// PriorTopics implements job.TopicStore: the creator's latest-version rows.
func (s *Store) PriorTopics(ctx context.Context, creatorID string) ([]job.TopicRow, error) {
	rows, err := s.conn.Query(ctx,
		`SELECT toString(topic_id), toString(parent_id), label, description, version, updated_at
		 FROM topics FINAL WHERE creator_id = ?`, creatorID)
	if err != nil {
		return nil, fmt.Errorf("clickhouse prior topics: %w", err)
	}
	defer rows.Close()
	var out []job.TopicRow
	for rows.Next() {
		var r job.TopicRow
		if err := rows.Scan(&r.TopicID, &r.ParentID, &r.Label, &r.Description, &r.Version, &r.UpdatedAt); err != nil {
			return nil, fmt.Errorf("clickhouse scan topics: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpsertTopics implements job.TopicStore as one batched insert;
// ReplacingMergeTree(version) collapses over prior versions.
func (s *Store) UpsertTopics(ctx context.Context, creatorID string, topicRows []job.TopicRow) error {
	batch, err := s.conn.PrepareBatch(ctx,
		"INSERT INTO topics (creator_id, topic_id, parent_id, label, description, version, updated_at)")
	if err != nil {
		return fmt.Errorf("clickhouse prepare topics: %w", err)
	}
	for _, r := range topicRows {
		if err := batch.Append(creatorID, r.TopicID, r.ParentID, r.Label, r.Description, r.Version, r.UpdatedAt); err != nil {
			return fmt.Errorf("clickhouse append topics: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("clickhouse send topics: %w", err)
	}
	return nil
}

// DeleteTopicsExcept implements job.TopicStore with a lightweight delete,
// so topics dissolved by a recluster do not linger for the §8.8 API.
func (s *Store) DeleteTopicsExcept(ctx context.Context, creatorID string, keep []string) error {
	var err error
	if len(keep) == 0 {
		err = s.conn.Exec(ctx, `DELETE FROM topics WHERE creator_id = ?`, creatorID)
	} else {
		err = s.conn.Exec(ctx,
			`DELETE FROM topics WHERE creator_id = ? AND toString(topic_id) NOT IN (?)`, creatorID, keep)
	}
	if err != nil {
		return fmt.Errorf("clickhouse delete stale topics: %w", err)
	}
	return nil
}

// AssignedSince sums the creator's live topic_counts assignments from
// since on — the numerator of the unassigned-rate trigger approximation.
func (s *Store) AssignedSince(ctx context.Context, creatorID string, since time.Time) (int64, error) {
	row := s.conn.QueryRow(ctx,
		`SELECT toInt64(sum(count)) FROM topic_counts WHERE creator_id = ? AND bucket_hour >= ?`,
		creatorID, since)
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("clickhouse assigned counts: %w", err)
	}
	return n, nil
}

// State is one clusters_job_state row.
type State struct {
	CreatorID      string
	LastRunAt      time.Time
	LastPointCount int64
	TopicsVersion  uint32
}

// GetState returns the creator's job state, ok=false when never run.
func (s *Store) GetState(ctx context.Context, creatorID string) (State, bool, error) {
	row := s.conn.QueryRow(ctx,
		`SELECT last_run_at, toInt64(last_point_count), topics_version
		 FROM clusters_job_state FINAL WHERE creator_id = ?`, creatorID)
	st := State{CreatorID: creatorID}
	if err := row.Scan(&st.LastRunAt, &st.LastPointCount, &st.TopicsVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return State{}, false, nil
		}
		return State{}, false, fmt.Errorf("clickhouse job state: %w", err)
	}
	return st, true, nil
}

// PutState records the state after a run.
func (s *Store) PutState(ctx context.Context, st State) error {
	err := s.conn.Exec(ctx,
		`INSERT INTO clusters_job_state (creator_id, last_run_at, last_point_count, topics_version, updated_at)
		 VALUES (?, ?, ?, ?, now())`,
		st.CreatorID, st.LastRunAt, uint64(st.LastPointCount), st.TopicsVersion)
	if err != nil {
		return fmt.Errorf("clickhouse put job state: %w", err)
	}
	return nil
}

// Close releases the connection pool.
func (s *Store) Close() error { return s.conn.Close() }
