package topics

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// bucketExpr maps a granularity to the ClickHouse rollup expression over
// the hourly base buckets (§8.7 — days and months are rollups over the same
// table, no separate schema).
func bucketExpr(granularity string) (string, error) {
	switch granularity {
	case GranularityHour:
		return "bucket_hour", nil
	case GranularityDay:
		return "toStartOfDay(bucket_hour)", nil
	case GranularityMonth:
		return "toStartOfMonth(bucket_hour)", nil
	default:
		return "", fmt.Errorf("unknown granularity %q", granularity)
	}
}

// CHStore implements Store on ClickHouse.
type CHStore struct {
	conn driver.Conn
}

// OpenCH builds a CHStore from a ClickHouse DSN. The connection is lazy:
// ClickHouse being down surfaces on the first query, not here.
func OpenCH(dsn string) (*CHStore, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	return &CHStore{conn: conn}, nil
}

// Series implements Store over topic_counts. SummingMergeTree merges lazily,
// so sum(count) — never a raw read — collapses the live increments.
func (s *CHStore) Series(ctx context.Context, q SeriesQuery) ([]SeriesRow, error) {
	expr, err := bucketExpr(q.Granularity)
	if err != nil {
		return nil, err
	}
	idCol := "topic_id"
	if q.ByTheme {
		idCol = "theme_id"
	}
	sql := "SELECT toString(" + idCol + ") AS id, " + expr + " AS bucket, sum(count) AS total" +
		" FROM topic_counts WHERE creator_id = ? AND bucket_hour >= ? AND bucket_hour < ?"
	args := []any{q.CreatorID, q.From.UTC(), q.To.UTC()}
	if q.ContentID != "" {
		sql += " AND content_id = ?"
		args = append(args, q.ContentID)
	}
	if q.TopicID != "" {
		sql += " AND topic_id = ?"
		args = append(args, q.TopicID)
	}
	if q.ByTheme {
		sql += " AND theme_id != toUUID(?)"
		args = append(args, ZeroUUID)
	}
	sql += " GROUP BY id, bucket ORDER BY id, bucket"

	rows, err := s.conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("clickhouse series: %w", err)
	}
	defer rows.Close()
	var out []SeriesRow
	for rows.Next() {
		var r SeriesRow
		if err := rows.Scan(&r.ID, &r.Bucket, &r.Count); err != nil {
			return nil, fmt.Errorf("clickhouse series scan: %w", err)
		}
		r.Bucket = r.Bucket.UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// metaSQL reads the topics ReplacingMergeTree; FINAL collapses versions so
// a recluster's relabel wins (§8.7).
const metaSQL = "SELECT toString(topic_id), label, description FROM topics FINAL WHERE creator_id = ? AND parent_id = toUUID(?)"

// Meta implements Store over the topics table.
func (s *CHStore) Meta(ctx context.Context, creatorID, parentID string) ([]Meta, error) {
	rows, err := s.conn.Query(ctx, metaSQL+" ORDER BY topic_id", creatorID, parentID)
	if err != nil {
		return nil, fmt.Errorf("clickhouse meta: %w", err)
	}
	defer rows.Close()
	var out []Meta
	for rows.Next() {
		var m Meta
		if err := rows.Scan(&m.ID, &m.Label, &m.Description); err != nil {
			return nil, fmt.Errorf("clickhouse meta scan: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// Get implements Store: one topic, creator-scoped so another creator's
// topic id reads as absent (ownership-as-404 upstream).
func (s *CHStore) Get(ctx context.Context, creatorID, topicID string) (Meta, bool, error) {
	rows, err := s.conn.Query(ctx, metaSQL+" AND topic_id = toUUID(?)", creatorID, ZeroUUID, topicID)
	if err != nil {
		return Meta{}, false, fmt.Errorf("clickhouse get: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		return Meta{}, false, rows.Err()
	}
	var m Meta
	if err := rows.Scan(&m.ID, &m.Label, &m.Description); err != nil {
		return Meta{}, false, fmt.Errorf("clickhouse get scan: %w", err)
	}
	return m, true, nil
}

// Close releases the connection pool.
func (s *CHStore) Close() error { return s.conn.Close() }
