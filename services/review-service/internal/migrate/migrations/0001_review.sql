-- 0001_review.sql — review schema per docs §7.6.
--
-- The only persisted state of review-service: one cursor row per creator.
-- The queue itself is a position in the creator's flagged.v1 partition.
--
-- Deviation from §7.6 verbatim (repo convention): the cross-schema
-- REFERENCES creators(id) ON DELETE CASCADE is omitted — identity tables
-- live in the identity schema owned by user-service, and services do not
-- take foreign keys across schema ownership boundaries. A deleted
-- creator's cursor row is harmless (one small orphan row).
CREATE SCHEMA IF NOT EXISTS review;

CREATE TABLE review.review_cursors (
    creator_id  UUID PRIMARY KEY,
    partition   INT    NOT NULL,
    next_offset BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
