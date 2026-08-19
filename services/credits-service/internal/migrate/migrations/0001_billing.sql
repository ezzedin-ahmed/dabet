-- 0001_billing.sql — billing schema per docs §5.3.
--
-- Deviation (repo convention): the REFERENCES creators(id) ON DELETE
-- CASCADE clauses of §5.3 are omitted. creators lives in the identity
-- schema owned by user-service; cross-schema FKs would couple the two
-- services' migration ordering, so each service owns its schema without
-- foreign keys into another's. Orphaned billing rows on creator deletion
-- are accepted in v1.
CREATE SCHEMA IF NOT EXISTS billing;

CREATE TABLE billing.credit_entries (
    id              BIGSERIAL PRIMARY KEY,
    creator_id      UUID NOT NULL,
    delta           BIGINT NOT NULL,           -- credits; positive = topup, negative = usage
    reason          TEXT NOT NULL,             -- 'topup' | 'messages_processed' | 'messages_reclustered' | 'adjustment'
    idempotency_key TEXT NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX credit_entries_idem_uniq ON billing.credit_entries (idempotency_key);
CREATE INDEX credit_entries_creator_idx ON billing.credit_entries (creator_id, created_at DESC);

CREATE TABLE billing.creator_balances (
    creator_id UUID PRIMARY KEY,
    balance    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
