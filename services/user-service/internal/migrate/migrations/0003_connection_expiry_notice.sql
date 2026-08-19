-- 0003_connection_expiry_notice.sql — the A6 notification bookkeeping.
--
-- §5.5: platform-side revocation is detected lazily by provider-adapter,
-- which moves the connection to 'expired'; the creator is then notified
-- by email, because there is no in-app notification system in v1.
--
-- The adapter is not the sender. It writes 'expired' inside its own
-- refresh transaction (§5.6) and knows nothing about mail; user-service
-- owns the creator's address, so it owns the message. This column is the
-- handshake between the two: a row that is 'expired' with a NULL
-- expired_notified_at is a mail user-service still owes, and stamping it
-- makes the send exactly-once without a queue, a lock, or a new endpoint.
ALTER TABLE identity.connections
    ADD COLUMN IF NOT EXISTS expired_notified_at TIMESTAMPTZ;

-- The sweeper's only query. Partial, so it indexes the (normally empty)
-- backlog rather than the whole table.
CREATE INDEX IF NOT EXISTS connections_expiry_notice_idx
    ON identity.connections (updated_at)
    WHERE status = 'expired' AND expired_notified_at IS NULL;

-- Connections that expired before this migration are stamped as already
-- notified: switching the feature on must not mail every creator whose
-- token lapsed at some point in the past.
UPDATE identity.connections
SET expired_notified_at = now()
WHERE status = 'expired' AND expired_notified_at IS NULL;
