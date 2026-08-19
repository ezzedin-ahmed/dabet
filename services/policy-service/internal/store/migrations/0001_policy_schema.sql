-- Policy schema per docs §6.3.
--
-- Deviation (deliberate, noted in the implementation report): the docs
-- declare creator_id REFERENCES creators(id), but creators lives in the
-- identity schema owned by user-service. A cross-schema FK would couple
-- the two services' migrations and deployments, so the column is kept
-- without the constraint.

CREATE TYPE policy.policy_scope_t AS ENUM ('creator', 'platform', 'content');
CREATE TYPE policy.spam_mode_t    AS ENUM ('identical', 'semantic', 'none');
CREATE TYPE policy.rc_action_t    AS ENUM ('auto', 'review');

CREATE TABLE policy.policies (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id                UUID NOT NULL,
    scope                     policy.policy_scope_t NOT NULL,
    scope_id                  TEXT NOT NULL,

    rate_limit_messages       INT,          -- NULL = no rate limiting
    rate_limit_seconds        INT,
    spam                      policy.spam_mode_t NOT NULL DEFAULT 'none',
    restricted_words          TEXT[] NOT NULL DEFAULT '{}',
    restricted_content        JSONB  NOT NULL DEFAULT '[]',
    restricted_content_action policy.rc_action_t NOT NULL DEFAULT 'auto',

    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT rate_limit_pair CHECK (
        (rate_limit_messages IS NULL) = (rate_limit_seconds IS NULL)
    )
);

-- Exactly one policy per (scope, scope_id) — §6.1.
CREATE UNIQUE INDEX policies_scope_uniq ON policy.policies (scope, scope_id);
CREATE INDEX policies_creator_idx ON policy.policies (creator_id);
