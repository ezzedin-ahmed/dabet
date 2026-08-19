-- 0001_identity.sql — identity schema per docs §5.2.
--
-- Deviations from §5.2 verbatim, both required by §5.4/A2 and the
-- verify flow:
--   * refresh_tokens gains family_id: rotation keeps the family, and
--     reuse of a rotated token revokes the whole family.
--   * email_verifications is added: no mailer exists in v1, so the
--     opaque verification token issued at register is stored hashed
--     here and surfaced through the dev-mode debug log.
CREATE SCHEMA IF NOT EXISTS identity;
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE identity.creators (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email             CITEXT      NOT NULL UNIQUE,
    fullname          VARCHAR(32) NOT NULL,
    password_hash     TEXT        NOT NULL,
    email_verified_at TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE identity.platform_t AS ENUM ('youtube', 'twitch', 'discord');
CREATE TYPE identity.connection_status_t AS ENUM ('active', 'expired', 'revoked');

CREATE TABLE identity.connections (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id        UUID NOT NULL REFERENCES identity.creators(id) ON DELETE CASCADE,
    platform          identity.platform_t NOT NULL,
    provider_user_id  TEXT NOT NULL,
    display_name      TEXT NOT NULL,
    access_token      TEXT NOT NULL,
    refresh_token     TEXT,
    expires_at        TIMESTAMPTZ,
    scopes            TEXT[] NOT NULL DEFAULT '{}',
    status            identity.connection_status_t NOT NULL DEFAULT 'active',
    connected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One platform account may be actively connected once, globally (A4).
CREATE UNIQUE INDEX connections_active_uniq
    ON identity.connections (platform, provider_user_id)
    WHERE status = 'active';

CREATE INDEX connections_creator_idx ON identity.connections (creator_id) WHERE status = 'active';

CREATE TABLE identity.oauth_states (
    state          TEXT PRIMARY KEY,
    creator_id     UUID NOT NULL REFERENCES identity.creators(id) ON DELETE CASCADE,
    platform       identity.platform_t NOT NULL,
    code_verifier  TEXT NOT NULL,
    redirect_after TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE identity.refresh_tokens (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id  UUID NOT NULL REFERENCES identity.creators(id) ON DELETE CASCADE,
    family_id   UUID NOT NULL,
    token_hash  TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX refresh_tokens_family_idx ON identity.refresh_tokens (family_id);

CREATE TABLE identity.email_verifications (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id  UUID NOT NULL REFERENCES identity.creators(id) ON DELETE CASCADE,
    token_hash  TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX email_verifications_creator_idx ON identity.email_verifications (creator_id);
