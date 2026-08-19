-- 0002_platform_mock.sql — add the 'mock' platform enum value.
--
-- DEVIATION from §5.2 (documented): the platform_t enum gains 'mock' so
-- local e2e can exercise the full OAuth connect flow against a stub
-- provider without real platform credentials. The API only accepts the
-- mock platform when OAUTH_MOCK_ENABLED is set; the enum value existing
-- in the database is harmless otherwise.
ALTER TYPE identity.platform_t ADD VALUE IF NOT EXISTS 'mock';
