locals {
  tags = merge(
    {
      Project     = "dabet"
      Environment = var.environment
      ManagedBy   = "opentofu"
    },
    var.tags,
  )

  secrets_prefix = "dabet/${var.environment}"

  # The identity instance carries identity, billing and review — §5.2, §5.3 and
  # §7.6 all have foreign keys to creators(id). The policy instance carries
  # §6.3 only, or nothing at all when create_policy_instance is false.
  # one() rather than [0]: a count-indexed reference inside a conditional still
  # has to be a valid expression when the count is zero.
  policy_db = {
    address    = coalesce(one(module.rds_policy[*].address), module.rds_identity.address)
    port       = coalesce(one(module.rds_policy[*].port), module.rds_identity.port)
    database   = coalesce(one(module.rds_policy[*].database_name), module.rds_identity.database_name)
    username   = coalesce(one(module.rds_policy[*].username), module.rds_identity.username)
    secret_arn = coalesce(one(module.rds_policy[*].master_user_secret_arn), module.rds_identity.master_user_secret_arn)
  }

  identity_db_secret = module.rds_identity.master_user_secret_arn
  policy_db_secret   = local.policy_db.secret_arn

  app_secrets = module.secrets.secret_arns

  # Which services may read which secrets. This is the Secrets Store CSI path;
  # External Secrets Operator uses its own role and does not need these. A
  # service appears here only if it actually reads the secret:
  #
  #   - moderation-service has no database and no third-party credential. It
  #     talks to policy-service over gRPC and credits-service over HTTP (§7.3),
  #     so it gets an identity and no secret permissions at all.
  #   - the JWT signing key goes only to user-service, which mints tokens
  #     (§5.4). Everything that merely validates one needs the public half.
  service_secret_arns = {
    user-service = [
      local.identity_db_secret,
      local.app_secrets["jwt/private-key"],
      local.app_secrets["jwt/public-key"],
      local.app_secrets["oauth/youtube"],
      local.app_secrets["oauth/twitch"],
      local.app_secrets["oauth/discord"],
    ]

    credits-service = [
      local.identity_db_secret,
      local.app_secrets["jwt/public-key"],
      local.app_secrets["stripe/secret-key"],
      local.app_secrets["stripe/webhook-secret"],
    ]

    policy-service = [
      local.policy_db_secret,
      local.app_secrets["jwt/public-key"],
    ]

    # §5.6: the adapter refreshes platform tokens itself, so it reads and writes
    # identity.connections and needs the platform OAuth client credentials.
    provider-adapter = [
      local.identity_db_secret,
      local.app_secrets["oauth/youtube"],
      local.app_secrets["oauth/twitch"],
      local.app_secrets["oauth/discord"],
    ]

    moderation-service = []

    # §7.6: review_cursors lives on the identity instance.
    review-service = [
      local.identity_db_secret,
      local.app_secrets["jwt/public-key"],
    ]

    insights-service = []

    clustering-service = [
      local.app_secrets["clickhouse/password"],
    ]

    clusters-job = [
      local.app_secrets["clickhouse/password"],
    ]
  }
}
