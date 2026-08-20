# ElastiCache: a Redis replication group for the §4.3 moderation keyspace, and a
# Memcached pool for §6.8's policy cache.
#
# Both sit in the isolated data subnets with security groups that reference the
# EKS cluster security group. Neither is reachable from outside the VPC.

data "aws_secretsmanager_secret_version" "redis_auth" {
  count = var.redis_auth_token_secret_arn == null ? 0 : 1

  secret_id = var.redis_auth_token_secret_arn
}

resource "aws_elasticache_subnet_group" "this" {
  name       = var.name
  subnet_ids = var.subnet_ids
  tags       = var.tags
}

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------

resource "aws_security_group" "redis" {
  name_prefix = "${var.name}-redis-"
  description = "Redis ${var.name}"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, { Name = "${var.name}-redis" })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "redis" {
  for_each = toset(var.allowed_security_group_ids)

  security_group_id            = aws_security_group.redis.id
  description                  = "Redis from ${each.value}"
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
}

locals {
  redis_parameters = merge(
    {
      "maxmemory-policy" = var.redis_maxmemory_policy
    },
    # cluster-enabled is what turns a redis7-family group into a cluster-mode
    # group. The stock alternative is the AWS-provided default.redis7.cluster.on
    # group, which cannot carry maxmemory-policy.
    var.redis_cluster_mode ? { "cluster-enabled" = "yes" } : {},
    var.redis_extra_parameters,
  )
}

# aws_elasticache_parameter_group has no name_prefix, so the name is fixed.
# Parameter edits apply in place; a family change forces replacement, and a
# parameter group attached to a running cluster cannot be deleted — so an engine
# major-version bump is a two-step change (new group, move the cluster to it,
# drop the old one), not a single apply.
resource "aws_elasticache_parameter_group" "redis" {
  name        = "${var.name}-redis"
  family      = var.redis_parameter_group_family
  description = "dabet ${var.name} redis"

  dynamic "parameter" {
    for_each = local.redis_parameters

    content {
      name  = parameter.key
      value = parameter.value
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "redis" {
  for_each = toset(["slow-log", "engine-log"])

  name              = "/aws/elasticache/${var.name}/${each.value}"
  retention_in_days = var.redis_log_retention_days
  tags              = var.tags
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "${var.name}-redis"
  description          = "dabet moderation state (§4.3)"

  engine               = "redis"
  engine_version       = var.redis_engine_version
  node_type            = var.redis_node_type
  parameter_group_name = aws_elasticache_parameter_group.redis.name
  port                 = 6379

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.redis.id]

  # Cluster mode: num_node_groups shards, each with replicas_per_node_group
  # replicas. Non-cluster mode: one primary plus replicas behind a single
  # primary endpoint.
  num_node_groups         = var.redis_cluster_mode ? var.redis_shards : null
  replicas_per_node_group = var.redis_cluster_mode ? var.redis_replicas_per_shard : null
  num_cache_clusters      = var.redis_cluster_mode ? null : var.redis_replicas_per_shard + 1

  automatic_failover_enabled = var.redis_replicas_per_shard > 0
  multi_az_enabled           = var.redis_replicas_per_shard > 0

  at_rest_encryption_enabled = true
  kms_key_id                 = var.kms_key_arn
  transit_encryption_enabled = var.redis_transit_encryption_enabled
  auth_token = (
    var.redis_auth_token_secret_arn == null
    ? null
    : data.aws_secretsmanager_secret_version.redis_auth[0].secret_string
  )

  snapshot_retention_limit   = var.redis_snapshot_retention_days
  apply_immediately          = false
  auto_minor_version_upgrade = true

  log_delivery_configuration {
    destination      = aws_cloudwatch_log_group.redis["slow-log"].name
    destination_type = "cloudwatch-logs"
    log_format       = "json"
    log_type         = "slow-log"
  }

  log_delivery_configuration {
    destination      = aws_cloudwatch_log_group.redis["engine-log"].name
    destination_type = "cloudwatch-logs"
    log_format       = "json"
    log_type         = "engine-log"
  }

  tags = merge(var.tags, { Name = "${var.name}-redis" })

  lifecycle {
    # An auth token rotated out of band should not be dragged back by an apply.
    ignore_changes = [auth_token]

    precondition {
      condition     = !var.redis_cluster_mode || var.redis_shards >= 1
      error_message = "Cluster mode needs at least one shard."
    }

    precondition {
      condition     = var.redis_auth_token_secret_arn == null || var.redis_transit_encryption_enabled
      error_message = "ElastiCache only accepts an auth token when transit encryption is on."
    }
  }
}

# ---------------------------------------------------------------------------
# Memcached
# ---------------------------------------------------------------------------

resource "aws_security_group" "memcached" {
  count = var.memcached_enabled ? 1 : 0

  name_prefix = "${var.name}-memcached-"
  description = "Memcached ${var.name}"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, { Name = "${var.name}-memcached" })

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "memcached" {
  for_each = var.memcached_enabled ? toset(var.allowed_security_group_ids) : toset([])

  security_group_id            = aws_security_group.memcached[0].id
  description                  = "Memcached from ${each.value}"
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = 11211
  to_port                      = 11211
}

resource "aws_elasticache_parameter_group" "memcached" {
  count = var.memcached_enabled ? 1 : 0

  name        = "${var.name}-memcached"
  family      = var.memcached_parameter_group_family
  description = "dabet ${var.name} memcached"

  tags = var.tags
}

resource "aws_elasticache_cluster" "memcached" {
  count = var.memcached_enabled ? 1 : 0

  cluster_id           = "${var.name}-memcached"
  engine               = "memcached"
  engine_version       = var.memcached_engine_version
  node_type            = var.memcached_node_type
  num_cache_nodes      = var.memcached_node_count
  parameter_group_name = aws_elasticache_parameter_group.memcached[0].name
  port                 = 11211

  # Spread the nodes over AZs so losing one AZ costs a third of the cache
  # rather than all of it. §6.8 reads through to Postgres on a miss, so this is
  # a latency and load property, not an availability one.
  az_mode = var.memcached_node_count > 1 ? "cross-az" : "single-az"

  subnet_group_name  = aws_elasticache_subnet_group.this.name
  security_group_ids = [aws_security_group.memcached[0].id]

  transit_encryption_enabled = var.memcached_transit_encryption_enabled

  apply_immediately = false

  tags = merge(var.tags, { Name = "${var.name}-memcached" })
}
