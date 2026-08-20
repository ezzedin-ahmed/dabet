# One managed PostgreSQL instance. The root module instantiates this twice —
# identity and policy — because §3 asks for separate clusters for the two.
#
# Encrypted at rest with a customer-managed key, TLS enforced at the server,
# Multi-AZ, automated backups, and no password anywhere in the Terraform state:
# RDS generates the master credential into Secrets Manager and the charts read
# it from there.

data "aws_region" "current" {}

locals {
  # Defaults that apply to both instances, overridable per instance.
  #
  # pg_stat_statements is a static parameter: it only takes effect after a
  # reboot, so it is marked pending-reboot rather than immediate. Marking a
  # static parameter "immediate" makes the apply fail.
  base_parameters = merge(
    {
      shared_preload_libraries = {
        value        = "pg_stat_statements"
        apply_method = "pending-reboot"
      }
      # 500 ms and above is a slow query on either of these instances. Query
      # text is logged; per P4 no chat message text ever reaches Postgres, so
      # this cannot leak message content.
      log_min_duration_statement = {
        value        = "500"
        apply_method = "immediate"
      }
      log_connections = {
        value        = "1"
        apply_method = "immediate"
      }
      log_disconnections = {
        value        = "1"
        apply_method = "immediate"
      }
      # §5.6 takes pg_advisory_xact_lock on connection refresh and §5.3 runs a
      # two-statement transaction per credit entry. Neither should ever wait
      # minutes; failing fast surfaces it instead.
      lock_timeout = {
        value        = "10000"
        apply_method = "immediate"
      }
      idle_in_transaction_session_timeout = {
        value        = "60000"
        apply_method = "immediate"
      }
    },
    var.force_ssl ? {
      rds_force_ssl = {
        value        = "1"
        apply_method = "immediate"
      }
    } : {},
  )

  # rds.force_ssl has a dot in its name, which is not a legal local map key
  # spelled bare, so it is remapped here.
  parameter_names = {
    rds_force_ssl = "rds.force_ssl"
  }

  parameters = merge(local.base_parameters, var.extra_parameters)

  final_snapshot_identifier = coalesce(var.final_snapshot_identifier, "${var.identifier}-final")
}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

resource "aws_db_subnet_group" "this" {
  name       = var.identifier
  subnet_ids = var.subnet_ids
  tags       = merge(var.tags, { Name = var.identifier })
}

resource "aws_security_group" "this" {
  name_prefix = "${var.identifier}-"
  description = "PostgreSQL ${var.identifier}"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, { Name = var.identifier })

  lifecycle {
    create_before_destroy = true
  }
}

# Ingress from named security groups only. No CIDR rule exists in this module,
# so there is no configuration of it that can expose Postgres to the internet.
resource "aws_vpc_security_group_ingress_rule" "postgres" {
  for_each = toset(var.allowed_security_group_ids)

  security_group_id            = aws_security_group.this.id
  description                  = "PostgreSQL from ${each.value}"
  referenced_security_group_id = each.value
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
}

# No egress rules: an RDS instance does not initiate outbound connections, and
# an empty egress set is the tighter default.

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

resource "aws_db_parameter_group" "this" {
  name_prefix = "${var.identifier}-"
  family      = var.parameter_group_family
  description = "dabet ${var.identifier}"

  dynamic "parameter" {
    for_each = local.parameters

    content {
      name         = lookup(local.parameter_names, parameter.key, parameter.key)
      value        = parameter.value.value
      apply_method = parameter.value.apply_method
    }
  }

  tags = merge(var.tags, { Name = var.identifier })

  lifecycle {
    create_before_destroy = true
  }
}

# ---------------------------------------------------------------------------
# Enhanced monitoring
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "monitoring_assume" {
  count = var.monitoring_interval > 0 ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["monitoring.rds.amazonaws.com"]
    }
  }
}

data "aws_partition" "current" {}

resource "aws_iam_role" "monitoring" {
  count = var.monitoring_interval > 0 ? 1 : 0

  name_prefix        = "${substr(var.identifier, 0, 20)}-mon-"
  assume_role_policy = data.aws_iam_policy_document.monitoring_assume[0].json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "monitoring" {
  count = var.monitoring_interval > 0 ? 1 : 0

  role       = aws_iam_role.monitoring[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
}

# ---------------------------------------------------------------------------
# Log groups
#
# Pre-created for the same reason as the EKS control-plane group: RDS creates
# them on first write with infinite retention otherwise.
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "this" {
  for_each = toset(var.enabled_cloudwatch_logs_exports)

  name              = "/aws/rds/instance/${var.identifier}/${each.value}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.log_kms_key_arn
  tags              = var.tags
}

# ---------------------------------------------------------------------------
# Instance
# ---------------------------------------------------------------------------

resource "aws_db_instance" "this" {
  identifier = var.identifier

  engine               = "postgres"
  engine_version       = var.engine_version
  instance_class       = var.instance_class
  parameter_group_name = aws_db_parameter_group.this.name

  db_name  = var.database_name
  username = var.username

  # Either RDS owns the password in Secrets Manager, or you set one out of band
  # and import it. There is no password variable in this module on purpose.
  manage_master_user_password   = var.manage_master_user_password
  master_user_secret_kms_key_id = var.manage_master_user_password ? var.kms_key_arn : null

  allocated_storage     = var.allocated_storage_gb
  max_allocated_storage = var.max_allocated_storage_gb > var.allocated_storage_gb ? var.max_allocated_storage_gb : null
  storage_type          = var.storage_type
  iops                  = var.iops
  storage_throughput    = var.storage_throughput
  storage_encrypted     = true
  kms_key_id            = var.kms_key_arn

  multi_az               = var.multi_az
  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [aws_security_group.this.id]
  publicly_accessible    = false
  port                   = 5432
  ca_cert_identifier     = var.ca_cert_identifier

  backup_retention_period  = var.backup_retention_days
  backup_window            = var.backup_window
  maintenance_window       = var.maintenance_window
  copy_tags_to_snapshot    = true
  delete_automated_backups = false

  deletion_protection       = var.deletion_protection
  skip_final_snapshot       = var.skip_final_snapshot
  final_snapshot_identifier = var.skip_final_snapshot ? null : local.final_snapshot_identifier

  auto_minor_version_upgrade = true
  apply_immediately          = var.apply_immediately

  performance_insights_enabled          = var.performance_insights_enabled
  performance_insights_retention_period = var.performance_insights_enabled ? var.performance_insights_retention_days : null
  performance_insights_kms_key_id       = var.performance_insights_enabled ? var.kms_key_arn : null

  monitoring_interval = var.monitoring_interval
  monitoring_role_arn = var.monitoring_interval > 0 ? aws_iam_role.monitoring[0].arn : null

  enabled_cloudwatch_logs_exports = var.enabled_cloudwatch_logs_exports

  tags = merge(var.tags, { Name = var.identifier })

  lifecycle {
    # engine_version is deliberately NOT ignored. The provider prefix-matches a
    # major-only value like "17" against the running "17.x", so an automatic
    # minor upgrade produces no diff; if you pin a full version instead, a drift
    # after a maintenance window is something you want to see rather than
    # silently suppress.

    precondition {
      condition     = length(var.allowed_security_group_ids) > 0
      error_message = "allowed_security_group_ids is empty: the instance would be unreachable. Pass the EKS cluster security group."
    }
  }

  depends_on = [aws_cloudwatch_log_group.this]
}
