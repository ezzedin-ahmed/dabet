# CloudWatch log groups, infrastructure alarms, and an optional Amazon Managed
# Prometheus workspace.
#
# Scope note: the metric §4.5 calls "the single most important metric in the
# system" — fail_open_total — is a Prometheus counter exposed on :9090/metrics by
# each service. Nothing in this module can see it. The alarms here cover the
# managed infrastructure underneath (is the database out of disk, has a Kafka
# partition gone offline, is Redis saturated); the application SLOs belong to
# Prometheus, and create_prometheus_workspace is the AWS-side option for keeping
# them somewhere durable.

data "aws_region" "current" {}
data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# Application logs
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "application" {
  count = var.create_application_log_group ? 1 : 0

  name              = "/aws/eks/${var.cluster_name}/application"
  retention_in_days = var.application_log_retention_days
  kms_key_id        = var.log_kms_key_arn
  tags              = var.tags
}

# ---------------------------------------------------------------------------
# Alarm routing
# ---------------------------------------------------------------------------

resource "aws_sns_topic" "alarms" {
  count = var.create_sns_topic ? 1 : 0

  name = "${var.name_prefix}-alarms"

  # SNS supports KMS, but the CloudWatch alarm service principal then needs
  # kms:GenerateDataKey* on the key. Left on the AWS-managed SNS key rather
  # than adding a key policy that is easy to get subtly wrong; alarm bodies
  # carry no message content (P4).
  tags = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  for_each = var.create_sns_topic ? toset(var.alarm_email_subscriptions) : toset([])

  topic_arn = aws_sns_topic.alarms[0].arn
  protocol  = "email"
  endpoint  = each.value
}

locals {
  alarm_actions = concat(
    var.create_sns_topic ? [aws_sns_topic.alarms[0].arn] : [],
    var.additional_alarm_actions,
  )
}

# ---------------------------------------------------------------------------
# RDS
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "rds_cpu" {
  for_each = toset(var.rds_instance_identifiers)

  alarm_name          = "${var.name_prefix}-rds-${each.value}-cpu"
  alarm_description   = "Sustained CPU on ${each.value}."
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = var.rds_cpu_threshold_percent
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = { DBInstanceIdentifier = each.value }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "rds_storage" {
  for_each = toset(var.rds_instance_identifiers)

  alarm_name          = "${var.name_prefix}-rds-${each.value}-storage"
  alarm_description   = "Free storage on ${each.value} is low. Storage autoscaling grows but never shrinks."
  namespace           = "AWS/RDS"
  metric_name         = "FreeStorageSpace"
  statistic           = "Minimum"
  period              = 300
  evaluation_periods  = 2
  threshold           = var.rds_free_storage_bytes_threshold
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = { DBInstanceIdentifier = each.value }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "rds_connections" {
  for_each = toset(var.rds_instance_identifiers)

  alarm_name          = "${var.name_prefix}-rds-${each.value}-connections"
  alarm_description   = "Connection count on ${each.value} is unusually high; usually a pod crash loop recreating pools."
  namespace           = "AWS/RDS"
  metric_name         = "DatabaseConnections"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 3
  threshold           = var.rds_connection_threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = { DBInstanceIdentifier = each.value }

  alarm_actions = local.alarm_actions
  tags          = var.tags
}

# ---------------------------------------------------------------------------
# MSK
#
# Only cluster-scoped metrics, which MSK publishes at the DEFAULT monitoring
# level. Per-broker alarms would need each broker id as a dimension, and broker
# ids are not known until after the cluster exists.
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "msk_offline_partitions" {
  count = var.msk_cluster_name == null ? 0 : 1

  alarm_name          = "${var.name_prefix}-msk-offline-partitions"
  alarm_description   = "Partitions without a leader. §4.7 tolerates lag, not unavailability: a producer to an offline partition fails and moderation fails open."
  namespace           = "AWS/Kafka"
  metric_name         = "OfflinePartitionsCount"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = { "Cluster Name" = var.msk_cluster_name }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

resource "aws_cloudwatch_metric_alarm" "msk_active_controller" {
  count = var.msk_cluster_name == null ? 0 : 1

  alarm_name          = "${var.name_prefix}-msk-active-controller"
  alarm_description   = "Exactly one active controller is expected. Zero means no metadata operations; more than one means a split."
  namespace           = "AWS/Kafka"
  metric_name         = "ActiveControllerCount"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"

  dimensions = { "Cluster Name" = var.msk_cluster_name }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# ---------------------------------------------------------------------------
# ElastiCache
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "cache_cpu" {
  for_each = toset(var.elasticache_cluster_ids)

  alarm_name          = "${var.name_prefix}-cache-${each.value}-cpu"
  alarm_description   = "Engine CPU on ${each.value}. F2 in test/load/README.md shows a degraded Redis collapsing moderation throughput ~97%, so this is a throughput alarm, not a capacity one."
  namespace           = "AWS/ElastiCache"
  metric_name         = "EngineCPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 3
  threshold           = var.elasticache_cpu_threshold_percent
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = { CacheClusterId = each.value }

  alarm_actions = local.alarm_actions
  ok_actions    = local.alarm_actions
  tags          = var.tags
}

# ---------------------------------------------------------------------------
# Amazon Managed Prometheus
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "prometheus" {
  count = var.create_prometheus_workspace ? 1 : 0

  name              = "/aws/prometheus/${var.name_prefix}"
  retention_in_days = var.prometheus_log_retention_days
  kms_key_id        = var.log_kms_key_arn
  tags              = var.tags
}

# AMP writes rule-evaluation logs as the aps.amazonaws.com service principal, so
# the log group needs a CloudWatch Logs *resource* policy in addition to the log
# group itself. Without it the workspace is created and then logs nothing.
#
# The source ARN is a wildcard over workspaces in this account and region rather
# than the workspace's own ARN, because the workspace depends on this policy and
# naming it here would be circular.
data "aws_iam_policy_document" "prometheus_logs" {
  count = var.create_prometheus_workspace ? 1 : 0

  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]

    resources = ["${aws_cloudwatch_log_group.prometheus[0].arn}:log-stream:*"]

    principals {
      type        = "Service"
      identifiers = ["aps.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:${data.aws_partition.current.partition}:aps:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:workspace/*"]
    }
  }
}

resource "aws_cloudwatch_log_resource_policy" "prometheus" {
  count = var.create_prometheus_workspace ? 1 : 0

  policy_name     = "${var.name_prefix}-amp"
  policy_document = data.aws_iam_policy_document.prometheus_logs[0].json
}

resource "aws_prometheus_workspace" "this" {
  count = var.create_prometheus_workspace ? 1 : 0

  alias = var.name_prefix
  tags  = var.tags

  logging_configuration {
    log_group_arn = "${aws_cloudwatch_log_group.prometheus[0].arn}:*"
  }

  depends_on = [aws_cloudwatch_log_resource_policy.prometheus]
}

resource "aws_prometheus_rule_group_namespace" "dabet" {
  count = var.create_prometheus_workspace && var.prometheus_alert_rules_yaml != null ? 1 : 0

  name         = "dabet"
  workspace_id = aws_prometheus_workspace.this[0].id
  data         = var.prometheus_alert_rules_yaml
}
