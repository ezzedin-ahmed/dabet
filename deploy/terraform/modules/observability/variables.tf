variable "name_prefix" {
  description = "Prefix for log groups, topics and alarm names."
  type        = string
}

variable "cluster_name" {
  description = "EKS cluster name, used to name the application log groups."
  type        = string
}

variable "log_kms_key_arn" {
  description = "KMS key for the CloudWatch log groups. Null uses the CloudWatch service key."
  type        = string
  default     = null
}

variable "application_log_retention_days" {
  description = <<-EOT
    Retention for the application log group.

    §4.5 puts structured JSON logs on stdout and P4 forbids message text in them,
    so these are operational logs, not a data store. At nine services under
    sustained load this group is one of the larger CloudWatch line items, and 30
    days is already generous for something Prometheus and traces answer better.
  EOT
  type        = number
  default     = 30
}

variable "create_application_log_group" {
  description = <<-EOT
    Create the log group a Fluent Bit / OTel collector DaemonSet writes into.
    Pre-creating it is the only way to get retention and a KMS key onto it — the
    agent creates it without either.
  EOT
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------

variable "create_sns_topic" {
  description = "Create an SNS topic for alarm notifications."
  type        = bool
  default     = true
}

variable "alarm_email_subscriptions" {
  description = <<-EOT
    Email addresses subscribed to the alarm topic. Each needs a manual
    confirmation click; Terraform cannot complete the subscription for you, and
    an unconfirmed subscription silently receives nothing.
  EOT
  type        = list(string)
  default     = []
}

variable "additional_alarm_actions" {
  description = "Extra ARNs (PagerDuty via SNS, a Lambda, a chatbot) added to every alarm's actions."
  type        = list(string)
  default     = []
}

variable "rds_instance_identifiers" {
  description = "RDS instance identifiers to alarm on."
  type        = list(string)
  default     = []
}

variable "rds_free_storage_bytes_threshold" {
  description = "Alarm when free storage on an instance drops below this. Default 20 GiB."
  type        = number
  default     = 21474836480
}

variable "rds_cpu_threshold_percent" {
  description = "Alarm when average CPU stays above this."
  type        = number
  default     = 80
}

variable "rds_connection_threshold" {
  description = <<-EOT
    Alarm on connection count. §5.3 sizes the identity write path at ~800
    writes/s from the usage consumer, and connection pooling in the services
    means a healthy number here is low hundreds; a spike means pools are being
    recreated, which is usually a crash loop.
  EOT
  type        = number
  default     = 400
}

variable "msk_cluster_name" {
  description = "MSK cluster name to alarm on. Null skips the Kafka alarms."
  type        = string
  default     = null
}

variable "elasticache_cluster_ids" {
  description = <<-EOT
    Cache cluster ids to alarm on. ElastiCache publishes CloudWatch metrics
    against CacheClusterId, so this is the member cluster list from the
    replication group, not the group id.
  EOT
  type        = list(string)
  default     = []
}

variable "elasticache_cpu_threshold_percent" {
  description = "Alarm when engine CPU stays above this. Redis is single-threaded per shard, so this saturates early."
  type        = number
  default     = 70
}

# ---------------------------------------------------------------------------
# Amazon Managed Prometheus
# ---------------------------------------------------------------------------

variable "create_prometheus_workspace" {
  description = <<-EOT
    Create an Amazon Managed Prometheus workspace for the in-cluster Prometheus
    to remote-write into.

    §4.5's metrics — fail_open_total above all — are Prometheus metrics, not
    CloudWatch ones, and the alarms in this module cannot see them. AMP gives
    them somewhere durable to live and a rules engine to alert from, without
    running a highly available Prometheus yourself. Off by default: a
    self-hosted Prometheus in the charts is a complete answer and this is a
    second bill.
  EOT
  type        = bool
  default     = false
}

variable "prometheus_alert_rules_yaml" {
  description = <<-EOT
    Optional Prometheus rule group definition, applied to the AMP workspace.
    Null installs no rules. §4.5 says fail_open_total "must be alerted on, and
    must be zero in steady state" — this is where that rule belongs when AMP is
    in use.
  EOT
  type        = string
  default     = null
}

variable "prometheus_log_retention_days" {
  description = "Retention for the AMP workspace's rule-evaluation log group."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
