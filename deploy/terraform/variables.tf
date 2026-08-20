variable "name" {
  description = "Deployment name, used as the prefix for everything. e.g. \"dabet-prod\"."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]{3,24}$", var.name))
    error_message = "name must be 3-24 lowercase letters, digits or hyphens: it seeds S3 bucket names and IAM role names."
  }
}

variable "environment" {
  description = "Environment label, used in tags and in the Secrets Manager path."
  type        = string
}

variable "azs" {
  description = <<-EOT
    Availability zones. Three is the floor: §3 wants 3+ Kafka brokers across
    AZs and Multi-AZ Postgres, and MSK requires the broker count to divide
    evenly by the AZ count.
  EOT
  type        = list(string)
}

variable "vpc_cidr" {
  description = "VPC CIDR."
  type        = string
  default     = "10.0.0.0/16"
}

variable "network" {
  description = "Overrides for the network module. Subnet lists must have one entry per AZ."
  type = object({
    private_subnet_cidrs        = optional(list(string))
    public_subnet_cidrs         = optional(list(string))
    data_subnet_cidrs           = optional(list(string))
    single_nat_gateway          = optional(bool, false)
    enable_flow_logs            = optional(bool, true)
    interface_endpoint_services = optional(list(string))
  })
  default = {}
}

# ---------------------------------------------------------------------------
# EKS
# ---------------------------------------------------------------------------

variable "kubernetes_version" {
  description = "Kubernetes minor version for the EKS cluster, e.g. \"1.33\"."
  type        = string
}

variable "cluster_endpoint_public_access" {
  description = "Expose the Kubernetes API publicly. Requires cluster_public_access_cidrs."
  type        = bool
  default     = false
}

variable "cluster_public_access_cidrs" {
  description = "Source CIDRs for a public API endpoint. 0.0.0.0/0 is rejected."
  type        = list(string)
  default     = []
}

variable "cluster_admin_principal_arns" {
  description = "IAM principals granted cluster-admin through EKS access entries."
  type        = list(string)
  default     = []
}

variable "general_node_group" {
  description = <<-EOT
    General node pool. See the sizing note in the README: F3 in
    test/load/README.md measures ~170-200 msg/s per moderation-service
    instance, so the N6 baseline of 50 000 msg/s implies a few hundred pods and
    this pool needs a max_size that can carry them.
  EOT
  type = object({
    instance_types = optional(list(string))
    capacity_type  = optional(string)
    ami_type       = optional(string)
    min_size       = optional(number)
    desired_size   = optional(number)
    max_size       = optional(number)
    disk_size_gb   = optional(number)
    labels         = optional(map(string))
  })
  default = {}
}

variable "gpu_node_group" {
  description = "Optional GPU pool for vLLM. Off by default."
  type = object({
    enabled        = optional(bool, false)
    instance_types = optional(list(string))
    capacity_type  = optional(string)
    ami_type       = optional(string)
    min_size       = optional(number)
    desired_size   = optional(number)
    max_size       = optional(number)
    disk_size_gb   = optional(number)
  })
  default = {}
}

variable "stateful_node_group" {
  description = "Optional pool for in-cluster ClickHouse and Milvus. Off by default."
  type = object({
    enabled        = optional(bool, false)
    instance_types = optional(list(string))
    capacity_type  = optional(string)
    ami_type       = optional(string)
    min_size       = optional(number)
    desired_size   = optional(number)
    max_size       = optional(number)
    disk_size_gb   = optional(number)
  })
  default = {}
}

# ---------------------------------------------------------------------------
# Postgres (§3: separate clusters for identity and policy)
# ---------------------------------------------------------------------------

variable "rds_identity" {
  description = <<-EOT
    The identity instance. It carries §5.2's identity schema, §5.3's billing
    schema and §7.6's review_cursors — all three have foreign keys to
    creators(id), so they cannot be split apart.

    allocated_storage_gb should start well above §5.2's ~25 GB of logical data
    at 10M creators; indexes, WAL and autovacuum headroom are the rest.
  EOT
  type = object({
    instance_class           = string
    allocated_storage_gb     = optional(number, 200)
    max_allocated_storage_gb = optional(number, 1000)
    multi_az                 = optional(bool, true)
    backup_retention_days    = optional(number, 14)
    deletion_protection      = optional(bool, true)
    skip_final_snapshot      = optional(bool, false)
    engine_version           = optional(string, "17")
    parameter_group_family   = optional(string, "postgres17")
    monitoring_interval      = optional(number, 60)
    extra_parameters = optional(map(object({
      value        = string
      apply_method = optional(string, "immediate")
    })), {})
  })
}

variable "rds_policy" {
  description = <<-EOT
    The policy instance (§6.3).

    Read volume against it is small: §6.8 puts an in-process LRU in front of a
    Memcached layer, and a negative result is cached like a positive one, so
    Postgres sees a resolution only on a cold cache. Write volume is a creator
    editing a policy. This instance is sized for durability, not throughput.

    Worth stating plainly: §6.3 declares policies.creator_id REFERENCES
    creators(id), and creators lives on the identity instance. Splitting the
    two per §3 therefore drops that foreign key — the relationship becomes an
    application invariant. If you would rather keep it, run one instance with
    both schemas and set create_policy_instance to false.
  EOT
  type = object({
    instance_class           = string
    allocated_storage_gb     = optional(number, 100)
    max_allocated_storage_gb = optional(number, 500)
    multi_az                 = optional(bool, true)
    backup_retention_days    = optional(number, 14)
    deletion_protection      = optional(bool, true)
    skip_final_snapshot      = optional(bool, false)
    engine_version           = optional(string, "17")
    parameter_group_family   = optional(string, "postgres17")
    monitoring_interval      = optional(number, 60)
    extra_parameters = optional(map(object({
      value        = string
      apply_method = optional(string, "immediate")
    })), {})
  })
}

variable "create_policy_instance" {
  description = <<-EOT
    Create a second RDS instance for the policy schema, per §3. False collapses
    both schemas onto the identity instance, which keeps the §6.3 foreign key
    to creators(id) intact and halves the Postgres bill.
  EOT
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

variable "msk" {
  description = "MSK sizing and authentication. See modules/msk for how the numbers come out of §4.2."
  type = object({
    kafka_version                       = optional(string, "3.9.x")
    broker_count                        = optional(number, 6)
    instance_type                       = optional(string, "kafka.m5.large")
    broker_storage_gb                   = optional(number, 1000)
    storage_autoscaling_max_gb          = optional(number, 4000)
    client_authentication               = optional(string, "iam")
    encryption_in_transit_client_broker = optional(string, "TLS")
    enhanced_monitoring                 = optional(string, "PER_BROKER")
    provisioned_throughput_mibps        = optional(number)
    extra_server_properties             = optional(map(string), {})
  })
  default = {}
}

# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------

variable "elasticache" {
  description = "Redis (§4.3) and Memcached (§6.8) sizing."
  type = object({
    redis_cluster_mode               = optional(bool, true)
    redis_node_type                  = optional(string, "cache.r7g.large")
    redis_shards                     = optional(number, 3)
    redis_replicas_per_shard         = optional(number, 1)
    redis_transit_encryption_enabled = optional(bool, true)
    redis_engine_version             = optional(string, "7.1")
    redis_parameter_group_family     = optional(string, "redis7")
    memcached_enabled                = optional(bool, true)
    memcached_node_type              = optional(string, "cache.m7g.large")
    memcached_node_count             = optional(number, 3)
  })
  default = {}
}

# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

variable "s3" {
  description = "Embeddings corpus and supporting buckets. §4.8 and §8.4."
  type = object({
    force_destroy                   = optional(bool, false)
    embeddings_versioning_enabled   = optional(bool, false)
    deny_object_deletion            = optional(bool, true)
    delete_exempt_principal_arns    = optional(list(string), [])
    embeddings_expiration_days      = optional(number)
    create_milvus_bucket            = optional(bool, true)
    create_clickhouse_backup_bucket = optional(bool, true)
    create_access_log_bucket        = optional(bool, false)
    lifecycle_transitions = optional(list(object({
      days          = number
      storage_class = string
    })))
  })
  default = {}
}

# ---------------------------------------------------------------------------
# Kubernetes wiring
# ---------------------------------------------------------------------------

variable "kubernetes_namespace" {
  description = "Namespace the dabet chart deploys into. Half of every IRSA trust condition."
  type        = string
  default     = "dabet"
}

variable "service_account_names" {
  description = "ServiceAccount name per service. Must match what the app chart creates."
  type        = map(string)
  default = {
    user-service       = "user-service"
    credits-service    = "credits-service"
    policy-service     = "policy-service"
    provider-adapter   = "provider-adapter"
    moderation-service = "moderation-service"
    review-service     = "review-service"
    insights-service   = "insights-service"
    clustering-service = "clustering-service"
    clusters-job       = "clusters-job"
  }
}

variable "external_secrets_namespace" {
  description = "Namespace External Secrets Operator runs in."
  type        = string
  default     = "external-secrets"
}

variable "external_secrets_service_account" {
  description = "ServiceAccount name for External Secrets Operator."
  type        = string
  default     = "external-secrets"
}

variable "extra_irsa_roles" {
  description = <<-EOT
    Additional IRSA roles for cluster addons — the AWS Load Balancer
    Controller, Karpenter, the cluster autoscaler. Their policies are not
    vendored here because AWS revises them; fetch the current document from the
    controller's release and pass it in.
  EOT
  type = map(object({
    namespace            = string
    service_account_name = string
    policy_arns          = optional(list(string), [])
    inline_policy_json   = optional(string)
  }))
  default = {}
}

# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

variable "observability" {
  description = "Log retention, alarm routing, and the optional Amazon Managed Prometheus workspace."
  type = object({
    application_log_retention_days = optional(number, 30)
    create_sns_topic               = optional(bool, true)
    alarm_email_subscriptions      = optional(list(string), [])
    additional_alarm_actions       = optional(list(string), [])
    create_prometheus_workspace    = optional(bool, false)
    prometheus_alert_rules_yaml    = optional(string)
  })
  default = {}
}

variable "log_retention_days" {
  description = "Default retention for the infrastructure log groups (EKS control plane, RDS, MSK)."
  type        = number
  default     = 30
}

# ---------------------------------------------------------------------------
# KMS
# ---------------------------------------------------------------------------

variable "kms_deletion_window_days" {
  description = <<-EOT
    Waiting period before a scheduled KMS key deletion completes. Thirty days is
    the maximum and the right answer for a key that encrypts data with no other
    copy: deleting it makes the embeddings corpus permanently unreadable.
  EOT
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags merged onto every resource."
  type        = map(string)
  default     = {}
}
