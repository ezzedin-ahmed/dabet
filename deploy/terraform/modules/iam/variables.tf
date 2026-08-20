variable "name_prefix" {
  description = "Prefix for role names, e.g. \"dabet-prod\"."
  type        = string
}

variable "oidc_provider_arn" {
  description = "IAM OIDC provider ARN from the eks module."
  type        = string
}

variable "oidc_provider_url" {
  description = "OIDC issuer host and path, without the https:// scheme."
  type        = string
}

variable "namespace" {
  description = <<-EOT
    Kubernetes namespace the dabet ServiceAccounts live in. This is half of the
    IRSA trust condition — a token from any other namespace is rejected — so it
    has to match what the app chart actually deploys into.
  EOT
  type        = string
  default     = "dabet"
}

variable "service_account_names" {
  description = <<-EOT
    Map of dabet service to the ServiceAccount name the chart creates for it.
    The defaults are the service names themselves; the chart annotates each
    with eks.amazonaws.com/role-arn from this module's outputs.

    If the chart prefixes ServiceAccount names with the Helm release, override
    every entry here to match. A mismatch does not fail at apply — it fails at
    runtime, as an AssumeRoleWithWebIdentity denial in the pod's logs.
  EOT
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

# ---------------------------------------------------------------------------
# Resources the roles are scoped to
# ---------------------------------------------------------------------------

variable "msk_cluster_arn" {
  description = <<-EOT
    MSK cluster ARN. Topic and group ARNs are derived from it, so every
    kafka-cluster:* action is scoped to this cluster's own topics and groups —
    never to \"*\". Null disables all Kafka statements, which is what you want
    if the cluster runs unauthenticated.
  EOT
  type        = string
  default     = null
}

variable "kafka_consumer_groups" {
  description = <<-EOT
    Consumer group names per service, matching what the binaries actually join.
    These come from the code: moderation-service and credits-service take theirs
    from MOD_CONSUMER_GROUP / CREDITS_CONSUMER_GROUP and default to the service
    name; insights-service hardcodes "insights-service"; provider-adapter uses
    "provider-adapter" on deletions.v1 and "provider-adapter-shards" for its
    §7.2 sharding coordinator. review-service assigns partitions itself (§7.6)
    and joins no group, but is granted one anyway so that switching it to a
    group later is not an IAM change.
  EOT
  type        = map(list(string))
  default = {
    moderation-service = ["moderation-service", "moderation-service-*"]
    credits-service    = ["credits-service", "credits-service-*"]
    insights-service   = ["insights-service", "insights-service-*"]
    provider-adapter   = ["provider-adapter", "provider-adapter-shards"]
    review-service     = ["review-service", "review-service-*"]
    clusters-job       = ["clusters-job", "clusters-job-*"]
  }
}

variable "adapter_coordination_topic" {
  description = <<-EOT
    The §7.2 sharding coordination topic. provider-adapter creates it itself on
    startup (a CreateTopics call, best effort), so with
    auto.create.topics.enable=false the adapter's role needs CreateTopic on
    exactly this name and nothing else.
  EOT
  type        = string
  default     = "adapter.shards.v1"
}

variable "embeddings_bucket_arn" {
  description = "Embeddings bucket ARN. insights-service writes it, clusters-job reads it."
  type        = string
  default     = null
}

variable "milvus_bucket_arn" {
  description = "Bucket ARN for an in-cluster Milvus. Null skips the Milvus role."
  type        = string
  default     = null
}

variable "clickhouse_bucket_arn" {
  description = "Bucket ARN for in-cluster ClickHouse backups. Null skips the ClickHouse role."
  type        = string
  default     = null
}

variable "service_secret_arns" {
  description = <<-EOT
    Secrets Manager ARNs each service may read, keyed by service name.

    This is the Secrets Store CSI driver path, where the pod's own identity
    fetches the secret. The External Secrets Operator path uses the separate
    role below instead, and does not need these — but granting both costs
    nothing and lets a team pick either without a Terraform change.
  EOT
  type        = map(list(string))
  default     = {}
}

variable "kms_key_arns" {
  description = <<-EOT
    KMS keys the roles need kms:Decrypt on: the secrets key (to read Secrets
    Manager values) and the data key (to read and write SSE-KMS objects). Both
    are scoped by ARN; there is no kms:* on "*" anywhere in this module.
  EOT
  type = object({
    secrets = optional(string)
    data    = optional(string)
  })
  default = {}
}

# ---------------------------------------------------------------------------
# Platform roles
# ---------------------------------------------------------------------------

variable "external_secrets" {
  description = <<-EOT
    IRSA role for External Secrets Operator. secret_arn_prefixes is what it may
    read; keep it to the deployment's own path so the operator cannot be pointed
    at an unrelated secret by a ExternalSecret manifest someone commits.
  EOT
  type = object({
    enabled              = optional(bool, true)
    namespace            = optional(string, "external-secrets")
    service_account_name = optional(string, "external-secrets")
    secret_arn_prefixes  = optional(list(string), [])
  })
  default = {}
}

variable "milvus_service_account" {
  description = "Namespace and ServiceAccount for an in-cluster Milvus that needs S3."
  type = object({
    namespace            = optional(string, "dabet")
    service_account_name = optional(string, "milvus")
  })
  default = {}
}

variable "clickhouse_service_account" {
  description = "Namespace and ServiceAccount for an in-cluster ClickHouse that needs S3."
  type = object({
    namespace            = optional(string, "dabet")
    service_account_name = optional(string, "clickhouse")
  })
  default = {}
}

variable "extra_roles" {
  description = <<-EOT
    Additional IRSA roles for cluster addons this module does not model —
    the AWS Load Balancer Controller, Karpenter, the cluster autoscaler, a
    Prometheus that remote-writes to AMP.

    They take policy ARNs and/or an inline policy document rather than being
    hardcoded: the Load Balancer Controller's policy in particular is a long
    document that AWS revises, and a copy pasted in here would go stale
    silently. Fetch the current one from the controller's release and attach it
    as a customer-managed policy.
  EOT
  type = map(object({
    namespace            = string
    service_account_name = string
    policy_arns          = optional(list(string), [])
    inline_policy_json   = optional(string)
  }))
  default = {}
}

variable "permissions_boundary_arn" {
  description = "Optional permissions boundary applied to every role created here."
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
