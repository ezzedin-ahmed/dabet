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

    The defaults match deploy/k8s/charts/dabet, whose serviceaccount.yaml names
    each account "<release>-<service>" through the dabet.svcName helper — so at
    the conventional release name "dabet" the account for user-service is
    "dabet-user-service", not "user-service". Override every entry if you
    install under a different release name.

    A mismatch does not fail at apply. It fails at runtime, as an
    AssumeRoleWithWebIdentity denial in the pod's logs, which is a much longer
    walk back to the cause.
  EOT
  type        = map(string)
  default = {
    user-service       = "dabet-user-service"
    credits-service    = "dabet-credits-service"
    policy-service     = "dabet-policy-service"
    provider-adapter   = "dabet-provider-adapter"
    moderation-service = "dabet-moderation-service"
    review-service     = "dabet-review-service"
    insights-service   = "dabet-insights-service"
    clustering-service = "dabet-clustering-service"
    clusters-job       = "dabet-clusters-job"
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

variable "kafka_access" {
  description = <<-EOT
    Which topics each service reads, writes and creates. §1.5, verified against
    the code rather than the prose — see modules/kafka-acls/variables.tf for the
    call site behind every entry.

    `read_also` exists for a topic that is subscribed to but never read for
    records; it is folded into the same ConsumeTopics statement as `read` and
    is kept separate only so the distinction survives in the source.

    Pass modules/kafka-acls's `kafka_access` output here when both modules are
    in play, so the IAM policies and the Kafka ACLs are generated from one
    table and cannot drift.

    NOTE on clusters-job: it DOES consume messages.v1. §8.6's text sample
    (services/clusters-job/internal/job/textsample.go) is a groupless bounded
    read with kgo.ConsumeTopics + AfterMilli. An earlier revision of this
    module had `read = []` for it, which would have produced a role that could
    produce usage.v1 and then fail on every fetch.
  EOT
  type = map(object({
    read      = optional(list(string), [])
    write     = optional(list(string), [])
    create    = optional(list(string), [])
    read_also = optional(list(string), [])
  }))
  default = {
    provider-adapter = {
      read  = ["deletions.v1"]
      write = ["messages.v1"]
      # The adapter creates the coordination topic itself on startup with a
      # best-effort CreateTopics, and the cluster runs with
      # auto.create.topics.enable=false.
      create = ["adapter.shards.v1"]
      # Subscribed to but never read for records; the group membership is the
      # whole point (§7.2/A13).
      read_also = ["adapter.shards.v1"]
    }
    moderation-service = {
      read  = ["messages.v1"]
      write = ["flagged.v1", "deletions.v1", "usage.v1"]
    }
    review-service = {
      read  = ["flagged.v1"]
      write = ["deletions.v1"]
    }
    insights-service = {
      read = ["messages.v1", "flagged.v1"]
    }
    credits-service = {
      read = ["usage.v1"]
    }
    clusters-job = {
      read  = ["messages.v1"]
      write = ["usage.v1"]
    }
  }
}

variable "kafka_consumer_groups" {
  description = <<-EOT
    Consumer group names per service, matching what the binaries actually join.
    These come from the code: moderation-service and credits-service take theirs
    from MOD_CONSUMER_GROUP / CREDITS_CONSUMER_GROUP and default to the service
    name; insights-service hardcodes "insights-service" in
    internal/ingest/pipeline.go; provider-adapter uses "provider-adapter" on
    deletions.v1 (const in internal/deletion/deletion.go) and
    "provider-adapter-shards" for its §7.2 sharding coordinator.

    review-service and clusters-job join NO GROUP — review-service assigns
    partitions itself with kgo.ConsumePartitions (§7.6) and clusters-job takes
    a groupless bounded sample of messages.v1. The defaults below still list
    them, because on THIS path an IAM statement scoped to a group ARN that is
    never used is inert, and switching either service to a group later is then
    not an IAM change.

    The Kafka ACL path does not do the same thing, and the asymmetry is
    deliberate: an ACL on an unused group flips that group from "open because
    unlisted" to "closed to everyone the binding does not name" on a broker
    with allow.everyone.if.no.acl.found=true. Inert on one path, a real change
    on the other. modules/kafka-acls therefore emits only the groups the
    binaries actually join, and a root wiring both should pass that output here
    if it wants the two to match exactly.

    The "-*" entries are prefixed patterns covering a deployment that overrides
    MOD_CONSUMER_GROUP or CREDITS_CONSUMER_GROUP with a suffixed name.
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
