variable "principals" {
  description = <<-EOT
    Who does what on Kafka. THIS IS §1.5, AND IT IS DERIVED FROM THE CODE, not
    from prose — every entry below was checked against the call site that makes
    the request. Getting it wrong fails at runtime: loudly for a producer
    (TOPIC_AUTHORIZATION_FAILED on the first send), and much more quietly for a
    consumer, which simply never joins its group.

    The evidence, service by service:

      provider-adapter
        produces  messages.v1   services/provider-adapter/internal/ingest/ingest.go
        consumes  deletions.v1  cmd/provider-adapter/main.go, group const in
                                internal/deletion/deletion.go ("provider-adapter")
        subscribes adapter.shards.v1 in group "provider-adapter-shards"
                                internal/shard/kafka.go. The topic carries no
                                records — the group's MEMBERSHIP is the fleet
                                view (§7.2/A13) — and the adapter creates it
                                itself with a best-effort CreateTopics(-1,-1),
                                which is why it needs Create on that one name.

      moderation-service
        consumes  messages.v1                       cmd/.../main.go
        produces  flagged.v1, deletions.v1          internal/mod/pipeline.go
        produces  usage.v1                          internal/mod/usage.go
        group     "moderation-service"              MOD_CONSUMER_GROUP, defaulted

      review-service
        consumes  flagged.v1    WITH NO CONSUMER GROUP. internal/queue/kafka.go
                                uses kgo.ConsumePartitions with explicit offsets
                                (§7.6), so there is no group to authorise and
                                none is granted. Read on the topic is enough.
        produces  deletions.v1  internal/api/handler.go

      insights-service
        consumes  messages.v1 AND flagged.v1, two consumer objects sharing one
                                group. cmd/insights-service/main.go; the group
                                name is a compile-time constant in
                                internal/ingest/pipeline.go, NOT env-overridable.
        produces  nothing

      credits-service
        consumes  usage.v1      cmd/credits-service/main.go
        group     "credits-service"  CREDITS_CONSUMER_GROUP, defaulted

      clusters-job
        produces  usage.v1      internal/job/usage.go
        consumes  messages.v1   internal/job/textsample.go — groupless, a
                                bounded read with kgo.ConsumeTopics + AfterMilli
                                and no commits, so again no group ACL.

      user-service, policy-service and clustering-service do not touch Kafka at
      all — no franz-go dependency, no topic constant, no consumer — so they
      appear nowhere here and get no credential.

    `groups` carries a pattern so that a deployment which overrides
    MOD_CONSUMER_GROUP or CREDITS_CONSUMER_GROUP can widen to `prefixed`
    without hand-writing a rule. The default is `literal`, matching exactly the
    name the binary joins.
  EOT

  type = map(object({
    # SCRAM username. Null derives it as "<username_prefix><service>".
    username = optional(string)

    read_topics   = optional(list(string), [])
    write_topics  = optional(list(string), [])
    create_topics = optional(list(string), [])

    groups = optional(list(object({
      name    = string
      pattern = optional(string, "literal")
    })), [])

    # Nothing in the codebase uses transactions today — there is no
    # TransactionalID anywhere in pkg/ or services/ — but the shape is here so
    # that adding one is a values change rather than a module change.
    transactional_ids = optional(list(string), [])
  }))

  default = {
    provider-adapter = {
      write_topics  = ["messages.v1"]
      read_topics   = ["deletions.v1", "adapter.shards.v1"]
      create_topics = ["adapter.shards.v1"]
      groups = [
        { name = "provider-adapter" },
        { name = "provider-adapter-shards" },
      ]
    }

    moderation-service = {
      read_topics  = ["messages.v1"]
      write_topics = ["flagged.v1", "deletions.v1", "usage.v1"]
      groups       = [{ name = "moderation-service" }]
    }

    review-service = {
      read_topics  = ["flagged.v1"]
      write_topics = ["deletions.v1"]
      groups       = []
    }

    insights-service = {
      read_topics = ["messages.v1", "flagged.v1"]
      groups      = [{ name = "insights-service" }]
    }

    credits-service = {
      read_topics = ["usage.v1"]
      groups      = [{ name = "credits-service" }]
    }

    clusters-job = {
      write_topics = ["usage.v1"]
      read_topics  = ["messages.v1"]
      groups       = []
    }
  }
}

variable "mode" {
  description = <<-EOT
    "per_service"  One SCRAM credential per service, each with its own Secrets
                   Manager secret, and ACLs scoped to exactly what that service
                   does. THE DEFAULT, and the only setting under which the ACL
                   matrix means anything: with distinct principals, a
                   compromised moderation-service credential cannot read
                   messages.v1 as insights-service or produce to a topic
                   moderation never writes.

    "shared"       One credential for everything, named by shared_username.
                   Every rule collapses onto that one principal, so the union of
                   the matrix is what it holds — which is, by construction,
                   blanket access. Keep it for a dev cluster or a single-tenant
                   scratch environment where six extra secrets and six extra
                   ExternalSecrets are more operational cost than the isolation
                   is worth.

    The trade-off is not security-versus-convenience in the abstract; it is six
    Secrets Manager secrets, six ExternalSecret manifests and six per-service
    env blocks against a blast radius that is currently "any service that is
    compromised can do anything any other service can do". For anything
    handling real creator data that is not a close call.
  EOT
  type        = string
  default     = "per_service"

  validation {
    condition     = contains(["per_service", "shared"], var.mode)
    error_message = "mode must be \"per_service\" or \"shared\"."
  }
}

variable "username_prefix" {
  description = <<-EOT
    Prefix for derived SCRAM usernames, so principals from two deployments in
    one account do not collide. "dabet-prod-" gives
    "dabet-prod-moderation-service".
  EOT
  type        = string
  default     = "dabet-"
}

variable "shared_username" {
  description = "SCRAM username used by every service when mode is \"shared\"."
  type        = string
  default     = "dabet"
}

variable "admin_username" {
  description = <<-EOT
    The credential the two chart reconciler Jobs use.

    It is deliberately NOT one of the service credentials: it is the only
    principal holding CreateTopic, Alter and the cluster-level Alter that
    managing ACLs requires, and lending a service those rights would undo the
    point of the matrix.

    It is also never granted Delete on anything, so the worst a compromised
    admin credential can do to the data is change a retention.
  EOT
  type        = string
  default     = "dabet-admin"
}

variable "admin_topics" {
  description = <<-EOT
    Topic patterns the admin credential may create and alter.

    The default is the literal wildcard, which is what makes the chart's topic
    registry the single place a topic is declared: narrowing this to a fixed
    list means every new topic in values.kafka.topics.registry silently fails
    to be created until Terraform is changed too, and that failure looks like
    an application bug.

    Narrowing buys less than it appears to, because the admin already holds
    cluster Alter — it manages the ACLs — and because Delete is never in the
    operation list, so it cannot destroy a topic whatever the pattern says.
    Narrow it anyway if a compliance regime asks for it:

      admin_topics = [
        { name = "messages.v1" }, { name = "flagged.v1" },
        { name = "deletions.v1" }, { name = "usage.v1" },
        { name = "adapter.shards.v1" },
      ]
  EOT
  type = list(object({
    name    = string
    pattern = optional(string, "literal")
  }))
  default = [{ name = "*", pattern = "literal" }]
}

variable "grant_idempotent_write" {
  description = <<-EOT
    Grant IdempotentWrite on the Cluster resource to every producing principal.

    §4.2 asks for enable.idempotence=true, and pkg/kafkax gets it by setting
    kgo.RequiredAcks(AllISRAcks) and leaving franz-go's default idempotent
    producer on. Before KIP-679 (Kafka 3.0) an idempotent producer needed
    IdempotentWrite on the CLUSTER in addition to Write on the topic; from 3.0
    the broker accepts topic Write alone. This deployment targets Kafka 3.9, so
    the grant is redundant — but it is cheap, it is scoped to one operation on
    one resource, and without it a downgrade to an older broker turns every
    produce into a CLUSTER_AUTHORIZATION_FAILED that reads like a bug in the
    producer. Left on.

    On the IAM authentication path the equivalent is
    kafka-cluster:WriteDataIdempotently on the cluster ARN, which modules/iam
    grants to the same set of principals.
  EOT
  type        = bool
  default     = true
}

variable "bootstrap_brokers" {
  description = <<-EOT
    Bootstrap string, used only to render the ready-to-run kafka-acls.sh
    commands in the `commands` output. Empty renders them with a placeholder.
  EOT
  type        = string
  default     = ""
}
