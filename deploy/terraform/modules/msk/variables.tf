variable "name" {
  description = "MSK cluster name."
  type        = string
}

variable "vpc_id" {
  description = "VPC the brokers live in."
  type        = string
}

variable "subnet_ids" {
  description = <<-EOT
    Isolated data-tier subnets, one per AZ. MSK requires the broker count to be
    a whole multiple of the number of subnets, which is enforced below.
  EOT
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security groups allowed to reach the brokers. Normally just the EKS cluster security group."
  type        = list(string)
}

variable "kafka_version" {
  description = <<-EOT
    Kafka version. Check the MSK supported-versions list before changing:
    MSK removes versions, and an unsupported value fails at apply, not at plan.
    A version change is an in-place rolling update, not a replacement.
  EOT
  type        = string
  default     = "3.9.x"
}

variable "broker_count" {
  description = <<-EOT
    Total brokers, a multiple of the AZ count. Three is the §3 floor.

    Six is the production default and comes out of §4.2's partition counts:
    (512 + 128 + 128 + 32) topics-partitions x 3 replicas = 2400 partition
    replicas. MSK's own guidance caps a kafka.m5.large at roughly 1000
    partitions per broker and a 4xlarge at roughly 4000; at six brokers that is
    ~400 replicas each, which leaves headroom for consumer-group and internal
    topics. Three brokers would put 800 on each, which is fine for the smaller
    instance sizes but leaves nothing spare for a broker being replaced.
  EOT
  type        = number
  default     = 6
}

variable "instance_type" {
  description = <<-EOT
    Broker instance type.

    Sizing from N6: 50K msg/s at ~300 B per §4.2 record is ~15 MB/s of ingress,
    tripled by replication and doubled again by the two consumer groups on
    messages.v1. The 500K msg/s peak is ten times that. CPU, not network, is the
    binding constraint once 512 partitions and zstd are in play, which is why
    the production example uses a 2xlarge rather than a large.
  EOT
  type        = string
  default     = "kafka.m5.large"
}

variable "broker_storage_gb" {
  description = <<-EOT
    EBS per broker in GiB.

    From §4.2's retention: messages.v1 at 50K msg/s x ~300 B x 24 h is ~1.3 TB
    raw, and zstd takes maybe a third of that on the wire and on disk;
    flagged.v1 at 7 days and a low single-digit flag rate is a few hundred GB;
    usage.v1 is aggregated per creator-minute (§5.3, ~800 writes/s) and is
    small. Replicated three ways and spread over six brokers, ~400 GiB each
    covers the sustained rate. The default doubles that, because a consumer
    group that falls behind (§4.7 says accept the lag) keeps segments alive that
    would otherwise have aged out.

    This applies at CREATE time only. The cluster ignores later changes to it so
    that Application Auto Scaling can grow the volumes without every subsequent
    plan proposing a shrink that MSK would refuse — see the lifecycle block in
    main.tf.
  EOT
  type        = number
  default     = 1000
}

variable "enable_storage_autoscaling" {
  description = "Grow broker storage automatically when utilisation crosses the target. Storage never shrinks."
  type        = bool
  default     = true
}

variable "storage_autoscaling_max_gb" {
  description = "Ceiling for broker storage autoscaling, per broker. MSK's own hard limit is 16384 GiB."
  type        = number
  default     = 4000
}

variable "storage_autoscaling_target_percent" {
  description = "Utilisation percentage that triggers a storage expansion."
  type        = number
  default     = 70
}

variable "provisioned_throughput_mibps" {
  description = <<-EOT
    Provisioned EBS throughput per broker. Null uses the volume's baseline.

    UNVERIFIED: AWS documents provisioned throughput as available only from
    kafka.m5.4xlarge (and the equivalent m7g size) upward, and the valid MiBps
    range depends on the instance type. Leave it null unless you have checked
    the current matrix for your instance type — an invalid pair fails at apply.
  EOT
  type        = number
  default     = null
}

variable "client_authentication" {
  description = <<-EOT
    How clients authenticate. One of "scram", "iam" or "unauthenticated".

    "scram"           SASL/SCRAM-SHA-512 over TLS, with the credential held in
                      Secrets Manager and associated with the cluster. **This is
                      the default, and it is the one that works from a pod.**

                      pkg/kafkax supports AWS_MSK_IAM, but franz-go's MSK IAM
                      mechanism reads AWS credentials from the environment and
                      does no STS exchange — so an IRSA projected service-account
                      token cannot drive it, and using it would mean shipping a
                      static IAM user key, which is the one thing a managed
                      deployment must not need. SCRAM avoids that: the pod reads
                      one Secrets Manager value through its IRSA role (or through
                      External Secrets Operator) and authenticates with it.

    "iam"             SASL/OAUTHBEARER authorised by IAM. Keeps per-service topic
                      authorisation in IAM policy, which is the nicer model, and
                      the iam module writes those policies either way. Choose it
                      only if the client can obtain AWS credentials — a CI job
                      with a role, or a build of kafkax that pulls in
                      aws-sdk-go-v2 for the STS exchange.

    "unauthenticated" No client auth. Defensible here — the brokers sit in
                      subnets with no internet route and a security group only
                      the cluster can traverse — but it gives up per-service
                      topic authorisation entirely.
  EOT
  type        = string
  default     = "scram"

  validation {
    condition     = contains(["scram", "iam", "unauthenticated"], var.client_authentication)
    error_message = "client_authentication must be \"scram\", \"iam\" or \"unauthenticated\"."
  }
}

variable "scram_username" {
  description = <<-EOT
    SASL/SCRAM username for the SINGLE-CREDENTIAL fallback, used only when
    `scram_users` is empty. Goes into the Secrets Manager value as
    {username, password}.
  EOT
  type        = string
  default     = "dabet"
}

variable "scram_users" {
  description = <<-EOT
    SASL/SCRAM users to create, as a map of username to description. One
    Secrets Manager secret each, one generated password each, all of them
    associated with the cluster.

    Empty falls back to a single credential named by `scram_username`. That
    fallback is the dev shape and it is worth being explicit about what it
    costs: SCRAM authenticates but does not authorise, authorisation on a SCRAM
    cluster is Kafka ACLs, and one credential shared by every service means the
    ACL matrix can only ever grant that one principal the UNION of what all of
    them need. Blast radius is then "any compromised service can do anything
    any service can do".

    Populate it from modules/kafka-acls's `scram_users` output for the
    least-privilege shape: one principal per service plus the reconciler admin,
    each matched by ACLs scoped to what that service actually does.

    Secret names are "AmazonMSK_<cluster>_<username>", which is both the prefix
    MSK requires and enough to tell two deployments apart in one account.
  EOT
  type        = map(string)
  default     = {}
}

variable "allow_everyone_if_no_acl_found" {
  description = <<-EOT
    Whether a resource with NO ACL is open to every authenticated principal.

    MSK's default is `true`, and this module keeps it — turning it off is a
    two-phase operation and doing it in one apply bricks the cluster:

      Phase 1  Create the cluster with this `true`, apply the ACL matrix (the
               chart's ACL Job, or the `commands` output from a bastion), and
               confirm every service is connected and consuming.
      Phase 2  Set this to `false` and apply. MSK adds a configuration revision
               and rolls the brokers.

    Doing phase 2 first leaves a cluster where nothing is authorised for
    anything, including the credential that would write the ACLs — recovering
    means another configuration revision and another rolling restart.

    Leaving it `true` is not equivalent to having no ACLs. Once a resource has
    a binding the flag stops applying TO THAT RESOURCE, so every topic and
    group in the matrix is genuinely restricted. What stays open is anything
    NOT in the matrix: a topic someone creates by hand, or an internal topic.
    Flip it to `false` in production once the matrix is proven.
  EOT
  type        = bool
  default     = true
}

variable "scram_password_length" {
  description = <<-EOT
    Length of the generated SCRAM password.

    The password is generated by Terraform rather than supplied, because a
    password variable would be plaintext in state AND in whatever CI prints the
    plan; generating it means it is in state only. To take it out of state
    entirely, rotate it out of band after the first apply — the resource ignores
    changes to the secret's value, so the new one is not reverted, and MSK reads
    the current version.
  EOT
  type        = number
  default     = 32
}

variable "encryption_in_transit_client_broker" {
  description = <<-EOT
    TLS, TLS_PLAINTEXT or PLAINTEXT for client-to-broker traffic. TLS_PLAINTEXT
    exists only to migrate an existing plaintext fleet; it is not a resting
    state. IAM authentication requires TLS.
  EOT
  type        = string
  default     = "TLS"

  validation {
    condition     = contains(["TLS", "TLS_PLAINTEXT", "PLAINTEXT"], var.encryption_in_transit_client_broker)
    error_message = "encryption_in_transit_client_broker must be TLS, TLS_PLAINTEXT or PLAINTEXT."
  }
}

variable "kms_key_arn" {
  description = "KMS key for encryption at rest on the broker volumes."
  type        = string
}

variable "enhanced_monitoring" {
  description = <<-EOT
    DEFAULT, PER_BROKER, PER_TOPIC_PER_BROKER or PER_TOPIC_PER_PARTITION.
    PER_TOPIC_PER_PARTITION on a 512-partition topic emits a very large number
    of CloudWatch metrics and is billed per metric; PER_BROKER is the sane
    default and open monitoring below covers the rest.
  EOT
  type        = string
  default     = "PER_BROKER"
}

variable "enable_open_monitoring" {
  description = <<-EOT
    Expose the JMX and node exporters so the in-cluster Prometheus (§4.5) can
    scrape the brokers directly, rather than paying CloudWatch per metric.
  EOT
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Retention for the broker log group."
  type        = number
  default     = 30
}

variable "log_kms_key_arn" {
  description = "KMS key for the broker CloudWatch log group."
  type        = string
  default     = null
}

variable "extra_server_properties" {
  description = "Extra broker properties merged over the module defaults, as a map of name to value."
  type        = map(string)
  default     = {}
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
