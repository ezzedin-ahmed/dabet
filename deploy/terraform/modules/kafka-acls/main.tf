# The Kafka authorisation matrix.
#
# THIS MODULE CREATES NOTHING. It has no provider, no resource and no data
# source: it is the one place the §1.5 producer/consumer table is written down,
# turned into the three shapes that need it —
#
#   * `rules`         the deps chart's kafka.acls.rules, applied by a Job
#                     inside the cluster (see below)
#   * `commands`      the same bindings as kafka-acls.sh invocations, for a
#                     bastion or a runbook
#   * `kafka_access`  the shape modules/iam wants, so the SCRAM path and the
#                     IAM path can never disagree about who reads what
#
# WHY THE ACLS ARE NOT APPLIED HERE, which is the question a reader will have:
#
# Kafka ACLs are a Kafka protocol operation. The AWS provider has no resource
# for them — `aws_msk_*` covers the cluster, its configuration and its SCRAM
# secret associations, and stops there. The only Terraform provider that does
# manage them, Mongey/kafka, speaks the Kafka protocol directly, which means
# whatever runs `tofu apply` must open a TCP connection to a broker.
#
# It cannot. modules/network puts the brokers in the isolated data-tier
# subnets, whose route tables carry no default route in either direction, and
# modules/msk admits ingress only from a referenced security group id. A
# Terraform run from a laptop, from GitHub Actions, or from any runner outside
# the VPC has no path to port 9096 at all. Adding the provider would also make
# `tofu plan` require a live broker and a credential just to read state, which
# would break the credential-free validation this directory is built around.
#
# The honest answer is that ACLs have to be applied from inside the cluster —
# so that is what happens. The EKS cluster already has the network path (its
# security group is the one the brokers admit), already has the credential
# (External Secrets Operator projects it), and already has a scheduler. The
# deps chart carries a Job that runs kafka-acls.sh; Terraform hands it this
# matrix through helm values. Terraform stays the source of truth, the cluster
# is the applier, and neither has to pretend it can reach something it cannot.
#
# The escape hatch for a team that wants Terraform to own the bindings is real
# but is a different topology: run Terraform on a runner inside the VPC (or
# through a bastion with a forwarded port), add
#
#   terraform { required_providers { kafka = { source = "Mongey/kafka" } } }
#
# and feed it `local.rules` below. The matrix does not change; only who applies
# it does. That choice belongs to whoever owns the CI network, which is why it
# is documented rather than decided here.

locals {
  # The username each service authenticates as. In "shared" mode every service
  # collapses onto one principal, which is exactly why the union of the matrix
  # becomes blanket access — see the `mode` variable.
  derived_usernames = {
    for svc, p in var.principals :
    svc => coalesce(p.username, "${var.username_prefix}${svc}")
  }

  usernames = {
    for svc, u in local.derived_usernames :
    svc => var.mode == "shared" ? var.shared_username : u
  }

  # ---------------------------------------------------------------------------
  # Operation sets
  #
  # Describe accompanies everything because a Kafka client cannot use a
  # resource it cannot see: Metadata, ListOffsets and OffsetFetch are all
  # Describe, and a principal with Read and no Describe fails at the metadata
  # request with a message about the topic not existing.
  # ---------------------------------------------------------------------------
  ops_read_topic  = ["Read", "Describe"]
  ops_write_topic = ["Write", "Describe"]

  # CreateTopics checks Create on the Topic resource first and falls back to
  # Create on the Cluster, so a topic-scoped grant is enough and the adapter
  # never needs cluster-wide create rights.
  ops_create_topic = ["Create", "Describe"]

  # JoinGroup/SyncGroup/Heartbeat need Read on the Group; OffsetFetch needs
  # Describe. franz-go issues both.
  ops_group = ["Read", "Describe"]

  ops_txn = ["Write", "Describe"]

  # ---------------------------------------------------------------------------
  # Flatten the matrix into one binding per (principal, resource) pair.
  # ---------------------------------------------------------------------------
  raw = flatten([
    for svc, p in var.principals : concat(
      [for t in p.read_topics : {
        svc = svc, rtype = "topic", rname = t, pattern = "literal", ops = local.ops_read_topic
      }],
      [for t in p.write_topics : {
        svc = svc, rtype = "topic", rname = t, pattern = "literal", ops = local.ops_write_topic
      }],
      [for t in p.create_topics : {
        svc = svc, rtype = "topic", rname = t, pattern = "literal", ops = local.ops_create_topic
      }],
      [for g in p.groups : {
        svc = svc, rtype = "group", rname = g.name, pattern = g.pattern, ops = local.ops_group
      }],
      [for i in p.transactional_ids : {
        svc = svc, rtype = "transactional-id", rname = i, pattern = "literal", ops = local.ops_txn
      }],
      var.grant_idempotent_write && length(p.write_topics) > 0 ? [{
        svc = svc, rtype = "cluster", rname = "kafka-cluster", pattern = "literal", ops = ["IdempotentWrite"]
      }] : [],
    )
  ])

  # A principal that both reads and writes a topic (nothing does today, but
  # deletions.v1 is one product decision away from it) must end up with ONE
  # binding carrying both operations, not two bindings racing each other.
  # Keyed on the tuple, then the operation lists are unioned.
  #
  # '|' is safe as a separator: SCRAM usernames here are derived from service
  # names, and Kafka resource names in this deployment are dotted lowercase.
  binding_keys = sort(distinct([
    for r in local.raw : "${local.usernames[r.svc]}|${r.rtype}|${r.rname}|${r.pattern}"
  ]))

  service_rules = [
    for k in local.binding_keys : {
      principal    = "User:${split("|", k)[0]}"
      resourceType = split("|", k)[1]
      resourceName = split("|", k)[2]
      patternType  = split("|", k)[3]
      operations = sort(distinct(flatten([
        for r in local.raw : r.ops
        if "${local.usernames[r.svc]}|${r.rtype}|${r.rname}|${r.pattern}" == k
      ])))
    }
  ]

  # ---------------------------------------------------------------------------
  # The admin credential, and why its bindings come first.
  #
  # MSK ships with allow.everyone.if.no.acl.found = true, and that flag is
  # evaluated PER RESOURCE: a resource with no binding is open to everyone, a
  # resource with one binding is closed to everyone the binding does not name.
  # So the very first binding written against the Cluster resource decides who
  # may write bindings from then on. If it is not the admin's own Alter grant,
  # the run has just locked itself out of finishing.
  #
  # Emitting them first and having the applier walk the list in order is the
  # whole mitigation. It is also why the ACL Job runs at a lower hook weight
  # than the topic reconciler: CreateTopic is itself an ACL now.
  # ---------------------------------------------------------------------------
  admin_cluster_ops = concat(
    [
      # DescribeAcls / CreateAcls: kafka-acls.sh --list and --add.
      "Describe",
      "Alter",
      # kafka-configs.sh against broker entities, and the reconciler's own
      # readback of what it just set.
      "DescribeConfigs",
      "AlterConfigs",
    ],
  )

  admin_topic_ops = [
    "Create",          # kafka-topics.sh --create
    "Describe",        # --list, --describe
    "Alter",           # --alter --partitions (raising a count)
    "DescribeConfigs", # reading retention back
    "AlterConfigs",    # kafka-configs.sh --alter
    # Delete is deliberately absent. The reconciler refuses to shrink a topic
    # precisely because that would mean delete-and-recreate; not holding the
    # permission makes that refusal structural rather than a script check.
  ]

  admin_rules = concat(
    [{
      principal    = "User:${var.admin_username}"
      resourceType = "cluster"
      resourceName = "kafka-cluster"
      patternType  = "literal"
      operations   = local.admin_cluster_ops
    }],
    [for t in var.admin_topics : {
      principal    = "User:${var.admin_username}"
      resourceType = "topic"
      resourceName = t.name
      patternType  = t.pattern
      operations   = local.admin_topic_ops
    }],
  )

  rules = concat(local.admin_rules, local.service_rules)

  # ---------------------------------------------------------------------------
  # The same thing as shell, for a bastion or a runbook.
  # ---------------------------------------------------------------------------
  bootstrap = var.bootstrap_brokers != "" ? var.bootstrap_brokers : "<BOOTSTRAP>"

  commands = [
    for r in local.rules : join(" ", concat(
      [
        "kafka-acls.sh --bootstrap-server ${local.bootstrap}",
        "--command-config client.properties --add",
        "--allow-principal '${r.principal}'",
      ],
      r.resourceType == "cluster" ? ["--cluster"] : [
        "--${r.resourceType} '${r.resourceName}' --resource-pattern-type ${r.patternType}",
      ],
      [for o in r.operations : "--operation ${o}"],
    ))
  ]

  # ---------------------------------------------------------------------------
  # The IAM-path view of the same table, so modules/iam cannot drift from it.
  # ---------------------------------------------------------------------------
  kafka_access = {
    for svc, p in var.principals : svc => {
      read      = p.read_topics
      write     = p.write_topics
      create    = p.create_topics
      read_also = []
    }
  }

  kafka_consumer_groups = {
    for svc, p in var.principals : svc => [for g in p.groups : g.name]
    if length(p.groups) > 0
  }

  # Usernames that need a SCRAM credential: every service principal plus the
  # admin. In "shared" mode the service side collapses to one name, and the
  # admin still gets its own — a shared SERVICE credential is a deliberate
  # trade-off, a shared ADMIN credential would just hand every service
  # CreateTopic and cluster Alter, which is not a trade-off, it is a mistake.
  scram_users = merge(
    {
      (var.admin_username) = "dabet topic and ACL reconciler (chart Jobs only)"
    },
    {
      for u in distinct(values(local.usernames)) :
      u => "dabet ${var.mode == "shared" ? "shared service credential" : "service credential"}"
    },
  )
}
