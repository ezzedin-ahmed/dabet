# The §3 target topology, sized for the N5/N6 numbers.
#
# Read the cost note in this directory's README before applying. This is a
# five-figure-a-month deployment at the sizes below, and the S3 line grows every
# day it runs.
#
# Sizing summary — the reasoning is in deploy/terraform/README.md:
#
#   general pool   F3 in test/load/README.md measures ~170-200 msg/s per
#                  moderation-service instance, so N6's 50 000 msg/s baseline
#                  wants ~250-300 of them. desired_size starts far below that;
#                  max_size is what has to be able to carry it.
#   RDS identity   §5.2's ~25 GB of logical data at 10M creators, with room for
#                  indexes, WAL and bloat.
#   MSK            §4.2's 800 partitions at RF 3 = 2400 partition replicas, and
#                  ~15 MB/s of ingress at the sustained rate.
#   Redis          §4.3's keyspace is five minutes of state, so this is sized
#                  for request rate, not capacity.
#   S3             the dominant cost, growing at §8.4's ~3.5 TB/day. Nothing
#                  here changes that; the §8.4 sampling ceiling does.

module "dabet" {
  source = "../.."

  name        = "dabet-prod"
  environment = "prod"
  azs         = var.azs

  kubernetes_version = "1.33"

  # Private API endpoint. Reach it from a VPN, Direct Connect, or a bastion in
  # the VPC. Turning it on requires cluster_public_access_cidrs, and the module
  # refuses 0.0.0.0/0.
  cluster_endpoint_public_access = false
  cluster_admin_principal_arns   = var.cluster_admin_principal_arns

  network = {
    single_nat_gateway = false # one per AZ: egress survives losing one
    enable_flow_logs   = true
  }

  general_node_group = {
    instance_types = ["m7i.2xlarge"]
    capacity_type  = "ON_DEMAND"
    min_size       = 6
    desired_size   = 12
    max_size       = 60
    disk_size_gb   = 100
  }

  gpu_node_group = {
    enabled        = var.enable_gpu_pool
    instance_types = ["g6.2xlarge"] # 1x L4 each
    min_size       = 0
    desired_size   = 4
    max_size       = 12
    disk_size_gb   = 200
  }

  # ClickHouse and Milvus, tainted onto their own nodes so a moderation pod
  # never lands on a node that is about to be drained for a StatefulSet
  # rollout. Memory-optimised because both are memory-hungry.
  stateful_node_group = {
    enabled        = true
    instance_types = ["r7i.2xlarge"]
    min_size       = 3
    desired_size   = 3
    max_size       = 12
    disk_size_gb   = 200
  }

  # Two instances, per §3. Note the consequence recorded in the variable
  # description: §6.3's policies.creator_id -> creators(id) foreign key cannot
  # span two instances and becomes an application invariant.
  create_policy_instance = true

  rds_identity = {
    # identity + billing + review. §5.3 puts the write path at ~800 credit
    # ledger writes/s at 50K concurrently active creators, which is modest;
    # the instance is sized for connection count and index working set.
    instance_class           = "db.m7g.xlarge"
    allocated_storage_gb     = 200
    max_allocated_storage_gb = 1000
    multi_az                 = true
    backup_retention_days    = 30
    deletion_protection      = true
    skip_final_snapshot      = false
  }

  rds_policy = {
    # §6.8's two cache layers mean Postgres sees a policy read only on a cold
    # cache, and writes are a human editing a policy.
    instance_class           = "db.m7g.large"
    allocated_storage_gb     = 100
    max_allocated_storage_gb = 500
    multi_az                 = true
    backup_retention_days    = 30
    deletion_protection      = true
    skip_final_snapshot      = false
  }

  msk = {
    broker_count      = 6
    instance_type     = "kafka.m7g.2xlarge"
    broker_storage_gb = 1000

    storage_autoscaling_max_gb = 4000

    # SASL/IAM over TLS. This is what lets a pod reach Kafka with nothing but
    # its IRSA role — no static credential anywhere.
    #
    # BLOCKING on an application change: pkg/kafkax builds a plain franz-go
    # client with no SASL mechanism and no TLS dialer. Until that is fixed, set
    # client_authentication = "unauthenticated" and
    # encryption_in_transit_client_broker = "TLS_PLAINTEXT" as examples/dev
    # does, and switch back afterwards — the change is in place, not a rebuild.
    client_authentication               = "iam"
    encryption_in_transit_client_broker = "TLS"

    enhanced_monitoring = "PER_BROKER"

    # Provisioned EBS throughput is available only from the larger instance
    # sizes and the valid MiBps range varies by type; left at the volume
    # baseline rather than guessing. Check the current matrix before setting it.
    # provisioned_throughput_mibps = 250
  }

  elasticache = {
    # §3's target, and what §4.3's hash tags exist for.
    #
    # BLOCKING on the same kind of application change as MSK:
    # moderation-service uses redis.NewClient, which does not follow cluster
    # redirections, and passes no TLS config. Both flags below need
    # redis.NewClusterClient with a TLSConfig on the other side.
    redis_cluster_mode               = true
    redis_node_type                  = "cache.r7g.large"
    redis_shards                     = 3
    redis_replicas_per_shard         = 1
    redis_transit_encryption_enabled = true

    memcached_node_type  = "cache.m7g.large"
    memcached_node_count = 3
  }

  s3 = {
    force_destroy        = false
    deny_object_deletion = true

    # STANDARD -> STANDARD_IA at 30 days -> GLACIER_IR at 180. No DEEP_ARCHIVE:
    # §8.6's on-demand recluster reads arbitrary historical windows and cannot
    # wait hours for a restore. If you bound that feature to a fixed window,
    # adding a DEEP_ARCHIVE transition beyond it is the single largest saving
    # available in this file.
    lifecycle_transitions = [
      { days = 30, storage_class = "STANDARD_IA" },
      { days = 180, storage_class = "GLACIER_IR" },
    ]

    # Null: §4.8 keeps embeddings indefinitely, and they carry no author_id and
    # no text, so they are not attributable to an individual.
    embeddings_expiration_days = null

    create_milvus_bucket            = true
    create_clickhouse_backup_bucket = true
  }

  observability = {
    application_log_retention_days = 30
    create_sns_topic               = true
    alarm_email_subscriptions      = var.alarm_emails

    # AMP, because §4.5's fail_open_total is a Prometheus counter that no
    # CloudWatch alarm can see, and §4.5 says it must be alerted on.
    create_prometheus_workspace = true
    prometheus_alert_rules_yaml = file("${path.module}/alert-rules.yaml")
  }

  log_retention_days = 30

  tags = {
    CostCentre = "platform"
    DataClass  = "internal"
  }
}
