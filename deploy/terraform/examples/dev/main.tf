# A small, non-production dabet on AWS.
#
# The point of this file is to show which knobs a team actually turns. Everything
# not mentioned here takes the module default, which is the production-shaped
# one — so the diff between this file and examples/prod/main.tf is a fair
# summary of what "non-production" costs you.
#
# Three deliberate departures from the §3 target, each of which exists because
# the application code is not there yet rather than because dev is small:
#
#   1. Redis runs in NON-cluster mode. §3 targets Redis Cluster and §4.3's hash
#      tags are already in the keyspace for it, but moderation-service builds a
#      go-redis single-node client (redis.NewClient), which does not follow the
#      MOVED redirections a cluster-mode configuration endpoint returns. Dev
#      runs what the code can talk to; prod turns cluster mode on and the client
#      has to move to redis.NewClusterClient first.
#
#   2. MSK runs unauthenticated with TLS_PLAINTEXT. IAM authentication needs
#      pkg/kafkax to grow a SASL mechanism and a TLS dialer; until it does, the
#      plaintext listener is what franz-go can reach. TLS_PLAINTEXT opens both
#      ports, so the switch is a client change and not a cluster rebuild.
#
#   3. Transit encryption is off on the caches, for the same reason.
#
# None of the three is a cost decision, and all three are called out in the
# README as the work that stands between this and the target topology.

module "dabet" {
  source = "../.."

  name        = "dabet-dev"
  environment = "dev"
  azs         = var.azs

  kubernetes_version           = "1.33"
  cluster_admin_principal_arns = var.cluster_admin_principal_arns

  network = {
    # One NAT gateway instead of three. Saves ~$65/month and makes egress a
    # single-AZ dependency, which is exactly the trade a dev environment wants.
    single_nat_gateway = true
    enable_flow_logs   = false

    # No interface endpoints: six services across three AZs is ~$130/month in
    # hourly charges alone, and at dev traffic volumes the NAT gateway carries
    # the same calls for a few dollars. The S3 GATEWAY endpoint is created
    # unconditionally and is free — that one is not optional at any size,
    # because §8.4's parquet writes would otherwise be NAT-processed.
    interface_endpoint_services = []
  }

  general_node_group = {
    instance_types = ["m7i.xlarge"]
    min_size       = 2
    desired_size   = 2
    max_size       = 6
    disk_size_gb   = 50
  }

  # No GPU pool: tools/mockllm and tools/mockembed stand in for vLLM and the
  # embedder, and the whole moderation path works against them.
  gpu_node_group = {
    enabled = false
  }

  # No dedicated stateful pool either — ClickHouse and Milvus run on the general
  # pool here. They still need their S3 buckets, which the s3 module creates.
  stateful_node_group = {
    enabled = false
  }

  # One Postgres instance carrying every schema. §3 wants two in the target, but
  # collapsing them in dev keeps §6.3's policies.creator_id -> creators(id)
  # foreign key intact, which is the more faithful thing to develop against.
  create_policy_instance = false

  rds_identity = {
    instance_class        = "db.t4g.medium"
    allocated_storage_gb  = 20
    multi_az              = false
    backup_retention_days = 1
    deletion_protection   = false
    skip_final_snapshot   = true
  }

  # Unused when create_policy_instance is false, but the variable has no
  # default, so it still has to be given a value.
  rds_policy = {
    instance_class        = "db.t4g.medium"
    allocated_storage_gb  = 20
    multi_az              = false
    backup_retention_days = 1
    deletion_protection   = false
    skip_final_snapshot   = true
  }

  msk = {
    broker_count      = 3
    instance_type     = "kafka.t3.small"
    broker_storage_gb = 100

    client_authentication               = "unauthenticated"
    encryption_in_transit_client_broker = "TLS_PLAINTEXT"

    # 3 partitions per topic locally (docker-compose.yml) and 512 in the target
    # (§4.2). Neither is set here: topic creation belongs to the charts. What
    # the broker default controls is anything created implicitly, and a
    # t3.small carrying 512 x 3 replicas would not be a useful dev environment.
    extra_server_properties = {
      "default.replication.factor" = "2"
      "min.insync.replicas"        = "1"
    }

    storage_autoscaling_max_gb = 200
    enhanced_monitoring        = "DEFAULT"
  }

  elasticache = {
    redis_cluster_mode               = false
    redis_node_type                  = "cache.t4g.micro"
    redis_replicas_per_shard         = 0
    redis_transit_encryption_enabled = false

    memcached_node_type  = "cache.t4g.micro"
    memcached_node_count = 1
  }

  s3 = {
    # A dev environment has to be destroyable. Both of these are false in prod,
    # where the corpus cannot be recomputed once §4.8's 24 h of Kafka retention
    # has passed.
    force_destroy        = true
    deny_object_deletion = false

    # No tiering ladder: nothing here lives long enough to reach 30 days, and a
    # STANDARD_IA object deleted early is billed for the 30-day minimum anyway.
    lifecycle_transitions = []

    # Two weeks is plenty for development data and stops a forgotten load test
    # from accruing indefinitely.
    embeddings_expiration_days = 14

    create_access_log_bucket = false
  }

  observability = {
    application_log_retention_days = 7
    create_sns_topic               = false
    create_prometheus_workspace    = false
  }

  log_retention_days = 7

  tags = {
    CostCentre = "engineering"
  }
}
