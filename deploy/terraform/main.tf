# The §3 target topology, composed from the modules in ./modules.
#
# Dependency order is mostly implicit through references. The two places where
# it is not — the data tier's security groups referencing the EKS cluster
# security group, and IAM roles referencing the OIDC provider — are also
# references, so nothing here needs an explicit depends_on.
#
# What this file does NOT create, deliberately:
#
#   ClickHouse and Milvus. AWS has no managed equivalent of either, and the
#   charts must run on any cluster (that is the whole reason AWS is optional
#   here). Both therefore run in-cluster, from the charts, on the optional
#   stateful node pool. Terraform provides what they need from AWS: an S3
#   bucket each, an IRSA role each, the EBS CSI driver for their volumes, and
#   the node pool taint. See the README for the alternative — ClickHouse Cloud
#   and Zilliz Cloud over PrivateLink — and why it is not the default.
#
#   Kafka topics. §4.2's partition counts and retentions belong next to the code
#   that depends on them; the charts create them, as Compose already does.

module "network" {
  source = "./modules/network"

  name     = var.name
  vpc_cidr = var.vpc_cidr
  azs      = var.azs

  private_subnet_cidrs = coalesce(var.network.private_subnet_cidrs, [
    for i in range(length(var.azs)) : cidrsubnet(var.vpc_cidr, 2, i)
  ])
  public_subnet_cidrs = coalesce(var.network.public_subnet_cidrs, [
    for i in range(length(var.azs)) : cidrsubnet(var.vpc_cidr, 8, 192 + i)
  ])
  data_subnet_cidrs = coalesce(var.network.data_subnet_cidrs, [
    for i in range(length(var.azs)) : cidrsubnet(var.vpc_cidr, 6, 52 + i)
  ])

  single_nat_gateway      = var.network.single_nat_gateway
  enable_flow_logs        = var.network.enable_flow_logs
  flow_log_retention_days = var.log_retention_days
  eks_cluster_name        = var.name
  kms_key_arn             = aws_kms_key.logs.arn

  interface_endpoint_services = coalesce(var.network.interface_endpoint_services, [
    "ecr.api",
    "ecr.dkr",
    "logs",
    "secretsmanager",
    "sts",
    "kms",
  ])

  tags = local.tags
}

module "eks" {
  source = "./modules/eks"

  cluster_name       = var.name
  kubernetes_version = var.kubernetes_version

  vpc_id     = module.network.vpc_id
  subnet_ids = module.network.private_subnet_ids

  endpoint_public_access = var.cluster_endpoint_public_access
  public_access_cidrs    = var.cluster_public_access_cidrs

  kms_key_arn     = aws_kms_key.eks.arn
  log_kms_key_arn = aws_kms_key.logs.arn

  cluster_log_retention_days   = var.log_retention_days
  cluster_admin_principal_arns = var.cluster_admin_principal_arns

  general_node_group  = var.general_node_group
  gpu_node_group      = var.gpu_node_group
  stateful_node_group = var.stateful_node_group

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Postgres — two instances (§3)
# ---------------------------------------------------------------------------

module "rds_identity" {
  source = "./modules/rds"

  identifier = "${var.name}-identity"
  vpc_id     = module.network.vpc_id
  subnet_ids = module.network.data_subnet_ids

  allowed_security_group_ids = [module.eks.cluster_security_group_id]

  instance_class           = var.rds_identity.instance_class
  allocated_storage_gb     = var.rds_identity.allocated_storage_gb
  max_allocated_storage_gb = var.rds_identity.max_allocated_storage_gb
  engine_version           = var.rds_identity.engine_version
  parameter_group_family   = var.rds_identity.parameter_group_family
  multi_az                 = var.rds_identity.multi_az
  backup_retention_days    = var.rds_identity.backup_retention_days
  deletion_protection      = var.rds_identity.deletion_protection
  skip_final_snapshot      = var.rds_identity.skip_final_snapshot
  monitoring_interval      = var.rds_identity.monitoring_interval
  extra_parameters         = var.rds_identity.extra_parameters

  kms_key_arn        = aws_kms_key.data.arn
  log_kms_key_arn    = aws_kms_key.logs.arn
  log_retention_days = var.log_retention_days

  tags = merge(local.tags, { Schema = "identity,billing,review" })
}

module "rds_policy" {
  source = "./modules/rds"
  count  = var.create_policy_instance ? 1 : 0

  identifier = "${var.name}-policy"
  vpc_id     = module.network.vpc_id
  subnet_ids = module.network.data_subnet_ids

  allowed_security_group_ids = [module.eks.cluster_security_group_id]

  instance_class           = var.rds_policy.instance_class
  allocated_storage_gb     = var.rds_policy.allocated_storage_gb
  max_allocated_storage_gb = var.rds_policy.max_allocated_storage_gb
  engine_version           = var.rds_policy.engine_version
  parameter_group_family   = var.rds_policy.parameter_group_family
  multi_az                 = var.rds_policy.multi_az
  backup_retention_days    = var.rds_policy.backup_retention_days
  deletion_protection      = var.rds_policy.deletion_protection
  skip_final_snapshot      = var.rds_policy.skip_final_snapshot
  monitoring_interval      = var.rds_policy.monitoring_interval
  extra_parameters         = var.rds_policy.extra_parameters

  kms_key_arn        = aws_kms_key.data.arn
  log_kms_key_arn    = aws_kms_key.logs.arn
  log_retention_days = var.log_retention_days

  tags = merge(local.tags, { Schema = "policy" })
}

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

module "msk" {
  source = "./modules/msk"

  name       = var.name
  vpc_id     = module.network.vpc_id
  subnet_ids = module.network.data_subnet_ids

  allowed_security_group_ids = [module.eks.cluster_security_group_id]

  kafka_version                       = var.msk.kafka_version
  broker_count                        = var.msk.broker_count
  instance_type                       = var.msk.instance_type
  broker_storage_gb                   = var.msk.broker_storage_gb
  storage_autoscaling_max_gb          = var.msk.storage_autoscaling_max_gb
  client_authentication               = var.msk.client_authentication
  encryption_in_transit_client_broker = var.msk.encryption_in_transit_client_broker
  enhanced_monitoring                 = var.msk.enhanced_monitoring
  provisioned_throughput_mibps        = var.msk.provisioned_throughput_mibps
  extra_server_properties             = var.msk.extra_server_properties

  kms_key_arn        = aws_kms_key.data.arn
  log_kms_key_arn    = aws_kms_key.logs.arn
  log_retention_days = var.log_retention_days

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------

module "elasticache" {
  source = "./modules/elasticache"

  name       = var.name
  vpc_id     = module.network.vpc_id
  subnet_ids = module.network.data_subnet_ids

  allowed_security_group_ids = [module.eks.cluster_security_group_id]

  redis_cluster_mode               = var.elasticache.redis_cluster_mode
  redis_node_type                  = var.elasticache.redis_node_type
  redis_shards                     = var.elasticache.redis_shards
  redis_replicas_per_shard         = var.elasticache.redis_replicas_per_shard
  redis_transit_encryption_enabled = var.elasticache.redis_transit_encryption_enabled
  redis_engine_version             = var.elasticache.redis_engine_version
  redis_parameter_group_family     = var.elasticache.redis_parameter_group_family

  memcached_enabled    = var.elasticache.memcached_enabled
  memcached_node_type  = var.elasticache.memcached_node_type
  memcached_node_count = var.elasticache.memcached_node_count

  kms_key_arn = aws_kms_key.data.arn

  tags = local.tags
}

# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

module "s3" {
  source = "./modules/s3"

  name_prefix = var.name
  kms_key_arn = aws_kms_key.data.arn

  force_destroy                   = var.s3.force_destroy
  embeddings_versioning_enabled   = var.s3.embeddings_versioning_enabled
  deny_object_deletion            = var.s3.deny_object_deletion
  delete_exempt_principal_arns    = var.s3.delete_exempt_principal_arns
  embeddings_expiration_days      = var.s3.embeddings_expiration_days
  create_milvus_bucket            = var.s3.create_milvus_bucket
  create_clickhouse_backup_bucket = var.s3.create_clickhouse_backup_bucket
  create_access_log_bucket        = var.s3.create_access_log_bucket

  embeddings_lifecycle_transitions = coalesce(var.s3.lifecycle_transitions, [
    { days = 30, storage_class = "STANDARD_IA" },
    { days = 180, storage_class = "GLACIER_IR" },
  ])

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Secrets (§4.4)
# ---------------------------------------------------------------------------

module "secrets" {
  source = "./modules/secrets"

  name_prefix = local.secrets_prefix
  kms_key_arn = aws_kms_key.secrets.arn

  tags = local.tags
}

# ---------------------------------------------------------------------------
# IRSA
# ---------------------------------------------------------------------------

module "iam" {
  source = "./modules/iam"

  name_prefix       = var.name
  oidc_provider_arn = module.eks.oidc_provider_arn
  oidc_provider_url = module.eks.oidc_provider_url

  namespace             = var.kubernetes_namespace
  service_account_names = var.service_account_names

  msk_cluster_arn = var.msk.client_authentication == "iam" ? module.msk.cluster_arn : null

  embeddings_bucket_arn = module.s3.embeddings_bucket_arn
  milvus_bucket_arn     = module.s3.milvus_bucket_arn
  clickhouse_bucket_arn = module.s3.clickhouse_bucket_arn

  service_secret_arns = local.service_secret_arns

  kms_key_arns = {
    secrets = aws_kms_key.secrets.arn
    data    = aws_kms_key.data.arn
  }

  external_secrets = {
    enabled              = true
    namespace            = var.external_secrets_namespace
    service_account_name = var.external_secrets_service_account
    secret_arn_prefixes = distinct([
      module.secrets.secret_arn_prefix,
      # The RDS-managed master password secrets sit outside the module's path,
      # so they are granted by their own ARNs rather than by the prefix.
      module.rds_identity.master_user_secret_arn,
      local.policy_db.secret_arn,
    ])
  }

  milvus_service_account = {
    namespace            = var.kubernetes_namespace
    service_account_name = "milvus"
  }

  clickhouse_service_account = {
    namespace            = var.kubernetes_namespace
    service_account_name = "clickhouse"
  }

  extra_roles = var.extra_irsa_roles

  tags = local.tags
}

# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

module "observability" {
  source = "./modules/observability"

  name_prefix     = var.name
  cluster_name    = module.eks.cluster_name
  log_kms_key_arn = aws_kms_key.logs.arn

  application_log_retention_days = var.observability.application_log_retention_days

  create_sns_topic          = var.observability.create_sns_topic
  alarm_email_subscriptions = var.observability.alarm_email_subscriptions
  additional_alarm_actions  = var.observability.additional_alarm_actions

  rds_instance_identifiers = compact([
    module.rds_identity.identifier,
    var.create_policy_instance ? module.rds_policy[0].identifier : "",
  ])

  msk_cluster_name = module.msk.cluster_name

  elasticache_cluster_ids = concat(
    module.elasticache.redis_member_cluster_ids,
    compact([module.elasticache.memcached_cluster_id]),
  )

  create_prometheus_workspace   = var.observability.create_prometheus_workspace
  prometheus_alert_rules_yaml   = var.observability.prometheus_alert_rules_yaml
  prometheus_log_retention_days = var.log_retention_days

  tags = local.tags
}
