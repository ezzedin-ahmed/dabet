# The seam between this Terraform and the Helm chart.
#
# `helm_values_aws` at the bottom is the contract: its shape is what
# deploy/k8s/values-aws.yaml is expected to consume, and it is generated rather
# than transcribed so the two cannot drift.
#
#   tofu -chdir=examples/prod output -raw helm_values_aws_yaml > values-aws.yaml
#
# The individual outputs above it exist so a team that composes their own values
# file can take one piece at a time.

# ---------------------------------------------------------------------------
# Cluster
# ---------------------------------------------------------------------------

output "cluster_name" {
  description = "EKS cluster name, for `aws eks update-kubeconfig`."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "Kubernetes API endpoint."
  value       = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64 CA bundle for the API server."
  value       = module.eks.cluster_certificate_authority_data
}

output "cluster_security_group_id" {
  description = "EKS-managed cluster security group; the only thing allowed to reach the data tier."
  value       = module.eks.cluster_security_group_id
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider for IRSA."
  value       = module.eks.oidc_provider_arn
}

output "kubeconfig_command" {
  description = "Ready-to-run command that writes a kubeconfig entry for this cluster."
  value       = "aws eks update-kubeconfig --region ${local.region} --name ${module.eks.cluster_name}"
}

# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------

output "vpc_id" {
  description = "VPC id."
  value       = module.network.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnets carrying the EKS nodes."
  value       = module.network.private_subnet_ids
}

output "data_subnet_ids" {
  description = "Isolated data-tier subnets."
  value       = module.network.data_subnet_ids
}

output "nat_gateway_public_ips" {
  description = "Egress addresses, for provider allow-lists."
  value       = module.network.nat_gateway_public_ips
}

# ---------------------------------------------------------------------------
# Data services
# ---------------------------------------------------------------------------

output "postgres_identity" {
  description = <<-EOT
    Connection details for the identity instance (identity, billing and review
    schemas). The password is NOT here — it lives in the Secrets Manager secret
    named by secret_arn, as JSON {username, password}, generated and rotated by
    RDS.

    POSTGRES_DSN must carry sslmode=require: the parameter group sets
    rds.force_ssl=1, and local Compose's sslmode=disable will be refused.
  EOT
  value = {
    host       = module.rds_identity.address
    port       = module.rds_identity.port
    database   = module.rds_identity.database_name
    username   = module.rds_identity.username
    secret_arn = module.rds_identity.master_user_secret_arn
    sslmode    = "require"
  }
}

output "postgres_policy" {
  description = <<-EOT
    Connection details for the policy instance. Falls back to the identity
    instance when create_policy_instance is false.
  EOT
  value = {
    host       = local.policy_db.address
    port       = local.policy_db.port
    database   = local.policy_db.database
    username   = local.policy_db.username
    secret_arn = local.policy_db.secret_arn
    sslmode    = "require"
  }
}

output "kafka_brokers" {
  description = "KAFKA_BROKERS. SASL/IAM over TLS when msk.client_authentication is \"iam\"."
  value       = module.msk.bootstrap_brokers
}

output "kafka_cluster_arn" {
  description = "MSK cluster ARN."
  value       = module.msk.cluster_arn
}

output "kafka_auth" {
  description = "\"iam\" or \"unauthenticated\". Tells the chart whether to configure a SASL mechanism."
  value       = module.msk.client_authentication
}

output "redis_addr" {
  description = "REDIS_ADDR. The cluster-mode configuration endpoint when cluster mode is on."
  value       = module.elasticache.redis_addr
}

output "redis_cluster_mode" {
  description = "Whether the client must be cluster-aware."
  value       = module.elasticache.redis_cluster_mode
}

output "redis_tls" {
  description = "Whether the client must dial TLS."
  value       = module.elasticache.redis_transit_encryption_enabled
}

output "memcached_addrs" {
  description = <<-EOT
    MEMCACHED_ADDRS as a list of host:port. Node addresses, not the
    auto-discovery configuration endpoint — see modules/elasticache for why.
  EOT
  value       = module.elasticache.memcached_node_addresses
}

output "s3_embeddings_bucket" {
  description = "S3_BUCKET for insights-service and clusters-job."
  value       = module.s3.embeddings_bucket_name
}

output "s3_milvus_bucket" {
  description = "Object storage for an in-cluster Milvus."
  value       = module.s3.milvus_bucket_name
}

output "s3_clickhouse_bucket" {
  description = "Object storage for in-cluster ClickHouse backups or S3-backed disks."
  value       = module.s3.clickhouse_bucket_name
}

# ---------------------------------------------------------------------------
# Secrets and identity
# ---------------------------------------------------------------------------

output "secret_arns" {
  description = <<-EOT
    Every §4.4 secret ARN the charts need, keyed by role. The two postgres/*
    entries are RDS-managed secrets holding {username, password}; the rest are
    containers this Terraform created and a human populated.
  EOT
  value = merge(
    module.secrets.secret_arns,
    {
      "postgres/identity" = module.rds_identity.master_user_secret_arn
      "postgres/policy"   = local.policy_db.secret_arn
    },
  )
}

output "secret_arn_prefix" {
  description = "Wildcard ARN covering the deployment's Secrets Manager path."
  value       = module.secrets.secret_arn_prefix
}

output "irsa_role_arns" {
  description = <<-EOT
    IRSA role ARN per dabet service. The app chart annotates each ServiceAccount

      eks.amazonaws.com/role-arn: <this value>

    and the ServiceAccount name must match var.service_account_names or the
    trust policy will not accept the token.
  EOT
  value       = module.iam.service_role_arns
}

output "platform_role_arns" {
  description = "IRSA role ARNs for external-secrets, milvus, clickhouse and any extra_irsa_roles."
  value       = module.iam.platform_role_arns
}

output "external_secrets_role_arn" {
  description = "IRSA role for External Secrets Operator."
  value       = module.iam.external_secrets_role_arn
}

output "kms_key_arns" {
  description = "The four customer-managed keys."
  value = {
    data    = aws_kms_key.data.arn
    secrets = aws_kms_key.secrets.arn
    eks     = aws_kms_key.eks.arn
    logs    = aws_kms_key.logs.arn
  }
}

# ---------------------------------------------------------------------------
# Observability
# ---------------------------------------------------------------------------

output "application_log_group_name" {
  description = "CloudWatch log group for the log forwarder."
  value       = module.observability.application_log_group_name
}

output "alarm_topic_arn" {
  description = "SNS topic the infrastructure alarms publish to."
  value       = module.observability.alarm_topic_arn
}

output "prometheus_remote_write_url" {
  description = "AMP remote_write endpoint, when the workspace is enabled."
  value       = module.observability.prometheus_remote_write_url
}

# ---------------------------------------------------------------------------
# The contract
# ---------------------------------------------------------------------------

output "helm_values_aws" {
  description = <<-EOT
    Everything above, in the shape deploy/k8s/values-aws.yaml consumes.

    Only endpoints, names, ARNs and booleans. No secret VALUE ever appears here
    — the charts resolve secrets through External Secrets Operator or the
    Secrets Store CSI driver using the ARNs and the IRSA roles.
  EOT
  value       = local.helm_values_aws
}

output "helm_values_aws_yaml" {
  description = "helm_values_aws rendered as YAML, ready to redirect into values-aws.yaml."
  value       = yamlencode(local.helm_values_aws)
}
