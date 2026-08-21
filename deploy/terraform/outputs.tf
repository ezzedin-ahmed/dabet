# The seam between this Terraform and the Helm charts.
#
# The contract lives at the bottom: helm_values_dabet and helm_values_dabet_deps
# are shaped to the key paths deploy/k8s/charts/dabet and
# deploy/k8s/charts/dabet-deps actually read, and are generated from the
# resources rather than transcribed, so the two sides cannot drift.
#
#   tofu output -raw helm_values_dabet_yaml       > values-aws-generated.yaml
#   tofu output -raw helm_values_dabet_deps_yaml  > deps-aws-generated.yaml
#   tofu output -raw app_secret_document_skeleton > /tmp/app.json
#
# The individual outputs above them exist so a team that composes its own values
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

    and the ServiceAccount name must match the one the trust policy was built
    against — "<helm_release_name>-<service>" unless overridden — or the token
    is rejected at runtime.
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

output "helm_values_dabet" {
  description = <<-EOT
    Values for deploy/k8s/charts/dabet, in that chart's own key paths
    (config.*, secrets.create, externalSecrets.*,
    services.<name>.serviceAccount.annotations).

    Only endpoints, names, ARNs and booleans. No secret VALUE appears here.
  EOT
  value       = local.helm_values_dabet
}

output "helm_values_dabet_yaml" {
  description = <<-EOT
    helm_values_dabet as YAML:

      tofu output -raw helm_values_dabet_yaml > values-aws-generated.yaml
      helm upgrade --install dabet deploy/k8s/charts/dabet \
        --namespace dabet \
        -f deploy/k8s/charts/dabet/values-aws.yaml \
        -f values-aws-generated.yaml
  EOT
  value       = yamlencode(local.helm_values_dabet)
}

output "helm_values_dabet_deps" {
  description = <<-EOT
    Values for deploy/k8s/charts/dabet-deps: every component AWS provides
    switched off with its external.* address filled in, and ClickHouse, Milvus
    and the inference workloads left running in the cluster.
  EOT
  value       = local.helm_values_dabet_deps
}

output "helm_values_dabet_deps_yaml" {
  description = "helm_values_dabet_deps as YAML."
  value       = yamlencode(local.helm_values_dabet_deps)
}

output "app_secret_document_skeleton" {
  description = <<-EOT
    The aggregate Secrets Manager document the chart's ExternalSecret extracts,
    with every value Terraform legitimately knows already filled in and REPLACE
    markers where it does not.

    The Postgres DSNs are the reason this exists: the chart wants complete
    connection strings, and the password lives in an RDS-managed secret that
    Terraform never reads. Host, port, database, user and sslmode=require are
    supplied; paste the password in.

      tofu output -raw app_secret_document_skeleton > /tmp/app.json
      # edit /tmp/app.json, then:
      aws secretsmanager put-secret-value \
        --secret-id dabet/prod/app --secret-string file:///tmp/app.json
      shred -u /tmp/app.json
  EOT
  value       = jsonencode(local.app_secret_document)
}

output "postgres_password_commands" {
  description = "How to read the RDS-generated passwords for the DSNs above."
  value = {
    identity = "aws secretsmanager get-secret-value --secret-id ${module.rds_identity.master_user_secret_arn} --query SecretString --output text"
    policy   = "aws secretsmanager get-secret-value --secret-id ${local.policy_db.secret_arn} --query SecretString --output text"
    kafka    = try("aws secretsmanager get-secret-value --secret-id ${module.msk.scram_secret_arn} --query SecretString --output text", null)
  }
}

output "kafka_scram_secret_arn" {
  description = <<-EOT
    One Secrets Manager ARN holding an MSK SASL/SCRAM {username, password}.
    Null unless SCRAM is in use.

    Kept for the single-credential shape. Under msk.scram_mode="per_service"
    there are several, and `kafka_scram_secret_arns` is the one to read.
  EOT
  value       = module.msk.scram_secret_arn
}

output "kafka_scram_secret_arns" {
  description = <<-EOT
    Secrets Manager ARN per SCRAM username: one per service plus the
    reconciler admin under "per_service", one service credential plus the admin
    under "shared". Empty unless SCRAM is in use.
  EOT
  value       = module.msk.scram_secret_arns
}

output "kafka_scram_usernames" {
  description = <<-EOT
    SCRAM username per dabet service — the identity each one authenticates as
    and therefore the Kafka principal its ACLs name. Every service resolves to
    the same name under msk.scram_mode="shared".
  EOT
  value       = module.kafka_acls.usernames
}

output "kafka_admin_username" {
  description = <<-EOT
    SCRAM username for the chart's topic and ACL reconcilers.

    It is not any service's credential: it is the only principal holding
    CreateTopic, Alter and the cluster-level Alter that writing ACLs requires,
    and it never holds Delete on anything.
  EOT
  value       = module.kafka_acls.admin_username
}

output "kafka_acl_matrix" {
  description = <<-EOT
    Every Kafka ACL binding this deployment needs, admin grants first, in the
    shape charts/dabet-deps takes as kafka.acls.rules.

    Already embedded in helm_values_dabet_deps; exposed separately so the
    matrix can be diffed against `kafka-acls.sh --list` on a live cluster.
  EOT
  value       = module.kafka_acls.rules
}

output "kafka_acl_commands" {
  description = <<-EOT
    The same bindings as kafka-acls.sh invocations, for applying them from a
    bastion inside the VPC rather than from the chart Job.

    Terraform does not apply them. Kafka ACLs are a Kafka protocol operation —
    the AWS provider has no resource for them, and the only provider that does
    needs a TCP connection to a broker, which the isolated data-tier subnets do
    not offer to anything running outside the VPC. The chart's ACL Job is the
    supported path; this is the manual one.

    ORDER MATTERS. Run them in the order given: with
    allow.everyone.if.no.acl.found=true, the first binding written against the
    Cluster resource decides who may write bindings at all, so the admin's own
    Alter grant has to land first.

    They assume a client.properties in the working directory:

      security.protocol=SASL_SSL
      sasl.mechanism=SCRAM-SHA-512
      sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule \
        required username="<admin>" password="<from secretsmanager>";
  EOT
  value = [
    for c in module.kafka_acls.commands :
    replace(c, "<BOOTSTRAP>", module.msk.bootstrap_brokers_sasl_scram)
  ]
}

output "kafka_external_secret_manifests" {
  description = <<-EOT
    ExternalSecret manifests for the per-service Kafka credentials and the
    reconciler admin's, ready for `kubectl apply -f -`.

    They exist as an output rather than as chart values because charts/dabet
    renders exactly ONE ExternalSecret — for the aggregate secret document —
    and per-service Kafka credentials need one each. `helm_values_dabet`
    already points every service's envRaw at the Secret names below; these
    manifests are what create them. A chart gap, recorded rather than worked
    around, because the fix belongs in charts/dabet.

      tofu output -raw kafka_external_secret_manifests | kubectl apply -f -

    Empty unless SCRAM is in use.
  EOT
  value       = local.kafka_external_secret_manifests
}

output "kafka_sasl_mechanism" {
  description = "KAFKA_SASL_MECHANISM for pkg/kafkax. Empty when the cluster is unauthenticated."
  value       = module.msk.sasl_mechanism
}
