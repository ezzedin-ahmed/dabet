# The values-aws.yaml contract, built once and exposed by two outputs
# (helm_values_aws and helm_values_aws_yaml).
#
# Rule for anything added here: endpoints, names, ARNs, booleans. Never a secret
# value. The charts resolve secrets through External Secrets Operator or the
# Secrets Store CSI driver, using the ARNs below and the IRSA roles.

locals {
  helm_values_aws = {
    global = {
      aws = {
        region      = local.region
        clusterName = module.eks.cluster_name
      }

      namespace = var.kubernetes_namespace

      kafka = {
        brokers = module.msk.bootstrap_brokers
        auth    = module.msk.client_authentication
        tls     = var.msk.encryption_in_transit_client_broker == "TLS"
      }

      postgres = {
        identity = {
          host      = module.rds_identity.address
          port      = module.rds_identity.port
          database  = module.rds_identity.database_name
          username  = module.rds_identity.username
          sslmode   = "require"
          secretArn = module.rds_identity.master_user_secret_arn
        }
        policy = {
          host      = local.policy_db.address
          port      = local.policy_db.port
          database  = local.policy_db.database
          username  = local.policy_db.username
          sslmode   = "require"
          secretArn = local.policy_db.secret_arn
        }
      }

      redis = {
        addr        = module.elasticache.redis_addr
        clusterMode = module.elasticache.redis_cluster_mode
        tls         = module.elasticache.redis_transit_encryption_enabled
      }

      memcached = {
        addrs = module.elasticache.memcached_node_addresses
      }

      s3 = {
        region = local.region
        # Empty endpoint means real S3. Compose sets this to MinIO instead.
        endpoint         = ""
        bucket           = module.s3.embeddings_bucket_name
        milvusBucket     = module.s3.milvus_bucket_name
        clickhouseBucket = module.s3.clickhouse_bucket_name
      }

      secrets = merge(
        module.secrets.secret_arns,
        {
          "postgres/identity" = module.rds_identity.master_user_secret_arn
          "postgres/policy"   = local.policy_db.secret_arn
        },
      )

      observability = {
        applicationLogGroup   = module.observability.application_log_group_name
        prometheusRemoteWrite = module.observability.prometheus_remote_write_url
        alarmTopicArn         = module.observability.alarm_topic_arn
      }
    }

    serviceAccounts = {
      for svc, arn in module.iam.service_role_arns : svc => {
        name = var.service_account_names[svc]
        annotations = {
          "eks.amazonaws.com/role-arn" = arn
        }
      }
    }

    nodePools = {
      gpu = {
        enabled      = var.gpu_node_group.enabled
        nodeSelector = module.eks.gpu_node_selector
        tolerations  = module.eks.gpu_tolerations
      }
      stateful = {
        enabled      = var.stateful_node_group.enabled
        nodeSelector = { "dabet.io/workload" = "stateful" }
        tolerations  = module.eks.stateful_tolerations
      }
    }

    externalSecrets = {
      serviceAccountRoleArn = module.iam.external_secrets_role_arn
      namespace             = var.external_secrets_namespace
      serviceAccountName    = var.external_secrets_service_account
    }

    # AWS has no managed ClickHouse or Milvus, so both run from the charts.
    # What Terraform contributes is a bucket and an IRSA role for each.
    milvus = {
      bucket                = module.s3.milvus_bucket_name
      serviceAccountRoleArn = module.iam.milvus_role_arn
    }

    clickhouse = {
      bucket                = module.s3.clickhouse_bucket_name
      serviceAccountRoleArn = module.iam.clickhouse_role_arn
      passwordSecretArn     = module.secrets.secret_arns["clickhouse/password"]
    }
  }
}
