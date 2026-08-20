# The seam with deploy/k8s.
#
# Two documents, each shaped to the key paths its chart actually reads, so they
# can be fed straight to helm without a translation step:
#
#   local.helm_values_dabet       -> charts/dabet       (the nine services)
#   local.helm_values_dabet_deps  -> charts/dabet-deps  (the stateful deps)
#
# Rule for everything in them: endpoints, names, ARNs, booleans. Never a secret
# value. The chart resolves secrets through External Secrets Operator, and the
# separate app_secret_document_skeleton output is the document a human fills in
# and puts into Secrets Manager.
#
# Both are generated rather than transcribed, so the AWS side and the values
# file cannot drift.

locals {
  # ---------------------------------------------------------------------------
  # Scheduling, mapped onto the deps chart's per-component keys.
  #
  # Empty when the pool is off: the chart wraps both in `with`, which skips an
  # empty map or list, so an absent pool leaves the component unconstrained
  # rather than Pending against a selector that matches no node.
  # ---------------------------------------------------------------------------
  stateful_node_selector = var.stateful_node_group.enabled ? { "dabet.io/workload" = "stateful" } : {}

  stateful_tolerations = var.stateful_node_group.enabled ? [
    { key = "dabet.io/workload", operator = "Equal", value = "stateful", effect = "NoSchedule" },
  ] : []

  gpu_node_selector = var.gpu_node_group.enabled ? { "dabet.io/workload" = "vllm" } : {}

  gpu_tolerations = var.gpu_node_group.enabled ? [
    { key = "nvidia.com/gpu", operator = "Equal", value = "present", effect = "NoSchedule" },
    { key = "dabet.io/workload", operator = "Equal", value = "vllm", effect = "NoSchedule" },
  ] : []

  # ---------------------------------------------------------------------------
  # Environment variables that are configuration rather than credentials.
  #
  # The chart puts config.* into a ConfigMap under fixed §4.4 names and passes
  # anything in config.extra through verbatim. The extras below are the newer
  # variables the managed-connectivity work added to pkg/kafkax,
  # moderation-service and the two S3 consumers; the chart has no first-class
  # key for them, and config.extra is the documented hatch.
  # ---------------------------------------------------------------------------
  config_extra = merge(
    {
      # Reaches minio-go so the regional STS endpoint is used for IRSA, and so
      # virtual-host addressing resolves.
      S3_REGION = local.region

      # insights-service and clusters-job then assume their IRSA role instead of
      # needing a static access key. This is what makes S3_ACCESS_KEY and
      # S3_SECRET_KEY unnecessary.
      S3_CREDENTIALS_SOURCE = "irsa"
      S3_ADDRESSING_STYLE   = "virtual"
    },

    # moderation-service builds a cluster-aware go-redis client when asked.
    {
      REDIS_CLUSTER_ENABLED = tostring(module.elasticache.redis_cluster_mode)
      REDIS_TLS_ENABLED     = tostring(module.elasticache.redis_transit_encryption_enabled)
    },

    # pkg/kafkax. The SASL username and password are credentials and live in the
    # secret document, not here.
    module.msk.tls_enabled ? { KAFKA_TLS_ENABLED = "true" } : {},
    module.msk.sasl_mechanism == "" ? {} : { KAFKA_SASL_MECHANISM = module.msk.sasl_mechanism },
  )

  # ---------------------------------------------------------------------------
  # charts/dabet
  # ---------------------------------------------------------------------------
  helm_values_dabet = {
    config = {
      kafkaBrokers   = module.msk.bootstrap_brokers
      redisAddr      = module.elasticache.redis_addr
      memcachedAddrs = join(",", module.elasticache.memcached_node_addresses)

      # Both stay in-cluster: they are chart workloads, not AWS services, and
      # their addresses are Kubernetes service names the chart already knows.
      # Overridden here only so a reader sees they were considered.
      vllmEndpoint      = "http://dabet-vllm:8000"
      embeddingEndpoint = "http://dabet-embedding:8091"
      milvusAddr        = "dabet-milvus:19530"

      s3Endpoint = "https://s3.${local.region}.amazonaws.com"
      s3Bucket   = module.s3.embeddings_bucket_name
      logLevel   = "info"

      extra = local.config_extra
    }

    # The chart refuses to both create the Secret and let a secret manager fill
    # it, so this must be false whenever externalSecrets is on.
    secrets = {
      create = false
    }

    externalSecrets = {
      enabled = true

      secretStoreRef = {
        name = var.external_secrets_store_ref.name
        kind = var.external_secrets_store_ref.kind
      }

      refreshInterval = "1h"
      creationPolicy  = "Owner"

      # One extract of the aggregate document. Its required key list is in
      # charts/dabet/README.md; app_secret_document_skeleton renders it.
      dataFrom = [
        {
          extract = {
            key = module.secrets.secret_names["app"]
          }
        },
      ]
    }

    services = {
      for svc in local.services : svc => {
        serviceAccount = {
          annotations = {
            "eks.amazonaws.com/role-arn" = module.iam.service_role_arns[svc]
          }
        }
      }
    }
  }

  # ---------------------------------------------------------------------------
  # charts/dabet-deps
  #
  # Every component AWS provides is switched off and replaced by an external:
  # address. What is left running in the cluster is exactly the set AWS has no
  # managed equivalent of — ClickHouse, Milvus, and the inference workloads.
  # ---------------------------------------------------------------------------
  helm_values_dabet_deps = {
    kafka = {
      enabled = false
    }
    postgres = {
      enabled = false
    }
    redis = {
      enabled = false
    }
    memcached = {
      enabled = false
    }
    minio = {
      enabled = false
    }

    external = {
      kafka = {
        brokers = module.msk.bootstrap_brokers
      }

      # DSNs, so they carry a password; left empty on purpose. The chart
      # publishes them into its connection Secret, and a password in a
      # Terraform output would be a password in Terraform state. Fill these in
      # from the same secret document the app chart reads, or leave the app
      # chart to source POSTGRES_DSN_* through ESO and leave these empty.
      postgres = {
        identity = ""
        policy   = ""
      }

      redis = {
        addr    = module.elasticache.redis_addr
        cluster = module.elasticache.redis_cluster_mode
      }

      memcached = {
        addrs = join(",", module.elasticache.memcached_node_addresses)
      }

      s3 = {
        # Empty endpoint means real S3 rather than MinIO.
        endpoint = ""
        region   = local.region
        bucket   = module.s3.embeddings_bucket_name
        # No static keys: insights-service and clusters-job assume their IRSA
        # role via S3_CREDENTIALS_SOURCE above.
        accessKey = ""
        secretKey = ""
      }
    }

    # ClickHouse and Milvus stay in-cluster — AWS manages neither. When the
    # stateful node pool exists they are pinned to it, so a moderation pod never
    # lands on a node about to be drained for a StatefulSet rollout. Both keys
    # are always present and empty when the pool is off, because the chart uses
    # `with` and skips an empty map or list.
    clickhouse = {
      enabled      = true
      nodeSelector = local.stateful_node_selector
      tolerations  = local.stateful_tolerations
    }

    milvus = {
      enabled      = true
      nodeSelector = local.stateful_node_selector
      tolerations  = local.stateful_tolerations
    }

    # vLLM and the embedder go to the GPU pool when there is one.
    vllm = {
      enabled      = var.gpu_node_group.enabled
      nodeSelector = local.gpu_node_selector
      tolerations  = local.gpu_tolerations
    }

    embedding = {
      nodeSelector = local.gpu_node_selector
      tolerations  = local.gpu_tolerations
    }
  }

  # ---------------------------------------------------------------------------
  # The Secrets Manager document
  #
  # Rendered with every value Terraform legitimately knows already filled in,
  # and REPLACE markers everywhere it does not. The DSNs are the reason this
  # exists: the chart wants POSTGRES_DSN_* as complete connection strings, and
  # the password lives in an RDS-managed secret that Terraform deliberately
  # never reads. So the host, port, database, user and sslmode are supplied and
  # only the password is left to paste in.
  #
  # sslmode=require is not decoration: the parameter group sets
  # rds.force_ssl=1, and Compose's sslmode=disable is refused by the server.
  # ---------------------------------------------------------------------------
  identity_dsn = "postgres://${module.rds_identity.username}:REPLACE_WITH_PASSWORD@${module.rds_identity.address}:${module.rds_identity.port}/${module.rds_identity.database_name}?sslmode=require"
  policy_dsn   = "postgres://${local.policy_db.username}:REPLACE_WITH_PASSWORD@${local.policy_db.address}:${local.policy_db.port}/${local.policy_db.database}?sslmode=require"

  app_secret_document = merge(
    {
      POSTGRES_DSN          = local.identity_dsn
      POSTGRES_DSN_IDENTITY = local.identity_dsn
      POSTGRES_DSN_POLICY   = local.policy_dsn
      # §5.3's billing schema and §7.6's review_cursors both have foreign keys
      # to creators(id), so both live on the identity instance.
      POSTGRES_DSN_BILLING = local.identity_dsn
      POSTGRES_DSN_REVIEW  = local.identity_dsn

      CLICKHOUSE_DSN = "clickhouse://dabet:REPLACE_WITH_PASSWORD@dabet-clickhouse:9000/dabet"

      # Empty on purpose: S3_CREDENTIALS_SOURCE=irsa means the pod assumes its
      # role instead. The keys stay present because the chart's secretKeyRef
      # fails the pod on a missing key, and an empty string is a legitimate
      # value where absence is not.
      S3_ACCESS_KEY = ""
      S3_SECRET_KEY = ""

      JWT_PRIVATE_KEY = "REPLACE_WITH_RS256_PRIVATE_KEY_PEM"
      JWT_PUBLIC_KEY  = "REPLACE_WITH_RS256_PUBLIC_KEY_PEM"
      # Unused under RS256; present so jwt.alg can be flipped back.
      JWT_SECRET = ""

      STRIPE_SECRET_KEY     = "REPLACE_WITH_STRIPE_SECRET_KEY"
      STRIPE_WEBHOOK_SECRET = "REPLACE_WITH_STRIPE_WEBHOOK_SECRET"

      OAUTH_YOUTUBE_CLIENT_ID     = "REPLACE"
      OAUTH_YOUTUBE_CLIENT_SECRET = "REPLACE"
      OAUTH_TWITCH_CLIENT_ID      = "REPLACE"
      OAUTH_TWITCH_CLIENT_SECRET  = "REPLACE"
      OAUTH_DISCORD_CLIENT_ID     = "REPLACE"
      OAUTH_DISCORD_CLIENT_SECRET = "REPLACE"

      # The mock provider has no place in a real environment, but the key must
      # exist for the pod to start.
      OAUTH_MOCK_CLIENT_ID     = ""
      OAUTH_MOCK_CLIENT_SECRET = ""

      MAIL_SMTP_USERNAME = ""
      MAIL_SMTP_PASSWORD = ""
    },

    # SASL/SCRAM credentials for MSK. Terraform generated the password into its
    # own Secrets Manager secret, so this points at where to copy it from
    # rather than carrying it.
    module.msk.sasl_mechanism == "SCRAM-SHA-512" ? {
      KAFKA_SASL_USERNAME = "REPLACE_FROM_${module.msk.scram_secret_arn}"
      KAFKA_SASL_PASSWORD = "REPLACE_FROM_${module.msk.scram_secret_arn}"
    } : {},
  )
}
