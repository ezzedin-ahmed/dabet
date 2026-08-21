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
  # Per-service Kafka credentials
  #
  # Under least privilege each service authenticates as its own SCRAM
  # principal, so KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD cannot come from
  # the one shared secret document every pod reads — they have to be per pod.
  #
  # charts/dabet has a first-class hatch for this: services.<name>.envRaw is
  # spliced verbatim into the container's env list, so a secretKeyRef fits
  # without the chart needing a new key. What the chart does NOT have is a
  # per-service ExternalSecret template — externalsecret.yaml renders exactly
  # one, for the aggregate document — so the Secrets those refs point at are
  # created from the `kafka_external_secret_manifests` output, which is a
  # kubectl apply away. That is a chart gap, recorded rather than worked
  # around, because the fix belongs in charts/dabet.
  #
  # In "shared" mode every service resolves to the same username, and the
  # blocks below all point at the same Secret. The wiring is identical either
  # way, which is what makes flipping msk.scram_mode a one-line change.
  # ---------------------------------------------------------------------------
  kafka_service_secret_name = { for svc, u in module.kafka_acls.usernames : svc => "dabet-kafka-${svc}" }

  # Only the services that actually speak Kafka. user-service, policy-service
  # and clustering-service have no franz-go dependency at all and get no
  # credential — a service with no Kafka client and a Kafka credential is just
  # an unnecessary secret.
  kafka_service_env = module.msk.sasl_mechanism == "SCRAM-SHA-512" ? {
    for svc, u in module.kafka_acls.usernames : svc => [
      {
        name = "KAFKA_SASL_USERNAME"
        valueFrom = {
          secretKeyRef = { name = local.kafka_service_secret_name[svc], key = "KAFKA_SASL_USERNAME" }
        }
      },
      {
        name = "KAFKA_SASL_PASSWORD"
        valueFrom = {
          secretKeyRef = { name = local.kafka_service_secret_name[svc], key = "KAFKA_SASL_PASSWORD" }
        }
      },
    ]
  } : {}

  # The Secret the two reconciler Jobs read. Its own credential, deliberately
  # not any service's: it is the only principal with CreateTopic and cluster
  # Alter.
  kafka_admin_secret_name = "dabet-kafka-admin"

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
      for svc in local.services : svc => merge(
        {
          serviceAccount = {
            annotations = {
              "eks.amazonaws.com/role-arn" = module.iam.service_role_arns[svc]
            }
          }
        },
        # Per-service SCRAM credential, for the six services that speak Kafka.
        # Absent entirely for the other three, so their pods carry no Kafka
        # credential at all.
        contains(keys(local.kafka_service_env), svc) ? {
          envRaw = local.kafka_service_env[svc]
        } : {},
      )
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
      # No in-cluster brokers...
      enabled = false

      # ...but the §4.2 topics still have to exist, and MSK runs with
      # auto.create.topics.enable=false. The chart's reconciler is no longer
      # gated on kafka.enabled: it needs a broker ADDRESS, which
      # external.kafka.brokers below supplies. Without this the deployment
      # installs cleanly and then fails every produce with
      # UNKNOWN_TOPIC_OR_PARTITION.
      topics = {
        enabled = true
      }

      # How the reconciler authenticates. The credential is a Secret NAME, not
      # a value: the chart takes it as a secretKeyRef and there is no values
      # key that could hold a password.
      admin = {
        auth = {
          tls = {
            # MSK offers SASL only over TLS (port 9096), and its brokers
            # present a publicly trusted certificate, so no CA bundle.
            enabled = module.msk.tls_enabled
          }
          sasl = {
            mechanism = module.msk.sasl_mechanism == "SCRAM-SHA-512" ? "SCRAM-SHA-512" : ""
            existingSecret = {
              name = local.kafka_admin_secret_name
            }
          }
        }
      }

      # SASL authenticates; it does not authorise. Without these every
      # credential can reach every topic. The rules come from
      # modules/kafka-acls — the same §1.5 table that produced the IAM
      # policies — and are applied by a Job in the cluster, because the
      # brokers have no network path to wherever Terraform runs. Off unless
      # SCRAM is the authentication mode, since ACLs are meaningless on a
      # cluster that does not authenticate.
      acls = {
        enabled = module.msk.sasl_mechanism == "SCRAM-SHA-512"
        rules   = module.kafka_acls.rules
      }
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
        # The REGIONAL URL, not the empty string.
        #
        # Empty means "the SDK default" to a Go client, and that is what the
        # app chart's config.s3Endpoint relies on. It does NOT mean that to
        # this chart: the in-cluster Milvus needs an address it can split into
        # host, port and useSSL (templates/milvus/milvus.yaml does a urlParse),
        # and _validate.tpl fails the whole render with "milvus needs an object
        # store" when the key is empty and minio is disabled — which is exactly
        # the combination this file produces. Emitting the endpoint the app
        # chart already uses satisfies both.
        endpoint = "https://s3.${local.region}.amazonaws.com"
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

    # KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD are deliberately ABSENT.
    #
    # This document is one Secrets Manager value that all nine pods read
    # through a single ExternalSecret, so a Kafka credential in it would hand
    # the same credential to every service — which is exactly what per-service
    # SCRAM users exist to prevent. Nothing in charts/dabet references these
    # two keys either: no service's `secretEnv` maps them, so an entry here
    # would sit unused in nine pods' worth of Secret.
    #
    # The real values arrive per pod, from a per-service Secret, through
    # services.<name>.envRaw above. Under msk.scram_mode = "shared" that is
    # still true — the six envRaw blocks just resolve to the same credential.
  )

  # ---------------------------------------------------------------------------
  # ExternalSecret manifests for the Kafka credentials
  #
  # charts/dabet renders exactly one ExternalSecret, for the aggregate
  # document. Per-service Kafka credentials need one each, plus one for the
  # reconciler admin, and there is no chart key for them yet — so they are
  # rendered here and applied with kubectl. Recorded as a chart gap in the
  # README rather than worked around, because the fix belongs in charts/dabet.
  #
  # Each MSK SCRAM secret holds {"username": ..., "password": ...}, so the
  # remoteRef property extracts the two fields into the two env var names
  # pkg/kafkax reads.
  # ---------------------------------------------------------------------------
  kafka_external_secret_targets = module.msk.sasl_mechanism == "SCRAM-SHA-512" ? merge(
    {
      (local.kafka_admin_secret_name) = module.kafka_acls.admin_username
    },
    {
      for svc, u in module.kafka_acls.usernames :
      local.kafka_service_secret_name[svc] => u
    },
  ) : {}

  kafka_external_secret_manifests = join("\n---\n", [
    for k8s_name, scram_user in local.kafka_external_secret_targets : yamlencode({
      apiVersion = "external-secrets.io/v1beta1"
      kind       = "ExternalSecret"
      metadata = {
        name      = k8s_name
        namespace = var.kubernetes_namespace
      }
      spec = {
        refreshInterval = "1h"
        secretStoreRef = {
          name = var.external_secrets_store_ref.name
          kind = var.external_secrets_store_ref.kind
        }
        target = {
          name           = k8s_name
          creationPolicy = "Owner"
        }
        data = [
          {
            secretKey = "KAFKA_SASL_USERNAME"
            remoteRef = {
              key      = try(module.msk.scram_secret_names[scram_user], "")
              property = "username"
            }
          },
          {
            secretKey = "KAFKA_SASL_PASSWORD"
            remoteRef = {
              key      = try(module.msk.scram_secret_names[scram_user], "")
              property = "password"
            }
          },
        ]
      }
    })
  ])
}
