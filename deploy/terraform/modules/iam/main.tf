# IRSA roles: one per dabet service, plus the platform roles.
#
# Every role's trust policy pins BOTH conditions on the OIDC token:
#
#   <issuer>:sub  system:serviceaccount:<namespace>:<serviceaccount>
#   <issuer>:aud  sts.amazonaws.com
#
# The aud condition is the one that is routinely left out. Without it, the trust
# policy accepts any token this cluster's issuer signs — including a projected
# token minted for some other audience — and a pod that can read another
# ServiceAccount's token can assume the role.
#
# Nothing in this file grants an action on "*". Kafka permissions are scoped to
# topic and group ARNs derived from the cluster ARN; S3 to a bucket ARN; Secrets
# Manager to explicit secret ARNs; KMS to the two named keys.

locals {
  # arn:aws:kafka:<region>:<account>:cluster/<name>/<uuid>-<n>
  #   -> arn:aws:kafka:<region>:<account>:topic/<name>/<uuid>-<n>/<topic>
  #   -> arn:aws:kafka:<region>:<account>:group/<name>/<uuid>-<n>/<group>
  kafka_enabled   = var.msk_cluster_arn != null
  kafka_topic_arn = local.kafka_enabled ? replace(var.msk_cluster_arn, ":cluster/", ":topic/") : ""
  kafka_group_arn = local.kafka_enabled ? replace(var.msk_cluster_arn, ":cluster/", ":group/") : ""

  # §1.5's producer/consumer table, restated as who may do what. It is a
  # VARIABLE (see kafka_access) so that a root calling both this module and
  # modules/kafka-acls generates the IAM policies and the Kafka ACLs from ONE
  # table. Its default is the same matrix, for a root that calls this module
  # alone.
  kafka_access = var.kafka_access

  services = keys(var.service_account_names)
}

# ---------------------------------------------------------------------------
# Statement builders
#
# Every statement is built with Action and Resource as list(string), and every
# list is wrapped in tolist(). That is not decoration: a for-expression that
# produces a map has to unify its value types, and a tuple of two strings and a
# tuple of one string are different types. Uniform shapes keep the whole
# structure a list(object(...)) that jsonencode renders as valid IAM.
# ---------------------------------------------------------------------------

locals {
  kafka_statements = {
    for svc in local.services : svc => concat(
      # Connect, plus the idempotent-producer permission. §4.2 sets
      # enable.idempotence=true on every producer, and MSK gates that behind its
      # own action on the *cluster* resource rather than on the topic.
      local.kafka_enabled && (
        length(try(local.kafka_access[svc].read, [])) > 0 ||
        length(try(local.kafka_access[svc].write, [])) > 0 ||
        length(try(local.kafka_access[svc].read_also, [])) > 0
        ) ? [
        {
          Sid    = "Connect"
          Effect = "Allow"
          Action = tolist(concat(
            [
              "kafka-cluster:Connect",
              "kafka-cluster:DescribeCluster",
              "kafka-cluster:DescribeClusterDynamicConfiguration",
            ],
            length(try(local.kafka_access[svc].write, [])) > 0 ? ["kafka-cluster:WriteDataIdempotently"] : [],
          ))
          Resource = tolist([var.msk_cluster_arn])
        },
      ] : [],

      local.kafka_enabled && length(concat(
        try(local.kafka_access[svc].read, []),
        try(local.kafka_access[svc].read_also, []),
        )) > 0 ? [
        {
          Sid    = "ConsumeTopics"
          Effect = "Allow"
          Action = tolist([
            "kafka-cluster:DescribeTopic",
            "kafka-cluster:DescribeTopicDynamicConfiguration",
            "kafka-cluster:ReadData",
          ])
          Resource = tolist([
            for t in distinct(concat(
              try(local.kafka_access[svc].read, []),
              try(local.kafka_access[svc].read_also, []),
            )) : "${local.kafka_topic_arn}/${t}"
          ])
        },
      ] : [],

      local.kafka_enabled && length(try(local.kafka_access[svc].write, [])) > 0 ? [
        {
          Sid    = "ProduceTopics"
          Effect = "Allow"
          Action = tolist([
            "kafka-cluster:DescribeTopic",
            "kafka-cluster:WriteData",
          ])
          Resource = tolist([for t in local.kafka_access[svc].write : "${local.kafka_topic_arn}/${t}"])
        },
      ] : [],

      # provider-adapter creates its own coordination topic on startup, and the
      # cluster runs with auto.create.topics.enable=false.
      local.kafka_enabled && length(try(local.kafka_access[svc].create, [])) > 0 ? [
        {
          Sid    = "CreateCoordinationTopic"
          Effect = "Allow"
          Action = tolist([
            "kafka-cluster:CreateTopic",
            "kafka-cluster:AlterTopicDynamicConfiguration",
          ])
          Resource = tolist([for t in local.kafka_access[svc].create : "${local.kafka_topic_arn}/${t}"])
        },
      ] : [],

      local.kafka_enabled && length(try(var.kafka_consumer_groups[svc], [])) > 0 ? [
        {
          Sid    = "ConsumerGroups"
          Effect = "Allow"
          Action = tolist([
            "kafka-cluster:AlterGroup",
            "kafka-cluster:DescribeGroup",
          ])
          Resource = tolist([for g in var.kafka_consumer_groups[svc] : "${local.kafka_group_arn}/${g}"])
        },
      ] : [],
    )
  }

  s3_statements = {
    for svc in local.services : svc => (
      var.embeddings_bucket_arn == null ? [] :
      svc == "insights-service" ? [
        {
          Sid      = "ListCorpus"
          Effect   = "Allow"
          Action   = tolist(["s3:ListBucket", "s3:GetBucketLocation"])
          Resource = tolist([var.embeddings_bucket_arn])
        },
        {
          Sid    = "WriteCorpus"
          Effect = "Allow"
          Action = tolist([
            "s3:PutObject",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts",
          ])
          Resource = tolist(["${var.embeddings_bucket_arn}/*"])
        },
      ] :
      svc == "clusters-job" ? [
        {
          Sid      = "ListCorpus"
          Effect   = "Allow"
          Action   = tolist(["s3:ListBucket", "s3:GetBucketLocation"])
          Resource = tolist([var.embeddings_bucket_arn])
        },
        {
          # Read only. §8.6 reads a window of parquet and writes its results to
          # Milvus and ClickHouse, never back to the corpus.
          Sid      = "ReadCorpus"
          Effect   = "Allow"
          Action   = tolist(["s3:GetObject"])
          Resource = tolist(["${var.embeddings_bucket_arn}/*"])
        },
      ] : []
    )
  }

  secrets_statements = {
    for svc in local.services : svc => (
      length(try(var.service_secret_arns[svc], [])) == 0 ? [] : [
        {
          Sid    = "ReadOwnSecrets"
          Effect = "Allow"
          Action = tolist([
            "secretsmanager:GetSecretValue",
            "secretsmanager:DescribeSecret",
          ])
          Resource = tolist(var.service_secret_arns[svc])
        },
      ]
    )
  }

  # kms:Decrypt on the secrets key for anything reading Secrets Manager;
  # Decrypt plus GenerateDataKey on the data key for anything touching an
  # SSE-KMS object. Encrypt is not granted to the read-only consumers.
  kms_statements = {
    for svc in local.services : svc => concat(
      length(try(var.service_secret_arns[svc], [])) > 0 && try(var.kms_key_arns.secrets, null) != null ? [
        {
          Sid      = "DecryptSecrets"
          Effect   = "Allow"
          Action   = tolist(["kms:Decrypt"])
          Resource = tolist([var.kms_key_arns.secrets])
        },
      ] : [],

      length(local.s3_statements[svc]) > 0 && try(var.kms_key_arns.data, null) != null ? [
        {
          Sid    = "DataKey"
          Effect = "Allow"
          Action = svc == "insights-service" ? tolist([
            "kms:Decrypt",
            "kms:GenerateDataKey",
            ]) : tolist([
            "kms:Decrypt",
          ])
          Resource = tolist([var.kms_key_arns.data])
        },
      ] : [],
    )
  }

  service_statements = {
    for svc in local.services : svc => concat(
      local.kafka_statements[svc],
      local.s3_statements[svc],
      local.secrets_statements[svc],
      local.kms_statements[svc],
    )
  }
}

# ---------------------------------------------------------------------------
# Trust policies
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "service_assume" {
  for_each = var.service_account_names

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [var.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:sub"
      values   = ["system:serviceaccount:${var.namespace}:${each.value}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "service" {
  for_each = var.service_account_names

  name                 = "${var.name_prefix}-${each.key}"
  assume_role_policy   = data.aws_iam_policy_document.service_assume[each.key].json
  permissions_boundary = var.permissions_boundary_arn

  tags = merge(var.tags, {
    Name           = "${var.name_prefix}-${each.key}"
    ServiceAccount = "${var.namespace}/${each.value}"
  })
}

# A role with no statements would be an invalid policy document, so services
# that need nothing from AWS get a role and no inline policy. They still get an
# identity, which is what makes adding a permission later a one-line change.
resource "aws_iam_role_policy" "service" {
  for_each = {
    for svc, statements in local.service_statements : svc => statements
    if length(statements) > 0
  }

  name = "dabet"
  role = aws_iam_role.service[each.key].id

  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = each.value
  })
}
