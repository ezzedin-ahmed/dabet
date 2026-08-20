# Platform roles: the operators and stores that are not dabet services but still
# need an AWS identity.
#
# The trust policies are built by the same rule as the service roles — sub and
# aud, both pinned.

locals {
  platform_roles = merge(
    var.external_secrets.enabled ? {
      external-secrets = {
        namespace            = var.external_secrets.namespace
        service_account_name = var.external_secrets.service_account_name
      }
    } : {},
    var.milvus_bucket_arn == null ? {} : {
      milvus = {
        namespace            = var.milvus_service_account.namespace
        service_account_name = var.milvus_service_account.service_account_name
      }
    },
    var.clickhouse_bucket_arn == null ? {} : {
      clickhouse = {
        namespace            = var.clickhouse_service_account.namespace
        service_account_name = var.clickhouse_service_account.service_account_name
      }
    },
    {
      for k, v in var.extra_roles : k => {
        namespace            = v.namespace
        service_account_name = v.service_account_name
      }
    },
  )
}

data "aws_iam_policy_document" "platform_assume" {
  for_each = local.platform_roles

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
      values   = ["system:serviceaccount:${each.value.namespace}:${each.value.service_account_name}"]
    }

    condition {
      test     = "StringEquals"
      variable = "${var.oidc_provider_url}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "platform" {
  for_each = local.platform_roles

  name                 = "${var.name_prefix}-${each.key}"
  assume_role_policy   = data.aws_iam_policy_document.platform_assume[each.key].json
  permissions_boundary = var.permissions_boundary_arn

  tags = merge(var.tags, {
    Name           = "${var.name_prefix}-${each.key}"
    ServiceAccount = "${each.value.namespace}/${each.value.service_account_name}"
  })
}

# ---------------------------------------------------------------------------
# External Secrets Operator
#
# Reads under an explicit set of ARN prefixes, so a committed ExternalSecret
# manifest cannot point the operator at an unrelated secret in the account. The
# ListSecrets action has no resource-level scoping in Secrets Manager and is
# granted on "*" of necessity; it discloses names and tags, never values.
# ---------------------------------------------------------------------------

locals {
  eso_statements = var.external_secrets.enabled ? concat(
    length(var.external_secrets.secret_arn_prefixes) > 0 ? [
      {
        Sid    = "ReadDabetSecrets"
        Effect = "Allow"
        Action = tolist([
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
          "secretsmanager:GetResourcePolicy",
          "secretsmanager:ListSecretVersionIds",
        ])
        Resource = tolist(var.external_secrets.secret_arn_prefixes)
      },
    ] : [],
    [
      {
        Sid      = "ListSecretsHasNoResourceScope"
        Effect   = "Allow"
        Action   = tolist(["secretsmanager:ListSecrets"])
        Resource = tolist(["*"])
      },
    ],
    try(var.kms_key_arns.secrets, null) == null ? [] : [
      {
        Sid      = "DecryptSecrets"
        Effect   = "Allow"
        Action   = tolist(["kms:Decrypt"])
        Resource = tolist([var.kms_key_arns.secrets])
      },
    ],
  ) : []

  milvus_statements = var.milvus_bucket_arn == null ? [] : concat(
    [
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = tolist(["s3:ListBucket", "s3:GetBucketLocation"])
        Resource = tolist([var.milvus_bucket_arn])
      },
      {
        Sid    = "Objects"
        Effect = "Allow"
        Action = tolist([
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ])
        Resource = tolist(["${var.milvus_bucket_arn}/*"])
      },
    ],
    try(var.kms_key_arns.data, null) == null ? [] : [
      {
        Sid      = "DataKey"
        Effect   = "Allow"
        Action   = tolist(["kms:Decrypt", "kms:GenerateDataKey"])
        Resource = tolist([var.kms_key_arns.data])
      },
    ],
  )

  clickhouse_statements = var.clickhouse_bucket_arn == null ? [] : concat(
    [
      {
        Sid      = "ListBucket"
        Effect   = "Allow"
        Action   = tolist(["s3:ListBucket", "s3:GetBucketLocation"])
        Resource = tolist([var.clickhouse_bucket_arn])
      },
      {
        Sid    = "Objects"
        Effect = "Allow"
        Action = tolist([
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ])
        Resource = tolist(["${var.clickhouse_bucket_arn}/*"])
      },
    ],
    try(var.kms_key_arns.data, null) == null ? [] : [
      {
        Sid      = "DataKey"
        Effect   = "Allow"
        Action   = tolist(["kms:Decrypt", "kms:GenerateDataKey"])
        Resource = tolist([var.kms_key_arns.data])
      },
    ],
  )

  platform_statements = merge(
    var.external_secrets.enabled ? { external-secrets = local.eso_statements } : {},
    var.milvus_bucket_arn == null ? {} : { milvus = local.milvus_statements },
    var.clickhouse_bucket_arn == null ? {} : { clickhouse = local.clickhouse_statements },
  )
}

resource "aws_iam_role_policy" "platform" {
  for_each = {
    for k, statements in local.platform_statements : k => statements
    if length(statements) > 0
  }

  name = "dabet"
  role = aws_iam_role.platform[each.key].id

  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = each.value
  })
}

# ---------------------------------------------------------------------------
# Caller-supplied roles
# ---------------------------------------------------------------------------

resource "aws_iam_role_policy" "extra_inline" {
  for_each = {
    for k, v in var.extra_roles : k => v
    if v.inline_policy_json != null
  }

  name   = "inline"
  role   = aws_iam_role.platform[each.key].id
  policy = each.value.inline_policy_json
}

locals {
  # The leading {} keeps merge() from being called with zero arguments when
  # extra_roles is empty, which is an error rather than an empty map.
  extra_role_policy_attachments = merge(concat([{}], [
    for k, v in var.extra_roles : {
      for arn in v.policy_arns : "${k}:${arn}" => { role = k, policy_arn = arn }
    }
  ])...)
}

resource "aws_iam_role_policy_attachment" "extra" {
  for_each = local.extra_role_policy_attachments

  role       = aws_iam_role.platform[each.value.role].name
  policy_arn = each.value.policy_arn
}
