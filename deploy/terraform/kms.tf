# Customer-managed KMS keys, one per blast radius, all with annual rotation.
#
#   data     RDS storage, MSK broker volumes, ElastiCache at rest, S3 objects
#   secrets  Secrets Manager values, including the RDS-managed master passwords
#   eks      Kubernetes secret envelope encryption and the node EBS volumes
#   logs     CloudWatch log groups
#
# Every key policy starts with the account-root statement. That statement looks
# like the "kms:* on *" that a review should flag, and in any other context it
# would be — but a KMS key with no root statement cannot be administered by IAM
# at all, and AWS refuses to create one that would be unmanageable. It is the
# documented default key policy, scoped to this account's root principal.

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.region
  partition  = data.aws_partition.current.partition

  root_arn = "arn:${local.partition}:iam::${local.account_id}:root"
}

data "aws_iam_policy_document" "kms_root" {
  statement {
    sid       = "EnableIAMUserPermissions"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = [local.root_arn]
    }
  }
}

# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------

resource "aws_kms_key" "data" {
  description             = "${var.name} data at rest (RDS, MSK, ElastiCache, S3)"
  enable_key_rotation     = true
  rotation_period_in_days = 365
  deletion_window_in_days = var.kms_deletion_window_days
  policy                  = data.aws_iam_policy_document.kms_root.json
  tags                    = local.tags
}

resource "aws_kms_alias" "data" {
  name          = "alias/${var.name}-data"
  target_key_id = aws_kms_key.data.key_id
}

# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------

resource "aws_kms_key" "secrets" {
  description             = "${var.name} Secrets Manager values (§4.4)"
  enable_key_rotation     = true
  rotation_period_in_days = 365
  deletion_window_in_days = var.kms_deletion_window_days
  policy                  = data.aws_iam_policy_document.kms_root.json
  tags                    = local.tags
}

resource "aws_kms_alias" "secrets" {
  name          = "alias/${var.name}-secrets"
  target_key_id = aws_kms_key.secrets.key_id
}

# ---------------------------------------------------------------------------
# eks
#
# The extra statement is not optional. Managed node groups launch instances
# through EC2 Auto Scaling, and the Auto Scaling service-linked role — not the
# caller — is what attaches an encrypted EBS volume. Without a grant, the node
# group fails with a bare "Client.InternalError: Client error on launch" and no
# indication that KMS is involved.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "kms_eks" {
  source_policy_documents = [data.aws_iam_policy_document.kms_root.json]

  statement {
    sid    = "AllowAutoScalingToUseTheKey"
    effect = "Allow"

    actions = [
      "kms:Encrypt",
      "kms:Decrypt",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:DescribeKey",
    ]

    resources = ["*"]

    principals {
      type = "AWS"
      identifiers = [
        "arn:${local.partition}:iam::${local.account_id}:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
      ]
    }
  }

  statement {
    sid       = "AllowAutoScalingToCreateGrants"
    effect    = "Allow"
    actions   = ["kms:CreateGrant"]
    resources = ["*"]

    principals {
      type = "AWS"
      identifiers = [
        "arn:${local.partition}:iam::${local.account_id}:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
      ]
    }

    condition {
      test     = "Bool"
      variable = "kms:GrantIsForAWSResource"
      values   = ["true"]
    }
  }
}

resource "aws_kms_key" "eks" {
  description             = "${var.name} EKS secrets envelope encryption and node EBS"
  enable_key_rotation     = true
  rotation_period_in_days = 365
  deletion_window_in_days = var.kms_deletion_window_days
  policy                  = data.aws_iam_policy_document.kms_eks.json
  tags                    = local.tags
}

resource "aws_kms_alias" "eks" {
  name          = "alias/${var.name}-eks"
  target_key_id = aws_kms_key.eks.key_id
}

# ---------------------------------------------------------------------------
# logs
#
# CloudWatch Logs encrypts with the key on the caller's behalf, so the service
# principal needs its own statement. The EncryptionContext condition confines it
# to log groups in this account and region rather than any log group anywhere.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "kms_logs" {
  source_policy_documents = [data.aws_iam_policy_document.kms_root.json]

  statement {
    sid    = "AllowCloudWatchLogs"
    effect = "Allow"

    actions = [
      "kms:Encrypt*",
      "kms:Decrypt*",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:Describe*",
    ]

    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["logs.${local.region}.amazonaws.com"]
    }

    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = ["arn:${local.partition}:logs:${local.region}:${local.account_id}:log-group:*"]
    }
  }
}

resource "aws_kms_key" "logs" {
  description             = "${var.name} CloudWatch log groups"
  enable_key_rotation     = true
  rotation_period_in_days = 365
  deletion_window_in_days = var.kms_deletion_window_days
  policy                  = data.aws_iam_policy_document.kms_logs.json
  tags                    = local.tags
}

resource "aws_kms_alias" "logs" {
  name          = "alias/${var.name}-logs"
  target_key_id = aws_kms_key.logs.key_id
}
