# S3 buckets.
#
# The embeddings bucket is the dominant storage cost in the whole system. §8.4
# puts it at ~40 MB/s sustained, ~3.5 TB/day of parquet before compression, kept
# indefinitely per §4.8. Three consequences are baked in below:
#
#   1. SSE-KMS with an S3 Bucket Key. Without the bucket key, every object PUT
#      and GET is a separate KMS API call, billed per request. With it, S3
#      caches a data key per bucket and the KMS bill stops tracking the object
#      count.
#   2. Versioning off. See the variable description — write-once parquet has
#      nothing to version.
#   3. A lifecycle ladder that stops at GLACIER_IR, because §8.6's on-demand
#      recluster reads arbitrary historical windows and cannot wait hours for a
#      DEEP_ARCHIVE restore.
#
# The lever that actually controls this bill is the §8.4 sampling ceiling, and
# that is a chart values decision, not a Terraform one. Nothing in this file can
# make an unsampled firehose affordable.

data "aws_caller_identity" "current" {}

locals {
  suffix = data.aws_caller_identity.current.account_id

  buckets = merge(
    {
      embeddings = {
        purpose    = "insights embeddings corpus (§8.4)"
        versioning = var.embeddings_versioning_enabled
      }
    },
    var.create_milvus_bucket ? {
      milvus = {
        purpose    = "milvus segment and index storage"
        versioning = false
      }
    } : {},
    var.create_clickhouse_backup_bucket ? {
      clickhouse = {
        purpose    = "clickhouse backups / S3-backed disks"
        versioning = false
      }
    } : {},
  )

  bucket_names = {
    for k, v in local.buckets : k => "${var.name_prefix}-${k}-${local.suffix}"
  }
}

resource "aws_s3_bucket" "this" {
  for_each = local.buckets

  bucket        = local.bucket_names[each.key]
  force_destroy = var.force_destroy

  tags = merge(var.tags, {
    Name    = local.bucket_names[each.key]
    Purpose = each.value.purpose
  })
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ACLs off entirely. Every object is owned by the bucket owner and access is
# decided by IAM and the bucket policy, which is one fewer way to make an object
# public by accident.
resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  versioning_configuration {
    status = local.buckets[each.key].versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }

    # The line that keeps SSE-KMS affordable at this object count.
    bucket_key_enabled = true
  }
}

# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

resource "aws_s3_bucket_lifecycle_configuration" "embeddings" {
  bucket = aws_s3_bucket.this["embeddings"].id

  rule {
    id     = "corpus"
    status = "Enabled"

    filter {}

    dynamic "transition" {
      for_each = var.embeddings_lifecycle_transitions

      content {
        days          = transition.value.days
        storage_class = transition.value.storage_class
      }
    }

    dynamic "expiration" {
      for_each = var.embeddings_expiration_days == null ? [] : [var.embeddings_expiration_days]

      content {
        days = expiration.value
      }
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_incomplete_multipart_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}

# Milvus and ClickHouse rewrite and delete their own objects, so the only rule
# they want is the incomplete-upload sweep.
resource "aws_s3_bucket_lifecycle_configuration" "supporting" {
  for_each = { for k, v in local.buckets : k => v if k != "embeddings" }

  bucket = aws_s3_bucket.this[each.key].id

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_incomplete_multipart_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}

# ---------------------------------------------------------------------------
# Bucket policies
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "bucket" {
  for_each = aws_s3_bucket.this

  # Encryption in transit. S3 speaks HTTPS by default, but "by default" is not
  # "only", and a misconfigured client can still fall back to port 80.
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]

    resources = [
      each.value.arn,
      "${each.value.arn}/*",
    ]

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  # Refuse an upload that asks for anything other than the bucket's own KMS key.
  statement {
    sid     = "DenyWrongEncryptionKey"
    effect  = "Deny"
    actions = ["s3:PutObject"]

    resources = ["${each.value.arn}/*"]

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    condition {
      test     = "StringNotEqualsIfExists"
      variable = "s3:x-amz-server-side-encryption-aws-kms-key-id"
      values   = [var.kms_key_arn]
    }
  }

  # Deletion guard for the corpus. Lifecycle expiration is performed by S3
  # itself and is unaffected.
  dynamic "statement" {
    for_each = var.deny_object_deletion && each.key == "embeddings" ? [1] : []

    content {
      sid    = "DenyObjectDeletion"
      effect = "Deny"

      actions = [
        "s3:DeleteObject",
        "s3:DeleteObjectVersion",
      ]

      resources = ["${each.value.arn}/*"]

      principals {
        type        = "AWS"
        identifiers = ["*"]
      }

      dynamic "condition" {
        for_each = length(var.delete_exempt_principal_arns) > 0 ? [1] : []

        content {
          test     = "ArnNotEquals"
          variable = "aws:PrincipalArn"
          values   = var.delete_exempt_principal_arns
        }
      }
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  for_each = aws_s3_bucket.this

  bucket = each.value.id
  policy = data.aws_iam_policy_document.bucket[each.key].json

  # The public access block must be in place before a policy referencing
  # Principal "*" is attached, or block_public_policy can reject it.
  depends_on = [aws_s3_bucket_public_access_block.this]
}

# ---------------------------------------------------------------------------
# Access logs (opt-in)
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "access_logs" {
  count = var.create_access_log_bucket ? 1 : 0

  bucket        = "${var.name_prefix}-access-logs-${local.suffix}"
  force_destroy = var.force_destroy

  tags = merge(var.tags, { Name = "${var.name_prefix}-access-logs-${local.suffix}" })
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  count = var.create_access_log_bucket ? 1 : 0

  bucket = aws_s3_bucket.access_logs[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# The log delivery service writes with SSE-S3, not SSE-KMS, so this bucket does
# not get the module's KMS key.
resource "aws_s3_bucket_server_side_encryption_configuration" "access_logs" {
  count = var.create_access_log_bucket ? 1 : 0

  bucket = aws_s3_bucket.access_logs[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "access_logs" {
  count = var.create_access_log_bucket ? 1 : 0

  bucket = aws_s3_bucket.access_logs[0].id

  rule {
    id     = "expire"
    status = "Enabled"

    filter {}

    expiration {
      days = var.access_log_retention_days
    }
  }
}

# With BucketOwnerEnforced ownership the log delivery group has no ACL to write
# through, so the service principal has to be granted in the bucket policy
# instead. Without this the source buckets accept the logging configuration and
# then silently deliver nothing.
data "aws_iam_policy_document" "access_logs" {
  count = var.create_access_log_bucket ? 1 : 0

  statement {
    sid     = "AllowS3ServerAccessLogDelivery"
    effect  = "Allow"
    actions = ["s3:PutObject"]

    resources = ["${aws_s3_bucket.access_logs[0].arn}/*"]

    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [for b in aws_s3_bucket.this : b.arn]
    }
  }

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.access_logs[0].arn,
      "${aws_s3_bucket.access_logs[0].arn}/*",
    ]

    principals {
      type        = "AWS"
      identifiers = ["*"]
    }

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "access_logs" {
  count = var.create_access_log_bucket ? 1 : 0

  bucket = aws_s3_bucket.access_logs[0].id
  policy = data.aws_iam_policy_document.access_logs[0].json

  depends_on = [aws_s3_bucket_public_access_block.access_logs]
}

resource "aws_s3_bucket_logging" "this" {
  for_each = var.create_access_log_bucket ? aws_s3_bucket.this : {}

  bucket        = each.value.id
  target_bucket = aws_s3_bucket.access_logs[0].id
  target_prefix = "${each.key}/"

  depends_on = [aws_s3_bucket_policy.access_logs]
}
