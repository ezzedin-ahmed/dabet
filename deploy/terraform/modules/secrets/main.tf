# AWS Secrets Manager containers for the §4.4 secrets.
#
# The important property of this module is what it does NOT do: it never takes a
# secret value as an input variable. Terraform state is not a secret store —
# anything passed in here would sit in plaintext in the state file, in every
# backup of it, and in the output of `tofu show`.
#
# So Terraform creates the container, the KMS key, the resource policy scope and
# the ARN; a human or a deployment pipeline puts the value in:
#
#   aws secretsmanager put-secret-value \
#     --secret-id dabet/prod/stripe/secret-key \
#     --secret-string "$(pass show dabet/stripe)"
#
# and the lifecycle block below means that does not show up as drift.
#
# The Postgres passwords named in §4.4 are deliberately absent: RDS generates
# and rotates them into its own managed secret (see modules/rds), so the ARN the
# charts consume for those comes from there.

resource "aws_secretsmanager_secret" "this" {
  for_each = var.secrets

  name        = "${var.name_prefix}/${each.key}"
  description = each.value
  kms_key_id  = var.kms_key_arn

  recovery_window_in_days = var.recovery_window_days

  tags = merge(var.tags, {
    Name = "${var.name_prefix}/${each.key}"
  })
}

resource "aws_secretsmanager_secret_version" "placeholder" {
  for_each = var.create_placeholder_versions ? var.secrets : {}

  secret_id     = aws_secretsmanager_secret.this[each.key].id
  secret_string = var.placeholder_value

  lifecycle {
    # The real value is written out of band. Without this, every apply would
    # propose reverting it to the placeholder.
    ignore_changes = [secret_string, version_stages]
  }
}

resource "aws_secretsmanager_secret_rotation" "this" {
  for_each = var.rotation

  secret_id           = aws_secretsmanager_secret.this[each.key].id
  rotation_lambda_arn = each.value.lambda_arn

  rotation_rules {
    automatically_after_days = each.value.automatically_after_days
  }

  # A rotation configured against a secret with no version fails; the
  # placeholder version is what makes the first rotation possible.
  depends_on = [aws_secretsmanager_secret_version.placeholder]
}
