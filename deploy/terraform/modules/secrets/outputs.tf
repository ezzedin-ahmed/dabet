output "secret_arns" {
  description = <<-EOT
    Secret ARN per short name, e.g. "stripe/secret-key" -> arn:aws:secretsmanager:...

    These are what the Helm values-aws.yaml carries: an ExternalSecret's
    remoteRef.key, or a SecretProviderClass objectName, is the secret's full
    name or ARN.
  EOT
  value       = { for k, s in aws_secretsmanager_secret.this : k => s.arn }
}

output "secret_names" {
  description = "Full secret name per short name, for tooling that addresses secrets by name rather than ARN."
  value       = { for k, s in aws_secretsmanager_secret.this : k => s.name }
}

output "secret_arn_prefix" {
  description = <<-EOT
    Wildcard ARN covering every secret this module manages. Secrets Manager
    appends a random six-character suffix to a secret's ARN, so an ARN written
    by hand never matches; this is the pattern an IAM policy has to use to
    grant "all of the deployment's secrets".
  EOT
  value       = "arn:${data.aws_partition.current.partition}:secretsmanager:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:secret:${var.name_prefix}/*"
}

data "aws_partition" "current" {}
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}
