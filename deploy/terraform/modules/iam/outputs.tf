output "service_role_arns" {
  description = <<-EOT
    IRSA role ARN per dabet service, keyed by service name.

    This is one half of the seam with the app chart: each ServiceAccount needs

      metadata.annotations:
        eks.amazonaws.com/role-arn: <this value>

    and the ServiceAccount name must match service_account_names for the trust
    policy to accept the token.
  EOT
  value       = { for k, r in aws_iam_role.service : k => r.arn }
}

output "service_role_names" {
  description = "IRSA role name per service."
  value       = { for k, r in aws_iam_role.service : k => r.name }
}

output "service_account_annotations" {
  description = <<-EOT
    Ready-made annotation map per service, so the chart can render it without
    reconstructing the key. Shape: { "<service>" = { "eks.amazonaws.com/role-arn" = "<arn>" } }.
  EOT
  value = {
    for k, r in aws_iam_role.service : k => {
      "eks.amazonaws.com/role-arn" = r.arn
    }
  }
}

output "platform_role_arns" {
  description = "IRSA role ARNs for the platform ServiceAccounts (external-secrets, milvus, clickhouse, extra_roles)."
  value       = { for k, r in aws_iam_role.platform : k => r.arn }
}

output "external_secrets_role_arn" {
  description = "IRSA role for External Secrets Operator, or null when disabled."
  value       = try(aws_iam_role.platform["external-secrets"].arn, null)
}

output "milvus_role_arn" {
  description = "IRSA role for an in-cluster Milvus, or null when no Milvus bucket exists."
  value       = try(aws_iam_role.platform["milvus"].arn, null)
}

output "clickhouse_role_arn" {
  description = "IRSA role for an in-cluster ClickHouse, or null when no ClickHouse bucket exists."
  value       = try(aws_iam_role.platform["clickhouse"].arn, null)
}

output "namespace" {
  description = "Namespace the trust policies were built against."
  value       = var.namespace
}

output "service_account_names" {
  description = "ServiceAccount names the trust policies were built against."
  value       = var.service_account_names
}
