output "cluster_arn" {
  description = "MSK cluster ARN. The IRSA policies scope kafka-cluster:* to this ARN and to topic/group ARNs derived from it."
  value       = aws_msk_cluster.this.arn
}

output "cluster_name" {
  description = "MSK cluster name."
  value       = aws_msk_cluster.this.cluster_name
}

output "bootstrap_brokers" {
  description = <<-EOT
    The bootstrap string for KAFKA_BROKERS, matching the configured
    authentication mode: SASL/IAM over TLS when client_authentication is "iam",
    otherwise TLS or plaintext.
  EOT
  value = (
    var.client_authentication == "iam"
    ? aws_msk_cluster.this.bootstrap_brokers_sasl_iam
    : var.client_authentication == "scram"
    ? aws_msk_cluster.this.bootstrap_brokers_sasl_scram
    : (
      var.encryption_in_transit_client_broker == "PLAINTEXT"
      ? aws_msk_cluster.this.bootstrap_brokers
      : aws_msk_cluster.this.bootstrap_brokers_tls
    )
  )
}

output "bootstrap_brokers_sasl_scram" {
  description = "SASL/SCRAM bootstrap string, empty unless SCRAM authentication is on."
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_scram
}

output "scram_secret_arns" {
  description = <<-EOT
    Secrets Manager ARN per SCRAM username, each holding
    {username, password}. Empty unless SCRAM is in use.

    This is what the charts point KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD
    at — per service under least privilege, and at the one shared entry when
    `scram_users` was left empty.
  EOT
  value       = { for u, s in aws_secretsmanager_secret.scram : u => s.arn }
}

output "scram_secret_names" {
  description = "Secrets Manager secret NAME per SCRAM username, which is what an ExternalSecret's remoteRef takes."
  value       = { for u, s in aws_secretsmanager_secret.scram : u => s.name }
}

output "scram_usernames" {
  description = "Every SCRAM username with a credential on this cluster."
  value       = sort(keys(local.scram_users_effective))
}

output "scram_secret_arn" {
  description = <<-EOT
    A single SCRAM secret ARN, kept for the one-credential shape and for
    callers written before per-user credentials existed. It resolves to
    `scram_username`'s secret when that user exists, and otherwise to the
    lowest-named one, which is arbitrary — use `scram_secret_arns` when more
    than one user is configured.
  EOT
  value = (
    length(aws_secretsmanager_secret.scram) == 0 ? null :
    contains(keys(aws_secretsmanager_secret.scram), var.scram_username)
    ? aws_secretsmanager_secret.scram[var.scram_username].arn
    : aws_secretsmanager_secret.scram[sort(keys(aws_secretsmanager_secret.scram))[0]].arn
  )
}

output "scram_secret_arn_prefix" {
  description = <<-EOT
    Wildcard ARN covering every SCRAM secret for this cluster, for granting
    External Secrets Operator read access to the lot without listing them.
    Null unless SCRAM is in use.

    Built from a real secret's ARN because a Secrets Manager ARN carries a
    random six-character suffix that cannot be predicted.
  EOT
  value = (
    length(aws_secretsmanager_secret.scram) == 0 ? null :
    format(
      "%s:secret:AmazonMSK_%s_*",
      # arn:aws:secretsmanager:<region>:<account>
      join(":", slice(split(":", values(aws_secretsmanager_secret.scram)[0].arn), 0, 5)),
      var.name,
    )
  )
}

output "sasl_mechanism" {
  description = "Value for KAFKA_SASL_MECHANISM, or empty when the cluster is unauthenticated."
  value = (
    var.client_authentication == "scram" ? "SCRAM-SHA-512" :
    var.client_authentication == "iam" ? "AWS_MSK_IAM" :
    ""
  )
}

output "tls_enabled" {
  description = "Value for KAFKA_TLS_ENABLED. MSK brokers present a publicly trusted certificate, so no CA file is needed."
  value       = var.encryption_in_transit_client_broker != "PLAINTEXT"
}

output "bootstrap_brokers_sasl_iam" {
  description = "SASL/IAM bootstrap string, empty unless IAM authentication is on."
  value       = aws_msk_cluster.this.bootstrap_brokers_sasl_iam
}

output "bootstrap_brokers_tls" {
  description = "TLS bootstrap string."
  value       = aws_msk_cluster.this.bootstrap_brokers_tls
}

output "bootstrap_brokers_plaintext" {
  description = "Plaintext bootstrap string. Empty unless encryption in transit allows PLAINTEXT."
  value       = aws_msk_cluster.this.bootstrap_brokers
}

output "zookeeper_connect_string" {
  description = "ZooKeeper connect string. Empty on KRaft-mode clusters; kept only for tooling that still asks."
  value       = aws_msk_cluster.this.zookeeper_connect_string
}

output "security_group_id" {
  description = "Security group in front of the brokers."
  value       = aws_security_group.this.id
}

output "client_authentication" {
  description = "Authentication mode, so the chart knows whether to configure SASL/IAM."
  value       = var.client_authentication
}

output "configuration_arn" {
  description = "MSK configuration ARN."
  value       = aws_msk_configuration.this.arn
}
