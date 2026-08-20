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
    : (
      var.encryption_in_transit_client_broker == "PLAINTEXT"
      ? aws_msk_cluster.this.bootstrap_brokers
      : aws_msk_cluster.this.bootstrap_brokers_tls
    )
  )
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
