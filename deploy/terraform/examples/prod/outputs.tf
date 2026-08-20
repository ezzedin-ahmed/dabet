output "kubeconfig_command" {
  description = "Write a kubeconfig entry for this cluster."
  value       = module.dabet.kubeconfig_command
}

output "helm_values_dabet_yaml" {
  description = <<-EOT
    Values for the app chart, in its own key paths:

      tofu output -raw helm_values_dabet_yaml > values-aws-generated.yaml
      helm upgrade --install dabet ../../../k8s/charts/dabet \
        --namespace dabet --create-namespace \
        -f ../../../k8s/charts/dabet/values-aws.yaml \
        -f values-aws-generated.yaml
  EOT
  value       = module.dabet.helm_values_dabet_yaml
}

output "helm_values_dabet_deps_yaml" {
  description = <<-EOT
    Values for the deps chart: AWS-provided components off with their
    external.* addresses filled in, ClickHouse and Milvus left in-cluster.
  EOT
  value       = module.dabet.helm_values_dabet_deps_yaml
}

output "app_secret_document_skeleton" {
  description = <<-EOT
    The Secrets Manager document the chart's ExternalSecret extracts, with the
    endpoints filled in and REPLACE markers where a human has to paste.
  EOT
  value       = module.dabet.app_secret_document_skeleton
}

output "postgres_password_commands" {
  description = "How to read the RDS-generated and MSK SCRAM passwords for the DSNs above."
  value       = module.dabet.postgres_password_commands
}

output "irsa_role_arns" {
  description = "IRSA role ARN per service."
  value       = module.dabet.irsa_role_arns
}

output "secret_arns" {
  description = "Secrets Manager ARNs. The placeholders must be populated before the charts start."
  value       = module.dabet.secret_arns
}

output "nat_gateway_public_ips" {
  description = "Egress addresses, if a platform provider needs them allow-listed."
  value       = module.dabet.nat_gateway_public_ips
}

output "prometheus_remote_write_url" {
  description = "Where the in-cluster Prometheus should remote-write."
  value       = module.dabet.prometheus_remote_write_url
}
