output "kubeconfig_command" {
  description = "Write a kubeconfig entry for this cluster."
  value       = module.dabet.kubeconfig_command
}

output "helm_values_aws_yaml" {
  description = <<-EOT
    The values-aws.yaml contract:

      tofu output -raw helm_values_aws_yaml > ../../../k8s/values-aws.yaml
  EOT
  value       = module.dabet.helm_values_aws_yaml
}

output "helm_values_aws" {
  description = "The same contract as structured data, for a caller that wants to merge rather than replace."
  value       = module.dabet.helm_values_aws
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
