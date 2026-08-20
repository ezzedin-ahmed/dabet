output "kubeconfig_command" {
  description = "Write a kubeconfig entry for this cluster."
  value       = module.dabet.kubeconfig_command
}

output "helm_values_aws_yaml" {
  description = <<-EOT
    The values-aws.yaml contract. Write it out with:

      tofu output -raw helm_values_aws_yaml > ../../../k8s/values-aws.yaml
  EOT
  value       = module.dabet.helm_values_aws_yaml
}

output "irsa_role_arns" {
  description = "IRSA role ARN per service, for the ServiceAccount annotations."
  value       = module.dabet.irsa_role_arns
}

output "secret_arns" {
  description = "Secrets Manager ARNs. Populate the placeholders before deploying the charts."
  value       = module.dabet.secret_arns
}
