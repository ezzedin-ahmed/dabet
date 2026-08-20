output "kubeconfig_command" {
  description = "Write a kubeconfig entry for this cluster."
  value       = module.dabet.kubeconfig_command
}

output "helm_values_dabet_yaml" {
  description = <<-EOT
    Values for the app chart:

      tofu output -raw helm_values_dabet_yaml > values-aws-generated.yaml
      helm upgrade --install dabet ../../../k8s/charts/dabet \
        --namespace dabet --create-namespace \
        -f ../../../k8s/charts/dabet/values-aws.yaml \
        -f values-aws-generated.yaml
  EOT
  value       = module.dabet.helm_values_dabet_yaml
}

output "helm_values_dabet_deps_yaml" {
  description = "Values for the deps chart."
  value       = module.dabet.helm_values_dabet_deps_yaml
}

output "app_secret_document_skeleton" {
  description = "The Secrets Manager document to populate before deploying the charts."
  value       = module.dabet.app_secret_document_skeleton
}

output "postgres_password_commands" {
  description = "How to read the RDS-generated passwords for the DSNs above."
  value       = module.dabet.postgres_password_commands
}

output "irsa_role_arns" {
  description = "IRSA role ARN per service, for the ServiceAccount annotations."
  value       = module.dabet.irsa_role_arns
}

output "secret_arns" {
  description = "Secrets Manager ARNs. Populate the placeholders before deploying the charts."
  value       = module.dabet.secret_arns
}
