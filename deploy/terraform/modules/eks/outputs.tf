output "cluster_name" {
  description = "EKS cluster name."
  value       = aws_eks_cluster.this.name
}

output "cluster_arn" {
  description = "EKS cluster ARN."
  value       = aws_eks_cluster.this.arn
}

output "cluster_endpoint" {
  description = "Kubernetes API endpoint."
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_certificate_authority_data" {
  description = "Base64 CA bundle for the API server, for kubeconfig."
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_version" {
  description = "Kubernetes version actually running."
  value       = aws_eks_cluster.this.version
}

output "cluster_security_group_id" {
  description = <<-EOT
    The EKS-managed cluster security group, which is attached to every managed
    node. Data-tier security groups allow ingress from this group rather than
    from a CIDR, so nothing outside the cluster can reach Postgres, Kafka,
    Redis or Memcached even from inside the VPC.
  EOT
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}

output "oidc_provider_arn" {
  description = "IAM OIDC provider ARN, consumed by the iam module to build IRSA trust policies."
  value       = aws_iam_openid_connect_provider.this.arn
}

output "oidc_provider_url" {
  description = "OIDC issuer URL without the scheme, for IRSA trust policy conditions."
  value       = local.oidc_condition_prefix
}

output "node_role_arn" {
  description = "IAM role assumed by the worker nodes."
  value       = aws_iam_role.node.arn
}

output "node_group_names" {
  description = "Managed node groups that were actually created."
  value = compact([
    aws_eks_node_group.general.node_group_name,
    one(aws_eks_node_group.gpu[*].node_group_name),
    one(aws_eks_node_group.stateful[*].node_group_name),
  ])
}

output "gpu_node_selector" {
  description = "nodeSelector the vLLM chart should use when the GPU pool is enabled."
  value       = { "dabet.io/workload" = "vllm" }
}

output "gpu_tolerations" {
  description = "Tolerations the vLLM chart needs to schedule onto the GPU pool."
  value = [
    { key = "nvidia.com/gpu", operator = "Equal", value = "present", effect = "NoSchedule" },
    { key = "dabet.io/workload", operator = "Equal", value = "vllm", effect = "NoSchedule" },
  ]
}

output "stateful_tolerations" {
  description = "Tolerations for ClickHouse and Milvus when the stateful pool is enabled."
  value = [
    { key = "dabet.io/workload", operator = "Equal", value = "stateful", effect = "NoSchedule" },
  ]
}
