variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-west-1"
}

variable "azs" {
  description = "Three AZs. §3's Kafka floor and Multi-AZ Postgres both want three."
  type        = list(string)
  default     = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]
}

variable "cluster_admin_principal_arns" {
  description = <<-EOT
    IAM principals granted cluster-admin through EKS access entries. Fill this
    in: with bootstrap_cluster_creator_admin_permissions left at its default,
    the identity that ran the first apply is an admin, and if that was a CI
    role nobody else can reach the cluster.
  EOT
  type        = list(string)
  default     = []
}

variable "alarm_emails" {
  description = "Addresses subscribed to the infrastructure alarm topic. Each needs a confirmation click."
  type        = list(string)
  default     = []
}

variable "enable_gpu_pool" {
  description = <<-EOT
    Turn on the vLLM GPU pool.

    Off by default because it roughly doubles the compute bill and the platform
    runs end to end without it. See the README's sizing note for how many GPUs
    §7.5's sampler ceiling actually implies.
  EOT
  type        = bool
  default     = false
}
