variable "region" {
  description = "AWS region."
  type        = string
  default     = "eu-west-1"
}

variable "azs" {
  description = <<-EOT
    Three AZs even in dev. MSK requires the broker count to be a whole multiple
    of the subnet count, and the smallest cluster §3 allows is three brokers, so
    two AZs would mean four brokers — more expensive than three, not less.
  EOT
  type        = list(string)
  default     = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]
}

variable "cluster_admin_principal_arns" {
  description = "IAM principals granted cluster-admin. Add your own role ARN before the first apply."
  type        = list(string)
  default     = []
}
