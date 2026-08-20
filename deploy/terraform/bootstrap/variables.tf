variable "region" {
  description = "Region for the state bucket. Usually the same region as the deployment."
  type        = string
}

variable "state_bucket_name" {
  description = "Globally unique name for the state bucket, e.g. \"dabet-tfstate-<account-id>\"."
  type        = string
}

variable "create_dynamodb_lock_table" {
  description = <<-EOT
    Also create the legacy DynamoDB lock table.

    Off by default. The S3 backend has supported native locking through a
    .tflock object since OpenTofu 1.10 and Terraform 1.10 (`use_lockfile =
    true`), which removes a whole second resource, its IAM, and its bill for
    what was always a workaround for S3 not having conditional writes. S3 has
    conditional writes now.

    Turn it on only if some consumer of this state is pinned to a CLI older
    than that. Do not run both mechanisms against the same state expecting
    them to interlock — they are independent, and a run using one will not see
    a lock held by the other.
  EOT
  type        = bool
  default     = false
}

variable "dynamodb_table_name" {
  description = "Name for the optional lock table."
  type        = string
  default     = "dabet-tfstate-lock"
}

variable "noncurrent_version_expiration_days" {
  description = "How long superseded state versions are kept. State files are small; keep plenty of history."
  type        = number
  default     = 365
}
