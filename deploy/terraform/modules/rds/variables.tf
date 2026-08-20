variable "identifier" {
  description = "DB instance identifier, e.g. \"dabet-prod-identity\"."
  type        = string
}

variable "vpc_id" {
  description = "VPC the instance and its security group live in."
  type        = string
}

variable "subnet_ids" {
  description = "Isolated data-tier subnets. Must span at least two AZs."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = <<-EOT
    Security groups allowed to reach port 5432. Normally just the EKS cluster
    security group. There is deliberately no CIDR-based ingress variable: the
    data tier is reachable from the cluster and from nothing else.
  EOT
  type        = list(string)
}

variable "engine_version" {
  description = <<-EOT
    PostgreSQL version. A major-only value like "17" tracks the latest minor at
    create time and pairs with auto_minor_version_upgrade; pin the full version
    if your change control needs it. §5.2's schema uses citext and
    gen_random_uuid(), both of which are available on any supported major.
  EOT
  type        = string
  default     = "17"
}

variable "parameter_group_family" {
  description = "RDS parameter group family, e.g. \"postgres17\". Must match the engine major version."
  type        = string
  default     = "postgres17"
}

variable "instance_class" {
  description = "RDS instance class."
  type        = string
}

variable "allocated_storage_gb" {
  description = <<-EOT
    Initial storage in GiB.

    For the identity instance, §5.2 sizes the data at ~25 GB at 10M creators
    (a ~200 B creator row and a ~1.5 KB connection row at 1.5 connections per
    creator: 2 GB + 22 GB). That is the heap alone. Indexes, WAL, bloat and
    autovacuum headroom mean the volume wants several times the logical size,
    which is why the production example asks for 200 GiB rather than 32.
  EOT
  type        = number
}

variable "max_allocated_storage_gb" {
  description = <<-EOT
    Ceiling for RDS storage autoscaling. Set equal to allocated_storage_gb to
    disable. Autoscaling only grows — it never shrinks — so the ceiling is a
    budget decision, not a capacity one.
  EOT
  type        = number
  default     = 0
}

variable "storage_type" {
  description = "gp3 unless you have measured a need for io2."
  type        = string
  default     = "gp3"
}

variable "iops" {
  description = <<-EOT
    Provisioned IOPS. Leave null on gp3 under 400 GiB: RDS gp3 volumes below
    that threshold are fixed at the 3000 IOPS / 125 MiBps baseline and reject a
    provisioned value. Above 400 GiB you may set it, and must also set
    storage_throughput.
  EOT
  type        = number
  default     = null
}

variable "storage_throughput" {
  description = "gp3 throughput in MiBps. Only valid alongside iops on volumes of 400 GiB or more."
  type        = number
  default     = null
}

variable "database_name" {
  description = "Initial database created on the instance."
  type        = string
  default     = "dabet"
}

variable "username" {
  description = "Master username. Not a secret, but not a useful one to guess either."
  type        = string
  default     = "dabet"
}

variable "manage_master_user_password" {
  description = <<-EOT
    Let RDS generate the master password and store it in Secrets Manager,
    rotating it on the schedule AWS manages.

    On by default because the alternative — a password variable — puts the
    plaintext in the state file and in whatever CI logs the plan. With this on,
    Terraform never sees the password at all; the secret ARN is an output and
    External Secrets Operator reads the {username, password} JSON from it.
  EOT
  type        = bool
  default     = true
}

variable "multi_az" {
  description = "Synchronous standby in a second AZ. §3's target is a managed, multi-AZ cluster."
  type        = bool
  default     = true
}

variable "backup_retention_days" {
  description = "Automated backup retention. Zero disables backups entirely and is almost never right."
  type        = number
  default     = 14

  validation {
    condition     = var.backup_retention_days >= 1
    error_message = "Automated backups must be enabled: §4.8 keeps creator, connection, policy and credits data for the life of the account."
  }
}

variable "backup_window" {
  description = "Daily backup window, UTC."
  type        = string
  default     = "03:00-04:00"
}

variable "maintenance_window" {
  description = "Weekly maintenance window, UTC. Must not overlap the backup window."
  type        = string
  default     = "sun:04:30-sun:05:30"
}

variable "deletion_protection" {
  description = "Refuse to delete the instance through the API."
  type        = bool
  default     = true
}

variable "skip_final_snapshot" {
  description = "Skip the final snapshot on destroy. True is for throwaway environments only."
  type        = bool
  default     = false
}

variable "final_snapshot_identifier" {
  description = "Name for the final snapshot. Null derives one from the identifier."
  type        = string
  default     = null
}

variable "kms_key_arn" {
  description = "KMS key for storage encryption at rest, Performance Insights, and the managed master password secret."
  type        = string
}

variable "performance_insights_enabled" {
  description = "Enable Performance Insights."
  type        = bool
  default     = true
}

variable "performance_insights_retention_days" {
  description = "7 is the free tier; 731 is two years and is billed."
  type        = number
  default     = 7
}

variable "monitoring_interval" {
  description = "Enhanced monitoring interval in seconds. 0 disables it; 60 is a reasonable default."
  type        = number
  default     = 60
}

variable "force_ssl" {
  description = <<-EOT
    Set rds.force_ssl = 1, so the server rejects any non-TLS connection.

    On by default. This means POSTGRES_DSN must carry sslmode=require (or
    verify-full with the RDS CA bundle) — local Compose uses sslmode=disable,
    so the AWS values file has to override it. See the outputs contract in the
    README.
  EOT
  type        = bool
  default     = true
}

variable "extra_parameters" {
  description = "Additional DB parameters, merged over the module defaults. apply_method defaults to immediate."
  type = map(object({
    value        = string
    apply_method = optional(string, "immediate")
  }))
  default = {}
}

variable "enabled_cloudwatch_logs_exports" {
  description = "Log types shipped to CloudWatch Logs."
  type        = list(string)
  default     = ["postgresql", "upgrade"]
}

variable "log_retention_days" {
  description = "Retention on the pre-created RDS log groups."
  type        = number
  default     = 30
}

variable "log_kms_key_arn" {
  description = "KMS key for the RDS CloudWatch log groups. Null uses the CloudWatch service key."
  type        = string
  default     = null
}

variable "apply_immediately" {
  description = "Apply changes at once instead of during the maintenance window. False in production."
  type        = bool
  default     = false
}

variable "ca_cert_identifier" {
  description = <<-EOT
    RDS CA bundle. rds-ca-rsa2048-g1 expires in 2061; the older rds-ca-2019
    bundle has already expired. Changing this is an in-place modification that
    requires a reboot, so schedule it.
  EOT
  type        = string
  default     = "rds-ca-rsa2048-g1"
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
