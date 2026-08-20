variable "name_prefix" {
  description = "Prefix for bucket names, e.g. \"dabet-prod\". The account id is appended to keep names globally unique."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for SSE-KMS on every bucket in this module."
  type        = string
}

variable "force_destroy" {
  description = <<-EOT
    Allow `destroy` to delete a non-empty bucket. False everywhere except
    throwaway environments — the embeddings corpus is the one store in this
    system with no other copy (§4.8: message text is gone after 24 h, so the
    vectors cannot be recomputed).
  EOT
  type        = bool
  default     = false
}

# ---------------------------------------------------------------------------
# Embeddings corpus (§8.4, §4.8)
# ---------------------------------------------------------------------------

variable "embeddings_versioning_enabled" {
  description = <<-EOT
    Off, deliberately.

    §8.4's parquet objects are written once, under a creator_id/date key, and
    never rewritten — there is no overwrite for versioning to protect against.
    Turning it on would keep a second copy of the largest data set in the system
    for no recoverable scenario, and delete markers on an object set growing by
    ~3.5 TB/day are their own operational problem. The protection that does
    apply is force_destroy = false plus a bucket policy that denies deletes;
    see deny_object_deletion.
  EOT
  type        = bool
  default     = false
}

variable "deny_object_deletion" {
  description = <<-EOT
    Add a bucket policy statement denying s3:DeleteObject to every principal
    except the ARNs in delete_exempt_principal_arns. Lifecycle expiration still
    works — it is an S3 service action, not an API call from a principal.
  EOT
  type        = bool
  default     = true
}

variable "delete_exempt_principal_arns" {
  description = "Principals still allowed to delete objects, e.g. a break-glass admin role."
  type        = list(string)
  default     = []
}

variable "embeddings_lifecycle_transitions" {
  description = <<-EOT
    Storage-class transitions for the embeddings corpus, in days since creation.

    The default stops at GLACIER_IR and does NOT include DEEP_ARCHIVE, and that
    is a §8.6 decision rather than a cost one: on-demand reclustering reads a
    creator's parquet for an arbitrary historical window ("recluster last
    month"), and DEEP_ARCHIVE retrieval is measured in hours. GLACIER_IR keeps
    millisecond reads at roughly a sixth of Standard's price, so a recluster
    still completes — it just costs a retrieval fee per GB. If you never expose
    on-demand reclustering beyond a bounded window, adding a DEEP_ARCHIVE
    transition past that window is the single largest saving available here.
  EOT
  type = list(object({
    days          = number
    storage_class = string
  }))
  default = [
    { days = 30, storage_class = "STANDARD_IA" },
    { days = 180, storage_class = "GLACIER_IR" },
  ]
}

variable "embeddings_expiration_days" {
  description = <<-EOT
    Delete embeddings after this many days. Null means never, which is what
    §4.8 specifies: embeddings carry creator_id, content_id and a timestamp but
    never author_id and never text, so they are not attributable to an
    individual and indefinite retention is defensible.

    Setting it is a product decision with a compliance argument behind it, not a
    cost lever — the cost lever is the §8.4 sampling ceiling, which lives in the
    chart's values, not here.
  EOT
  type        = number
  default     = null
}

variable "abort_incomplete_multipart_days" {
  description = <<-EOT
    Abandon incomplete multipart uploads after this many days. At the write rate
    in §8.4 a crashed insights-service pod can leave parts behind that are
    billed as storage and are invisible in the object listing.
  EOT
  type        = number
  default     = 7
}

# ---------------------------------------------------------------------------
# Supporting buckets
# ---------------------------------------------------------------------------

variable "create_milvus_bucket" {
  description = <<-EOT
    Object storage for an in-cluster Milvus. Milvus keeps its segments and
    indexes in object storage rather than on the pod's disk, so a Milvus
    deployment on EKS needs a bucket even though AWS offers no managed Milvus.
    See the README's ClickHouse/Milvus decision.
  EOT
  type        = bool
  default     = true
}

variable "create_clickhouse_backup_bucket" {
  description = "Bucket for clickhouse-backup, or for S3-backed MergeTree disks on an in-cluster ClickHouse."
  type        = bool
  default     = true
}

variable "create_access_log_bucket" {
  description = <<-EOT
    Create a bucket to receive S3 server access logs.

    Off by default. Server access logging on the embeddings bucket produces a
    log record per request against a bucket taking tens of thousands of PUTs a
    day, and the log objects themselves are billed. CloudTrail data events are
    the same trade. Turn it on if an audit requirement asks for it, with eyes
    open.
  EOT
  type        = bool
  default     = false
}

variable "access_log_retention_days" {
  description = "Expiration for objects in the access log bucket."
  type        = number
  default     = 90
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
