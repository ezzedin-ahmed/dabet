variable "cluster_name" {
  description = "EKS cluster name."
  type        = string
}

variable "kubernetes_version" {
  description = <<-EOT
    Kubernetes minor version, e.g. "1.33". Pin it: leaving it unset lets AWS pick,
    and an auto-upgrade is not something you want arriving unannounced on the hot
    path. Check the EKS release calendar before bumping — support windows are
    roughly 14 months.
  EOT
  type        = string
}

variable "vpc_id" {
  description = "VPC to place the cluster in."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnets for the control-plane ENIs and the node groups."
  type        = list(string)
}

variable "endpoint_public_access" {
  description = <<-EOT
    Expose the Kubernetes API endpoint to the internet. Off by default: with it
    off, kubectl and CI reach the API through a VPN, Direct Connect, or a bastion
    inside the VPC. If you turn it on you MUST narrow public_access_cidrs.
  EOT
  type        = bool
  default     = false
}

variable "public_access_cidrs" {
  description = <<-EOT
    Source CIDRs allowed to reach a public API endpoint. Deliberately empty:
    setting endpoint_public_access without filling this in is refused by a
    precondition on aws_eks_cluster rather than silently becoming 0.0.0.0/0.
    An explicit 0.0.0.0/0 in this list is refused too.
  EOT
  type        = list(string)
  default     = []
}

variable "kms_key_arn" {
  description = "KMS key used for envelope encryption of Kubernetes secrets and for node EBS volumes."
  type        = string
}

variable "log_kms_key_arn" {
  description = "KMS key for the control-plane CloudWatch log group. Null uses the CloudWatch service key."
  type        = string
  default     = null
}

variable "enabled_cluster_log_types" {
  description = "Control-plane log types shipped to CloudWatch."
  type        = list(string)
  default     = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
}

variable "cluster_log_retention_days" {
  description = "Retention for the control-plane log group. Audit logs at cluster scale are not cheap."
  type        = number
  default     = 30
}

variable "cluster_admin_principal_arns" {
  description = <<-EOT
    IAM principals granted AmazonEKSClusterAdminPolicy through EKS access entries.
    Access entries replace the aws-auth ConfigMap; the cluster runs with
    authentication_mode = "API" so the ConfigMap path is off entirely.
  EOT
  type        = list(string)
  default     = []
}

variable "bootstrap_cluster_creator_admin_permissions" {
  description = <<-EOT
    Give the identity that creates the cluster admin access. Handy for a first
    apply, but it is a one-shot decision at creation time and flipping it later
    forces the cluster to be replaced — so it is exposed rather than hardcoded.
  EOT
  type        = bool
  default     = true
}

# ---------------------------------------------------------------------------
# General node pool
# ---------------------------------------------------------------------------

variable "general_node_group" {
  description = <<-EOT
    The pool every dabet service runs on. See the README sizing note: the
    measured ceiling is ~170-200 msg/s per moderation-service instance, so the
    N6 baseline of 50K msg/s wants a few hundred pods and this pool has to be
    able to grow into that.
  EOT
  type = object({
    instance_types = optional(list(string), ["m7i.2xlarge"])
    capacity_type  = optional(string, "ON_DEMAND")
    ami_type       = optional(string, "AL2023_x86_64_STANDARD")
    min_size       = optional(number, 3)
    desired_size   = optional(number, 3)
    max_size       = optional(number, 30)
    disk_size_gb   = optional(number, 100)
    labels         = optional(map(string), {})
  })
  default = {}
}

# ---------------------------------------------------------------------------
# GPU node pool for vLLM
# ---------------------------------------------------------------------------

variable "gpu_node_group" {
  description = <<-EOT
    Optional GPU pool for vLLM (§7.9). Off by default: GPUs are the single most
    expensive line item here and the platform runs end to end against
    tools/mockllm without one.

    Both taints keep everything else off the pool; the chart schedules vLLM here
    with a matching nodeSelector and tolerations. The NVIDIA device plugin is
    still the chart's job — the AMI ships the driver, not the plugin, so
    nvidia.com/gpu is not an allocatable resource until the plugin is running.
  EOT
  type = object({
    enabled        = optional(bool, false)
    instance_types = optional(list(string), ["g6.2xlarge"])
    capacity_type  = optional(string, "ON_DEMAND")
    ami_type       = optional(string, "AL2023_x86_64_NVIDIA")
    min_size       = optional(number, 0)
    desired_size   = optional(number, 2)
    max_size       = optional(number, 8)
    disk_size_gb   = optional(number, 200)
  })
  default = {}
}

# ---------------------------------------------------------------------------
# Stateful node pool for in-cluster ClickHouse / Milvus
# ---------------------------------------------------------------------------

variable "stateful_node_group" {
  description = <<-EOT
    Optional pool for the two stores AWS has no managed equivalent of —
    ClickHouse and Milvus (see the README). Tainted so only workloads that
    tolerate dabet.io/workload=stateful land on it, which keeps a moderation
    pod from being scheduled onto a node that is about to be drained for a
    StatefulSet rollout.
  EOT
  type = object({
    enabled        = optional(bool, false)
    instance_types = optional(list(string), ["r7i.2xlarge"])
    capacity_type  = optional(string, "ON_DEMAND")
    ami_type       = optional(string, "AL2023_x86_64_STANDARD")
    min_size       = optional(number, 3)
    desired_size   = optional(number, 3)
    max_size       = optional(number, 12)
    disk_size_gb   = optional(number, 200)
  })
  default = {}
}

variable "node_metadata_hop_limit" {
  description = <<-EOT
    IMDSv2 hop limit on the nodes. EKS defaults to 2 so that pods can reach the
    instance metadata service; 1 blocks pods from it and forces every pod to use
    IRSA, which is stricter but breaks any addon that has not been migrated.
    IMDSv2 itself is always required regardless of this value.
  EOT
  type        = number
  default     = 2
}

variable "additional_node_security_group_ids" {
  description = "Extra security groups on the nodes, on top of the EKS-managed cluster security group."
  type        = list(string)
  default     = []
}

variable "enable_ssm" {
  description = "Attach AmazonSSMManagedInstanceCore so nodes are reachable through Session Manager instead of SSH."
  type        = bool
  default     = true
}

variable "addons" {
  description = <<-EOT
    EKS managed addons. Versions resolve to the latest compatible release rather
    than being pinned here, because the compatible set is a function of the
    Kubernetes version and pinning a stale one is the more common failure. Pin
    them per environment if your change control needs it.
  EOT
  type = object({
    vpc_cni                 = optional(bool, true)
    coredns                 = optional(bool, true)
    kube_proxy              = optional(bool, true)
    ebs_csi_driver          = optional(bool, true)
    pod_identity_agent      = optional(bool, true)
    addon_version_overrides = optional(map(string), {})
  })
  default = {}
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
