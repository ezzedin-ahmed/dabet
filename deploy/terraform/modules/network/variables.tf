variable "name" {
  description = "Name prefix for every resource in this module (e.g. \"dabet-prod\")."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC. A /16 leaves room for the default subnet layout."
  type        = string
  default     = "10.0.0.0/16"
}

variable "azs" {
  description = <<-EOT
    Availability zones to spread across. §3 wants 3+ Kafka brokers across AZs and
    Multi-AZ Postgres, so three is the practical floor. The subnet CIDR lists below
    must have one entry per AZ.
  EOT
  type        = list(string)

  validation {
    condition     = length(var.azs) >= 2
    error_message = "At least two AZs are required: RDS subnet groups and MSK both refuse a single-AZ layout."
  }
}

variable "private_subnet_cidrs" {
  description = <<-EOT
    Subnets for EKS nodes and pods. These are large by default because the VPC CNI
    hands every pod a VPC address, and the N6 baseline needs a few hundred
    moderation-service pods (see the sizing note in the README).
  EOT
  type        = list(string)
  default     = ["10.0.0.0/18", "10.0.64.0/18", "10.0.128.0/18"]
}

variable "public_subnet_cidrs" {
  description = "Subnets for internet-facing load balancers and NAT gateways only. No workload runs here."
  type        = list(string)
  default     = ["10.0.192.0/24", "10.0.193.0/24", "10.0.194.0/24"]
}

variable "data_subnet_cidrs" {
  description = <<-EOT
    Isolated subnets for RDS, MSK and ElastiCache. Their route tables carry no
    default route at all: the data tier has no path to the internet in either
    direction, and reaches S3 through the gateway endpoint.
  EOT
  type        = list(string)
  default     = ["10.0.208.0/22", "10.0.212.0/22", "10.0.216.0/22"]
}

variable "enable_nat_gateway" {
  description = "Create NAT gateways so private subnets can reach the internet (platform APIs, Stripe, image pulls)."
  type        = bool
  default     = true
}

variable "single_nat_gateway" {
  description = <<-EOT
    Route every private subnet through one NAT gateway instead of one per AZ.
    Cheaper (~$33/month each) but a single AZ failure takes egress with it, and
    all cross-AZ NAT traffic is billed. Non-production only.
  EOT
  type        = bool
  default     = false
}

variable "eks_cluster_name" {
  description = <<-EOT
    Cluster name used for the kubernetes.io/cluster/<name> subnet tags that the AWS
    Load Balancer Controller relies on for subnet discovery. Empty skips the tags.
  EOT
  type        = string
  default     = ""
}

variable "interface_endpoint_services" {
  description = <<-EOT
    Interface VPC endpoints to create in the private subnets. Each costs roughly
    $7/month per AZ plus data processing, so the default list is only the ones a
    private-subnet EKS cluster genuinely needs. The S3 gateway endpoint is always
    created and is free — see the README for why that matters at 3.5 TB/day.
  EOT
  type        = list(string)
  default = [
    "ecr.api",
    "ecr.dkr",
    "logs",
    "secretsmanager",
    "sts",
    "kms",
  ]
}

variable "enable_flow_logs" {
  description = "Send VPC flow logs to CloudWatch Logs."
  type        = bool
  default     = true
}

variable "flow_log_retention_days" {
  description = "CloudWatch retention for VPC flow logs."
  type        = number
  default     = 30
}

variable "kms_key_arn" {
  description = "KMS key for CloudWatch log encryption. Null uses the CloudWatch service key."
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
