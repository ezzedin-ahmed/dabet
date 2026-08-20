# Managed node groups.
#
# Three pools, only the first of which is on by default:
#
#   general   every dabet service
#   gpu       vLLM only, tainted so nothing else can land there (off by default)
#   stateful  in-cluster ClickHouse and Milvus (off by default)
#
# Each pool uses a launch template so that the root volume is gp3, encrypted with
# the module's KMS key, and IMDSv2 is required. Note that a node group may set
# either disk_size or a launch template, never both — the size lives in the
# template.

# ---------------------------------------------------------------------------
# Shared node IAM role
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "node_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name_prefix        = "${var.cluster_name}-node-"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
  tags               = var.tags
}

locals {
  node_policy_arns = concat(
    [
      "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSWorkerNodePolicy",
      "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
      # The VPC CNI runs as a DaemonSet on the node identity. Moving it to its
      # own IRSA role is stricter; it is a cluster-addon change, not a Terraform
      # one, and is called out in the README.
      "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKS_CNI_Policy",
    ],
    var.enable_ssm ? ["arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"] : [],
  )
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset(local.node_policy_arns)

  role       = aws_iam_role.node.name
  policy_arn = each.value
}

# ---------------------------------------------------------------------------
# Launch templates
# ---------------------------------------------------------------------------

locals {
  node_security_group_ids = concat(
    [aws_eks_cluster.this.vpc_config[0].cluster_security_group_id],
    var.additional_node_security_group_ids,
  )

  launch_templates = merge(
    { general = var.general_node_group.disk_size_gb },
    var.gpu_node_group.enabled ? { gpu = var.gpu_node_group.disk_size_gb } : {},
    var.stateful_node_group.enabled ? { stateful = var.stateful_node_group.disk_size_gb } : {},
  )
}

resource "aws_launch_template" "node" {
  for_each = local.launch_templates

  name_prefix = "${var.cluster_name}-${each.key}-"
  description = "EKS ${each.key} pool for ${var.cluster_name}"

  # No image_id: EKS injects the AMI matching the node group's ami_type and
  # kubernetes version, along with the bootstrap user data. Setting one here
  # would make this a custom-AMI node group and take over that responsibility.

  vpc_security_group_ids = local.node_security_group_ids

  block_device_mappings {
    device_name = "/dev/xvda"

    ebs {
      volume_size           = each.value
      volume_type           = "gp3"
      encrypted             = true
      kms_key_id            = var.kms_key_arn
      delete_on_termination = true
    }
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = var.node_metadata_hop_limit
    instance_metadata_tags      = "disabled"
  }

  monitoring {
    enabled = true
  }

  tag_specifications {
    resource_type = "instance"
    tags          = merge(var.tags, { Name = "${var.cluster_name}-${each.key}" })
  }

  tag_specifications {
    resource_type = "volume"
    tags          = merge(var.tags, { Name = "${var.cluster_name}-${each.key}" })
  }

  tags = var.tags

  lifecycle {
    create_before_destroy = true
  }
}

# ---------------------------------------------------------------------------
# General pool
# ---------------------------------------------------------------------------

resource "aws_eks_node_group" "general" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "general"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids

  instance_types = var.general_node_group.instance_types
  capacity_type  = var.general_node_group.capacity_type
  ami_type       = var.general_node_group.ami_type

  scaling_config {
    min_size     = var.general_node_group.min_size
    desired_size = var.general_node_group.desired_size
    max_size     = var.general_node_group.max_size
  }

  update_config {
    max_unavailable_percentage = 25
  }

  launch_template {
    id      = aws_launch_template.node["general"].id
    version = aws_launch_template.node["general"].latest_version
  }

  labels = merge({ "dabet.io/pool" = "general" }, var.general_node_group.labels)

  tags = var.tags

  lifecycle {
    # The cluster autoscaler (or a human) owns desired_size after creation;
    # without this, every apply drags the pool back to the configured number.
    ignore_changes = [scaling_config[0].desired_size]
  }

  # A node group whose role has not yet been granted the worker policies comes
  # up unable to register, and destroy ordering is worse: the policies detach
  # first and the nodes cannot deregister. There is no implicit dependency
  # between a node group and the role's policy attachments.
  depends_on = [aws_iam_role_policy_attachment.node]
}

# ---------------------------------------------------------------------------
# GPU pool (vLLM)
# ---------------------------------------------------------------------------

resource "aws_eks_node_group" "gpu" {
  count = var.gpu_node_group.enabled ? 1 : 0

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "gpu"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids

  instance_types = var.gpu_node_group.instance_types
  capacity_type  = var.gpu_node_group.capacity_type
  ami_type       = var.gpu_node_group.ami_type

  scaling_config {
    min_size     = var.gpu_node_group.min_size
    desired_size = var.gpu_node_group.desired_size
    max_size     = var.gpu_node_group.max_size
  }

  update_config {
    max_unavailable = 1
  }

  launch_template {
    id      = aws_launch_template.node["gpu"].id
    version = aws_launch_template.node["gpu"].latest_version
  }

  labels = {
    "dabet.io/pool"     = "gpu"
    "dabet.io/workload" = "vllm"
  }

  # Two taints, on purpose. nvidia.com/gpu is what the NVIDIA device plugin and
  # most GPU charts already tolerate; dabet.io/workload is ours, so that a
  # third-party chart that blanket-tolerates nvidia.com/gpu still cannot take a
  # GPU we are paying for.
  taint {
    key    = "nvidia.com/gpu"
    value  = "present"
    effect = "NO_SCHEDULE"
  }

  taint {
    key    = "dabet.io/workload"
    value  = "vllm"
    effect = "NO_SCHEDULE"
  }

  tags = var.tags

  lifecycle {
    ignore_changes = [scaling_config[0].desired_size]
  }

  depends_on = [aws_iam_role_policy_attachment.node]
}

# ---------------------------------------------------------------------------
# Stateful pool (ClickHouse, Milvus)
# ---------------------------------------------------------------------------

resource "aws_eks_node_group" "stateful" {
  count = var.stateful_node_group.enabled ? 1 : 0

  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "stateful"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = var.subnet_ids

  instance_types = var.stateful_node_group.instance_types
  capacity_type  = var.stateful_node_group.capacity_type
  ami_type       = var.stateful_node_group.ami_type

  scaling_config {
    min_size     = var.stateful_node_group.min_size
    desired_size = var.stateful_node_group.desired_size
    max_size     = var.stateful_node_group.max_size
  }

  update_config {
    max_unavailable = 1
  }

  launch_template {
    id      = aws_launch_template.node["stateful"].id
    version = aws_launch_template.node["stateful"].latest_version
  }

  labels = {
    "dabet.io/pool"     = "stateful"
    "dabet.io/workload" = "stateful"
  }

  taint {
    key    = "dabet.io/workload"
    value  = "stateful"
    effect = "NO_SCHEDULE"
  }

  tags = var.tags

  lifecycle {
    ignore_changes = [scaling_config[0].desired_size]
  }

  depends_on = [aws_iam_role_policy_attachment.node]
}
