# EKS control plane, OIDC provider for IRSA, and the managed addons.
#
# The cluster API endpoint is private by default. Nodes live in private subnets
# and are reachable only through Session Manager. Kubernetes secrets are
# envelope-encrypted with a customer-managed KMS key.

data "aws_partition" "current" {}

# ---------------------------------------------------------------------------
# Control-plane IAM
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "cluster_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name_prefix        = "${var.cluster_name}-cluster-"
  assume_role_policy = data.aws_iam_policy_document.cluster_assume.json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "cluster" {
  for_each = toset([
    "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSClusterPolicy",
    "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonEKSVPCResourceController",
  ])

  role       = aws_iam_role.cluster.name
  policy_arn = each.value
}

# ---------------------------------------------------------------------------
# Control-plane logging
#
# EKS creates this log group itself on first use, with no retention and no
# encryption. Creating it first means the cluster adopts ours instead, which is
# the only way to get a retention policy onto audit logs.
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "cluster" {
  name              = "/aws/eks/${var.cluster_name}/cluster"
  retention_in_days = var.cluster_log_retention_days
  kms_key_id        = var.log_kms_key_arn
  tags              = var.tags
}

# ---------------------------------------------------------------------------
# Cluster
# ---------------------------------------------------------------------------

resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  role_arn = aws_iam_role.cluster.arn
  version  = var.kubernetes_version

  enabled_cluster_log_types = var.enabled_cluster_log_types

  vpc_config {
    subnet_ids              = var.subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = var.endpoint_public_access
    public_access_cidrs     = var.endpoint_public_access ? var.public_access_cidrs : null
  }

  access_config {
    authentication_mode                         = "API"
    bootstrap_cluster_creator_admin_permissions = var.bootstrap_cluster_creator_admin_permissions
  }

  encryption_config {
    resources = ["secrets"]

    provider {
      key_arn = var.kms_key_arn
    }
  }

  tags = var.tags

  lifecycle {
    precondition {
      condition     = !var.endpoint_public_access || length(var.public_access_cidrs) > 0
      error_message = "endpoint_public_access is on but public_access_cidrs is empty, which AWS would widen to 0.0.0.0/0. Set the CIDRs explicitly."
    }

    precondition {
      condition     = !contains(var.public_access_cidrs, "0.0.0.0/0")
      error_message = "public_access_cidrs contains 0.0.0.0/0. Narrow it to your office, VPN or CI egress ranges."
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.cluster,
    aws_cloudwatch_log_group.cluster,
  ]
}

resource "aws_eks_access_entry" "admin" {
  for_each = toset(var.cluster_admin_principal_arns)

  cluster_name  = aws_eks_cluster.this.name
  principal_arn = each.value
  type          = "STANDARD"
  tags          = var.tags
}

resource "aws_eks_access_policy_association" "admin" {
  for_each = toset(var.cluster_admin_principal_arns)

  cluster_name  = aws_eks_cluster.this.name
  principal_arn = each.value
  policy_arn    = "arn:${data.aws_partition.current.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }

  depends_on = [aws_eks_access_entry.admin]
}

# ---------------------------------------------------------------------------
# IRSA trust anchor
#
# Every per-service role in the iam module federates against this provider. The
# audience is pinned to sts.amazonaws.com; without that condition on the trust
# policy, any token from the cluster's issuer would be accepted.
# ---------------------------------------------------------------------------

data "tls_certificate" "oidc" {
  url = aws_eks_cluster.this.identity[0].oidc[0].issuer
}

resource "aws_iam_openid_connect_provider" "this" {
  url             = aws_eks_cluster.this.identity[0].oidc[0].issuer
  client_id_list  = ["sts.amazonaws.com"]
  thumbprint_list = [data.tls_certificate.oidc.certificates[0].sha1_fingerprint]
  tags            = var.tags
}

# ---------------------------------------------------------------------------
# Managed addons
# ---------------------------------------------------------------------------

locals {
  addons = merge(
    var.addons.vpc_cni ? { "vpc-cni" = { service_account_role_arn = null } } : {},
    var.addons.kube_proxy ? { "kube-proxy" = { service_account_role_arn = null } } : {},
    var.addons.coredns ? { "coredns" = { service_account_role_arn = null } } : {},
    var.addons.pod_identity_agent ? { "eks-pod-identity-agent" = { service_account_role_arn = null } } : {},
    var.addons.ebs_csi_driver ? { "aws-ebs-csi-driver" = {
      # one() rather than [0] so the reference is safe when the count is zero.
      service_account_role_arn = one(aws_iam_role.ebs_csi[*].arn)
    } } : {},
  )
}

resource "aws_eks_addon" "this" {
  for_each = local.addons

  cluster_name  = aws_eks_cluster.this.name
  addon_name    = each.key
  addon_version = lookup(var.addons.addon_version_overrides, each.key, null)

  # Latest compatible version when no explicit pin was given.
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "PRESERVE"

  service_account_role_arn = each.value.service_account_role_arn

  tags = var.tags

  # coredns and the EBS CSI controller are Deployments: without a node to land
  # on, the addon install sits in DEGRADED until it times out. There is no
  # implicit dependency between an addon and a node group, so it is spelled out.
  depends_on = [aws_eks_node_group.general]
}

# ---------------------------------------------------------------------------
# EBS CSI driver IRSA role
#
# Lives here rather than in the iam module because it is a property of the
# cluster, not of a dabet service, and the addon above needs it at create time.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "ebs_csi_assume" {
  count = var.addons.ebs_csi_driver ? 1 : 0

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [aws_iam_openid_connect_provider.this.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_condition_prefix}:sub"
      values   = ["system:serviceaccount:kube-system:ebs-csi-controller-sa"]
    }

    condition {
      test     = "StringEquals"
      variable = "${local.oidc_condition_prefix}:aud"
      values   = ["sts.amazonaws.com"]
    }
  }
}

locals {
  oidc_condition_prefix = replace(aws_iam_openid_connect_provider.this.url, "https://", "")
}

resource "aws_iam_role" "ebs_csi" {
  count = var.addons.ebs_csi_driver ? 1 : 0

  name_prefix        = "${var.cluster_name}-ebs-csi-"
  assume_role_policy = data.aws_iam_policy_document.ebs_csi_assume[0].json
  tags               = var.tags
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  count = var.addons.ebs_csi_driver ? 1 : 0

  role       = aws_iam_role.ebs_csi[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
