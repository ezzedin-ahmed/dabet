terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
  }

  # Partial backend configuration: the bucket and region come from backend.hcl,
  # which is not committed because it names an account-specific bucket.
  #
  #   tofu init -backend-config=backend.hcl
  #
  # use_lockfile is S3-native locking (a .tflock object next to the state),
  # supported since OpenTofu 1.10 / Terraform 1.10. There is no DynamoDB table:
  # S3 gained conditional writes, which is the only thing the table was ever
  # standing in for. deploy/terraform/bootstrap creates the bucket.
  backend "s3" {
    key          = "dabet/dev/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project     = "dabet"
      Environment = "dev"
      ManagedBy   = "opentofu"
    }
  }
}
