terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

# Chicken and egg: this root creates the bucket the other roots keep their state
# in, so it cannot itself use that bucket. Run it once with local state and
# commit the resulting terraform.tfstate somewhere safe, or run it and then
# migrate it into its own bucket with `tofu init -migrate-state`.
provider "aws" {
  region = var.region

  default_tags {
    tags = {
      Project   = "dabet"
      ManagedBy = "opentofu"
      Component = "tf-state"
    }
  }
}
