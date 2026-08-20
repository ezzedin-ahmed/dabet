# Root composition module.
#
# There is deliberately no `provider "aws"` block and no `backend` block here.
# This directory is meant to be called as a module by an environment root
# (examples/dev, examples/prod), and a provider block inside a called module
# cannot be removed cleanly once state depends on it. The environment roots own
# the provider configuration, the region and the backend.
#
# `tofu init -backend=false && tofu validate` still works in this directory on
# its own — validation does not need a configured provider.

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
}
