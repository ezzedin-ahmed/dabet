# No required_providers: this module creates nothing. It is the §1.5 matrix
# rendered into the three shapes that consume it. `tofu init -backend=false`
# on a root that calls it therefore needs no credentials and downloads nothing
# on its account.
terraform {
  required_version = ">= 1.6.0"
}
