output "state_bucket" {
  description = "Bucket name for the `bucket` field of the S3 backend."
  value       = aws_s3_bucket.state.bucket
}

output "dynamodb_lock_table" {
  description = "Lock table name, when the legacy table was created."
  value       = try(aws_dynamodb_table.lock[0].name, null)
}

output "backend_config" {
  description = <<-EOT
    A ready-made backend block for an environment root. Native S3 locking, so
    no DynamoDB table appears in it.
  EOT
  value       = <<-EOT
    terraform {
      backend "s3" {
        bucket       = "${aws_s3_bucket.state.bucket}"
        key          = "dabet/<environment>/terraform.tfstate"
        region       = "${var.region}"
        encrypt      = true
        use_lockfile = true
      }
    }
  EOT
}
