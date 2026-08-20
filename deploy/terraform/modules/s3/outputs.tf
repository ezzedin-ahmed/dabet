output "embeddings_bucket_name" {
  description = "S3_BUCKET for insights-service and clusters-job."
  value       = aws_s3_bucket.this["embeddings"].bucket
}

output "embeddings_bucket_arn" {
  description = "ARN of the embeddings bucket, used to scope the IRSA policies."
  value       = aws_s3_bucket.this["embeddings"].arn
}

output "milvus_bucket_name" {
  description = "Bucket for an in-cluster Milvus. Null when create_milvus_bucket is false."
  value       = try(aws_s3_bucket.this["milvus"].bucket, null)
}

output "milvus_bucket_arn" {
  description = "ARN of the Milvus bucket."
  value       = try(aws_s3_bucket.this["milvus"].arn, null)
}

output "clickhouse_bucket_name" {
  description = "Bucket for ClickHouse backups or S3-backed disks."
  value       = try(aws_s3_bucket.this["clickhouse"].bucket, null)
}

output "clickhouse_bucket_arn" {
  description = "ARN of the ClickHouse bucket."
  value       = try(aws_s3_bucket.this["clickhouse"].arn, null)
}

output "access_log_bucket_name" {
  description = "Access log bucket, when enabled."
  value       = try(aws_s3_bucket.access_logs[0].bucket, null)
}

output "bucket_arns" {
  description = "Every bucket ARN this module created, keyed by role."
  value       = { for k, v in aws_s3_bucket.this : k => v.arn }
}
