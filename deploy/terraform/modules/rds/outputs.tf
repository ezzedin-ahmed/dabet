output "identifier" {
  description = "DB instance identifier."
  value       = aws_db_instance.this.identifier
}

output "arn" {
  description = "DB instance ARN."
  value       = aws_db_instance.this.arn
}

output "address" {
  description = "Hostname. Goes into POSTGRES_DSN in the Helm values, with sslmode=require."
  value       = aws_db_instance.this.address
}

output "port" {
  description = "Port."
  value       = aws_db_instance.this.port
}

output "endpoint" {
  description = "host:port."
  value       = aws_db_instance.this.endpoint
}

output "database_name" {
  description = "Initial database name."
  value       = aws_db_instance.this.db_name
}

output "username" {
  description = "Master username. Also present inside the managed secret's JSON."
  value       = aws_db_instance.this.username
}

output "master_user_secret_arn" {
  description = <<-EOT
    ARN of the RDS-managed Secrets Manager secret holding {username, password}.
    Null when manage_master_user_password is false. This is what External
    Secrets Operator points at; the password never exists in Terraform state.
  EOT
  value       = try(aws_db_instance.this.master_user_secret[0].secret_arn, null)
}

output "security_group_id" {
  description = "Security group in front of the instance."
  value       = aws_security_group.this.id
}

output "resource_id" {
  description = "DbiResourceId, needed if you ever grant rds-db:connect for IAM database authentication."
  value       = aws_db_instance.this.resource_id
}
