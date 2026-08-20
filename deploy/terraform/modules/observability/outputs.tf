output "application_log_group_name" {
  description = "CloudWatch log group for application logs, for the log-forwarder DaemonSet's config."
  value       = try(aws_cloudwatch_log_group.application[0].name, null)
}

output "alarm_topic_arn" {
  description = "SNS topic alarms publish to."
  value       = try(aws_sns_topic.alarms[0].arn, null)
}

output "prometheus_workspace_id" {
  description = "AMP workspace id."
  value       = try(aws_prometheus_workspace.this[0].id, null)
}

output "prometheus_workspace_arn" {
  description = "AMP workspace ARN, for scoping the remote-write IRSA policy."
  value       = try(aws_prometheus_workspace.this[0].arn, null)
}

output "prometheus_remote_write_url" {
  description = "remote_write endpoint for the in-cluster Prometheus, when AMP is enabled."
  value       = try("${aws_prometheus_workspace.this[0].prometheus_endpoint}api/v1/remote_write", null)
}

output "prometheus_query_url" {
  description = "Query endpoint for the AMP workspace, for Grafana."
  value       = try("${aws_prometheus_workspace.this[0].prometheus_endpoint}api/v1/query", null)
}
