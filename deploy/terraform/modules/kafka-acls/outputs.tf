output "rules" {
  description = <<-EOT
    kafka.acls.rules for charts/dabet-deps, admin bindings first.

    Order is part of the contract: on a broker with
    allow.everyone.if.no.acl.found=true the first binding written against the
    Cluster resource decides who may write bindings at all, so the admin's own
    Alter grant has to lead. The chart's applier walks the list in order.
  EOT
  value       = local.rules
}

output "commands" {
  description = <<-EOT
    The same bindings as kafka-acls.sh invocations, for applying them from a
    bastion inside the VPC instead of from the chart Job. They assume a
    client.properties in the working directory carrying the admin credential.
  EOT
  value       = local.commands
}

output "usernames" {
  description = "SCRAM username per service. Every service maps to the same name when mode is \"shared\"."
  value       = local.usernames
}

output "admin_username" {
  description = "SCRAM username for the topic and ACL reconcilers."
  value       = var.admin_username
}

output "scram_users" {
  description = <<-EOT
    Every SCRAM username that needs a credential, as a map of username to
    description. Feed this straight into modules/msk's `scram_users`: it
    creates one Secrets Manager secret per entry, each with its own generated
    password, and associates the lot with the cluster.
  EOT
  value       = local.scram_users
}

output "kafka_access" {
  description = <<-EOT
    The same matrix in the shape modules/iam consumes, so the SCRAM path and
    the IAM path are generated from one table and cannot drift.
  EOT
  value       = local.kafka_access
}

output "kafka_consumer_groups" {
  description = <<-EOT
    Consumer group names per service, for modules/iam.

    Services that consume WITHOUT a group are absent rather than present and
    empty: review-service reads flagged.v1 through kgo.ConsumePartitions with
    explicit offsets (§7.6) and clusters-job takes a bounded groupless sample
    of messages.v1, so neither has a group to authorise.
  EOT
  value       = local.kafka_consumer_groups
}

output "principal_names" {
  description = "Kafka principal strings (\"User:<username>\") per service, for wiring a chart or a policy by hand."
  value       = { for svc, u in local.usernames : svc => "User:${u}" }
}
