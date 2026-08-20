output "redis_configuration_endpoint" {
  description = <<-EOT
    Cluster-mode configuration endpoint (host:port). This is the address a
    cluster-aware client bootstraps from. Empty when cluster mode is off.
  EOT
  value = (
    var.redis_cluster_mode
    ? "${aws_elasticache_replication_group.redis.configuration_endpoint_address}:${aws_elasticache_replication_group.redis.port}"
    : ""
  )
}

output "redis_primary_endpoint" {
  description = "Primary endpoint (host:port) for a non-cluster-mode group. Empty in cluster mode."
  value = (
    var.redis_cluster_mode
    ? ""
    : "${aws_elasticache_replication_group.redis.primary_endpoint_address}:${aws_elasticache_replication_group.redis.port}"
  )
}

output "redis_addr" {
  description = <<-EOT
    What REDIS_ADDR should be set to, whichever mode is in force. Cluster mode
    yields the configuration endpoint, which needs a cluster-aware client — see
    the note on redis_cluster_mode.
  EOT
  value = (
    var.redis_cluster_mode
    ? "${aws_elasticache_replication_group.redis.configuration_endpoint_address}:${aws_elasticache_replication_group.redis.port}"
    : "${aws_elasticache_replication_group.redis.primary_endpoint_address}:${aws_elasticache_replication_group.redis.port}"
  )
}

output "redis_cluster_mode" {
  description = "Whether cluster mode is on, so the chart knows which client to configure."
  value       = var.redis_cluster_mode
}

output "redis_transit_encryption_enabled" {
  description = "Whether the client must dial TLS."
  value       = var.redis_transit_encryption_enabled
}

output "redis_security_group_id" {
  description = "Security group in front of Redis."
  value       = aws_security_group.redis.id
}

output "redis_replication_group_id" {
  description = "Replication group id."
  value       = aws_elasticache_replication_group.redis.id
}

output "redis_member_cluster_ids" {
  description = <<-EOT
    Individual cache cluster ids inside the replication group. CloudWatch
    publishes ElastiCache metrics against CacheClusterId, not against the
    replication group, so this is what an alarm has to be dimensioned on.
  EOT
  value       = tolist(aws_elasticache_replication_group.redis.member_clusters)
}

output "memcached_cluster_id" {
  description = "Memcached cluster id, for CloudWatch alarm dimensions."
  value       = try(aws_elasticache_cluster.memcached[0].cluster_id, null)
}

output "memcached_configuration_endpoint" {
  description = <<-EOT
    Memcached auto-discovery configuration endpoint (host:port).

    Do NOT put this in MEMCACHED_ADDRS. Auto-discovery is an ElastiCache
    protocol extension that the client has to speak, and policy-service uses
    gomemcache, which does not: it would hash keys against a single "node" that
    happens to be the discovery endpoint. Use memcached_node_addresses instead.
  EOT
  value       = try("${aws_elasticache_cluster.memcached[0].configuration_endpoint}", "")
}

output "memcached_node_addresses" {
  description = <<-EOT
    Individual Memcached node addresses, host:port, one per node. This is the
    value MEMCACHED_ADDRS wants — gomemcache takes a static server list and
    hashes across it client-side.

    The trade-off is that adding or removing a node changes this list, so the
    chart has to be re-rendered and policy-service restarted for the new node to
    be used. §6.8 makes that survivable: a cache miss reads through to Postgres.
  EOT
  value = try(
    [for n in aws_elasticache_cluster.memcached[0].cache_nodes : "${n.address}:${n.port}"],
    [],
  )
}

output "memcached_security_group_id" {
  description = "Security group in front of Memcached."
  value       = try(aws_security_group.memcached[0].id, null)
}
