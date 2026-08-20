variable "name" {
  description = "Name prefix for the Redis replication group and the Memcached cluster."
  type        = string
}

variable "vpc_id" {
  description = "VPC the caches live in."
  type        = string
}

variable "subnet_ids" {
  description = "Isolated data-tier subnets."
  type        = list(string)
}

variable "allowed_security_group_ids" {
  description = "Security groups allowed to reach the caches. Normally just the EKS cluster security group."
  type        = list(string)
}

variable "kms_key_arn" {
  description = "KMS key for Redis encryption at rest."
  type        = string
}

# ---------------------------------------------------------------------------
# Redis (§4.3, §7.4)
# ---------------------------------------------------------------------------

variable "redis_cluster_mode" {
  description = <<-EOT
    Run Redis in cluster mode, sharded across node groups. §3's target says
    Redis Cluster, and §4.3's hash tags exist so that every key for one
    (content_id, author_id) pair lands in the same slot — which is what makes
    the Lua read-modify-write scripts atomic on a sharded cluster.

    NOTE for the app: moderation-service builds a go-redis *single-node* client
    (redis.NewClient). Pointed at a cluster-mode configuration endpoint it will
    take MOVED redirections it does not follow. Cluster mode therefore needs the
    client swapped to redis.NewClusterClient before it can be turned on. See
    the README.
  EOT
  type        = bool
  default     = true
}

variable "redis_engine_version" {
  description = "Redis engine version, e.g. \"7.1\"."
  type        = string
  default     = "7.1"
}

variable "redis_parameter_group_family" {
  description = <<-EOT
    Parameter group family. Must match the engine version: "redis7" for 7.x.
    The module creates its own group rather than using default.redis7.cluster.on
    so that maxmemory-policy can be set.
  EOT
  type        = string
  default     = "redis7"
}

variable "redis_node_type" {
  description = <<-EOT
    Node type per shard.

    Sizing from §4.3: every key has a TTL of five minutes or less, so the
    working set is roughly five minutes of active senders, not a growing corpus.
    The largest entries are the packed embedding lists for semantic spam
    (emb:{content}:{author}, up to ~20 vectors of 384 fp16 values, so ~15 KB per
    active sender-content pair) and those only exist where spam = semantic,
    which §7.4 says is off by default. Memory is rarely the constraint; request
    rate is. F3 in test/load/README.md measures four sequential Redis round
    trips per message on the moderation hot path, so at 50K msg/s this cluster
    sees ~200K operations/second and wants shards for the connection and CPU
    budget rather than for capacity.
  EOT
  type        = string
  default     = "cache.r7g.large"
}

variable "redis_shards" {
  description = "Number of shards (node groups) in cluster mode. Ignored when redis_cluster_mode is false."
  type        = number
  default     = 3
}

variable "redis_replicas_per_shard" {
  description = "Read replicas per shard. One gives automatic failover within the shard."
  type        = number
  default     = 1
}

variable "redis_transit_encryption_enabled" {
  description = <<-EOT
    TLS between clients and Redis.

    On by default because §4.4's secrets story and "encryption in transit
    everywhere it is available" both point that way, but the client must dial
    TLS. go-redis needs a non-nil TLSConfig; the current moderation-service
    build passes none, so turning this on without the client change makes Redis
    unreachable — which, per §4.7, degrades to fail-open rather than an outage,
    and will show up as fail_open_total{component="redis"} pinned at the message
    rate. Set it to false for a first bring-up if the client has not been
    updated yet.

    Changing this on an existing group is not always an in-place update; on
    older engine versions it forces the replication group to be replaced. Check
    the plan before flipping it. Per §4.3 nothing in that keyspace is older than
    five minutes, so a replacement costs a burst of fail-opens rather than data.
  EOT
  type        = bool
  default     = true
}

variable "redis_snapshot_retention_days" {
  description = <<-EOT
    Zero on purpose. Every key in §4.3 has a TTL of five minutes or less and
    holds nothing that survives a restart being useful: dedup hashes, token
    buckets, and a redelivery guard. A backup of that is a backup of nothing,
    and snapshots on a large cluster cost both money and a fork of the process.
    §4.7 already treats a cold Redis as a fail-open, not a data-loss event.
  EOT
  type        = number
  default     = 0
}

variable "redis_maxmemory_policy" {
  description = <<-EOT
    Eviction policy. volatile-lru because every key carries a TTL and the hot
    path must never see an OOM write error: dropping the oldest moderation state
    degrades a detector, whereas a write failure stalls the consumer.
  EOT
  type        = string
  default     = "volatile-lru"
}

variable "redis_extra_parameters" {
  description = "Extra Redis parameters merged over the module defaults."
  type        = map(string)
  default     = {}
}

variable "redis_auth_token_secret_arn" {
  description = <<-EOT
    Optional Secrets Manager ARN holding a Redis AUTH token. Left null the
    cluster relies on network isolation alone, which is the data-tier subnet
    plus a security group that only the cluster can reach. Populate it only if
    the client is configured to send the token.
  EOT
  type        = string
  default     = null
}

variable "redis_log_retention_days" {
  description = "Retention for the Redis slow and engine log groups."
  type        = number
  default     = 14
}

# ---------------------------------------------------------------------------
# Memcached (§6.8)
# ---------------------------------------------------------------------------

variable "memcached_enabled" {
  description = "Create the Memcached pool that policy-service uses for its 300 s policy cache."
  type        = bool
  default     = true
}

variable "memcached_engine_version" {
  description = "Memcached engine version."
  type        = string
  default     = "1.6.22"
}

variable "memcached_parameter_group_family" {
  description = "Memcached parameter group family, e.g. \"memcached1.6\"."
  type        = string
  default     = "memcached1.6"
}

variable "memcached_node_type" {
  description = <<-EOT
    Node type.

    §6.8 caches one resolved policy document per (creator, platform, content)
    for 300 s, and §6.5 caps a policy at roughly 20 rubric entries plus 500
    restricted words — call it a few KB serialised. Even a million hot entries
    is single-digit GB, and negative results (§6.7) are tiny. This pool is small
    on purpose; the in-process LRU absorbs most of the traffic before it gets
    here.
  EOT
  type        = string
  default     = "cache.m7g.large"
}

variable "memcached_node_count" {
  description = "Number of Memcached nodes, spread across AZs."
  type        = number
  default     = 3
}

variable "memcached_transit_encryption_enabled" {
  description = <<-EOT
    TLS to Memcached. Off by default: policy-service uses gomemcache, which
    dials plain TCP and has no TLS path, so enabling this makes the cache
    unreachable. §6.8 says an unreachable Memcached reads through to Postgres,
    so it degrades rather than breaks — but it degrades the hot path's cache
    hit rate to zero, which at 500K msg/s is not something to discover in
    production.
  EOT
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags applied to every resource in this module."
  type        = map(string)
  default     = {}
}
