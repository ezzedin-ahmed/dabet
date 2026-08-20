# MSK provisioned cluster.
#
# Topic creation is NOT done here. §4.2's four topics, their partition counts and
# their retentions belong to the charts, which already create them under Compose
# (deploy/compose/docker-compose.yml, the kafka-init job). Terraform owns the
# brokers, the storage and the defaults that make those partition counts
# survivable; the topic table stays in one place, next to the code that depends
# on it.
#
# What Terraform does have to get right is that the cluster can carry them:
# 512 + 128 + 128 + 32 partitions at replication factor 3 is 2400 partition
# replicas, and MSK's per-broker partition guidance is what sets broker_count
# and instance_type. See the variable descriptions.

locals {
  # min.insync.replicas = 2 with acks=all (§4.2's producer setting) is what
  # makes a single broker loss survivable without data loss: the write needs two
  # of three replicas, so one broker can be down and producers keep going.
  default_server_properties = {
    "auto.create.topics.enable"      = "false"
    "default.replication.factor"     = "3"
    "min.insync.replicas"            = "2"
    "num.partitions"                 = "3"
    "unclean.leader.election.enable" = "false"
    "delete.topic.enable"            = "true"

    # Producers compress with zstd (§4.2); "producer" tells the broker to store
    # the batch exactly as sent instead of recompressing it, which is both
    # cheaper and preserves the producer's choice.
    "compression.type" = "producer"

    # Cross-AZ data transfer is a real line on this bill: the sustained rate is
    # tens of MB/s and two consumer groups read messages.v1. Rack-aware replica
    # selection lets a consumer fetch from a replica in its own AZ instead of
    # always from the leader. The client has to opt in by setting its rack
    # (franz-go: kgo.Rack); without that this property is inert but harmless.
    "replica.selector.class" = "org.apache.kafka.common.replica.RackAwareReplicaSelector"

    # §7.3 relies on partition ordering and a single owner per key. Longer
    # session and rebalance windows keep a GC pause or a slow poll from
    # triggering a rebalance that moves state between consumers.
    "group.initial.rebalance.delay.ms" = "3000"

    # 512 partitions x 3 replicas means a lot of concurrent fetches between
    # brokers during a replacement.
    "num.replica.fetchers" = "4"
  }

  server_properties = merge(local.default_server_properties, var.extra_server_properties)

  server_properties_text = join("\n", [
    for k, v in local.server_properties : "${k}=${v}"
  ])
}

resource "aws_security_group" "this" {
  name_prefix = "${var.name}-msk-"
  description = "MSK ${var.name}"
  vpc_id      = var.vpc_id

  tags = merge(var.tags, { Name = "${var.name}-msk" })

  lifecycle {
    create_before_destroy = true
  }
}

# 9098 is SASL/IAM over TLS, 9094 is mutual/plain TLS, 9092 is plaintext. Only
# the ports the chosen authentication mode actually uses are opened.
locals {
  broker_ports = concat(
    var.client_authentication == "iam" ? [9098] : [],
    var.client_authentication == "unauthenticated" && var.encryption_in_transit_client_broker != "TLS" ? [9092] : [],
    var.client_authentication == "unauthenticated" && var.encryption_in_transit_client_broker != "PLAINTEXT" ? [9094] : [],
  )

  client_rules = {
    for pair in setproduct(var.allowed_security_group_ids, local.broker_ports) :
    "${pair[0]}-${pair[1]}" => { sg = pair[0], port = pair[1] }
  }
}

resource "aws_vpc_security_group_ingress_rule" "clients" {
  for_each = local.client_rules

  security_group_id            = aws_security_group.this.id
  description                  = "Kafka ${each.value.port} from ${each.value.sg}"
  referenced_security_group_id = each.value.sg
  ip_protocol                  = "tcp"
  from_port                    = each.value.port
  to_port                      = each.value.port
}

# Brokers replicate between themselves; without this they cannot form a quorum.
resource "aws_vpc_security_group_ingress_rule" "inter_broker" {
  security_group_id            = aws_security_group.this.id
  description                  = "Inter-broker replication"
  referenced_security_group_id = aws_security_group.this.id
  ip_protocol                  = "-1"
}

resource "aws_vpc_security_group_egress_rule" "inter_broker" {
  security_group_id            = aws_security_group.this.id
  description                  = "Inter-broker replication"
  referenced_security_group_id = aws_security_group.this.id
  ip_protocol                  = "-1"
}

resource "aws_cloudwatch_log_group" "broker" {
  name              = "/aws/msk/${var.name}/broker"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.log_kms_key_arn
  tags              = var.tags
}

resource "aws_msk_configuration" "this" {
  name           = "${var.name}-config"
  kafka_versions = [var.kafka_version]
  description    = "dabet ${var.name} broker defaults"

  # A server_properties change adds a revision in place and MSK rolls the
  # brokers; a kafka_versions change replaces the configuration, which is why
  # create_before_destroy is NOT set here — a same-named replacement would
  # collide with the existing configuration.
  server_properties = local.server_properties_text
}

resource "aws_msk_cluster" "this" {
  cluster_name           = var.name
  kafka_version          = var.kafka_version
  number_of_broker_nodes = var.broker_count
  enhanced_monitoring    = var.enhanced_monitoring

  broker_node_group_info {
    instance_type   = var.instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.this.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_storage_gb

        dynamic "provisioned_throughput" {
          for_each = var.provisioned_throughput_mibps == null ? [] : [var.provisioned_throughput_mibps]

          content {
            enabled           = true
            volume_throughput = provisioned_throughput.value
          }
        }
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.this.arn
    revision = aws_msk_configuration.this.latest_revision
  }

  client_authentication {
    unauthenticated = var.client_authentication == "unauthenticated"

    dynamic "sasl" {
      for_each = var.client_authentication == "iam" ? [1] : []

      content {
        iam = true
      }
    }
  }

  encryption_info {
    encryption_at_rest_kms_key_arn = var.kms_key_arn

    encryption_in_transit {
      client_broker = var.encryption_in_transit_client_broker
      in_cluster    = true
    }
  }

  dynamic "open_monitoring" {
    for_each = var.enable_open_monitoring ? [1] : []

    content {
      prometheus {
        jmx_exporter {
          enabled_in_broker = true
        }

        node_exporter {
          enabled_in_broker = true
        }
      }
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.broker.name
      }
    }
  }

  tags = merge(var.tags, { Name = var.name })

  lifecycle {
    # Application Auto Scaling grows the broker volumes, so after the first
    # expansion the API reports a size larger than broker_storage_gb and the
    # next plan proposes shrinking it back — which MSK refuses, failing the
    # apply. broker_storage_gb is therefore the size at CREATE time and the
    # floor; growth after that belongs to the autoscaling policy.
    #
    # To resize by hand, disable enable_storage_autoscaling, remove this
    # ignore_changes entry, and apply. Storage can only ever grow.
    ignore_changes = [
      broker_node_group_info[0].storage_info[0].ebs_storage_info[0].volume_size,
    ]

    precondition {
      condition     = var.broker_count % length(var.subnet_ids) == 0
      error_message = "MSK requires number_of_broker_nodes to be a whole multiple of the number of client subnets."
    }

    precondition {
      condition     = var.broker_count >= 3
      error_message = "§3 asks for 3+ brokers across AZs; min.insync.replicas=2 with RF=3 needs at least three."
    }

    precondition {
      condition     = var.client_authentication != "iam" || var.encryption_in_transit_client_broker == "TLS"
      error_message = "IAM authentication is SASL over TLS: encryption_in_transit_client_broker must be TLS."
    }
  }
}

# ---------------------------------------------------------------------------
# Storage autoscaling
#
# Separate from the cluster resource: MSK exposes broker storage as an
# Application Auto Scaling target rather than a cluster attribute.
# ---------------------------------------------------------------------------

resource "aws_appautoscaling_target" "storage" {
  count = var.enable_storage_autoscaling ? 1 : 0

  service_namespace  = "kafka"
  resource_id        = aws_msk_cluster.this.arn
  scalable_dimension = "kafka:broker-storage:VolumeSize"
  min_capacity       = 1
  max_capacity       = var.storage_autoscaling_max_gb
}

resource "aws_appautoscaling_policy" "storage" {
  count = var.enable_storage_autoscaling ? 1 : 0

  name               = "${var.name}-broker-storage"
  service_namespace  = aws_appautoscaling_target.storage[0].service_namespace
  resource_id        = aws_appautoscaling_target.storage[0].resource_id
  scalable_dimension = aws_appautoscaling_target.storage[0].scalable_dimension
  policy_type        = "TargetTrackingScaling"

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "KafkaBrokerStorageUtilization"
    }

    target_value = var.storage_autoscaling_target_percent
  }
}
