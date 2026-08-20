# `examples/prod` — the §3 target topology

> **Read the cost section before you apply.** This is a five-figure-a-month
> deployment, and the largest line grows every day it runs.

```sh
cp backend.hcl.example backend.hcl && $EDITOR backend.hcl
tofu init -backend-config=backend.hcl
tofu plan -var 'cluster_admin_principal_arns=["arn:aws:iam::<account>:role/Platform"]' \
          -var 'alarm_emails=["oncall@example.com"]'
tofu apply
```

## What a team edits

`main.tf` is the whole configuration. In practice the values that get changed
are:

| Knob | Why you would change it |
| --- | --- |
| `general_node_group.max_size` | The N6 baseline needs a few hundred moderation pods; the sizing note in the root README works through why. |
| `msk.broker_count` / `instance_type` | §4.2's 800 partitions at RF 3 is 2 400 replicas to place. |
| `enable_gpu_pool` | Off by default. Roughly $2,900/month at four `g6.2xlarge`. |
| `s3.lifecycle_transitions` | The single largest saving here, if you can bound §8.6's on-demand recluster window. |
| `rds_identity.instance_class` | §5.2's ~25 GB at 10M creators is the storage floor, not the compute one. |
| `create_policy_instance` | `false` collapses to one instance and keeps §6.3's foreign key. |

## Two things to know before applying

**Kafka authenticates with SCRAM, not IAM.** `pkg/kafkax` supports
`AWS_MSK_IAM`, but franz-go's implementation reads AWS credentials from the
environment and does no STS exchange, so an IRSA token cannot drive it —
choosing IAM would mean shipping a static IAM user key. SCRAM keeps the pod on
one Secrets Manager value it reads through its own role. The trade is where
authorisation lives: SCRAM authorises through Kafka ACLs, which this module
does not create, so one credential shared by nine services means every service
can reach every topic. The root README works through the choice.

**Nothing creates the §4.2 topics.** The deps chart's topics Job is gated on an
in-cluster Kafka. With MSK, run that Job's script against the bootstrap string
or create the topics by hand before the services start —
`auto.create.topics.enable` is `false`, so a missing topic is a clear error
rather than a silent one-partition topic.

## Alerting

`alert-rules.yaml` is installed into the Amazon Managed Prometheus workspace.
It carries the §4.5 signals no CloudWatch alarm can see — `fail_open_total`
above all, which §4.5 calls the single most important metric in the system —
plus rules for findings F2, F4 and F5 from `test/load/README.md`. The
infrastructure alarms (RDS storage and CPU, MSK offline partitions, ElastiCache
CPU) are CloudWatch and go to the SNS topic; each email subscription needs a
confirmation click that Terraform cannot perform.

## Cost

Around **$13,000–16,000/month in the first month** at the sizes in this file,
without the GPU pool. Add ~$2,900/month for four `g6.2xlarge`, and ~$8,000/month
if the general pool actually grows toward the N6 baseline.

The S3 line is the one that changes character over time: §8.4 puts the corpus at
~3.5 TB/day before compression, so month one is ~$1,700 and month twelve is
~$10,000–15,000/month even with the IA and GLACIER_IR ladder working. It never
plateaus, because §4.8 keeps embeddings indefinitely. The lever is the §8.4
sampling ceiling, which lives in the chart's values, not in this file.

Full breakdown, including the reasoning behind every size, is in
`../../README.md`.
