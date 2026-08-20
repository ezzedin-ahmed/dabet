# `deploy/terraform` — the optional AWS layer

Dabet runs on Docker Compose locally and on Kubernetes in the target (§3). **AWS
is optional.** The Helm charts in `deploy/k8s` must run on any cluster — kind,
k3s, GKE, a bare-metal cluster with an in-cluster Postgres — and nothing in this
directory is a prerequisite for them.

What this directory is for is the other case: a team that wants AWS-managed
services under those charts. It builds the §3 target column with EKS, RDS, MSK,
ElastiCache and S3, wires per-service IRSA roles so pods reach AWS without static
credentials, and emits a single `values-aws.yaml` document that the app chart
consumes.

> **Nothing here has been applied.** There are no AWS credentials in this
> environment and creating real infrastructure was out of scope. Every
> configuration below has been checked with `tofu fmt`, `tofu init
> -backend=false` and `tofu validate` against the real AWS provider schema — so
> resource names, argument names and types are verified — but no `plan` and no
> `apply` has ever run against an account. See [Unverified](#unverified) for the
> specific things that only an apply can settle.

---

## Layout

```
deploy/terraform/
├── README.md
├── versions.tf              root composition: required_version + providers
├── variables.tf             everything a caller can turn
├── locals.tf                tags, per-service secret grants
├── kms.tf                   four customer-managed keys, all rotating
├── main.tf                  module composition
├── values_contract.tf       the values-aws.yaml document, built once
├── outputs.tf               the seam with the Helm chart
├── bootstrap/               state bucket; run once, before anything else
├── modules/
│   ├── network/             VPC, three subnet tiers, NAT, VPC endpoints
│   ├── eks/                 cluster, OIDC provider, node groups, addons
│   ├── rds/                 one PostgreSQL instance (instantiated twice)
│   ├── msk/                 Kafka brokers, configuration, storage autoscaling
│   ├── elasticache/         Redis (cluster mode) + Memcached
│   ├── s3/                  embeddings corpus and supporting buckets
│   ├── iam/                 IRSA roles, one per service plus platform roles
│   ├── secrets/             Secrets Manager containers for §4.4
│   └── observability/       log groups, alarms, optional AMP workspace
└── examples/
    ├── dev/                 small, destroyable
    └── prod/                the §3 target at N5/N6 sizes
```

**The root is a composition module, not a root you run.** It has no `provider`
block and no `backend` block: a provider inside a called module cannot be
removed cleanly once state depends on it. `examples/dev` and `examples/prod` are
the real roots, and they own the provider, the region and the backend.

**Every module under `modules/` is independently usable.** They take plain
inputs — a VPC id, a list of subnet ids, a security group id to allow — and none
of them reach into another. A team that wants only MSK can call
`./modules/msk` from their own root and ignore the rest. The root module is the
convenience of having all of it wired together, not a requirement.

---

## What Terraform does not create

**Kafka topics.** §4.2's four topics, their partition counts and their
retentions are created by the charts, exactly as `kafka-init` does under
Compose. Keeping the topic table in one place matters more than owning it here,
and topic configuration is the kind of thing that changes with the application,
not with the infrastructure. What Terraform does own is a cluster that can carry
those numbers — see [Sizing](#sizing).

**ClickHouse and Milvus.** See the next section.

**Secret values.** The `secrets` module creates containers and never accepts a
value. See [Secrets](#secrets).

---

## ClickHouse and Milvus: the decision

AWS has no managed ClickHouse and no managed Milvus. There is no equivalent of
RDS or MSK for either, and there is no AWS service that can be swapped in — §8.5
needs vector search with per-creator partitions, §8.7 needs `SummingMergeTree`
and `ReplacingMergeTree` semantics, and neither OpenSearch nor Redshift is a
substitute for those without rewriting Area D.

**Both run in-cluster, from the charts.** Terraform provides the AWS substrate
they need and nothing more:

| What Terraform provides | Why |
| --- | --- |
| An S3 bucket for Milvus | Milvus keeps segments and indexes in object storage, not on the pod's disk. It cannot run without a bucket. |
| An S3 bucket for ClickHouse | Backups, and optionally S3-backed MergeTree disks for the cold end of §8.7's hourly buckets. |
| An IRSA role for each | So neither needs a static access key. Both roles are scoped to their own bucket. |
| The EBS CSI driver addon and a gp3-capable node pool | Both are StatefulSets with persistent volumes. |
| An optional tainted `stateful` node pool | So a moderation pod never lands on a node about to be drained for a StatefulSet rollout. |

The reasoning, in order of weight:

1. **AWS is optional and the charts are not.** If Terraform owned ClickHouse and
   Milvus, the two stores would only exist on AWS, and a team running the charts
   on any other cluster would have no Insights at all. Whatever runs them has to
   be in the charts, so putting a second implementation here forks the
   deployment story for no gain.
2. **The managed alternatives are third-party, not AWS.** ClickHouse Cloud and
   Zilliz Cloud (managed Milvus) both run on AWS and both offer PrivateLink.
   They are credible — they remove real operational work, and for a small team
   they may well be the right call — but they are provisioned through their own
   providers or consoles, not through the AWS provider, and they put the two
   stores holding all of Insights outside the account's own IAM and KMS
   boundary. That is a decision for the team adopting them, not a default.
3. **§8.5 already constrains the Milvus deployment shape.** One collection
   partitioned by `creator_id`, centroids only, never per-message vectors. That
   is a chart-level configuration, and it is where it belongs.

**If you want the managed option instead:** point the charts at it. Set
`s3.create_milvus_bucket = false` and `s3.create_clickhouse_backup_bucket =
false`, disable the in-cluster StatefulSets in the chart values, and set
`MILVUS_ADDR` and `CLICKHOUSE_DSN` to the provider's PrivateLink endpoints. The
`extra_irsa_roles` variable is there for any additional AWS-side identity the
integration needs. Terraform does not model those endpoints because it cannot
create them.

---

## Sizing

Every number in `examples/prod` traces back to a section of
`docs/implementation-reference.md` or a measurement in `test/load/README.md`.

### The general node pool, and a finding that outruns it

`test/load/README.md` finding **F3** is the one to read before sizing anything:

> `moderation-service` is single-threaded per instance ... the observed ceiling
> is ~170–200 msg/s per instance. Extrapolating, N6's 50 000 msg/s baseline
> needs ~250–300 instances and the 500 000 msg/s peak ~2 500–3 000 — above
> §4.2's 512-partition ceiling, which caps the consumer group at 512 members.

Two things follow, and Terraform can only help with one of them.

**The baseline is reachable but expensive.** ~250–300 moderation pods at roughly
one vCPU each is ~300 vCPU, or ~40 `m7i.2xlarge` nodes for that service alone,
before the other eight services. `examples/prod` therefore sets
`desired_size = 12` and `max_size = 60`: the desired size is a starting point
for a system that is not yet at N6 volume, and the maximum is what has to exist
so the autoscaler can get there.

**The peak is not reachable as specified, and no Terraform change fixes it.** At
500 000 msg/s the same extrapolation needs 2 500–3 000 consumers against a
512-partition topic that can seat at most 512. Either the consumer becomes
concurrent (F3 identifies the four sequential Redis round trips as the binding
constraint, and F2 the missing circuit breaker) or `messages.v1` grows past 512
partitions — which is a §4.2 change and would re-key the topic. This is recorded
here because a reader sizing a node pool from N6 deserves to know the pool is
not the limit.

The measurements were taken on an 8-core laptop running the whole stack, so the
absolute number is not portable; the *per-instance* figure is, which is why it
is the one used.

### RDS

`allocated_storage_gb = 200` on the identity instance comes from **§5.2**:

> A creator row is ~200 B; a connection with tokens is ~1.5 KB. 10M creators
> averaging 1.5 connections ≈ 2 GB + 22 GB ≈ **~25 GB**.

That is the logical heap. The volume has to hold indexes (`connections` alone
carries a partial unique index and a partial creator index), WAL, autovacuum
headroom and bloat, so 200 GiB with autoscaling to 1 000 is the starting point,
not 32.

The identity instance carries three schemas, not one: `identity` (§5.2),
`billing` (§5.3) and `review` (§7.6). All three have foreign keys to
`creators(id)` and cannot be separated.

**A consequence of §3 worth stating plainly.** §6.3 declares
`policies.creator_id UUID NOT NULL REFERENCES creators(id) ON DELETE CASCADE`,
and §3 asks for separate clusters for identity and policy. A foreign key cannot
span two RDS instances. Splitting them means that constraint stops being
enforced by the database and becomes an application invariant — including the
`ON DELETE CASCADE`, which no longer happens. `create_policy_instance = false`
collapses the two onto one instance and keeps the constraint; `examples/dev`
does exactly that.

`db.m7g.large` for the policy instance is small on purpose. §6.8 puts an
in-process LRU in front of a Memcached layer and caches negative results at both
— Postgres sees a policy read only on a cold cache, and §6.7 is explicit that
not caching the negative result is "the single easiest way to destroy this
system".

### MSK, and what 512 partitions means

§4.2's partition counts add up to 512 + 128 + 128 + 32 = **800 partitions**, and
at replication factor 3 that is **2 400 partition replicas** the brokers have to
carry, plus internal topics and consumer-group state.

AWS publishes a per-broker partition guideline that scales with instance size —
roughly 1 000 partitions on a `kafka.m5.large`, roughly 4 000 on a
`kafka.m5.4xlarge`. At the §3 floor of three brokers, 2 400 replicas is 800 per
broker, which fits the small sizes on paper but leaves nothing spare while a
broker is being replaced: the survivors take the load of the missing one, and a
replacement re-replicates 800 partitions' worth of log. `examples/prod`
therefore uses **six** brokers, so each carries ~400 replicas and a broker loss
is a 20% increase on the rest rather than a 50% one.

Throughput sets the instance type rather than the broker count. At the N6
baseline of 50 000 msg/s and ~300 B per §4.2 record that is ~15 MB/s of ingress;
replication triples it and the two consumer groups on `messages.v1`
(`moderation-service` and `insights-service`) double the egress. The 500 000
msg/s peak is ten times that. What actually saturates first is CPU — 512
partitions of leader election, index maintenance and zstd batches — which is why
the example uses a `2xlarge` rather than a `large`.

Storage per broker comes from retention, not from rate:

| Topic | Retention | Sustained volume | ×3 replication |
| --- | --- | --- | --- |
| `messages.v1` | 24 h | 50K/s × ~300 B ≈ 1.3 TB/day | ~3.9 TB, less zstd |
| `flagged.v1` | 7 d | a low single-digit % of the above | a few hundred GB |
| `deletions.v1` | 24 h | no text (§4.2), small | small |
| `usage.v1` | 7 d | aggregated per creator-minute (§5.3, ~800/s) | small |

Call it ~2.4 TB across the cluster at the sustained rate with zstd. Over six
brokers that is ~400 GiB each; the example provisions 1 000 GiB, because §4.7
says to *accept* consumer lag, and a consumer that is hours behind keeps
segments alive that would otherwise have aged out. Storage autoscaling is on and
never shrinks, so the ceiling is a budget decision.

**One MSK setting worth knowing about.** The module sets
`replica.selector.class=RackAwareReplicaSelector`. Cross-AZ traffic between
brokers and clients is billed, and at ~45 MB/s sustained that is on the order of
100 TB/month. Rack-aware replica selection lets a consumer fetch from a replica
in its own AZ instead of always from the leader — but only if the client sets
its rack (franz-go: `kgo.Rack`). Without the client change the property is inert
and harmless; with it, it removes most of that line.

### Redis

§4.3's entire keyspace has a TTL of five minutes or less. The working set is
five minutes of active senders, not a growing corpus, so **memory is rarely the
constraint — request rate is**. F3 measures four sequential Redis round trips per
message on the moderation hot path (`seen`, `rate`, `dup`, `samp`), so 50 000
msg/s is ~200 000 operations/second. Three shards of `cache.r7g.large` with one
replica each is a starting point for that, and shards are the lever because
Redis is single-threaded per shard.

Snapshots are **off** (`redis_snapshot_retention_days = 0`). There is nothing in
that keyspace whose loss is a data-loss event — §4.7 already classifies a cold
Redis as fail-open — and a snapshot fork on a busy shard is a latency event for
no benefit.

### S3, and the number that dominates everything

§8.4:

> 384 floats at fp16 is 768 B, ~800 B with metadata. At a sustained sampled rate
> of 50K/s that is ~40 MB/s, ~3.5 TB/day of parquet before compression. **This
> is the dominant storage cost in the system** and the sampling ceiling is the
> lever that controls it.

Three things in `modules/s3` follow from that sentence, and one thing does not.

**Versioning is off, deliberately.** §8.4's objects are written once under a
`creator_id/date` key and never rewritten. There is no overwrite for versioning
to protect against, so turning it on would keep a second copy of the largest
data set in the system for a scenario that does not occur. The protection that
does apply is `force_destroy = false` and a bucket policy denying
`s3:DeleteObject` to every principal (lifecycle expiration is an S3 service
action and is unaffected). A team that wants versioning anyway can set
`embeddings_versioning_enabled = true` — with eyes open about doubling the
largest line on the bill.

**SSE-KMS with a bucket key.** Without `bucket_key_enabled`, every object PUT
and GET is a billed KMS request, and this bucket takes tens of thousands of PUTs
a day forever. With it, S3 caches a data key per bucket and the KMS bill stops
tracking the object count.

**The lifecycle ladder stops at `GLACIER_IR`.** STANDARD → STANDARD_IA at 30
days → GLACIER_IR at 180. There is no DEEP_ARCHIVE transition, and that is a
§8.6 decision rather than a cost one: on-demand reclustering reads a creator's
parquet for an arbitrary historical window ("recluster last month"), and
DEEP_ARCHIVE retrieval is measured in hours. GLACIER_IR keeps millisecond reads
at roughly a sixth of Standard's price, so a recluster still completes — it just
pays a retrieval fee. **If you bound on-demand reclustering to a fixed window, a
DEEP_ARCHIVE transition beyond that window is the largest single saving
available in this whole directory.**

**And the thing Terraform cannot do.** No lifecycle rule, storage class or
compression setting makes an unsampled firehose affordable. The lever §8.4 names
is the *sampling ceiling*, and that is an environment variable in the chart's
values, not a Terraform resource. Tune it before launch, not after.

One structural saving that is in Terraform: the **S3 gateway VPC endpoint**,
created unconditionally by `modules/network` and attached to both the private
and data route tables. It is free. Without it, 3.5 TB/day of parquet writes
would be processed by a NAT gateway at roughly $0.045/GB — a six-figure annual
charge for traffic that never needed to leave the VPC.

### The GPU pool

Off by default, because it is the most expensive thing here and the platform
runs end to end against `tools/mockllm` and `tools/mockembed` without one.

When you do turn it on, §7.5's arithmetic is what sets the size. The sampler
bounds LLM load by the number of *active contents*, not by message volume; the
harness measured 1.68% of messages reaching the LLM in the sampler scenario. At
the N6 baseline that is ~840 messages/second admitted, and
`test/load/README.md` gives the conversion:

```
llm_batches_per_s  ≈ admitted_msg_per_s / observed_mean_batch_size
concurrent_gens    ≈ llm_batches_per_s × observed_llm_p50_seconds
```

With the measured mean batch size of 1–4 (finding **F5**: the 32-message trigger
never fires) and a ~900 ms p50, that is on the order of 200 concurrent
generations of a ~7–8B quantised model — single-digit L4-class GPUs, which is
why the example defaults to four `g6.2xlarge`. Fix F5 first: raising
`MOD_LLM_LINGER` so batches actually fill is a direct divisor on that number and
costs latency budget rather than money.

The pool carries two taints — `nvidia.com/gpu` and `dabet.io/workload=vllm` — and
matching labels. The second exists because a third-party chart that blanket-
tolerates `nvidia.com/gpu` should still not be able to take a GPU you are paying
for. The AMI ships the NVIDIA driver but not the device plugin, so
`nvidia.com/gpu` is not an allocatable resource until the chart installs it.

---

## The outputs contract

This is the seam between this directory and `deploy/k8s`. It is generated rather
than transcribed (`values_contract.tf`), so the two cannot drift:

```sh
tofu -chdir=examples/prod output -raw helm_values_aws_yaml > ../../k8s/values-aws.yaml
```

**Rule for anything in it: endpoints, names, ARNs and booleans. Never a secret
value.** The charts resolve secrets through External Secrets Operator or the
Secrets Store CSI driver using the ARNs and the IRSA roles below.

```yaml
global:
  aws:
    region: eu-west-1
    clusterName: dabet-prod
  namespace: dabet

  kafka:
    brokers: "b-1.dabet-prod...:9098,b-2...:9098,b-3...:9098"   # KAFKA_BROKERS
    auth: iam            # "iam" | "unauthenticated"
    tls: true

  postgres:
    identity:            # identity + billing + review schemas
      host: dabet-prod-identity.<id>.eu-west-1.rds.amazonaws.com
      port: 5432
      database: dabet
      username: dabet
      sslmode: require   # rds.force_ssl=1 is set; Compose's sslmode=disable is refused
      secretArn: arn:aws:secretsmanager:...:secret:rds!db-...
    policy:              # same shape; equals identity when create_policy_instance=false
      host: ...
      port: 5432
      database: dabet
      username: dabet
      sslmode: require
      secretArn: arn:aws:secretsmanager:...

  redis:
    addr: "clustercfg.dabet-prod-redis...:6379"   # REDIS_ADDR
    clusterMode: true    # requires redis.NewClusterClient
    tls: true            # requires a non-nil TLSConfig

  memcached:
    addrs:               # MEMCACHED_ADDRS — node addresses, NOT the config endpoint
      - "dabet-prod-memcached-0001...:11211"
      - "dabet-prod-memcached-0002...:11211"
      - "dabet-prod-memcached-0003...:11211"

  s3:
    region: eu-west-1
    endpoint: ""         # S3_ENDPOINT; empty means real S3. Compose sets MinIO here.
    bucket: dabet-prod-embeddings-<account>          # S3_BUCKET
    milvusBucket: dabet-prod-milvus-<account>
    clickhouseBucket: dabet-prod-clickhouse-<account>

  secrets:               # §4.4, all ARNs
    postgres/identity:      arn:aws:secretsmanager:...   # RDS-managed {username,password}
    postgres/policy:        arn:aws:secretsmanager:...   # RDS-managed {username,password}
    stripe/secret-key:      arn:aws:secretsmanager:...
    stripe/webhook-secret:  arn:aws:secretsmanager:...
    oauth/youtube:          arn:aws:secretsmanager:...   # {client_id, client_secret}
    oauth/twitch:           arn:aws:secretsmanager:...
    oauth/discord:          arn:aws:secretsmanager:...   # {client_id, client_secret, bot_token}
    jwt/private-key:        arn:aws:secretsmanager:...   # RS256 PEM, user-service only
    jwt/public-key:         arn:aws:secretsmanager:...   # RS256 PEM, every validator
    clickhouse/password:    arn:aws:secretsmanager:...

  observability:
    applicationLogGroup: /aws/eks/dabet-prod/application
    prometheusRemoteWrite: https://aps-workspaces...amazonaws.com/.../api/v1/remote_write
    alarmTopicArn: arn:aws:sns:...

serviceAccounts:         # one entry per service, all nine
  user-service:
    name: user-service
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::<account>:role/dabet-prod-user-service
  credits-service: { ... }
  policy-service: { ... }
  provider-adapter: { ... }
  moderation-service: { ... }
  review-service: { ... }
  insights-service: { ... }
  clustering-service: { ... }
  clusters-job: { ... }

nodePools:
  gpu:
    enabled: false
    nodeSelector: { "dabet.io/workload": "vllm" }
    tolerations:
      - { key: nvidia.com/gpu,    operator: Equal, value: present, effect: NoSchedule }
      - { key: dabet.io/workload, operator: Equal, value: vllm,    effect: NoSchedule }
  stateful:
    enabled: true
    nodeSelector: { "dabet.io/workload": "stateful" }
    tolerations:
      - { key: dabet.io/workload, operator: Equal, value: stateful, effect: NoSchedule }

externalSecrets:
  serviceAccountRoleArn: arn:aws:iam::<account>:role/dabet-prod-external-secrets
  namespace: external-secrets
  serviceAccountName: external-secrets

milvus:
  bucket: dabet-prod-milvus-<account>
  serviceAccountRoleArn: arn:aws:iam::<account>:role/dabet-prod-milvus

clickhouse:
  bucket: dabet-prod-clickhouse-<account>
  serviceAccountRoleArn: arn:aws:iam::<account>:role/dabet-prod-clickhouse
  passwordSecretArn: arn:aws:secretsmanager:...
```

### What the chart has to do with it

**ServiceAccount names must match.** The IRSA trust policy pins
`system:serviceaccount:<namespace>:<serviceaccount>`, so the chart's
ServiceAccount name has to equal `serviceAccounts.<service>.name`. The default
is the bare service name in namespace `dabet`; if the chart prefixes with the
Helm release, override `service_account_names` and `kubernetes_namespace` to
match. A mismatch does not fail at apply — it fails at runtime, as an
`AssumeRoleWithWebIdentity` denial in the pod's logs.

**`POSTGRES_DSN` is composed, not supplied.** §4.4 wants one `POSTGRES_DSN`
environment variable, but the password lives in Secrets Manager. The chart
builds it from `host`, `port`, `database`, `username` and `sslmode` above plus
the password key from the secret. `sslmode=require` is not optional: the
parameter group sets `rds.force_ssl = 1`, and Compose's `sslmode=disable` is
refused by the server.

**`MEMCACHED_ADDRS` takes the node list, not the configuration endpoint.**
ElastiCache's auto-discovery endpoint is a protocol extension the client has to
speak, and `policy-service` uses `gomemcache`, which does not. Pointing it at
the discovery endpoint would silently hash every key onto one "node". The
trade-off is that adding a node changes the list and needs a re-render and a
restart — survivable, because §6.8 reads through to Postgres on a miss.

---

## Security

**No public ingress on any data service.** RDS, MSK and ElastiCache live in a
third subnet tier whose route tables carry no default route at all — not "a
security group that happens to be closed", but no path to or from the internet
in either direction. Their security groups take ingress only from *referenced
security group ids*, never from CIDRs; there is no CIDR ingress variable in
those modules, so there is no configuration of them that opens one. In practice
the only referenced group is the EKS-managed cluster security group, so the data
tier is reachable from cluster pods and from nothing else in the VPC.

**The Kubernetes API endpoint is private by default.** Turning
`cluster_endpoint_public_access` on without `cluster_public_access_cidrs` is
refused by a resource precondition rather than silently becoming `0.0.0.0/0`,
and `0.0.0.0/0` in that list is refused explicitly.

**IRSA, with both trust conditions.** Every role pins

```
<issuer>:sub  = system:serviceaccount:<namespace>:<serviceaccount>
<issuer>:aud  = sts.amazonaws.com
```

The `aud` condition is the one routinely omitted; without it the trust policy
accepts any token the cluster's issuer signs.

**No `*` actions and no `*` resources**, with two deliberate exceptions, both
commented in place:

- The KMS key policies open with the account-root statement (`kms:*` on `*` for
  `arn:aws:iam::<account>:root`). This is the documented default key policy;
  AWS refuses to create a key that IAM cannot administer.
- `secretsmanager:ListSecrets` on `*` in the External Secrets Operator role.
  That action has no resource-level scoping in Secrets Manager. It discloses
  secret names and tags, never values; `GetSecretValue` is scoped to the
  deployment's own ARN prefix.

Kafka permissions are scoped to topic and group ARNs derived from the cluster
ARN, per service, from §4.2's table — `moderation-service` can read
`messages.v1` and write `flagged.v1`, `deletions.v1` and `usage.v1`, and nothing
else. `provider-adapter` additionally gets `CreateTopic` on exactly
`adapter.shards.v1`, because §7.2's coordinator creates that topic itself and
the brokers run with `auto.create.topics.enable=false`.

`moderation-service` has a role with **no permissions at all** — it has no
database, no bucket and no third-party credential (§7.3 reaches policy over gRPC
and credits over HTTP). It still gets an identity, so granting it something
later is a one-line change.

**Encryption.** At rest: RDS storage, MSK broker volumes, ElastiCache, S3
objects, EKS Kubernetes secrets, node EBS volumes and CloudWatch log groups, all
with customer-managed keys. In transit: `rds.force_ssl=1`, MSK
`client_broker = TLS` with `in_cluster = true`, ElastiCache transit encryption,
a bucket policy denying `aws:SecureTransport = false` on every bucket, and a
second bucket policy statement refusing an upload encrypted with the wrong key.

**Four KMS keys**, split by blast radius (`data`, `secrets`, `eks`, `logs`), all
with `enable_key_rotation` and a 30-day deletion window. The `eks` key carries
an extra statement for the Auto Scaling service-linked role; without it a
managed node group with an encrypted root volume fails to launch with a bare
`Client.InternalError` that says nothing about KMS.

**IMDSv2 is required** on every node. The hop limit is left at the EKS default
of 2 (`node_metadata_hop_limit`); 1 forces every pod onto IRSA and is stricter,
but breaks any addon not yet migrated, so it is exposed rather than imposed.

---

## Secrets

`modules/secrets` creates containers and **never accepts a value as an input
variable**. Terraform state is not a secret store: anything passed in would sit
in plaintext in the state file, in every backup of it, and in `tofu show`.

So Terraform creates the secret, the KMS key and the ARN, and a human or a
pipeline puts the value in:

```sh
aws secretsmanager put-secret-value \
  --secret-id dabet/prod/jwt/private-key \
  --secret-string "$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048)"
```

A placeholder version (`REPLACE_ME`) is created so that External Secrets
Operator does not error on a version-less secret, and `ignore_changes` on
`secret_string` means the real value does not show up as drift.

**The Postgres passwords are the exception, and the better pattern.**
`manage_master_user_password = true` has RDS generate the master credential
directly into its own Secrets Manager secret and rotate it on AWS's schedule.
Terraform never sees the password; the ARN is an output. This is why there is no
`postgres/*` entry in the `secrets` module and why the two `postgres/*` ARNs in
the contract come from the `rds` module.

**Both consumption paths are wired.** External Secrets Operator gets a role that
can read the deployment's whole prefix; each service *also* gets read access to
exactly its own secrets, which is what the Secrets Store CSI driver needs
(there, the pod's own identity fetches the secret). Granting both costs nothing
and lets a team pick either without a Terraform change.

Who reads what:

| Secret | Read by |
| --- | --- |
| `postgres/identity` | user-service, credits-service, provider-adapter, review-service |
| `postgres/policy` | policy-service |
| `stripe/*` | credits-service |
| `oauth/*` | user-service (§5.5 authorisation flow), provider-adapter (§5.6 lazy refresh) |
| `jwt/private-key` | user-service only — it is the only issuer |
| `jwt/public-key` | every service that validates a bearer token |
| `clickhouse/password` | clustering-service, clusters-job |

---

## State backend

**S3 with native locking, no DynamoDB table.**

```hcl
backend "s3" {
  key          = "dabet/prod/terraform.tfstate"
  encrypt      = true
  use_lockfile = true
}
```

`use_lockfile` writes a `.tflock` object next to the state and relies on S3's
conditional writes. It has been available since OpenTofu 1.10 and Terraform
1.10, and it removes a whole second resource, its IAM and its bill for what was
only ever a workaround for S3 not having conditional writes — which it now does.
The DynamoDB table is still available in `bootstrap/` behind
`create_dynamodb_lock_table` for anyone pinned to an older CLI, with a warning
that the two mechanisms do not interlock: a run using one will not see a lock
held by the other.

The bucket and region come from a `backend.hcl` you do not commit; see
`examples/*/backend.hcl.example`. `bootstrap/` creates the bucket, with
versioning **on** — the opposite of the embeddings bucket, and for the opposite
reason: a state file is overwritten on every apply, and a truncated write is
exactly what a previous version recovers from.

---

## Cost

Rough order of magnitude, `eu-west-1`, on-demand, no savings plans, at the sizes
in `examples/prod`. Treat these as "which line items dominate", not as a quote.

| Line | Monthly, month one |
| --- | ---: |
| EKS control plane | ~$75 |
| General pool, 12 × m7i.2xlarge | ~$3,600 |
| Stateful pool, 3 × r7i.2xlarge (ClickHouse + Milvus) | ~$1,150 |
| MSK, 6 × kafka.m7g.2xlarge + 6 TB storage | ~$2,650 |
| ElastiCache Redis, 3 shards × 2 nodes r7g.large | ~$990 |
| ElastiCache Memcached, 3 × m7g.large | ~$340 |
| RDS identity, db.m7g.xlarge Multi-AZ + 200 GB | ~$500 |
| RDS policy, db.m7g.large Multi-AZ + 100 GB | ~$250 |
| NAT gateways ×3 + processing | ~$200 |
| Interface VPC endpoints (6 services × 3 AZ) | ~$130 |
| Cross-AZ traffic to MSK, before rack-aware fetching | ~$1,000–1,500 |
| CloudWatch logs, metrics, alarms | ~$300–800 |
| S3 — **month one** | ~$1,700 |
| KMS, Secrets Manager, ALB | ~$50 |
| **Total, month one** | **~$13,000–16,000** |

Then:

- **Add ~$2,900/month** for the GPU pool at four `g6.2xlarge`.
- **Add ~$8,000/month** if the general pool actually scales toward the ~40 nodes
  the N6 baseline implies (see [Sizing](#sizing)).
- **S3 grows and never stops.** At §8.4's sustained rate the corpus adds ~75
  TB/month. Month one is ~$1,700; month twelve is ~900 TB accumulated, which is
  ~$10,000–15,000/month even with the IA and GLACIER_IR ladder doing its work.
  By year three the corpus is measured in petabytes.

So: **low tens of thousands of dollars a month at the start, dominated by MSK
and the node pools; within a year, dominated by S3.** The sampling ceiling in
§8.4 is the only lever with the right order of magnitude, and it lives in the
chart's values, not here.

`examples/dev` comes to roughly **$500–700/month**: a single-AZ Postgres, three
`kafka.t3.small` brokers, one NAT gateway, no interface endpoints, no GPU. That
is still not free, which is worth saying out loud — there is no configuration of
this topology that costs nothing while running.

---

## Using it

```sh
# 0. Install OpenTofu (or Terraform >= 1.10 for use_lockfile).

# 1. Once per account: the state bucket.
cd deploy/terraform/bootstrap
tofu init
tofu apply -var region=eu-west-1 -var state_bucket_name=dabet-tfstate-<account-id>

# 2. Pick an environment and point it at that bucket.
cd ../examples/prod
cp backend.hcl.example backend.hcl && $EDITOR backend.hcl
tofu init -backend-config=backend.hcl

# 3. Set your own admin role, then review the plan carefully.
tofu plan -var 'cluster_admin_principal_arns=["arn:aws:iam::<account>:role/YourRole"]'
tofu apply

# 4. Populate the secret placeholders.
aws secretsmanager put-secret-value --secret-id dabet/prod/stripe/secret-key --secret-string ...

# 5. Hand the contract to the charts.
tofu output -raw helm_values_aws_yaml > ../../k8s/values-aws.yaml
aws eks update-kubeconfig --region eu-west-1 --name dabet-prod
```

### Suggested Makefile targets

This directory deliberately does not edit the `Makefile`. If the parent wants
wiring:

```make
TF      := tofu
TF_ENV  ?= dev
TF_DIR  := deploy/terraform/examples/$(TF_ENV)

tf-init:
	$(TF) -chdir=$(TF_DIR) init -backend-config=backend.hcl

tf-plan:
	$(TF) -chdir=$(TF_DIR) plan

tf-apply:
	$(TF) -chdir=$(TF_DIR) apply

# Regenerate the Helm values contract from live outputs.
tf-values:
	$(TF) -chdir=$(TF_DIR) output -raw helm_values_aws_yaml > deploy/k8s/values-aws.yaml

# What CI should run: no credentials needed, no state touched.
tf-check:
	$(TF) -chdir=deploy/terraform fmt -check -recursive
	@for d in deploy/terraform deploy/terraform/bootstrap \
	          deploy/terraform/examples/dev deploy/terraform/examples/prod \
	          deploy/terraform/modules/*; do \
	  echo "== $$d"; \
	  $(TF) -chdir=$$d init -backend=false -input=false > /dev/null && \
	  $(TF) -chdir=$$d validate || exit 1; \
	done
```

---

## Application changes this topology needs

Three things in the §3 target column cannot be reached from Terraform alone.
None is a defect in the application — the code was written against the Compose
profile, which §3 says it must run on unchanged — but all three are blocking,
and `examples/dev` deliberately configures around them so a first bring-up
works.

1. **Redis Cluster.** §3 targets Redis Cluster and §4.3's hash tags are already
   in the keyspace for exactly this. `moderation-service` builds
   `redis.NewClient` — a single-node go-redis client, which does not follow the
   `MOVED` redirections a cluster-mode configuration endpoint returns. Needs
   `redis.NewClusterClient`. TLS needs a `TLSConfig` on the same options struct.
   *Symptom if ignored:* every Redis call fails, and per §4.7 the pipeline fails
   open — so it does not crash, it just stops moderating, with
   `fail_open_total{component="redis"}` pinned at the message rate. F2 says to
   expect a throughput collapse as well.

2. **MSK IAM authentication.** `pkg/kafkax` builds a plain franz-go client with
   no SASL mechanism and no TLS dialer. SASL/IAM needs
   `kgo.SASL(aws.ManagedStreamingIAM(...))` from the AWS MSK IAM signer plus
   `kgo.DialTLSConfig`. Until then, set `client_authentication =
   "unauthenticated"` and `encryption_in_transit_client_broker =
   "TLS_PLAINTEXT"`; both ports are then open, so the switch afterwards is a
   client change, not a cluster rebuild. *Related:* `kgo.Rack` is what makes the
   rack-aware replica selection above actually save money.

3. **Memcached TLS.** `policy-service` uses `gomemcache`, which dials plain TCP.
   `memcached_transit_encryption_enabled` is off by default for that reason.
   §6.8 makes an unreachable Memcached a read-through to Postgres, so it
   degrades rather than breaks — but it degrades the hot path's cache hit rate
   to zero.

None of these is in scope for this directory (`pkg/` and `services/` belong to
other work), so they are recorded rather than fixed.

---

## Validation

```sh
tofu fmt -check -recursive deploy/terraform
tofu -chdir=deploy/terraform             init -backend=false && tofu -chdir=deploy/terraform             validate
tofu -chdir=deploy/terraform/bootstrap   init -backend=false && tofu -chdir=deploy/terraform/bootstrap   validate
tofu -chdir=deploy/terraform/examples/dev  init -backend=false && tofu -chdir=deploy/terraform/examples/dev  validate
tofu -chdir=deploy/terraform/examples/prod init -backend=false && tofu -chdir=deploy/terraform/examples/prod validate
# and each of deploy/terraform/modules/*
```

All pass against AWS provider 6.61.0 and OpenTofu 1.10.6.

**No `plan` and no `apply` has been run against an AWS account**, because there
are no credentials in this environment and creating real infrastructure was out
of scope. `validate` checks configuration against the provider's schema — every
resource type, argument name and type in this directory is real and correctly
shaped — but it does not talk to AWS, so it cannot check that a value AWS
accepts in the abstract is one AWS accepts *here*.

One step further was possible without credentials, and was taken. Creating a
resource needs no API call at plan time, so the `iam`, `rds`, `msk` and
`elasticache` modules were planned offline against dummy inputs, and the
resulting plan inspected as JSON. That confirms the parts that are computed
rather than declared:

- the IRSA policy documents render as valid IAM, with topic and group ARNs
  correctly derived from the MSK cluster ARN and scoped per §4.2 — including
  `WriteDataIdempotently` on the cluster for every producer, and `CreateTopic`
  on `adapter.shards.v1` and nothing else for `provider-adapter`;
- `rds.force_ssl` survives the map-key remapping and `shared_preload_libraries`
  is marked `pending-reboot` rather than `immediate`;
- the MSK `server_properties` document renders as expected;
- every security group ingress rule carries a `referenced_security_group_id`
  and no `cidr_ipv4` at all.

The modules with an `aws_caller_identity` data source (`s3`, `secrets`,
`observability`) and the root, which has one in `kms.tf`, cannot be planned this
way — that data source is a real STS call.

<a id="unverified"></a>
### Unverified, and what would settle it

Ordered by how likely each is to bite on a first apply.

| Item | Concern | How to settle it |
| --- | --- | --- |
| **Instance types and engine versions** | `kafka.m7g.2xlarge`, `g6.2xlarge`, `cache.r7g.large`, `db.m7g.xlarge`, Kafka `3.9.x`, Redis `7.1`, Memcached `1.6.22`, Kubernetes `1.33` are all plausible and current at time of writing, but availability varies by region and AWS retires versions. | `aws ec2 describe-instance-type-offerings`, `aws kafka list-kafka-versions`, `aws elasticache describe-cache-engine-versions`, `aws eks describe-addon-versions`. A wrong value fails at apply with a clear message. |
| **`cluster-enabled = yes` in a custom Redis parameter group** | The module creates a `redis7`-family group and sets `cluster-enabled` rather than using the stock `default.redis7.cluster.on`, so that `maxmemory-policy` can also be set. This is a widely used pattern but I have not applied it. | If it is rejected, set `redis_parameter_group_family` aside and point the replication group at `default.redis7.cluster.on`, losing the eviction-policy setting. |
| **`AL2023_x86_64_NVIDIA` AMI type** | The correct AL2023 GPU AMI type for the `g6` family as far as I can tell, but AMI type names have churned. | `aws eks describe-addon-versions` is no help here; check the EKS optimised-AMI documentation, or set `gpu_node_group.ami_type` explicitly. |
| **MSK provisioned EBS throughput** | Left `null`. AWS documents it as available only from the larger instance sizes, with a MiBps range that depends on the type. I did not want to guess a pair that fails at apply. | Check the current matrix and set `provisioned_throughput_mibps` if the baseline is not enough. |
| **The AMP log resource policy** | `aws_prometheus_workspace.logging_configuration` needs a CloudWatch Logs *resource* policy granting `aps.amazonaws.com`, which the module creates. The condition keys and the source-ARN wildcard are from the documented pattern, not from an apply. | If AMP logs nothing, check the resource policy first; the workspace itself will have been created either way. |
| **Pre-creating the RDS and EKS CloudWatch log groups** | Both services create their own log group on first write, with no retention. Creating it first so the service adopts it is the standard workaround, but adoption is not contractual. | Confirm after the first apply that `/aws/rds/instance/<id>/postgresql` has the configured retention. |
| **`most_recent` addon resolution** | Addon versions are not pinned, so the first apply takes the newest compatible release. That is the safer default against a stale pin, but it is not reproducible across time. | Pin via `addons.addon_version_overrides` once you have a known-good set. |
| **Ordering on first apply** | The dependencies I could not express implicitly are spelled out with `depends_on` — node groups on the role's policy attachments, addons on the node group, the flow log on its role policy, bucket policies on the public access block, the log-delivery policy before `aws_s3_bucket_logging`. There may be an ordering AWS enforces that I have not anticipated. | The first `apply` will say so. Re-running is safe for all of them. |
| **Anything about cost** | Every figure above is derived from list prices and the spec's own volume numbers. None came from a bill. | AWS Pricing Calculator, then Cost Explorer once it is running. |
