# `deploy/terraform` — the optional AWS layer

Dabet runs on Docker Compose locally and on Kubernetes in the target (§3). **AWS
is optional.** The Helm charts in `deploy/k8s` must run on any cluster — kind,
k3s, GKE, a bare-metal cluster with an in-cluster Postgres — and nothing in this
directory is a prerequisite for them.

What this directory is for is the other case: a team that wants AWS-managed
services under those charts. It builds the §3 target column with EKS, RDS, MSK,
ElastiCache and S3, wires per-service IRSA roles so pods reach AWS without static
credentials, and emits values documents for both charts in `deploy/k8s` in
their own key paths, so no one has to transcribe an endpoint by hand.

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
├── values_contract.tf       the two chart values documents, built once
├── outputs.tf               the seam with the Helm chart
├── bootstrap/               state bucket; run once, before anything else
├── modules/
│   ├── network/             VPC, three subnet tiers, NAT, VPC endpoints
│   ├── eks/                 cluster, OIDC provider, node groups, addons
│   ├── rds/                 one PostgreSQL instance (instantiated twice)
│   ├── msk/                 Kafka brokers, configuration, storage autoscaling,
│   │                        one SCRAM credential per principal
│   ├── kafka-acls/          the §1.5 authorisation matrix. CREATES NOTHING —
│   │                        it renders one table into the chart's ACL rules,
│   │                        the IAM statements and the SCRAM user list
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
retentions belong next to the code that depends on them — `kafka-init` does it
under Compose, and `charts/dabet-deps` has a reconciling Job that carries the
same registry. Topic configuration changes with the application, not with the
infrastructure. What Terraform owns is a cluster that can carry those numbers;
see [Sizing](#sizing).

> An earlier revision of this file recorded a gap here: the deps chart's topics
> Job was gated on `kafka.enabled`, which is `false` with MSK, so **nothing
> created the topics** and a fresh deployment failed every produce with
> `UNKNOWN_TOPIC_OR_PARTITION`. That is fixed. The Job now needs a broker
> *address*, not a broker StatefulSet, and `helm_values_dabet_deps` supplies
> one along with the credential it authenticates with. See
> [Security](#security) for how it authenticates.

**Kafka ACLs — created here as data, applied from the cluster.** The
authorisation matrix lives in `modules/kafka-acls`, which creates no resources
at all: it turns §1.5 into the ACL rules the deps chart applies, the IAM
statements `modules/iam` writes, and the SCRAM users `modules/msk` creates. The
bindings themselves are written by a Job inside the cluster, because the
brokers sit in subnets with no network path to wherever Terraform runs. That is
a real constraint rather than a preference; the reasoning is in
[Security](#security) and in the module header.

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

This is the seam between this directory and `deploy/k8s`. Two documents, each
shaped to the key paths its chart actually reads, both generated from the
resources rather than transcribed (`values_contract.tf`), so the two sides
cannot drift:

```sh
tofu output -raw helm_values_dabet_yaml       > values-aws-generated.yaml
tofu output -raw helm_values_dabet_deps_yaml  > deps-aws-generated.yaml

helm upgrade --install dabet-deps deploy/k8s/charts/dabet-deps \
  --namespace dabet --create-namespace -f deps-aws-generated.yaml

helm upgrade --install dabet deploy/k8s/charts/dabet \
  --namespace dabet \
  -f deploy/k8s/charts/dabet/values-aws.yaml \
  -f values-aws-generated.yaml
```

The chart's own `values-aws.yaml` stays first in the list: it carries the
replica counts, ingress, autoscaling and topic ceilings, which are the chart
author's decisions. The generated file carries only the coordinates that come
out of an AWS account and could not have been written down in advance.

> **Note on `charts/dabet/values-aws.yaml` as it stands today.** Its header
> records three constraints — Kafka must be plaintext, Redis must be
> single-shard without TLS, S3 needs static IAM user keys — that were true when
> it was written and are no longer. `pkg/kafkax` has TLS and SASL,
> `moderation-service` builds a cluster-aware Redis client, and the two S3
> consumers resolve credentials through the AWS chain. The generated file
> overrides all three, which is why it comes second on the `helm` command line.
> The chart's own file is not mine to edit.

**Rule for everything in the generated files: endpoints, names, ARNs and
booleans. Never a secret value.**

### `charts/dabet`

```yaml
config:
  kafkaBrokers: "b-1...:9096,b-2...:9096,b-3...:9096"   # KAFKA_BROKERS
  redisAddr: "clustercfg.dabet-prod-redis...:6379"      # REDIS_ADDR
  memcachedAddrs: "node1:11211,node2:11211,node3:11211" # MEMCACHED_ADDRS
  vllmEndpoint: "http://dabet-vllm:8000"
  embeddingEndpoint: "http://dabet-embedding:8091"
  milvusAddr: "dabet-milvus:19530"
  s3Endpoint: "https://s3.eu-west-1.amazonaws.com"      # S3_ENDPOINT
  s3Bucket: "dabet-prod-embeddings-<account>"           # S3_BUCKET
  logLevel: info

  # config.extra is the chart's pass-through into the ConfigMap. These are the
  # variables the managed-connectivity work added, for which the chart has no
  # first-class key.
  extra:
    S3_REGION: eu-west-1
    S3_CREDENTIALS_SOURCE: irsa        # assume the IRSA role, no static keys
    S3_ADDRESSING_STYLE: virtual
    REDIS_CLUSTER_ENABLED: "true"      # redis.NewClusterClient
    REDIS_TLS_ENABLED: "true"
    KAFKA_TLS_ENABLED: "true"
    KAFKA_SASL_MECHANISM: SCRAM-SHA-512

secrets:
  create: false        # the chart refuses to both create it and let ESO fill it

externalSecrets:
  enabled: true
  secretStoreRef:
    name: aws-secrets-manager
    kind: ClusterSecretStore
  refreshInterval: 1h
  creationPolicy: Owner
  dataFrom:
    - extract:
        key: dabet/prod/app            # the aggregate Secrets Manager document

services:                              # one entry per service, all nine
  user-service:
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: arn:aws:iam::<account>:role/dabet-prod-user-service
    # no envRaw: user-service has no Kafka client at all

  moderation-service:
    serviceAccount: { annotations: { ... } }
    # Its OWN SCRAM credential. KAFKA_SASL_MECHANISM is shared config above;
    # the identity is not, which is the whole point of per-service users.
    envRaw:
      - name: KAFKA_SASL_USERNAME
        valueFrom:
          secretKeyRef: { name: dabet-kafka-moderation-service, key: KAFKA_SASL_USERNAME }
      - name: KAFKA_SASL_PASSWORD
        valueFrom:
          secretKeyRef: { name: dabet-kafka-moderation-service, key: KAFKA_SASL_PASSWORD }

  credits-service: { ... }
  policy-service: { ... }
  provider-adapter: { ... }
  review-service: { ... }
  insights-service: { ... }
  clustering-service: { ... }
  clusters-job: { ... }
```

`envRaw` is emitted for the six services that speak Kafka and omitted for
`user-service`, `policy-service` and `clustering-service`, which have no
franz-go dependency — a service with no Kafka client and a Kafka credential is
just an unnecessary secret. Under `msk.scram_mode = "shared"` the six blocks
are still emitted and all six point at the same Secret, which is what makes
flipping the mode a one-line change.

The Secrets those refs name are created by
`kafka_external_secret_manifests`, not by the chart — see
[What the chart has to do with it](#what-the-chart-has-to-do-with-it).

### `charts/dabet-deps`

Everything AWS provides is switched off and replaced by an `external.*`
address. What stays in the cluster is exactly the set AWS has no managed
equivalent of, plus the inference workloads:

```yaml
kafka:
  enabled: false                            # no in-cluster brokers...
  topics:  { enabled: true }                # ...but §4.2's topics still have
                                            # to exist, and MSK runs with
                                            # auto.create.topics.enable=false
  admin:
    auth:
      tls:  { enabled: true }               # MSK: SASL only over TLS, 9096
      sasl:
        mechanism: SCRAM-SHA-512
        existingSecret: { name: dabet-kafka-admin }
  acls:
    enabled: true
    rules: [ ... ]                          # the §1.5 matrix, admin first

postgres:  { enabled: false }
redis:     { enabled: false }
memcached: { enabled: false }
minio:     { enabled: false }

external:
  kafka:     { brokers: "b-1...:9096,..." }
  postgres:  { identity: "", policy: "" }   # DSNs carry a password — see below
  redis:     { addr: "clustercfg...:6379", cluster: true }
  memcached: { addrs: "node1:11211,..." }
  s3:
    endpoint: ""                            # empty means real S3, not MinIO
    region: eu-west-1
    bucket: dabet-prod-embeddings-<account>
    accessKey: ""                           # empty: IRSA, not static keys
    secretKey: ""

clickhouse:
  enabled: true
  nodeSelector: { "dabet.io/workload": stateful }
  tolerations: [{ key: dabet.io/workload, operator: Equal, value: stateful, effect: NoSchedule }]

milvus:
  enabled: true
  nodeSelector: { "dabet.io/workload": stateful }
  tolerations: [...]

vllm:
  enabled: false                            # true when the GPU pool is on
  nodeSelector: { "dabet.io/workload": vllm }
  tolerations:
    - { key: nvidia.com/gpu,    operator: Equal, value: present, effect: NoSchedule }
    - { key: dabet.io/workload, operator: Equal, value: vllm,    effect: NoSchedule }

embedding: { nodeSelector: ..., tolerations: ... }
```

Both scheduling keys are emitted empty when the corresponding pool is off. That
is deliberate: the chart wraps them in `with`, which skips an empty map, so an
absent pool leaves the component unconstrained rather than Pending against a
selector that matches no node.

`external.postgres.identity` and `.policy` are left **empty on purpose**: the
deps chart publishes them into a Kubernetes Secret, and they are DSNs, so they
carry a password. A password in a Terraform output is a password in Terraform
state. The app chart gets its `POSTGRES_DSN_*` through External Secrets
Operator instead, from the document below.

`kafka.admin.auth.sasl.existingSecret.name` is a Secret **name**, for the same
reason. The chart has no values key that could hold a SASL password: it renders
a `secretKeyRef` and the Job's entrypoint turns it into a `client.properties`
inside a memory-backed emptyDir. The Secret itself is created by one of the
ExternalSecrets in `kafka_external_secret_manifests`.

### The Secrets Manager document

The chart's ESO integration extracts one aggregate document, whose required key
list is in `deploy/k8s/charts/dabet/README.md` — every key must be present,
because a `secretKeyRef` to a missing key blocks the pod from starting, and
that fail-fast is intentional. An empty string is a legitimate value; absent is
not.

`app_secret_document_skeleton` renders that document with everything Terraform
legitimately knows already filled in:

```sh
tofu output -raw app_secret_document_skeleton > /tmp/app.json
tofu output -json postgres_password_commands      # how to read the passwords
$EDITOR /tmp/app.json                             # replace the REPLACE markers
aws secretsmanager put-secret-value --secret-id dabet/prod/app \
  --secret-string file:///tmp/app.json
shred -u /tmp/app.json
```

The Postgres DSNs are the whole reason that output exists. The chart wants
complete connection strings; the password lives in an RDS-managed secret that
Terraform deliberately never reads. So host, port, database, user and
`sslmode=require` are supplied and only the password is pasted in:

```
postgres://dabet:REPLACE_WITH_PASSWORD@dabet-prod-identity.<id>.eu-west-1.rds.amazonaws.com:5432/dabet?sslmode=require
```

`sslmode=require` is not decoration — the parameter group sets
`rds.force_ssl = 1`, and Compose's `sslmode=disable` is refused by the server.

`S3_ACCESS_KEY` and `S3_SECRET_KEY` are rendered as empty strings, because
`S3_CREDENTIALS_SOURCE=irsa` means the pod assumes its role instead. The keys
stay present so the pod starts.

`KAFKA_SASL_USERNAME` and `KAFKA_SASL_PASSWORD` are **absent** from it, and
that is deliberate rather than an omission. This document is one Secrets
Manager value that all nine pods read through a single ExternalSecret, so a
Kafka credential in it would hand the same credential to every service —
precisely what per-service SCRAM users exist to prevent. Nothing in
`charts/dabet` references those two keys either: no service's `secretEnv` maps
them. The real values arrive per pod, from a per-service Secret, through
`services.<name>.envRaw`.

### What the chart has to do with it

**ServiceAccount names must match.** The IRSA trust policy pins
`system:serviceaccount:<namespace>:<serviceaccount>`, and
`charts/dabet/templates/serviceaccount.yaml` names each account
`<release>-<service>` — so at release name `dabet` it is `dabet-user-service`,
not `user-service`. `var.helm_release_name` derives that;
`var.service_account_names` overrides it wholesale. A mismatch does not fail at
apply; it fails at runtime, as an `AssumeRoleWithWebIdentity` denial in the
pod's logs.

**The namespace is `helm --namespace`.** The app chart templates no namespace
of its own, so `var.kubernetes_namespace` (default `dabet`) has to match what
you install into, and nothing in the chart will tell you if it does not.

**`MEMCACHED_ADDRS` takes the node list, not the configuration endpoint.**
ElastiCache's auto-discovery endpoint is a protocol extension the client has to
speak, and `policy-service` uses `gomemcache`, which does not — pointing it at
the discovery endpoint would silently hash every key onto one "node". The
trade-off is that adding a node changes the list and needs a re-render and a
restart, which §6.8 makes survivable: a cache miss reads through to Postgres.

**Kafka topics are still the deps chart's job — and it now does them on AWS.**
`charts/dabet-deps` carries a reconciling topics Job with §4.2's exact
partition counts and retentions. It used to be gated on `kafka.enabled`, which
is `false` here, so with MSK nothing created the topics and every produce
failed against a cluster with `auto.create.topics.enable = false`. The gate is
now "a broker address is resolvable", and `helm_values_dabet_deps` supplies
`external.kafka.brokers` plus the TLS/SASL settings and the admin Secret name
the Job authenticates with. Nothing to do by hand.

**Per-service Kafka credentials need one `kubectl apply` the charts cannot do.**
`charts/dabet` renders exactly one ExternalSecret, for the aggregate secret
document. Per-service SCRAM credentials need one each, and there is no chart
key for them — so `helm_values_dabet` points every service's `envRaw` at a
per-service Secret and `kafka_external_secret_manifests` renders the
ExternalSecrets that create those Secrets:

```sh
tofu output -raw kafka_external_secret_manifests | kubectl apply -f -
```

Recorded rather than worked around, because the fix belongs in the chart.

**Two Terraform outputs have no chart key yet.**
`platform_role_arns["milvus"]` and `platform_role_arns["clickhouse"]` exist so
those two can reach their S3 buckets without static keys, but `dabet-deps` has
no ServiceAccount template at all. Until it grows one, annotate those
ServiceAccounts by hand or fall back to static credentials for those two
components. Recorded rather than worked around, because the fix belongs in the
chart.

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

**There are two shapes of the same secrets, on purpose.**

`dabet/<env>/app` is one aggregate JSON document, because that is what the
chart's ExternalSecret extracts — a single `dataFrom.extract.key` pulling every
key at once. `app_secret_document_skeleton` renders it with the endpoints
already filled in.

The per-concern secrets (`stripe/secret-key`, `oauth/twitch`,
`jwt/private-key`, …) carry the same values split up. That is what the Secrets
Store CSI path needs — there, the pod's own identity fetches the secret, so a
per-service IAM grant is the whole authorisation — and it is what lets
`user-service` hold the JWT signing key while nothing else can. Populate
whichever shape your deployment actually uses; both exist so the choice is not
a Terraform change.

**Both consumption paths are wired accordingly.** External Secrets Operator
gets a role scoped to the deployment's ARN prefix; each service *also* gets read
access to exactly its own split secrets.

Who reads what, on the split path:

| Secret | Read by |
| --- | --- |
| `postgres/identity` | user-service, credits-service, provider-adapter, review-service |
| `postgres/policy` | policy-service |
| `stripe/*` | credits-service |
| `oauth/*` | user-service (§5.5 authorisation flow), provider-adapter (§5.6 lazy refresh) |
| `jwt/private-key` | user-service only — it is the only issuer |
| `jwt/public-key` | every service that validates a bearer token |
| `clickhouse/password` | clustering-service, clusters-job |

**Two credentials Terraform does generate**, because neither has an
AWS-managed equivalent and both must exist for a first apply to produce a
working system: the MSK SASL/SCRAM password (`modules/msk`) and, if you enable
it, the ElastiCache auth token. Both land in state. Rotate them out of band
afterwards if that matters — `ignore_changes` on the secret value means the new
one is not reverted, and MSK reads the current version.

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

# 4. Fill in the aggregate secret document. The endpoints are already in it;
#    the passwords and third-party credentials are not.
tofu output -raw app_secret_document_skeleton > /tmp/app.json
tofu output -json postgres_password_commands
$EDITOR /tmp/app.json
aws secretsmanager put-secret-value --secret-id dabet/prod/app \
  --secret-string file:///tmp/app.json
shred -u /tmp/app.json

# 5. Kubeconfig, then the Kafka credentials.
#
#    Terraform generated one SCRAM password per principal into its own
#    Secrets Manager secret and never printed any of them. These
#    ExternalSecrets are what project them into the namespace: one per
#    service, plus the reconciler admin's. They must exist BEFORE the deps
#    chart runs, because the topic and ACL Jobs mount the admin one.
aws eks update-kubeconfig --region eu-west-1 --name dabet-prod
kubectl create namespace dabet --dry-run=client -o yaml | kubectl apply -f -
tofu output -raw kafka_external_secret_manifests | kubectl apply -f -

# 6. Hand the contract to the charts.
#
#    The deps chart now reconciles the §4.2 topics against MSK and applies
#    the ACL matrix — both as post-install hooks, both idempotent. Nothing
#    to run by hand.
tofu output -raw helm_values_dabet_deps_yaml > /tmp/deps-aws.yaml
tofu output -raw helm_values_dabet_yaml      > /tmp/values-aws.yaml

helm upgrade --install dabet-deps ../../k8s/charts/dabet-deps \
  --namespace dabet --create-namespace -f /tmp/deps-aws.yaml

helm upgrade --install dabet ../../k8s/charts/dabet \
  --namespace dabet \
  -f ../../k8s/charts/dabet/values-aws.yaml \
  -f /tmp/values-aws.yaml

# 7. Confirm the authorisation matrix landed. The Job prints it, but a
#    direct diff against Terraform's view is the check that matters.
kubectl -n dabet logs job/dabet-kafka-acls | tail -40
tofu output -json kafka_acl_matrix | jq -r \
  '.[] | "\(.principal) \(.resourceType):\(.resourceName) \(.operations | join(","))"'
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

# Regenerate the chart values from live outputs. Both files are derived, so
# they are written outside the tree rather than committed.
TF_OUT ?= build/terraform
tf-values:
	@mkdir -p $(TF_OUT)
	$(TF) -chdir=$(TF_DIR) output -raw helm_values_dabet_yaml      > $(TF_OUT)/values-aws.yaml
	$(TF) -chdir=$(TF_DIR) output -raw helm_values_dabet_deps_yaml > $(TF_OUT)/deps-aws.yaml

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

## How the application reaches the managed services

Three things about the §3 target column are properties of the client, not of
the infrastructure, and each of them determined a default here.

**Redis Cluster and TLS — resolved.** `moderation-service` builds
`redis.NewClusterClient` and dials TLS when asked, driven by
`REDIS_CLUSTER_ENABLED` and `REDIS_TLS_ENABLED`. The generated values file sets
both from `elasticache.redis_cluster_mode` and
`elasticache.redis_transit_encryption_enabled`, so the client and the cluster
cannot disagree about which mode they are in. §4.3's hash tags were already in
the keyspace for exactly this, and the accompanying slot audit pins that no Lua
script or pipeline crosses tag families.

**Kafka authentication — resolved, but not with IAM, and the reason matters.**
`pkg/kafkax` now takes TLS and SASL from `KAFKA_TLS_ENABLED`,
`KAFKA_SASL_MECHANISM` and friends, and supports `AWS_MSK_IAM` among its
mechanisms. But franz-go's MSK IAM implementation reads AWS credentials out of
the environment and performs no STS exchange — an IRSA projected
service-account token cannot drive it. Using IAM auth from a pod would
therefore mean shipping a static IAM user access key, which is precisely the
thing a managed-cloud deployment must not need.

So the module defaults to **SASL/SCRAM-SHA-512 over TLS**. Terraform generates
the credentials, stores each in Secrets Manager under the `AmazonMSK_` name MSK
insists on, encrypts them with the customer-managed key MSK insists on, grants
`kafka.amazonaws.com` read through a resource policy, and associates the lot
with the cluster. Each pod reads its own through External Secrets Operator. The
`iam` module still writes the per-service `kafka-cluster:*` policies, so
switching to `client_authentication = "iam"` later is a variable change and a
rolling broker update — no rebuild.

*Related:* `kgo.Rack` is what makes the rack-aware replica selection in the MSK
configuration actually save money. Without a rack set on the client, the
property is inert.

> **Open, and it will bite on the first MSK deploy: three Kafka clients bypass
> `pkg/kafkax` and therefore carry no TLS or SASL options at all.** Every
> client built through `kafkax.NewProducer` / `NewConsumer` picks up
> `SecurityConfig.Options()`. These three call `kgo.NewClient` directly and do
> not:
>
> | Call site | What breaks |
> | --- | --- |
> | `services/provider-adapter/internal/shard/kafka.go` | §7.2 shard ring never forms; the fleet runs on a frozen assignment |
> | `services/review-service/internal/queue/kafka.go` | §7.6 review queue cannot read `flagged.v1` |
> | `services/clusters-job/internal/job/textsample.go` | §8.6 text sample cannot read `messages.v1` |
>
> Against a plaintext broker they work, which is why nothing has caught it. On
> MSK they will fail to connect while the same service's `kafkax` clients
> succeed — a partial failure that reads like a broker problem rather than a
> client one. The fix is in Go (`services/`), outside this directory and
> outside the change that added the ACLs, so it is recorded here rather than
> worked around: each of the three needs the options from
> `kafkax.DefaultSecurityConfig().Options()` appended. The ACLs granted to
> those principals are correct and will simply go unused until it is done.

**Kafka authorisation — one credential no longer reaches every topic.**
SCRAM authenticates a client and says nothing about what it may do. On a SCRAM
cluster that is Kafka ACLs, and an earlier revision of this directory created
none: with one credential shared by all nine services, every service could
read and write every topic. Two things changed.

*Per-service SCRAM users.* `msk.scram_mode` defaults to `per_service`, which
gives each of the six services that speak Kafka its own username, its own
generated password and its own Secrets Manager secret — plus a seventh, the
reconciler admin, which is never any service's. `shared` keeps the old shape
for a dev cluster. The trade-off is concrete rather than philosophical: six
extra secrets, six ExternalSecret manifests and six `envRaw` blocks, against a
blast radius that is otherwise "any compromised service can do anything any
service can do".

*ACLs from §1.5.* `modules/kafka-acls` holds the matrix. It is derived from the
code, not from the prose — every entry names the call site that makes the
request — and it feeds three consumers so they cannot drift: the deps chart's
`kafka.acls.rules`, the IAM statements in `modules/iam`, and the SCRAM user
list in `modules/msk`.

| Principal | Reads | Writes | Creates | Groups |
| --- | --- | --- | --- | --- |
| `provider-adapter` | `deletions.v1`, `adapter.shards.v1` | `messages.v1` | `adapter.shards.v1` | `provider-adapter`, `provider-adapter-shards` |
| `moderation-service` | `messages.v1` | `flagged.v1`, `deletions.v1`, `usage.v1` | | `moderation-service` |
| `review-service` | `flagged.v1` | `deletions.v1` | | *none* |
| `insights-service` | `messages.v1`, `flagged.v1` | | | `insights-service` |
| `credits-service` | `usage.v1` | | | `credits-service` |
| `clusters-job` | `messages.v1` | `usage.v1` | | *none* |
| `<name>-admin` | | | `*` | |

Three entries are worth their own line, because each is a place a plausible
guess is wrong:

* **`clusters-job` consumes `messages.v1`.** §8.6's text sample
  (`internal/job/textsample.go`) is a bounded read with `ConsumeTopics` +
  `AfterMilli`. `modules/iam` previously had `read = []` for it, which would
  have produced a role that could produce `usage.v1` and then fail on every
  fetch. Fixed here.
* **`review-service` and `clusters-job` join no consumer group.**
  review-service assigns partitions itself with `kgo.ConsumePartitions` and
  explicit offsets (§7.6); clusters-job's sample is groupless. They get no
  group ACL. The IAM path still lists them, because an IAM statement naming an
  unused group ARN is inert — while an ACL naming one flips that group from
  "open because unlisted" to "closed to everyone else". Inert on one path, a
  real change on the other.
* **`adapter.shards.v1` is subscribed to and never produced to.** The consumer
  group's *membership* is the §7.2/A13 fleet view. The adapter also creates the
  topic itself, best effort, which is why it holds `Create` on that one name
  and nothing else.

*Idempotent producers.* §4.2 asks for `enable.idempotence=true`; `pkg/kafkax`
gets it by setting `acks=all` and leaving franz-go's default on. On the **IAM**
path that needs `kafka-cluster:WriteDataIdempotently` on the cluster resource,
which `modules/iam` grants to every producing principal. On the **ACL** path it
is `IdempotentWrite` on `Cluster` — and since KIP-679 (Kafka 3.0) the broker
accepts topic `Write` alone, so on this 3.9 cluster the grant is redundant. It
is kept because it is one operation on one resource and its absence, on an
older broker, turns every produce into a `CLUSTER_AUTHORIZATION_FAILED` that
reads like a client bug.

**How the ACLs actually get applied, and why not from here.**
Kafka ACLs are a Kafka protocol operation. The AWS provider has no resource for
them; the only Terraform provider that does (`Mongey/kafka`) speaks the Kafka
protocol directly, so whatever runs `tofu apply` must open a TCP connection to
a broker. It cannot: `modules/network` puts the brokers in data-tier subnets
whose route tables carry no default route in either direction, and
`modules/msk` admits ingress only from a referenced security group id. A run
from a laptop or a CI runner outside the VPC has no path to port 9096 at all,
and adding the provider would also make `tofu plan` require a live broker and a
credential just to read state — which breaks the credential-free validation
this directory is built around.

So the honest answer is that ACLs have to be applied from inside the cluster,
and that is what happens. **Terraform is the source of truth; the cluster is
the applier.** `charts/dabet-deps` runs a Job at hook weight `-2` (before the
topic reconciler at `0`, because `CreateTopic` is itself an ACL now) that walks
the rule list with `kafka-acls.sh --add`, which is idempotent. It reconciles
forward only: nothing is ever removed, because an automatic prune would turn a
values typo into an outage, and the full binding list is printed at the end of
every run so a stale one is visible.

Two things about that Job that are not incidental:

* **The admin bindings come first.** MSK ships with
  `allow.everyone.if.no.acl.found = true`, and that flag is evaluated *per
  resource*: a resource with no binding is open to everyone, a resource with
  one binding is closed to everyone it does not name. The first binding written
  against the `Cluster` resource therefore decides who may write bindings from
  then on. The matrix is emitted admin-first and applied in order, so the
  credential running it cannot lock itself out.
* **`allow_everyone_if_no_acl_found` stays `true`, and turning it off is a
  two-phase change.** Phase 1: apply the ACLs and confirm the fleet is
  connected. Phase 2: set it `false`, and MSK adds a configuration revision and
  rolls the brokers. Doing phase 2 first leaves a cluster where nothing is
  authorised — including the credential that would write the ACLs. Note that
  leaving it `true` is *not* the same as having no ACLs: every topic and group
  in the matrix has a binding and is therefore genuinely restricted. What stays
  open is what the matrix does not name.

If a team wants Terraform to own the bindings, the escape hatch is real but is
a different topology: run Terraform on a runner inside the VPC (or through a
bastion with a forwarded port), add the `Mongey/kafka` provider, and feed it
`module.kafka_acls.rules`. The matrix does not change; only who applies it
does. That is a decision about who owns the CI network, which is why it is
documented rather than made here. For a one-off from a bastion,
`tofu output -json kafka_acl_commands` renders the same bindings as
`kafka-acls.sh` invocations, in the order they must be run.

**S3 without static keys — resolved.** `insights-service` and `clusters-job`
build their MinIO client from a credential chain selected by
`S3_CREDENTIALS_SOURCE`; the generated values file sets it to `irsa`, along
with `S3_REGION` so the regional STS endpoint is used. That is what makes the
two S3 IRSA roles in this directory do anything, and why `S3_ACCESS_KEY` and
`S3_SECRET_KEY` are rendered as empty strings in the secret document rather
than as an IAM user's key.

**Memcached TLS — still open, and deliberately.** `policy-service` uses
`gomemcache`, which dials plain TCP, so `memcached_transit_encryption_enabled`
is off. §6.8 makes an unreachable Memcached a read-through to Postgres, so
turning it on without a client change degrades rather than breaks — but it
degrades the hot path's cache hit rate to zero, which at 500K msg/s is not a
thing to discover in production.

---

## Validation

```sh
tofu fmt -check -recursive deploy/terraform
tofu -chdir=deploy/terraform             init -backend=false && tofu -chdir=deploy/terraform             validate
tofu -chdir=deploy/terraform/bootstrap   init -backend=false && tofu -chdir=deploy/terraform/bootstrap   validate
tofu -chdir=deploy/terraform/examples/dev  init -backend=false && tofu -chdir=deploy/terraform/examples/dev  validate
tofu -chdir=deploy/terraform/examples/prod init -backend=false && tofu -chdir=deploy/terraform/examples/prod validate
# and each of deploy/terraform/modules/* — ten of them, including kafka-acls
```

All pass against AWS provider 6.61.0 and OpenTofu 1.10.6. `modules/kafka-acls`
declares no providers at all, so `init -backend=false` on anything that calls
it downloads nothing extra and needs no credentials.

The chart half of the Kafka work is validated from the chart:

```sh
helm lint deploy/k8s/charts/dabet-deps            # and -f values-{local,prod}.yaml
NO_KUBECTL=1 deploy/k8s/charts/dabet-deps/hack/render-matrix.sh
```

The matrix renders the topic reconciler against both an in-cluster broker and
an external one, with and without TLS/SASL, and asserts that the §4.2 numbers
survive every shipped profile and that the SASL credential is a `secretKeyRef`
and never a literal.

**No `plan` and no `apply` has been run against an AWS account**, because there
are no credentials in this environment and creating real infrastructure was out
of scope. `validate` checks configuration against the provider's schema — every
resource type, argument name and type in this directory is real and correctly
shaped — but it does not talk to AWS, so it cannot check that a value AWS
accepts in the abstract is one AWS accepts *here*.

One step further was possible without credentials, and was taken. Creating a
resource needs no API call at plan time, so the `iam`, `rds`, `msk` and
`elasticache` modules were planned offline against dummy inputs, and the
resulting plan inspected as JSON. `kafka-acls` goes further still: it creates
nothing, so it can be *applied* offline and its outputs read directly.
Together those confirm the parts that are computed rather than declared:

- the IRSA policy documents render as valid IAM, with topic and group ARNs
  correctly derived from the MSK cluster ARN and scoped per §1.5 — including
  `WriteDataIdempotently` on the cluster for every producer, `CreateTopic` on
  `adapter.shards.v1` and nothing else for `provider-adapter`, and `ReadData`
  on `messages.v1` for `clusters-job`;
- the ACL matrix renders 25 bindings with the admin's cluster grants first,
  and switching `msk.scram_mode` to `shared` collapses them onto one principal
  holding `Read,Write` on every topic — which is the documented trade-off,
  made visible;
- `modules/msk` plans seven Secrets Manager secrets (six services plus the
  reconciler admin), each `AmazonMSK_<cluster>_<user>`, each with its own
  resource policy, and one `aws_msk_scram_secret_association` carrying all
  seven;
- `rds.force_ssl` survives the map-key remapping and `shared_preload_libraries`
  is marked `pending-reboot` rather than `immediate`;
- the MSK `server_properties` document renders as expected, including
  `allow.everyone.if.no.acl.found=true`;
- every security group ingress rule carries a `referenced_security_group_id`
  and no `cidr_ipv4` at all.

The modules with an `aws_caller_identity` data source (`s3`, `secrets`,
`observability`) and the root, which has one in `kms.tf`, cannot be planned this
way — that data source is a real STS call.

**What still needs a live MSK cluster.** Everything above is shape, not
behaviour. A real cluster is the only thing that can settle:

- whether MSK accepts `allow.everyone.if.no.acl.found` as an editable property
  at the configured `kafka_version` (it is documented as editable; an
  unsupported property fails at apply, not at plan);
- whether the reconciler admin can in fact write the first `Cluster` binding —
  the whole bootstrap sequence rests on MSK's per-resource evaluation of that
  flag behaving as documented;
- that SASL/SCRAM over TLS from the `apache/kafka` image's shell tools
  negotiates against MSK's brokers with nothing but the JVM's default
  truststore;
- that seven SCRAM secrets associate cleanly in one
  `aws_msk_scram_secret_association` — the API takes a list, but the per-secret
  KMS and resource-policy preconditions are checked at association time;
- and that the matrix is *complete*: an ACL that is missing shows up as a
  `TOPIC_AUTHORIZATION_FAILED` on a producer or, worse, as a consumer that
  simply never joins its group. The end-to-end check is step 7 of the runbook.

<a id="unverified"></a>
### Unverified, and what would settle it

Ordered by how likely each is to bite on a first apply.

| Item | Concern | How to settle it |
| --- | --- | --- |
| **Instance types and engine versions** | `kafka.m7g.2xlarge`, `g6.2xlarge`, `cache.r7g.large`, `db.m7g.xlarge`, Kafka `3.9.x`, Redis `7.1`, Memcached `1.6.22`, Kubernetes `1.33` are all plausible and current at time of writing, but availability varies by region and AWS retires versions. | `aws ec2 describe-instance-type-offerings`, `aws kafka list-kafka-versions`, `aws elasticache describe-cache-engine-versions`, `aws eks describe-addon-versions`. A wrong value fails at apply with a clear message. |
| **`cluster-enabled = yes` in a custom Redis parameter group** | The module creates a `redis7`-family group and sets `cluster-enabled` rather than using the stock `default.redis7.cluster.on`, so that `maxmemory-policy` can also be set. This is a widely used pattern but I have not applied it. | If it is rejected, set `redis_parameter_group_family` aside and point the replication group at `default.redis7.cluster.on`, losing the eviction-policy setting. |
| **`AL2023_x86_64_NVIDIA` AMI type** | The correct AL2023 GPU AMI type for the `g6` family as far as I can tell, but AMI type names have churned. | `aws eks describe-addon-versions` is no help here; check the EKS optimised-AMI documentation, or set `gpu_node_group.ami_type` explicitly. |
| **MSK provisioned EBS throughput** | Left `null`. AWS documents it as available only from the larger instance sizes, with a MiBps range that depends on the type. I did not want to guess a pair that fails at apply. | Check the current matrix and set `provisioned_throughput_mibps` if the baseline is not enough. |
| **MSK SASL/SCRAM** | The credential has three MSK-specific constraints — the `AmazonMSK_` name prefix, a customer-managed KMS key (the default `aws/secretsmanager` key is rejected), and a resource policy granting `kafka.amazonaws.com`. All three are implemented from the documented requirements, and all three are apply-time failures if any is wrong. | The first apply says so. Then check that `pkg/kafkax` authenticates with the value: `KAFKA_SASL_MECHANISM=SCRAM-SHA-512` with the username and password from the secret. |
| **The AMP log resource policy** | `aws_prometheus_workspace.logging_configuration` needs a CloudWatch Logs *resource* policy granting `aps.amazonaws.com`, which the module creates. The condition keys and the source-ARN wildcard are from the documented pattern, not from an apply. | If AMP logs nothing, check the resource policy first; the workspace itself will have been created either way. |
| **Pre-creating the RDS and EKS CloudWatch log groups** | Both services create their own log group on first write, with no retention. Creating it first so the service adopts it is the standard workaround, but adoption is not contractual. | Confirm after the first apply that `/aws/rds/instance/<id>/postgresql` has the configured retention. |
| **`most_recent` addon resolution** | Addon versions are not pinned, so the first apply takes the newest compatible release. That is the safer default against a stale pin, but it is not reproducible across time. | Pin via `addons.addon_version_overrides` once you have a known-good set. |
| **Ordering on first apply** | The dependencies I could not express implicitly are spelled out with `depends_on` — node groups on the role's policy attachments, addons on the node group, the flow log on its role policy, bucket policies on the public access block, the log-delivery policy before `aws_s3_bucket_logging`. There may be an ordering AWS enforces that I have not anticipated. | The first `apply` will say so. Re-running is safe for all of them. |
| **Anything about cost** | Every figure above is derived from list prices and the spec's own volume numbers. None came from a bill. | AWS Pricing Calculator, then Cost Explorer once it is running. |
