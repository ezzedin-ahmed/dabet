# `dabet-deps`

Dabet's stateful dependencies, as a Helm chart. This is the layer that lets the
platform run on **any** Kubernetes cluster — bare metal, a laptop, or a cloud
where the managed equivalents are not used.

It is the Kubernetes counterpart of `deploy/compose/docker-compose.yml`, and it
targets the right-hand column of docs §3:

| | Local (Compose) | Target (§3) | This chart |
| --- | --- | --- | --- |
| Postgres | 1 instance, all schemas | Managed, separate clusters for identity/policy | two StatefulSets, `identity` and `policy` |
| Kafka | 1 broker, 3 partitions | 3+ brokers, §4.2 partition counts | 3-broker KRaft StatefulSet + a topic reconciler |
| Redis | 1 instance | Redis Cluster, sharded by `hash(author_id, content_id)` | multi-shard cluster (or single node for laptops) |
| Memcached | 1 instance | Managed pool | StatefulSet pool, client-side sharded |
| vLLM | 1 instance, small model | GPU fleet behind a load balancer | GPU Deployment + Service (opt-in) |
| Milvus | Standalone | Distributed | 5-component distributed mode + etcd (opt-in) |
| ClickHouse | 1 instance | Cluster | cluster-capable StatefulSet + optional Keeper |
| S3 | MinIO | S3 | MinIO, or disabled in favour of real S3 |

**Every component has an `enabled` flag, and the chart renders correctly with
any subset disabled.** That is the whole design: a deployment mixes self-hosted
components with AWS managed ones by turning a component off and filling in its
`external.*` entry. The connection contract below is published identically
either way, so the app chart never learns which is which.

---

## Quick start

```bash
# Laptop (kind). Build and load the mocks first — see "Mock images".
kind create cluster --name dabet
make -C ../../.. k8s-mock-images          # or: docker build ... (below)
kind load docker-image dabet/mockllm:dev dabet/mockembed:dev --name dabet

helm install deps deploy/k8s/charts/dabet-deps \
  -n dabet --create-namespace \
  -f deploy/k8s/charts/dabet-deps/values-local.yaml \
  --set fullnameOverride=dabet \
  --wait --timeout 12m
```

```bash
# Self-hosted target profile
helm install deps deploy/k8s/charts/dabet-deps \
  -n dabet --create-namespace \
  -f deploy/k8s/charts/dabet-deps/values-prod.yaml \
  --set global.storageClass=fast-ssd
```

`--set fullnameOverride=dabet` is worth doing on day one: without it every
resource is named `<release>-dabet-deps-<component>`, and those names end up in
DNS and therefore in the published connection strings. Changing it later
changes every address.

---

## Connection contract (docs §4.4)

This is the chart's entire public interface. It is published as a Secret whose
name is `.Values.connectionSecret.name`, defaulting to
**`dabet-deps-connection`**, plus (optionally) a credential-free ConfigMap
`dabet-deps-endpoints`.

### Keys

| Key | Shape | Notes |
| --- | --- | --- |
| `KAFKA_BROKERS` | `host:port,host:port,…` | one entry per broker, per-pod DNS names (not a ClusterIP) |
| `POSTGRES_DSN_IDENTITY` | `postgres://user:pw@host:5432/db?sslmode=…` | the `identity` cluster |
| `POSTGRES_DSN_POLICY` | `postgres://user:pw@host:5432/db?sslmode=…` | the `policy` cluster |
| `REDIS_ADDR` | `host:port` **or** `host:port,host:port,…` | one address standalone; the full seed list in cluster mode |
| `REDIS_CLUSTER` | `"true"` / `"false"` | **not** a §4.4 name — see the caveat below |
| `MEMCACHED_ADDRS` | `host:port,host:port,…` | every pool member; clients shard across the list |
| `CLICKHOUSE_DSN` | `clickhouse://user:pw@host:9000/db` | native protocol port, matching compose |
| `S3_ENDPOINT` | `http://host:port` | **absent** when using real AWS S3 |
| `S3_BUCKET` | bucket name | §8.4 embeddings parquet |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | credentials | absent when the workload uses IRSA |
| `S3_REGION` | region | |
| `MILVUS_ADDR` | `host:port` | gRPC; absent unless Milvus is enabled or external |
| `VLLM_ENDPOINT` | `http://host:port` | the real fleet, or the mock, or external |
| `EMBEDDING_ENDPOINT` | `http://host:port` | one Service shared by §7.4 and §8.4 |

### Two rules the consuming side must follow

**1. `POSTGRES_DSN` is published per database.** Services read the bare name
`POSTGRES_DSN` (`pkg/config`), so the app chart maps the right key onto it:

```yaml
env:
  - name: POSTGRES_DSN
    valueFrom:
      secretKeyRef:
        name: dabet-deps-connection
        key: POSTGRES_DSN_IDENTITY    # or POSTGRES_DSN_POLICY
        optional: true
```

| Service | Key |
| --- | --- |
| `user-service` | `POSTGRES_DSN_IDENTITY` |
| `credits-service` | `POSTGRES_DSN_IDENTITY` |
| `provider-adapter` | `POSTGRES_DSN_IDENTITY` |
| `policy-service` | `POSTGRES_DSN_POLICY` |
| `review-service` | `POSTGRES_DSN_POLICY` |

**2. A key may be absent, and absent is not the same as empty.** A component
that is neither enabled nor externally configured publishes *nothing* rather
than an empty string, so that a service falls back to its own default (§4.4)
instead of being handed `""`. Always set `optional: true` on the
`secretKeyRef`. Everything else can be pulled in wholesale:

```yaml
envFrom:
  - secretRef:
      name: dabet-deps-connection
      optional: true
```

### Reading it

```bash
kubectl -n dabet get secret dabet-deps-connection \
  -o go-template='{{range $k,$v := .data}}{{$k}}={{$v | base64decode}}{{"\n"}}{{end}}'
```

---

## Schema placement, and why

The services carry their own embedded migrations and run them at startup, so
this chart provisions **databases and credentials, never schemas**. Four schemas
exist; §3 names only two clusters, so the other two had to be placed
deliberately.

| Schema | Owner | Instance | Reason |
| --- | --- | --- | --- |
| `identity` | `user-service` | **identity** | §3 |
| `billing` | `credits-service` | **identity** | `credits-service` reads `identity.creators` directly from its own `POSTGRES_DSN` — a documented deviation for the mailer (`services/credits-service/internal/identity/identity.go`). A cross-*instance* read is not possible, so `billing` has to sit on the same instance as `identity`. |
| `policy` | `policy-service` | **policy** | §3 |
| `review` | `review-service` | **policy** | Free to go anywhere: `review-service` explicitly forbids a cross-schema foreign key into identity (`internal/migrate/migrate_test.go`). Placed on the policy instance so the identity instance carries only the hot auth + money path. The two are also correlated the right way round: a policy outage makes moderation fail open (P2) and chat keeps flowing, and a review queue outage is equally non-fatal, while an identity outage stops logins. Grouping the two survivable ones together keeps the unsurvivable one isolated. |

The one piece of DDL this chart does own is `CREATE EXTENSION citext`, run by
the initdb hook. `user-service/0001_identity.sql` also runs it, but extension
creation needs rights the app role should not hold, so doing it first as the
superuser makes the service's `IF NOT EXISTS` a no-op — exactly what
`deploy/compose/postgres-init/01-schemas.sql` does for compose.

---

## Kafka and the §4.2 registry

Three brokers in KRaft combined mode (broker + controller), as a StatefulSet
with per-broker PVCs. Node ids and advertised listeners are derived from the pod
ordinal inside the container, because a StatefulSet has no per-ordinal env.

Topics are created and reconciled by a post-install/post-upgrade Job driven by
`kafka.topics.registry`:

| Topic | Partitions | Retention |
| --- | --- | --- |
| `messages.v1` | 512 | 24 h |
| `flagged.v1` | 128 | 7 d |
| `deletions.v1` | 128 | 24 h |
| `usage.v1` | 32 | 7 d |

The job is idempotent and safe to re-run:

* **absent** → create with the exact partition count and config
* **too few partitions** → `--alter --partitions N` (Kafka allows increases)
* **too many partitions** → **refuses, loudly, and carries on**. Shrinking means
  delete-and-recreate: every retained record is dropped and, for `messages.v1`,
  every key is re-mapped to a different partition, which breaks the ordering
  guarantee the Redis dedup design rests on.
* **config drift** → `kafka-configs.sh --alter`, applied every run

Producer-side settings (`zstd`, `acks=all`, `enable.idempotence`) live in the
services, per §4.2. What lives here is `compression.type=producer` (keep what
the producer sent rather than recompressing) and `min.insync.replicas`, which is
capped at the replication factor so `acks=all` can never be unsatisfiable.

> `values-local.yaml` shrinks the counts to 12/6/6/3 and every retention to 1 h.
> The *ratios* are preserved so consumer-group behaviour still looks like
> production.

---

## Redis Cluster

§4.3 says the hash tags exist "so the cluster migration is a config change", and
`pkg/rediskeys` does carry them (`dup:{ct_9f2a:sd_3b71}`). This chart takes that
at its word and runs a real multi-shard cluster, not one node pretending:

* `redis.cluster.enabled: true` → N nodes, `cluster-enabled yes`, slots split
  across at least three masters, formed by an idempotent `redis-cli --cluster
  create` Job.
* `redis.cluster.enabled: false` → one standalone node, for laptops.

`cluster-preferred-endpoint-type hostname` plus a per-pod
`cluster-announce-hostname` is what makes this survive Kubernetes: without it a
node announces its pod IP, a `MOVED` reply hands the client an address that
changes on every reschedule, and the cached slot map rots.

`REDIS_ADDR` is therefore a **seed list** in cluster mode, and a single address
in standalone mode.

### ⚠ Outstanding app-side change

`moderation-service` currently builds a go-redis **single-node** client:

```go
rdb := redis.NewClient(&redis.Options{Addr: config.GetDefault(config.EnvRedisAddr, ...)})
```

The *keyspace* is cluster-ready; the *client* is not. Against a real cluster,
every key whose slot lives on another shard comes back `MOVED`. This chart
publishes `REDIS_CLUSTER=true` as the signal for that switch, but making it is
the service's job, not the chart's.

**Until that lands, set `redis.cluster.enabled=false`** — the keys are
unchanged, so flipping it back later is genuinely a config change.

---

## ClickHouse, and the auth trap

The official image's entrypoint **disables network access for the `default`
user** unless `CLICKHOUSE_USER` *and* `CLICKHOUSE_PASSWORD` are both set — while
`/ping` keeps answering 200 without authenticating. A readiness probe on `/ping`
therefore marks the pod Ready while every client is rejected with
`AUTHENTICATION_FAILED`. Both consequences are handled:

1. `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DB` are always set.
2. The **readiness probe runs an authenticated `SELECT 1`**, not `/ping`.
   (`/ping` is still used for liveness and startup, where "the process is up" is
   the right question.)

`<remote_servers>` is always rendered from `shards × replicasPerShard`, so a
1×1 install is a real one-node cluster as far as `ON CLUSTER` DDL and the
`Distributed` engine are concerned, and scaling up is a values change. Going
past one replica per shard requires `clickhouse.keeper.enabled=true`
(ReplicatedMergeTree has nowhere to coordinate otherwise); the chart refuses to
render the inconsistent combination.

The §8.7 tables (`topic_counts`, `topics`) are created by `clustering-service`
and `clusters-job`, which own their engines and ORDER BY keys.

---

## Milvus — opt-in

`milvus.enabled` defaults to **false**, matching how compose keeps Milvus behind
the `clustering` profile. Distributed mode is five component Deployments
(mixcoord, proxy, querynode, datanode, indexnode) plus a three-node etcd plus a
message queue, and it wants several GB before it indexes a single vector.

```bash
--set milvus.enabled=true                          # distributed (§3 target)
--set milvus.enabled=true --set milvus.mode=standalone   # one pod, dev
```

**The message queue is this chart's Kafka** (`milvus.messageQueue: kafka`).
Milvus needs a write-ahead log; its alternatives are Pulsar (four more pods,
including its own ZooKeeper) or RocksMQ (standalone only). Milvus creates its
own `by-dev-*` topics with its own retention — they do not touch the §4.2
registry and the topics job does not manage them.

Only **centroids** live in Milvus (§8.5); the full corpus is the S3 parquet.

---

## Inference: vLLM, the embedder, and the mocks

Both real backends are **disabled by default**, because a Deployment requesting
`nvidia.com/gpu` on a cluster with no device plugin does not fail — it sits
Pending forever.

```
VLLM_ENDPOINT      = external.vllm.endpoint  ?: vllm Service  ?: mockllm Service
EMBEDDING_ENDPOINT = external.embedding.endpoint ?: embedding Service ?: mockembed Service
```

* **vLLM** — Deployment with `nvidia.com/gpu` requests *and limits* (a GPU is not
  burstable, so Kubernetes requires them equal), `nodeSelector`/`tolerations`
  for the GPU pool, a Service in front, configurable `model` and `extraArgs`, a
  shared RWX model cache, and a long `startupProbe` so the liveness probe cannot
  kill a pod that is merely still loading weights.
* **Embedding** — **one** Deployment and **one** Service, because §8.4 requires
  a message to be embedded at most once and shares the service between semantic
  spam (§7.4) and Insights. `dimensions` is pinned at 384 and the chart refuses
  to change it: the S3 corpus and the Milvus collection are both built to that
  width. Default backend is `tei` (text-embeddings-inference), which serves a
  MiniLM-class model well on CPU — so `gpu.count: 0` is a real option here in a
  way it never is for the §7.9 verdict model.
* **Mocks** — `tools/mockllm` and `tools/mockembed`, the deterministic
  stand-ins the e2e suite already asserts against. Enabling a real backend and
  its mock at the same time is rejected: only one can win the endpoint, and the
  loser would be a pod nobody talks to.

### Mock images

They are built from `tools/` and are **not published to any registry**, so they
have to be side-loaded:

```bash
docker build -f tools/mockllm/Dockerfile   -t dabet/mockllm:dev   tools/
docker build -f tools/mockembed/Dockerfile -t dabet/mockembed:dev tools/
kind load docker-image dabet/mockllm:dev dabet/mockembed:dev --name <cluster>
```

Keep `pullPolicy: IfNotPresent` — `Always` would look for a registry that does
not have them.

---

## Mixing in AWS managed services

Terraform owns the managed path (`deploy/terraform/`). To hand over a component,
disable it and fill in its `external` entry:

```yaml
kafka:      {enabled: false}
postgres:   {enabled: false}
redis:      {enabled: false}
memcached:  {enabled: false}
minio:      {enabled: false}

external:
  kafka:     {brokers: "b-1.msk...:9092,b-2.msk...:9092"}
  postgres:
    identity: "postgres://dabet:pw@identity.rds...:5432/dabet_identity?sslmode=require"
    policy:   "postgres://dabet:pw@policy.rds...:5432/dabet_policy?sslmode=require"
  redis:     {addr: "dabet.abc.clustercfg.euw1.cache.amazonaws.com:6379", cluster: true}
  memcached: {addrs: "dabet.abc.cfg.euw1.cache.amazonaws.com:11211"}
  s3:        {endpoint: "", region: eu-west-1, bucket: dabet-embeddings-prod}
```

An `external.*` value wins even when the component is enabled, so it doubles as
a manual override.

With everything disabled the chart still renders — two objects, the Secret and
the ConfigMap, carrying only the keys that were externally configured. That
degenerate case is deliberately part of the test matrix.

---

## Validation

```bash
helm lint deploy/k8s/charts/dabet-deps
helm lint deploy/k8s/charts/dabet-deps -f .../values-local.yaml
helm lint deploy/k8s/charts/dabet-deps -f .../values-prod.yaml

# 22 enable/disable combinations, each rendered and pushed through
# `kubectl apply --dry-run=client`
./hack/render-matrix.sh
NO_KUBECTL=1 ./hack/render-matrix.sh      # render only, no cluster needed
KUBE_CONTEXT=kind-dabet ./hack/render-matrix.sh
```

Value combinations that would install cleanly and then not work are rejected at
render time (`templates/_validate.tpl`) rather than discovered hours later —
an even KRaft voter count, `minInsyncReplicas` above the broker count, a Redis
"cluster" with fewer than three masters, a ClickHouse topology that does not
match its pod count, replication without Keeper, both a real backend and its
mock claiming the same endpoint.

---

## Design notes

**Why hand-written manifests rather than upstream subcharts.** Bitnami's catalog
— the obvious candidate for most of these — moved its public images to a
`bitnamilegacy` holding pattern in 2025, and charts that pinned
`docker.io/bitnami/*` broke without a chart change. That is precisely the
"silently breaks" failure this chart has to avoid, since it is the base layer
everything else sits on. The images used here (`apache/kafka`, `postgres`,
`redis`, `memcached`, `clickhouse/clickhouse-server`, `minio/minio`,
`milvusdb/milvus`, `quay.io/coreos/etcd`) are the upstream-official ones, and
they are the *same tags the compose profile already proves*. Hand-rolling also
keeps the toggle matrix honest: nine subcharts would mean nine value schemas to
translate and nine chances for a disabled component to leave a dangling
reference.

**Why some components get per-pod DNS and others get a ClusterIP.**

| Component | Published as | Why |
| --- | --- | --- |
| Kafka | per-broker names | a client bootstraps once and is then told each broker's advertised listener; a ClusterIP would be bypassed anyway |
| Redis (cluster) | seed list | a ClusterIP would answer on a random node and be redirected for most keys |
| Memcached | full pool list | the client shards the keyspace across the address list; one ClusterIP would randomise placement and destroy the hit rate (§6.8) |
| Postgres | ClusterIP | one pod per instance; the Service is just a stable name |
| ClickHouse | ClusterIP | any node can serve any query against a `Distributed` table, so balancing is correct |
| MinIO / Milvus / vLLM / embedding | ClusterIP | stateless from the caller's point of view |

**Generated passwords survive upgrades.** `dabet-deps.password` reads the live
Secret via `lookup()` before falling back to `randAlphaNum`. Without that, every
`helm upgrade` would mint a new password, write it to the Secret, and leave the
running Postgres — whose PGDATA still has the old one — unreachable. The
side effect is that `helm template` output is not byte-stable, since `lookup()`
returns nothing outside a live cluster.

**Redis needs a PVC even though its data is disposable.** Everything in §4.3 has
a 5-minute TTL and is reconstructible. The PVC is for `nodes.conf`: a cluster
node that loses it loses its identity and its slot ownership, and rejoining is a
manual repair.

## Makefile targets worth adding

This chart does not own the `Makefile`. Suggested targets:

```make
k8s-mock-images:      ## build the tools/ mocks for k8s
	docker build -f tools/mockllm/Dockerfile   -t dabet/mockllm:dev   tools/
	docker build -f tools/mockembed/Dockerfile -t dabet/mockembed:dev tools/

k8s-kind-up: k8s-mock-images   ## kind cluster + deps chart, local profile
	kind create cluster --name dabet
	kind load docker-image dabet/mockllm:dev dabet/mockembed:dev --name dabet
	helm install deps deploy/k8s/charts/dabet-deps -n dabet --create-namespace \
	  -f deploy/k8s/charts/dabet-deps/values-local.yaml \
	  --set fullnameOverride=dabet --wait --timeout 12m

k8s-kind-down:
	kind delete cluster --name dabet

k8s-deps-lint:        ## lint + the full enable/disable render matrix
	helm lint deploy/k8s/charts/dabet-deps
	helm lint deploy/k8s/charts/dabet-deps -f deploy/k8s/charts/dabet-deps/values-local.yaml
	helm lint deploy/k8s/charts/dabet-deps -f deploy/k8s/charts/dabet-deps/values-prod.yaml
	deploy/k8s/charts/dabet-deps/hack/render-matrix.sh
```
