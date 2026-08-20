# `deploy/k8s/charts/dabet` — the Dabet application layer

The nine services of [`docs/implementation-reference.md`](../../../../docs/implementation-reference.md)
as a Helm chart. **Dependencies are not in here**: Postgres, Kafka, Redis,
Memcached, ClickHouse, Milvus, S3 and the LLM/embedding fleets are endpoints
this chart points at, installed by `deploy/k8s/charts/dabet-deps` in-cluster
or provisioned by Terraform as managed services.

Per §3 — *"All services must run against the Compose profile with no code
changes — topology differences are configuration only"* — nothing in this
chart required a change to `pkg/` or `services/`. Where the code makes that
impossible, it is written down under [Findings](#findings) rather than
worked around.

```sh
helm install dabet deploy/k8s/charts/dabet -n dabet --create-namespace          # bare cluster
helm install dabet deploy/k8s/charts/dabet -f .../values-local.yaml -n dabet    # laptop + deps chart
helm upgrade --install dabet deploy/k8s/charts/dabet -f .../values-aws.yaml     # EKS + managed deps
```

---

## The nine workloads

| Service | Kind | Public API | Kafka | Scales on |
| --- | --- | --- | --- | --- |
| `user-service` | Deployment | `/v1/auth/*`, `/v1/me`, `/v1/connections*` | — | CPU |
| `credits-service` | Deployment | `/v1/credits*`, `/v1/webhooks/stripe` | consumes `usage.v1` | **lag** (cap 32) |
| `policy-service` | Deployment | `/v1/policies*` + gRPC `:7101` | — | CPU |
| `provider-adapter` | **StatefulSet** | none | produces `messages.v1`, consumes `deletions.v1` | **nothing — manual** |
| `moderation-service` | Deployment | none | consumes `messages.v1`, produces 3 | **lag** (cap 512) |
| `review-service` | Deployment | `/v1/reviews` | produces `deletions.v1`, raw reads `flagged.v1` | CPU |
| `insights-service` | Deployment | `/v1/topics*` | consumes `messages.v1` + `flagged.v1` | **lag** (cap 512) |
| `clustering-service` | Deployment | none (`/internal/v1/assign`) | — | CPU |
| `clusters-job` | Deployment, **1 replica** | `/v1/topics/recluster` | produces `usage.v1` | **not autoscaled** |

Three of the choices in that table are load-bearing.

### `provider-adapter` is a StatefulSet

§7.2/A13 shards live connections onto a consistent-hash ring whose node names
are `ADAPTER_INSTANCE_ID`, defaulting to the hostname. A Deployment gives
every pod a fresh random name, so each rollout — and each individual pod
replacement — retires a ring member and introduces a new one, reshuffling a
segment and forcing those connections to reconnect for nothing. The compose
fragment says the same thing: the ID *"MUST be stable across restarts of the
same logical instance"*.

The StatefulSet gives `dabet-provider-adapter-0..N-1`, and `envRaw` wires
`ADAPTER_INSTANCE_ID` to `metadata.name`. Verified on a real cluster: delete
`dabet-provider-adapter-1` and it comes back with the same ring identity.

`podManagementPolicy: Parallel`, not `OrderedReady` — see
[Findings](#findings), it was changed after watching `OrderedReady` deadlock
the fleet on a broken pod-0.

Replica count is a **capacity** decision, not a load one:
`replicas >= ceil(active_connections / ADAPTER_SHARD_MAX_CONNECTIONS)` with
headroom. It is deliberately not autoscaled: every membership change
rebalances the ring.

### `clusters-job` is a Deployment, not a CronJob

`services/clusters-job/cmd/clusters-job/main.go` runs
`trigger.Scheduler.Loop(ctx)` — a sweep every `CLUSTERS_TRIGGER_INTERVAL`
over §8.6's four triggers — *and* serves `POST /v1/topics/recluster`
(JWT-authed, 202 + `job_id`) for the on-demand case. A CronJob has no HTTP
listener, so the on-demand trigger would simply not exist.

One replica, `strategy: Recreate`. The scheduler has no leader election: a
second instance would sweep the same triggers, start a duplicate clustering
pass for the same creator, and emit a second `usage.v1`
`messages_reclustered` event under a different `idempotency_key` — i.e.
charge the creator twice (§7.10, §5.7).

`RUN_ONCE=<creator>:<from>:<to>` turns the same binary into a one-shot that
never starts the scheduler or the API. That mode *is* a `Job`; this chart
does not render one.

### `review-service` scales on CPU, not lag

§7.6 makes a pending review a *position in a `flagged.v1` partition*, read
directly per request. It joins **no consumer group** (`queue.NewKafka`, not
`kafkax.Consumer`), commits no offsets, and publishes
`review_queue_lag_seconds`, not `kafka_consumer_lag_messages`. There is no
group lag for KEDA to read. It is request-driven, so CPU is the honest
signal.

---

## Probes — the part not to "fix"

```yaml
livenessProbe:  httpGet: { path: /healthz, port: http }   # process alive
readinessProbe: httpGet: { path: /readyz,  port: http }   # can serve
```

§4.5 defines the semantics and `pkg/obs/health.go` implements them: readiness
starts **true** and never flips on a dependency failure. Per P2 and §4.7 a
moderation-path service that has lost Redis, the LLM or `policy-service`
**keeps consuming and fails open**. §4.5 states the consequence outright:
*"`/readyz` returning 503 would remove it from service and stop chat, which is
exactly the wrong outcome."*

The failure this configuration exists to prevent is not a slow rollout. It is
an **outage amplifier**: a probe that notices a dependency outage, marks pods
unready or restarts them, and converts a degraded-but-serving pipeline into a
stopped one. So:

- **No dependency-aware readiness.** No exec probe checking Redis/Kafka/Postgres,
  no readiness endpoint that consults a backend.
- **No tightened thresholds.** The signals for a dependency outage are
  `fail_open_total` and `kafka_consumer_lag_messages` (§4.5, §4.7), both
  already scraped. A probe is not a monitoring tool.
- **No startup probe.** It is the one probe that can kill a pod that is up and
  correctly failing open, and no service here starts slowly enough to need one.

The liveness budget is `initialDelay 10s + 6 x 10s = 60s` of continuous
unresponsiveness before a restart, and it has to clear two things:

1. Under the congestion collapse measured in `test/load/README.md` a service
   is minutes behind on Kafka but still answers `/healthz` instantly off a
   separate goroutine — so it does not trip.
2. The services that bootstrap Postgres do so **before** starting the HTTP
   listener, so `/healthz` does not answer at all during that window.
   `provider-adapter` retries 10 x 3s (~30 s) then exits. 60 s is about 2x
   the worst pre-listener window. Shortening `initialDelaySeconds` or
   `failureThreshold` eats that margin and turns a slow-but-recovering
   database into a crash loop.

The same reasoning drives the ALB annotation in `values-aws.yaml`:
`healthcheck-path: /healthz`, never `/readyz`. A `/readyz` target group would
do exactly the thing the readiness probe is forbidden from doing.

---

## Config and secrets (§4.4)

| Where | What |
| --- | --- |
| `<release>-config` ConfigMap | the canonical §4.4 coordinates, `HTTP_ADDR`/`METRICS_ADDR`/`LOG_LEVEL`, `JWT_ALG`, every `KAFKA_CONSUMER_*` / `KAFKA_COMMIT_*` / `KAFKA_LAG_*`, and `OTEL_*` when tracing is on. Consumed by every pod with `envFrom`. |
| `<release>-<service>-config` ConfigMap | that service's own tunables — `MOD_*`, `ADAPTER_*`, `CLUSTERS_*`, `INSIGHTS_*`, `REVIEW_*`, `CREDITS_*`, `MAIL_*`, `OAUTH_*` URLs, `APP_*`. |
| `<release>-secrets` Secret | every credential. Referenced **key by key** with `secretKeyRef`, never with an `envFrom: secretRef` — that would put the Stripe key and all six OAuth client secrets into all nine pods. |

`POSTGRES_DSN`, `CLICKHOUSE_DSN`, `S3_ACCESS_KEY` and `S3_SECRET_KEY` carry
credentials and are **never** in a ConfigMap.

`services.<name>.secretEnv` maps an env var name to a key in the Secret. That
indirection is what makes §3's *"separate clusters for identity/policy"* a
values change: `user-service` reads `POSTGRES_DSN` from key
`POSTGRES_DSN_IDENTITY` while `policy-service` reads it from
`POSTGRES_DSN_POLICY`. Any `POSTGRES_DSN_*` key left empty falls back to
`POSTGRES_DSN`, so a single shared database is also one line.

### Three interchangeable secret sources

All three produce a Secret with the **same name and the same keys**, so the
pod specs are byte-identical between them:

```yaml
secrets.create: true              # plain Secret from values — bare-cluster fallback
externalSecrets.enabled: true     # External Secrets Operator (set secrets.create=false)
secretsStoreCSI.enabled: true     # Secrets Store CSI driver with syncSecret
```

ESO is the recommended path and what `values-aws.yaml` uses. The CSI driver
only materialises its synced Secret while a pod mounts the CSI volume, which
this chart's workloads do not do — see the comment in
`templates/secretproviderclass.yaml`.

**Rotation does not restart pods.** Env vars are read once at startup, so an
ESO refresh updates the Secret and changes nothing until a rollout. Deploy
something like Reloader, or roll deliberately.

### RS256 (§5.4)

`jwt.alg: RS256` switches the whole release. `user-service` — the only issuer
— mounts the **private** key; every other service that verifies mounts the
**public** key. Both are mounted as **files** and read through
`JWT_PRIVATE_KEY_FILE` / `JWT_PUBLIC_KEY_FILE`, so the PEM never appears in an
env var, a process listing, or `kubectl describe pod`.

`moderation-service`, `provider-adapter` and `clustering-service` get neither:
they call `httpx.VerifierFromEnv` nowhere, because they have no
creator-facing HTTP surface to protect.

`jwt.alg: HS256` (the default) keeps a bare install working with nothing but
`JWT_SECRET`. Rendering fails if RS256 is selected without both PEMs.

Rotation, since nothing automates it: publish the new **public** key to every
verifier first — each verifier accepts exactly one key at a time, so run the
old key until every verifier has the new one — then switch the issuer.
`jwt.kid` rides in the token header to make that sequencing auditable.

---

## Autoscaling on lag

§4.7 makes growing lag *the* overload signal, and `test/load/README.md` F3
explains why lag beats CPU: the moderation cascade is **latency**-bound (four
sequential Redis round trips plus a policy lookup), so an instance can be
minutes behind at low CPU. A CPU HPA under-scales exactly when it matters.

| `autoscaling.mode` | Signal | Trade-off |
| --- | --- | --- |
| `keda`, `trigger: kafka` *(default)* | broker offsets | Authoritative, and independent of the monitoring stack — the autoscaler keeps working during an incident, which is when it is needed. Costs a KEDA operator and, on a non-plaintext broker, credentials for the scaler. |
| `keda`, `trigger: prometheus` | `kafka_consumer_lag_messages` | Autoscaler and pager read the same number; no broker credentials. Inherits Prometheus's staleness (`KAFKA_LAG_INTERVAL` 15s + a 30s scrape ≈ 45s) and its availability. |
| `hpa` | CPU | No operator, works anywhere. A genuinely worse signal for the consumers, and the *right* signal for the request-driven services. |
| `none` | — | Fixed replicas. |

**The ceiling is the partition count.** A consumer group cannot usefully
exceed it — surplus members own nothing. `dabet.maxReplicas` clamps
`maxReplicas` to `kafka.topics.<topic>.partitions`, so §4.2's numbers are
enforced rather than merely documented:

| Service | Topic | Ceiling |
| --- | --- | --- |
| `moderation-service` | `messages.v1` | **512** |
| `insights-service` | `messages.v1` (+ `flagged.v1`) | **512** |
| `credits-service` | `usage.v1` | **32** |

Ask for 5000 and you get 512. Ask for a `minReplicas` above the ceiling and
the render fails.

Scaling to zero is refused: a consumer at zero accrues lag with nothing
draining it, and §4.7 forbids shedding.

> Note: when a service is autoscaled the chart omits `replicas` from the
> workload (otherwise every `helm upgrade` resets it and hands it back to the
> HPA, dropping capacity mid-backlog). Kubernetes therefore defaults a
> **fresh** install to 1 replica until the autoscaler's first reconcile pulls
> it up to `minReplicas`.

### Capacity, stated plainly

`test/load/README.md` measured **170–200 msg/s per `moderation-service`
instance**. N6's 50 000 msg/s baseline is therefore ~250–300 replicas, and
the 500 000 msg/s peak ~2 500–3 000 — **above §4.2's 512-partition ceiling**,
which caps the consumer group at 512 members. Reaching N6's peak needs more
partitions on `messages.v1`, or a faster cascade. Neither is a chart setting.

---

## Graceful shutdown

`terminationGracePeriodSeconds` must exceed `KAFKA_CONSUMER_DRAIN_TIMEOUT`
(30s default) plus the runner's 10s HTTP shutdown. Below that, the kubelet
SIGKILLs a pod with handlers still running: those records were never
committed, so they are redelivered and re-processed. At-least-once tolerates
it (P3), but it is silent duplicated work on **every rollout**, and the usual
"fix" is to shorten the grace period further, which makes it worse.

`_helpers.tpl` **fails the render** rather than shipping that, for any service
with `consumer.enabled`. Raising `kafka.consumer.drainTimeoutSeconds` without
raising the grace periods is caught the same way.

| Service | Grace | Why |
| --- | --- | --- |
| `provider-adapter` | 90s | Drain `deletions.v1`, let the ring rebalance, and a half-closed platform websocket costs a reconnect. |
| consumers | 60s | 30s drain + 10s HTTP + margin. |
| the rest | 30s | HTTP shutdown only. |

---

## Ingress

Only genuinely public routes are published, **path by path**. A `/`-per-service
catch-all would publish two internal surfaces:

- `credits-service` `/internal/v1/credits-ok/{creator_id}` — the §5.8 check
  `moderation-service` makes in-cluster. Public, it is an unauthenticated
  oracle for any creator's credit state.
- `clustering-service` `/internal/v1/assign` — takes an embedding and writes
  topic assignments, with no auth at all.

Three services get **no Ingress under any values**:

- `moderation-service` — registers nothing beyond `/healthz` and `/readyz`.
- `provider-adapter` — registers `POST /mock/messages` and
  `GET /mock/deletions` **unconditionally**, an unauthenticated injection
  surface that would let anyone push chat messages into any creator's
  moderation pipeline.
- `clustering-service` — see above.

**Path precedence.** `insights-service` serves `GET /v1/topics*` and
`clusters-job` serves `POST /v1/topics/recluster`. The recluster rule is
`pathType: Exact` so it out-ranks the insights `Prefix` rule; the template
emits Exact rules first so the ordering also reads correctly to a human
debugging a 404. ingress-nginx and the AWS Load Balancer Controller both
implement Exact-beats-Prefix; verify on any other controller, or split the two
onto separate hosts.

---

## Observability

`metrics.serviceMonitor.enabled` renders a `ServiceMonitor` (or `PodMonitor`)
against the **`metrics` port by name** — §4.5's `:9090/metrics` is a separate
listener from the app port. Guarded, because rendering a ServiceMonitor on a
cluster without the Prometheus Operator CRDs is a hard `no matches for kind`
failure at install time.

`PodMonitor` suits `provider-adapter`, whose per-pod identity is meaningful.

**Cardinality.** §4.5 forbids `message_id` / `author_id` / `content_id` labels
and permits `creator_id` only on `credits_*`. `review-service`'s
`review_queue_lag_seconds` carries `creator_id` by design (§7.6.3 requires
per-creator lag) — at N5's 10M creators that is a series count Prometheus will
not enjoy. `metrics.serviceMonitor.metricRelabelings` is where to drop or
aggregate it; the chart does not do it for you, because dropping a mandated
metric should be a deliberate act.

Tracing (§4.5) is off unless `tracing.endpoint` is set — `pkg/tracing`
installs the no-op provider and exports nothing. Default sample ratio 0.01,
parent-based.

---

## The dependency interface

`values-local.yaml` assumes `deploy/k8s/charts/dabet-deps` is installed as
release **`dabet-deps`** in the same namespace, exposing:

| Values key | Expected Service | Port |
| --- | --- | --- |
| `config.kafkaBrokers` | `dabet-deps-kafka` | 9092 |
| `config.redisAddr` | `dabet-deps-redis-master` | 6379 |
| `config.memcachedAddrs` | `dabet-deps-memcached` | 11211 |
| `config.vllmEndpoint` | `dabet-deps-mockllm` | 8089 |
| `config.embeddingEndpoint` | `dabet-deps-mockembed` | 8091 |
| `config.milvusAddr` | `dabet-deps-milvus` | 19530 |
| `config.s3Endpoint` | `dabet-deps-minio` | 9000 |
| `secrets.data.POSTGRES_DSN` | `dabet-deps-postgresql` | 5432 |
| `secrets.data.CLICKHOUSE_DSN` | `dabet-deps-clickhouse` | 9000 (native) |
| `user-service`/`provider-adapter` OAuth URLs | `dabet-deps-mockoauth` | 9099 |
| `credits-service` `STRIPE_API_BASE` | `dabet-deps-mockstripe` | 9098 |

Different names? Override the values — nothing in the templates hard-codes a
dependency name.

The deps chart also owns **topic creation**. Nothing in this chart creates
Kafka topics, and `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is false in compose.
`messages.v1`, `flagged.v1`, `deletions.v1` and `usage.v1` must exist with the
§4.2 partition counts before the consumers start, and those counts must match
`kafka.topics.*.partitions` here or the autoscaling ceilings are wrong. The
adapter's coordination topic `adapter.shards.v1` is the exception — it creates
itself on startup with broker defaults.

---

## The Terraform interface

What `values-aws.yaml` expects the Terraform layer to provide.

### 1. One Secrets Manager document

`externalSecrets.dataFrom[0].extract.key` (default `dabet/prod/app`), a JSON
document whose **keys are exactly these**. Every one must be present — a
`secretKeyRef` to a missing key blocks the pod from starting, which is the
intended fail-fast.

| Key | Contents |
| --- | --- |
| `POSTGRES_DSN` | fallback DSN for any service without a per-domain key |
| `POSTGRES_DSN_IDENTITY` | RDS `identity` — `user-service`, `provider-adapter` |
| `POSTGRES_DSN_POLICY` | RDS `policy` — `policy-service` |
| `POSTGRES_DSN_BILLING` | RDS `billing` — `credits-service` |
| `POSTGRES_DSN_REVIEW` | `review-service` |
| `CLICKHOUSE_DSN` | `insights-service`, `clustering-service`, `clusters-job` |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | **static IAM user keys** — see finding 3 |
| `JWT_PRIVATE_KEY` | RS256 signing key, PEM. `user-service` only |
| `JWT_PUBLIC_KEY` | RS256 verifying key, PEM. Every verifier |
| `JWT_SECRET` | unused under RS256; keep present so `jwt.alg` can be flipped back |
| `STRIPE_SECRET_KEY`, `STRIPE_WEBHOOK_SECRET` | **required** — `credits-service` exits without them |
| `OAUTH_YOUTUBE_CLIENT_ID` / `_SECRET` | §5.5 |
| `OAUTH_TWITCH_CLIENT_ID` / `_SECRET` | §5.5 |
| `OAUTH_DISCORD_CLIENT_ID` / `_SECRET` | §5.5 |
| `OAUTH_MOCK_CLIENT_ID` / `_SECRET` | may be empty in a real environment |
| `MAIL_SMTP_USERNAME`, `MAIL_SMTP_PASSWORD` | SES SMTP credentials; may be empty |

Empty string is a legitimate value (`MAIL_SMTP_USERNAME: ""` means anonymous
SMTP). Absent is not.

### 2. A `ClusterSecretStore`

Named by `externalSecrets.secretStoreRef.name` (default
`aws-secrets-manager`), bound to an IRSA role with
`secretsmanager:GetSecretValue` on that document. Terraform owns both the
store and the role.

### 3. IRSA role ARNs, one per service

Annotation key `eks.amazonaws.com/role-arn`, placed at
`services.<name>.serviceAccount.annotations`. The ServiceAccount names the
chart creates are:

```
dabet-user-service        dabet-credits-service     dabet-policy-service
dabet-provider-adapter    dabet-moderation-service  dabet-review-service
dabet-insights-service    dabet-clustering-service  dabet-clusters-job
```

(`<release>-<service>`; `dabet` is the default release name.) The trust policy
subject is `system:serviceaccount:<namespace>:<name>`.

### 4. Endpoints

Non-secret, so they go in `config.*`, not the Secret:
`kafkaBrokers` (MSK, plaintext listener), `redisAddr` (ElastiCache primary),
`memcachedAddrs` (ElastiCache configuration endpoint), `s3Endpoint`
(regional, e.g. `https://s3.eu-west-1.amazonaws.com`), `s3Bucket`,
`vllmEndpoint` and `embeddingEndpoint` (in-cluster Services in front of the
GPU node group), `milvusAddr`.

### 5. Images

ECR at `<account>.dkr.ecr.<region>.amazonaws.com`, one repository per service
under `global.image.repository` (default `dabet`), i.e.
`.../dabet/moderation-service:<tag>`. Immutable tags.

### 6. Ingress

`ingress.className: alb` with the ACM certificate ARN in
`alb.ingress.kubernetes.io/certificate-arn`, and a Route 53 record for
`ingress.host`. The ALB health check must be **`/healthz`** — see
[Probes](#probes--the-part-not-to-fix).

### 7. Operators

KEDA (for `autoscaling.mode: keda`), External Secrets Operator, and the
Prometheus Operator (for `metrics.serviceMonitor.enabled`). Each is guarded:
the chart installs cleanly without any of them, with the corresponding
feature off.

---

## Findings

Things the code forces on the infrastructure. **No code was changed** — §3
says topology is configuration only, and where that is not achievable it is
recorded here instead.

**K1 — `pkg/kafkax` supports neither SASL nor TLS.** The franz-go client is
built with no auth or transport options and exposes no environment variable
for either, so the broker must be reachable over **PLAINTEXT**. MSK with a
plaintext in-VPC listener works; **MSK Serverless, which is IAM-auth only,
cannot be used at all**. This is the clearest case where §3's "configuration
only" does not hold: a TLS/SASL broker needs `kgo.SASL(...)` and
`kgo.DialTLSConfig(...)` wired to new env vars.

**K2 — Redis is not cluster-aware.** `moderation-service` uses
`redis.NewClient`, not `NewClusterClient`, and sets no `TLSConfig`. §3's
target column says *"Redis Cluster, sharded by `hash(author_id, content_id)`"*
and §4.3 says the hash tags exist from day one *"so the cluster migration is a
config change"* — the hash tags are there, but the **client** is not. A real
Redis Cluster returns `MOVED` redirects that this client does not follow. So
the target must be ElastiCache with **cluster mode disabled** (one shard, plus
replicas), **encryption in transit off**. Reaching a genuinely sharded Redis
needs a client-type change or a cluster-aware proxy.

**K3 — S3 cannot use IRSA.** `insights-service` and `clusters-job` build their
MinIO client with `credentials.NewStaticV4(accessKey, secretKey, "")` — static
keys, **no session token**, no fallback to the AWS credential chain, and
path-style addressing with no region. IRSA hands out *temporary* STS
credentials, which require a session token, so an IRSA role cannot reach the
bucket. Terraform must provision an IAM user with a long-lived access key
scoped to the embeddings bucket. The IRSA annotations are still wired so that
switching to `credentials.NewIAM("")` later is a one-line code change and no
chart change.

**K4 — the Postgres bootstrap precedes the HTTP listener.** Services that use
Postgres retry it (10 x 3s in `provider-adapter`) *before* `svc.Run` starts
serving, so `/healthz` does not answer during that window. Not a bug — but it
is why the liveness budget is 60s and why it must not be tightened.

**K5 — `podManagementPolicy: OrderedReady` deadlocks the adapter fleet.**
Found on the kind cluster, not by reading: with `OrderedReady`, an
`adapter-0` that cannot start means `adapter-1..N` are never created, so one
unhealthy member takes the whole ingestion tier down — head-of-line blocking
on the fail-open path, which N2 and P2 both forbid. Changed to `Parallel`.
`podManagementPolicy` governs create/scale ordering only; updates still roll
one pod at a time in reverse ordinal order under `updateStrategy`, so nothing
is lost.

**K6 — a Prometheus metric family is absent until first observed.** A healthy
service exports no `fail_open_total` series at all (it is a `CounterVec`),
and `kafka_consumer_lag_messages` appears only after the first lag sample.
Alerts must treat absent as zero and must not use `absent()` as a health
signal. Already noted in the repo README's known gaps.

**K7 — `review-service` publishes no consumer lag.** It reads `flagged.v1`
partitions directly rather than joining a group (§7.6), so lag-based
autoscaling does not apply to it. It is on a CPU HPA instead.

---

## Validated

Against a real `kind` cluster (Kubernetes v1.36.1) with the KEDA,
Prometheus Operator and External Secrets CRDs installed:

- `helm lint` clean for `values.yaml`, `values-local.yaml`, `values-aws.yaml`.
- `helm template` renders for all three, plus every optional feature on
  (ServiceMonitor, PodMonitor, NetworkPolicy, Ingress, ESO, KEDA, RS256).
- `kubectl apply --dry-run=server` accepts all 52 objects of the AWS render
  and all 58 of the full local render, CRD-backed kinds included.
- `helm install -f values-local.yaml` reaches `helm exit=0` with pods Ready.
- `moderation-service`, `clustering-service` and both `provider-adapter`
  replicas reach **Ready with no Kafka, no Postgres, no Redis and no LLM
  reachable** — the fail-open probe property, under a real kubelet.
- `ADAPTER_INSTANCE_ID` resolves to `dabet-provider-adapter-{0,1}` and
  **survives pod deletion unchanged**; headless per-pod DNS resolves.
- ConfigMap/Secret env lands in the container; `moderation-service` has no
  `POSTGRES_DSN`, no `JWT_SECRET`, no S3 keys and no `CLICKHOUSE_DSN`.
- `readOnlyRootFilesystem` enforced (`touch /nope` fails, `/tmp` writable),
  running as `uid=65532(dabet)`.
- 16 render-time guards fire on bad input (grace period, secret ownership,
  RS256 keys, unsupported alg, partition ceiling, KEDA trigger, tracing
  endpoint, empty ingress).
- Env completeness diffed against `deploy/compose/docker-compose.yml` and all
  five fragments: no compose variable is unaccounted for. The narrowing is
  deliberate — compose applies `x-infra-env` to every service, this chart
  gives each service only what its code reads.
