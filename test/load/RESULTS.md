# Load campaign — 2026-08-21, Kubernetes

Re-run of the earlier compose campaign on a real cluster, with a rig built to
the standard the earlier one failed: clean state per rung, generator
utilisation recorded, and every limit attributed to a named component before
it is quoted.

## Deployment shape

| | |
| --- | --- |
| Cluster | k3s, single node, 20 cores / 30 GB |
| Under test | 1 × `moderation-service` (limits raised to 8 cores for the campaign) |
| Broker | 3 Kafka brokers, RF 3, **co-located on the same node** |
| Topics | `messages.v1` 12 partitions, `flagged.v1` / `deletions.v1` 6, `usage.v1` 3 |
| Other deps | 3-shard Redis cluster, 2 Postgres, ClickHouse, MinIO, Memcached |
| Load source | Separate machine, wired LAN, 0.38 ms RTT, ~50 MB/s |
| Path to system | Per-broker NodePorts. **No port-forward in the measured path** |

## Unit costs — the portable numbers

Taken from the clean, non-saturated 500 msg/s rung and cross-checked against
the saturated rung, which agrees to within 5%:

| Measure | Value |
| --- | --- |
| CPU per unit of throughput | **~1.3 millicores per msg/s** |
| Throughput per core | **~740–770 msg/s** |
| Moderation memory | ~110 MB resident (limit 8 cores / 2 GB; **consumption, not limit**) |

Both rungs agree: 682 m at 500 msg/s (1.36 m per msg/s) and 1 302 m at
1 000 msg/s (1.30 m per msg/s). Extrapolating the N6 baseline of 50 000 msg/s
gives roughly **65 cores of moderation**; the 500 000 msg/s peak gives ~650.
That extrapolation assumes the message mix below and a broker that is not
itself the constraint — see *Design boundaries*.

## Capacity

| Rung | Delivery | p50 | p95 | Under 1.5 s | Moderation CPU | Generator CPU |
| --- | --- | --- | --- | --- | --- | --- |
| 500 msg/s | **100%** (45 000/45 000) | 1.6 ms | **3.6 ms** | **100%** | 682 m | 68% of one core |
| 1 500 msg/s | 100% eventually | 8 126 ms | 15 466 ms | 6.5% | 1 302 m | 133% |

Steady-state consumer capacity is **~1 000 msg/s per replica**. Above it the
service still loses nothing — it queues, drains afterwards, and reports the
backlog — which is §4.7 behaving as written ("accept the lag"), not loss.

At 500 msg/s the p95 is **3.6 ms against a 2 000 ms budget** — 0.18% of it —
and every verdict landed inside the target.

Generator utilisation peaked at 1.5 of 8 cores with send scheduling accurate to
0.9 ms p95, and its self-benchmark measured 976 417 msg/s. Nothing here is
generator-bound.

### Cascade economics (135 000-message rung)

23 092 flagged: rate limit 15 777, restricted word 3 608, duplicate 2 530,
restricted content (LLM) 1 177. 4 261 messages were seeded to trigger the LLM
and the sampler admitted 1 177 — the §7.5 ceiling doing exactly what it is for.
`fail_open_total` was 0 throughout: nothing passed unmoderated.

## Attribution — what actually binds

**Kafka, not the application.** With its CPU limit raised to 8 cores,
moderation used **1.2–1.3** and could not exceed 1 000 msg/s, while the three
brokers drew **6.7 cores between them**. An application with six spare cores
that will not go faster is waiting on something else.

Three co-located brokers is not a realistic topology — in production they are
separate machines — so this ceiling is a property of the test rig, not of the
design. Every throughput figure above is therefore a **floor**.

## Defects found

1. **Moderation was CPU-throttled at its own limit.** `values-local.yaml` set
   `limits.cpu: 1` while the service draws ~1.3 cores at saturation, so it ran
   at 81–96% of its ceiling and was throttled. Every earlier number on this
   cluster measured that limit rather than the software. Raised to 4 cores in
   the chart, with the measurement recorded next to it.
2. **State was not reset between rungs.** The first ladder left 2.45 GB of
   retained log accumulating across runs, and identical configurations drifted
   from 12 ms to 50 s p95 as it grew. The harness now deletes and recreates all
   four topics and restarts the consumer between rungs.
3. **Debug tunnels in the measured path.** The first attempt drove APIs and
   scraped metrics through `kubectl port-forward`, which dropped under load and
   silently corrupted a delivery figure (reported 0% consumed). Replaced with
   NodePorts, and Kafka external access was added to the chart properly, each
   broker advertising its own NodePort.

## Rejected approach — per-key concurrency

Fanning each partition out to N key-routed workers (`KAFKA_CONSUMER_KEY_CONCURRENCY`)
was implemented on the reasoning that §7.3 requires ordering per
`(sender, content)` — the record key — not per partition, so unrelated senders
need not queue behind each other. The ordering argument is sound and unit-tested.

**It does not help here, and the measurement says why.** At 1 500 msg/s with the
throttle removed:

| | concurrency 1 | concurrency 8 |
| --- | --- | --- |
| Consume rate | 1 000/s | 1 000/s |
| Moderation CPU | 1 302 m | 1 188 m |
| p95 | 15 466 ms | 19 399 ms |

Identical throughput, no CPU relief, slightly worse latency. Widening
parallelism inside a component that is already waiting on the broker cannot buy
anything, and the extra machinery — per-partition coordinator, per-worker
queues, low-water-mark commit tracking — is overhead in that regime.

**It stays in the codebase, off by default.** The premise still holds where the
broker has headroom and moderation is provably the constraint; the signal to
look for is `kafka_consumer_lag_messages` rising while moderation CPU sits well
below its limit. It cannot be validated on a single-node Kafka, and that is the
honest state of it.

## Design boundaries

- **Latency was measured with a stubbed model.** `tools/mockllm` answers
  instantly. Only ~1% of messages reach the LLM stage, so the p95 is unaffected,
  but a real model would move the tail substantially. The separate
  latency-injection mode exists for that and was used in the compose campaign
  (see README).
- **Throughput figures are floors**, bounded by co-located brokers.
- **One replica, one node.** Multi-replica elasticity and rebalance-under-load
  were not measured in this campaign.

## Reproducing

Harness: `test/load/`, campaign driver in this directory's README. Structured
results are written as JSON per run; raw output is gitignored.
