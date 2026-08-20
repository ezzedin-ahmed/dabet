# `test/load` — load and stress harness

Dabet is designed for **50 000 msg/s baseline and 500 000 msg/s peak, heavily
hot-spotted** (N6), with **p95 end-to-end latency under 1.5 s** (N1). Before
this harness existed the system had processed thirteen messages, all of them
from `test/e2e`.

This module offers synthetic load, measures what comes out, and says plainly
whether the answer met the target. It adds no metrics to any service: the
measurement channel is scrape-and-diff over the `/metrics` endpoints that
already exist, plus the broker's own offsets.

---

## Quick start

```sh
# 1. Bring the stack up WITH the load overlay (realistic partitions, a slow
#    LLM, three moderation consumers). The base stack cannot be meaningfully
#    load tested — see "Why the overlay exists" below.
docker compose -f deploy/compose/docker-compose.yml \
               -f deploy/compose/fragments/load.yml up -d --build --wait

# 2. Point the harness at all three moderation replicas.
export LOAD_MODERATION_METRICS=http://localhost:9085,http://localhost:9185,http://localhost:9285

# 3. Run something.
go run ./cmd/dabet-load -list                       # the catalogue
go run ./cmd/dabet-load -scenario selfbench         # the generator's own ceiling
go run ./cmd/dabet-load -scenario baseline -rate 400 -out results/
go run ./cmd/dabet-load -scenario ramp              # find the knee
go run ./cmd/dabet-load -scenario failopen          # needs docker

# 4. Tear down.
docker compose -f deploy/compose/docker-compose.yml \
               -f deploy/compose/fragments/load.yml --profile clustering down -v
```

`-rate` rescales a scenario's profile so its **peak** becomes the given rate
(a staircase stays a staircase). `-duration` rescales its length. `-out DIR`
writes the machine-readable result; the summary table always goes to stdout.

**Run `selfbench` on any new machine before trusting a number.** Every other
scenario reports its achieved rate against the generator's ceiling, and a run
that approaches that ceiling measured the harness rather than Dabet.

---

## Why the overlay exists

Three properties of the base stack make load numbers taken against it
meaningless. `deploy/compose/fragments/load.yml` changes exactly those three.

**Partitions.** `kafka-init` creates every topic with 3 partitions; §4.2
targets 512 for `messages.v1`. Partition count is the hard ceiling on how many
`moderation-service` instances can share the work, so a 3-partition topic caps
the system at three consumers no matter how many you run. The overlay raises
`messages.v1` to 64 (`LOAD_MESSAGES_PARTITIONS`). Raising a partition count
**rekeys the topic** — a key may move — which breaks §7.3's guarantee that one
consumer owns all Redis state for a `(sender, content)` pair while records are
in flight. That is acceptable on a throwaway stack and would not be in
production; `down -v` between runs keeps it honest.

**The LLM mock.** `tools/mockllm` answers instantly, so the §7.9 batcher and
the §7.5 sampler never do anything: batches leave at size 1 on the linger
timer and no queue ever forms. The overlay turns on the latency injector
(`tools/mockllm/latency.go`) — a lognormal fitted to a p50/p99, a per-message
term, a concurrency ceiling, and optional error/timeout injection. All of it
is off unless `MOCKLLM_*` is set, so `test/e2e`'s deterministic FLAGME
behaviour is untouched.

**Consumers.** `moderation-service` processes records on **one goroutine** —
`kafkax.Consumer.Run` polls and iterates the fetch serially — so a single
instance is single-threaded regardless of partition count. The overlay adds
two more instances in the same consumer group.

---

## How the harness works

**The generator produces to `messages.v1` directly**, bypassing
`provider-adapter`. That is not a shortcut: the adapter's ingress is one HTTP
request per message and saturates two orders of magnitude below the design
target (measured below), so driving load through it would measure the ingress
and never reach moderation. `-scenario adapter` measures that hop separately.

**Partition keys come from `pkg/contracts.MessagesKey`**, so records land
exactly where production would put them. This is what makes the hot-spot
scenario's partition imbalance a real property of the keying scheme rather
than an artefact of the harness.

**The population is hot-spotted by construction.** Contents are drawn
Zipfian with a configurable skew, authors within a content are drawn Zipfian
too, and the number of senders in a content scales with its share of traffic —
because a stream taking 13% of all messages does not have forty people in it.
An explicit-weight mode pins individual contents to exact rates, which is how
the sampler scenario reproduces §7.5's coverage table and how the hot-spot
scenario builds one enormous content behind four senders.

**Rate control is coordinated-omission-free.** Message *i* is due at
`Start + Schedule.At(i)`, computed from the rate profile before the run
starts. Nothing recomputes it from `time.Now()`. When something stalls, the
stall lands as lag on the *following* messages instead of sliding the
schedule — which is exactly the difference between measuring a system and
flattering it. The intended send time is what goes into `ingested_at`, so
moderation-service's own SLI histogram inherits the correction.

**The SLI is measured twice.** `moderation_e2e_latency_seconds` is the
contractual SLI of §4.6 and is read off `/metrics` — but it has eleven buckets
and the 1.5 s N1 target falls *between* the 1 s and 2.5 s bounds, so a p95
near the target is decided by interpolation rather than by data. The harness
therefore also tails `flagged.v1` and measures `flagged_at − intended_send` at
microsecond resolution. `flagged.v1` carries no `ingested_at` (§4.2), so the
intended send time is round-tripped through the opaque `message_id` the
harness mints — an id only the harness parses, so P5 is intact.

**Consumer lag is read from the broker,** with `kadm`, not from
`kafka_consumer_lag_messages`. That metric is declared in `pkg/obs` and set by
no service, so the family is absent from every `/metrics` response (finding F1
below). §4.7 makes growing lag *the* overload signal, so the harness measures
it itself.

---

## The scenarios

| scenario | what it proves |
| --- | --- |
| `selfbench` | the generator's ceiling on this machine, with no broker and no consumers. Run it first. |
| `baseline` | steady-state health and the latency floor: lag flat, fail-opens zero, p95 inside the N1 budget. |
| `ramp` | the knee — a staircase, not a smooth ramp, so each plateau reaches its own steady state and the knee can be attributed to a rate. |
| `hotspot` | partition-level imbalance under the N6 shape: one content at 60% of traffic behind four senders, against a long tail. |
| `sampler` | §7.5's coverage table, measured. Four contents pinned to 1/20/100/6000 msg/min. |
| `failopen` | §4.7: kill Redis, the LLM and policy-service mid-run; the pipeline must keep consuming, count the fail-opens, and never stop. Requires docker. |
| `adapter` | the `POST /mock/messages` ingress ceiling, measured on its own. |

Each carries a written hypothesis (`-list` prints them) and its own pass/fail
criteria, so a failure names the thing that actually broke.

The `sampler` scenario deserves a note on method. Per-content LLM coverage is
not observable from metrics — §4.5's cardinality rule rightly forbids a
`content_id` label. So the run sends **only LLM-flag text**: every message
that survives the sampler and reaches stage 9 produces a `flagged.v1` event,
and that event carries `content_id`. Verdicts per content over messages sent
to it *is* the coverage.

---

## Reading the output

```
OFFERED       what the schedule asked for and what went out. `send lag`
              is the generator's own backlog against the ideal clock; if it
              is not tiny, the run measured the harness.
MODERATION    /metrics deltas. Outcomes, detector hits, the SLI histogram
              with its bucket bracket, batch sizes, and fail_open_total.
VERDICTS      the same latency taken off flagged.v1 at full resolution.
KAFKA         partitions, lag peak/final/slope, and how evenly this run's
              records spread across partitions.
CHECKS        the scenario's criteria. `[run validity]` marks a check whose
              failure means the run did not measure what it claims.
```

Three numbers decide most runs:

- **`lag slope`** — positive and sustained is the §4.7 overload signal. The
  spec says to accept lag, so lag is not a failure by itself; *unbounded
  growth* is.
- **`fail_open_total`** — §4.5 calls it the single most important metric in
  the system and says it must be zero in steady state.
- **`fraction under 1.5s`** — the N1 check, taken from bucket counts with no
  interpolation.

Two things the table will show that are **artefacts of direct-to-Kafka mode**,
not bugs:

- `provider-adapter fail_open{deletion_no_connection}` climbs, and its log
  says `opaque: unknown platform tag`. The harness mints its own `content_id`s
  rather than adapter-issued opaque ones, so verdicts on `deletions.v1` cannot
  be routed back to a platform. Only moderation's fail-opens count toward the
  criteria.
- `insights-service` and `review-service` consume the same topics and appear
  in the service table; they are reported for completeness.

---

## Known limits of laptop-scale results

The reference machine is 8 cores and ~15 GB RAM running a single-broker Kafka,
Postgres, Redis, ClickHouse, MinIO, four mocks and eleven Go services **on the
same host as the load generator**. Kafka alone took 235% CPU during a 2 000
msg/s run.

So:

- **Absolute throughput is not portable.** It says what this laptop does, not
  what a cluster does. What *is* portable is the shape: where lag turns over,
  how the population's skew maps onto partitions, and how each dependency
  degrades.
- **Per-instance capacity is the transferable number.** Divide the measured
  ceiling by the number of `moderation-service` replicas and you have
  something you can multiply back up — subject to Redis and the LLM scaling
  with it, which on one laptop they do not.
- **The generator competes with the system for cores.** `selfbench` is run
  with nothing else executing, so it overstates the headroom available during
  a real run. It is still two to three orders of magnitude above the rates
  used, so the conclusion holds.
- **The LLM is a mock with a chosen latency.** Every LLM number below is a
  statement about the *interaction* between that latency and the timeout,
  batching, and sampler settings — which is precisely what needed testing.
  It is not a statement about any real model's speed.
- **The host clock must not move.** The harness stamps the intended send time
  on the host and reads `flagged_at` from inside a container; an NTP
  correction or a suspend/resume between the two turns a 0.6 s message into an
  8-hour one. The tailer discards implausible samples, counts them as
  `clock_skewed_samples`, and fails the run's validity check. (This happened
  during development; it is why the guard exists.)

---

## Re-tuning A17 (sampler) and A18 (batching) from the numbers

These are the GPU-spend levers, so this is the section the harness exists for.

### A17 — sampler: 30 tokens/min, capacity 30, per content

Run `-scenario sampler` and read `extra.sampler_coverage` from the JSON. It
gives, per content, the offered rate and the measured LLM coverage, next to
§7.5's own table.

The number that matters is not the coverage percentage but the **absolute LLM
admission rate**, because §7.5's whole claim is that *LLM load is bounded by
the number of active contents, not by message volume*:

```
admitted_msg_per_s  ≈  active_contents × MOD_SAMPLER_REFILL_PER_MIN / 60
llm_batches_per_s   ≈  admitted_msg_per_s / observed_mean_batch_size
concurrent_gens     ≈  llm_batches_per_s × observed_llm_p50_seconds
```

Take those three lines to a GPU budget. If `concurrent_gens` exceeds what the
fleet can serve, the sampler ceiling is too high and A17 must come down —
`MOD_SAMPLER_REFILL_PER_MIN` and `MOD_SAMPLER_CAPACITY` are both environment
variables. Lowering it costs coverage only on **busy** content, which is
exactly the trade §7.5 says it is making: a violation in a firehose is caught
by a sample, one in a quiet channel is not.

Verify after retuning by re-running the scenario and checking that the quiet
rows still read ~100%. If they do not, the loss is *not* the sampler — check
`llm_requests_total{outcome="error"}` first (see A18).

### A18 — LLM batch: 32 messages or 50 ms, 1 000 ms timeout

Two independent things to read.

**The batch-size trigger.** Compare `llm_batch_size_mean` against 32. The
batcher releases on whichever trigger fires first, and the 32-message trigger
only fires when a single policy accumulates 32 messages within the linger
window:

```
messages_per_batch ≈ admitted_msg_per_s / instances × linger_seconds
```

If the measured mean is far below 32, the 50 ms linger is the binding trigger
and the policy rubric — the reason for batching by policy at all — is being
re-sent every few messages. Fix by **raising `MOD_LLM_LINGER`** until the mean
approaches the size trigger, or by **lowering `MOD_LLM_BATCH_SIZE`** to
whatever the arrival rate can actually fill. Raising the linger spends latency
budget; the budget for it is whatever §4.6 leaves after the LLM's own p50, so
compute the headroom before spending it.

**The timeout.** Compare `MOD_LLM_TIMEOUT` against the LLM's measured latency
distribution (`llm_latency_seconds`, and the mock's own `p99`). §7.9 fails
the **whole batch** open on timeout, with no retry, so the fraction of batches
over the timeout is multiplied by the batch size to get unmoderated messages:

```
unmoderated_per_s ≈ llm_batches_per_s × P(latency > timeout) × mean_batch_size
```

A timeout set equal to the service's *median* makes that fraction ~50% by
construction. Either raise `MOD_LLM_TIMEOUT` (spending latency budget, and
§4.6 has none spare) or make the LLM faster — a smaller model, more replicas,
or a lower sampler ceiling so it is not queueing. This is the trade the
harness makes visible; the spec's defaults do not resolve it.

---

## Measured results

First run of the harness, 2026-08-20, on the reference machine described
above: 8 cores, 15.5 GB, single-broker Kafka, `messages.v1` at 64 partitions,
three `moderation-service` replicas, mock LLM at p50 900 ms / p99 2500 ms.
Raw JSON results are produced by `-out`; the numbers below are from those
documents.

### Generator ceiling

**851 790 msg/s**, 269 MB/s of encoded records, 8 workers, null sink. Every
rate below is between three and four orders of magnitude under it, so no run
here measured the harness. Send-lag p95 was 1–2 ms in every scenario.

### The knee

Staircase, six 30 s plateaus, 133 → 2 000 msg/s:

| offered msg/s | lag slope | verdict |
| ---: | ---: | --- |
| 133 | +0 /s | flat |
| 507 | +12 /s | marginal — at capacity |
| 880 | +289 /s | over |
| 1 253 | +889 /s | over |
| 1 627 | +1 297 /s | over |
| 2 000 | +1 575 /s | over |

**The knee is between 507 and 880 msg/s** for three replicas — roughly
**170–200 msg/s per `moderation-service` instance**. Drain rate *fell* as
offered rate rose (591 → 330 msg/s), which is congestion collapse, not a flat
ceiling.

At 400 msg/s (below the knee) the system is healthy:

- SLI from `flagged.v1`: **p50 6.1 ms, p95 77.8 ms, p99 294.9 ms, max 1.12 s**
- **100% of verdicts under the 1.5 s N1 target**
- lag peak 109, slope +0.2 /s, final 0; 100% of produced messages consumed

At 2 000 msg/s (above it) the system does not fail, it falls behind, exactly
as §4.7 says it should: p95 **44 s**, peak lag 85 k, and a full drain to zero
after the load stopped. Nothing crashed and no message was lost.

### Hot-spot skew

| population | partition imbalance (max/mean of records produced) |
| --- | ---: |
| Zipf 1.1 over 1 000 contents, senders scaling with share | **1.41×** |
| one content at 60% of traffic behind **4** senders | **16.84×** |

Four partitions carried 62.6% of the run; the fifth-busiest had 3% of the
busiest one's volume. Work followed: in the hot-spot run the three replicas
consumed 15.5 k / 20.8 k / 35.7 k — instance 3 did **2.3×** the work of
instance 1 — while in the ordinary population they split 5 199 / 5 445 /
5 356.

The keying scheme is doing its job: `hash(author_id, content_id)` spreads a
hot content across partitions *in proportion to its sender count*. The
pathology is not a hot content, it is a hot content with **few senders** — a
bot, a relay, a raid — and there is no lever in the current design that
rebalances it.

### Sampler coverage (§7.5)

180 s, four contents pinned by weight, all traffic LLM-bound:

| content | offered | §7.5 says | measured | corrected for lost verdicts¹ |
| --- | ---: | ---: | ---: | ---: |
| `ct_ld0` | 1.2 msg/min | 100% | 55.6% (n=4) | ~99% |
| `ct_ld1` | 19.8 msg/min | 100% | 69.0% | ~100% |
| `ct_ld2` | 100.2 msg/min | ~30% | 22.6% | ~40% |
| `ct_ld3` | 6 000 msg/min | ~0.5% | 0.34% | ~0.6% |

¹ 44% of LLM batches in this run timed out and failed the whole batch open, so
a message could be admitted by the sampler and still produce no verdict. The
corrected column divides by the measured delivery rate.

**The sampler behaves as specified.** The shape of the table is reproduced:
quiet content essentially exhaustive, the firehose sampled to a fraction of a
percent, and total LLM admission (1.68% of 18 363 messages) bounded by content
count rather than volume. The shortfall in the raw column is A18's timeout,
not A17.

### Fail-open drills (§4.7)

400 msg/s steady, each dependency stopped and restarted mid-run.

| fault | window | fail-opens counted | lag behaviour |
| --- | --- | ---: | --- |
| `redis` stopped | t+20 → t+50 s | 972 `component="redis"` | **lag climbed at the full offered rate, +400/s, to 12 620** |
| `mockllm` stopped | t+80 → t+110 s | included in `component="llm"` | **flat at ~0 throughout** |
| `policy-service` stopped | t+140 → t+175 s | 2 776 `component="policy"` | **flat at ~0 throughout** |

The run **passed**: 100% of produced messages were consumed, the backlog
drained to zero, every degradation was counted, and no service stopped or
became unready. N2 holds.

But the three faults are not equal, and the difference is the most useful
thing this harness found — see F2.

### Adapter ingress

`POST /mock/messages`, staircase to 4 000 msg/s: **210 000 offered, 38 800
accepted (≈ 400 msg/s), 171 200 rejected** with `injection queue full`. The
ingress ceiling is roughly the same as the whole moderation pipeline's, and
about 2 000× below the generator's. Note this measures the *mock* driver's
bounded channel, not a real platform driver — but it settles the design
question: load must be produced to Kafka directly.

---

## Findings

Recorded, not fixed — this module measures.

**F1 — `kafka_consumer_lag_messages` is never set by any service.** `pkg/obs`
declares the gauge and §4.5 mandates it, but nothing writes to it, so the
family is absent from every `/metrics` response. §4.7 makes growing lag the
primary overload signal and §4.5 says it must be alerted on; today it cannot
be. The harness works around this by reading offsets from the broker.

**F2 — a Redis outage collapses throughput ~97%, where the LLM and policy
outages cost nothing.** §4.7 says Redis down means "skip rate/dup/semantic
stages, continue". The implementation does not skip: `Pipeline.Process` marks
Redis down only for the *current* message, so every subsequent message
re-attempts the `seen:` guard and pays the client's full failure latency —
on the single consumer goroutine. Measured: consumption fell from 400 msg/s
to ~11 msg/s per instance for the whole 30 s outage, building 12 620 messages
of backlog that took 14 s to clear. The LLM and policy paths, which have
1 s timeouts and (for the LLM) run off the consumer goroutine, showed no lag
at all. A short-lived circuit breaker — mark Redis down for N seconds, not for
one message — would make the Redis path behave like the other two.

**F3 — `moderation-service` is single-threaded per instance.**
`kafkax.Consumer.Run` polls and iterates the fetch serially, so per-instance
throughput is `1 / Σ(sequential stage latencies)` regardless of partition
count. The cascade issues four sequential Redis round trips (`seen`, `rate`,
`dup`, `samp`) plus a policy lookup and a publish; measured stage p95s sum to
~10 ms and the observed ceiling is ~170–200 msg/s per instance. Evidence that
Redis round trips are the binding constraint: the `hotspot` run, which
disables the rate-limit stage and so makes *three* Redis calls instead of
four, sustained 800 msg/s with flat lag where the `ramp` run was already
falling behind at 880. Extrapolating, N6's 50 000 msg/s baseline needs
~250–300 instances and the 500 000 msg/s peak ~2 500–3 000 — above §4.2's
512-partition ceiling, which caps the consumer group at 512 members.

**F4 — A18's 1 000 ms timeout equals §4.6's 1 000 ms budget for the same hop,
so a large fraction of batches fails open by construction.** With the mock at
p50 900 ms, **42% of LLM batches timed out** at a rate the system otherwise
handled comfortably (400 msg/s, lag flat, 2 228 messages unmoderated). §7.9
fails the *whole batch* open with no retry, so the cost is multiplied by the
batch size. The budget and the timeout cannot both be 1 000 ms.

**F5 — A18's 32-message trigger never fires at realistic rates.** Measured
mean batch size was **1.07 to 4.0** across every scenario (p50 of 1–3), never
close to 32, because batching is per-policy and the 50 ms linger always fires
first. The policy rubric — which §7.9 says is "a large share of the prompt
tokens" and is the entire reason for batching by policy — is therefore re-sent
every one to four messages instead of every 32. This is a direct multiplier on
GPU spend.

**F6 — commit granularity makes lag reporting jerky and the redelivery window
large.** `kafkax.Consumer.Run` commits only after the entire polled fetch has
been handled. Under backlog a single fetch is tens of thousands of records, so
committed offsets move in large jumps: during the ramp's drain, reported lag
sat at exactly 97 379 for over 40 s before moving. Operationally this makes
lag-based alerting coarse; on a crash it means re-processing a whole fetch.

**F7 — cosmetic contract divergence.** `flagged.v1` carries
`action="auto_delete"` (§4.2) while the policy document carries
`restricted_content_action="auto"` (§6.4, and `policy.RCActionAuto`). Both are
as implemented and neither is wrong, but a client speaking both APIs has to
know they differ.

---

## Wiring the parent may want

Suggested `Makefile` targets (this module deliberately does not edit the
`Makefile`):

```make
LOADC := $(COMPOSE) -f deploy/compose/fragments/load.yml
LOAD_METRICS := http://localhost:9085,http://localhost:9185,http://localhost:9285

# Stack with realistic partitions, a slow LLM and three moderation consumers.
up-load:
	$(LOADC) up -d --build --wait

# Generator self-benchmark: no stack needed, proves the harness has headroom.
load-selfbench:
	cd test/load && go run ./cmd/dabet-load -scenario selfbench

# One scenario: make load SCENARIO=ramp RATE=2000
SCENARIO ?= baseline
RATE     ?= 400
load:
	cd test/load && LOAD_MODERATION_METRICS=$(LOAD_METRICS) \
		go run ./cmd/dabet-load -scenario $(SCENARIO) -rate $(RATE) -out results/

# The §4.7 drills. Drives docker directly; requires the local stack.
load-drills:
	cd test/load && LOAD_MODERATION_METRICS=$(LOAD_METRICS) \
		go run ./cmd/dabet-load -scenario failopen -rate 400 -out results/
```

`make down` already tears the overlay's containers down, because they share
the `dabet` compose project name — but add `-f deploy/compose/fragments/load.yml`
to the `down` recipe if the extra moderation replicas should be removed by
name rather than by project sweep.

---

## Testing

```sh
cd test/load
go build ./... && go vet ./... && go test ./... && go test -race ./...
gofmt -l .
```

The unit tests cover the parts that would silently produce wrong numbers:
that the partition key is byte-identical to `pkg/contracts`; that the Zipfian
and explicit-weight samplers produce the intended distribution; that the rate
schedule is analytically exact and that a stall lands as lag rather than
sliding the clock; that the Prometheus parser handles real exposition text
including histograms, quoted label values and counter resets; and that the
result document's shape holds and never contains a NaN.
