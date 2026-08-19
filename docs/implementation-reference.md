# Dabet — Implementation Reference

**Event-driven moderation engine: sub-second LLM policy enforcement for high-volume chats, with pluggable platform adapters.**

| | |
| --- | --- |
| Status | Implementation-ready |
| Language | Go |
| Local infra | Docker Compose (all components single-instance) |
| Target infra | Kubernetes + Terraform (later) |
| Scope | v1 — English only, no model distillation |

---

## 0. How to use this document

This document is written so that four developers can work in parallel on four areas without reading each other's sections.

| Area | Sections | Services owned |
| --- | --- | --- |
| **A — Identity & Credits** | §5 | `user-service`, `credits-service` |
| **B — Policy** | §6 | `policy-service` |
| **C — Moderation** | §7 | `provider-adapter`, `moderation-service`, `review-service` |
| **D — Insights** | §8 | `insights-service`, `clustering-service`, `clusters-job` |

**Everything shared between areas lives in §4.** If you need something another area produces, it is defined there as a contract — you should not need to read their section. If you find yourself needing to, that is a bug in this document; raise it rather than working around it.

Anything marked **`ASSUMPTION`** was not specified by the product owner and was filled with a defensible default. These are collected in §9 and are the first things to challenge in review.

---

## 1. Overview

### 1.1 What the system does

A creator registers on Dabet, connects one or more of their YouTube / Twitch / Discord accounts, and defines moderation policies. Dabet then consumes chat messages from those platforms in real time, evaluates each message against the applicable policy, and either deletes the offending message automatically or queues it for the creator to review. Separately, it builds an aggregate picture of what the community is talking about, from messages that were *not* flagged.

### 1.2 Requirements

**Functional**

- F1 — Native Dabet account; connect YouTube, Twitch, and/or Discord (all, some, or one).
- F2 — Custom policies: content-based (what was said) and activity-based (how often, how repetitively).
- F3 — Violations are auto-deleted or routed to human review.
- F4 — Community insights: inferred topics and themes over time.

**Non-functional**

- N1 — E2E latency < 1.5 s at p95, measured from adapter ingress to verdict published (§4.6).
- N2 — Very high availability. The service does not stop; it degrades (§4.7).
- N3 — Extensible platforms — adding a platform is a new driver behind one interface, no changes elsewhere.
- N4 — Exposed metrics for developer observability.
- N5 — 10M registered creators.
- N6 — Throughput: 50K msg/s baseline, 500K msg/s peak, heavily hot-spotted.

**Explicitly out of scope for v1:** non-English moderation, distilling LLM verdicts into small classifiers, roles/teams/seats, API keys, server-side API rate limiting, cross-sender raid detection.

### 1.3 The one thing that makes this system work

At 500K msg/s, an LLM call per message is impossible on both cost and latency. Every design decision below follows from that constraint:

1. **A cheap-first cascade with first-hit-wins.** Rate limit → duplicate → semantic spam → restricted words → LLM. Each stage is orders of magnitude cheaper than the next, and a hit short-circuits the rest.
2. **Degressive sampling.** Content with 100 messages/month is evaluated exhaustively. Content with 100 messages/second is sampled — statistically, a violation pattern in a firehose is caught by a sample; a violation in a quiet channel is not, so quiet channels get full coverage. The LLM stage is what gets sampled; the cheap stages always run.
3. **No message storage.** Text lives only in Kafka, under retention. Nothing downstream persists it. Insights stores embeddings, never text.

### 1.4 Entities

| Entity | Persisted? | Owner | Notes |
| --- | --- | --- | --- |
| **Creator** | Postgres | A | The Dabet account. One row per registered user. |
| **Connection** | Postgres | A | A linked platform account. Holds OAuth tokens. |
| **Policy** | Postgres | B | Scoped to creator, platform, or content. |
| **Content** | No | C | Opaque platform-adapter ID for a stream / channel / server. Never resolved outside the adapter. |
| **Sender** | No | C | Opaque platform-adapter ID for a chat author. Deduplication key only. |
| **Message** | Kafka only | C | Never lands in a database. |
| **Message Review** | Kafka only | C | A pending review is a position in a Kafka topic, not a row. |
| **Topic** | ClickHouse + Milvus | D | Inferred by clustering. Creators do not define topics. |
| **Theme** | ClickHouse + Milvus | D | Sub-cluster within a topic. Also inferred. |

`Content` and `Sender` being opaque is load-bearing: it is what keeps platform knowledge inside `provider-adapter` and satisfies N3. No service outside the adapter may parse, pattern-match, or infer platform from these IDs.

### 1.5 Component map

```
                    ┌───────────────┐
   Platforms  ◄────►│provider-adapter│────► messages.v1 ────┐
   (YT/TW/DC)       └───────────────┘                       │
                            ▲                               ▼
                            │                     ┌───────────────────┐
                     deletions.v1 ◄───────────────│ moderation-service│───► LLM (vLLM)
                            ▲                     └───────────────────┘
                            │                          │    │    │
                   ┌────────────────┐                  │    │    └──► Redis (dedup, rate, sample)
                   │ review-service │◄── flagged.v1 ◄──┘    └──► policy-service ──► Postgres + Memcached
                   └────────────────┘         │
                            ▲                 │
                            │                 ▼
                         Creator      ┌────────────────┐
                                      │insights-service│───► Embedding ───► S3 (parquet)
                                      └────────────────┘           │
                                               │                   ▼
                                               │        ┌────────────────────┐
                                               │        │ clustering-service │◄──► Milvus
                                               │        └────────────────────┘
                                               ▼                   │
                                          ClickHouse ◄─────────────┘
                                               ▲
                                          clusters-job (HDBSCAN + LLM labelling)

   user-service ──► Postgres          credits-service ──► Postgres + Stripe
                                             ▲
                                             └── usage.v1
```

---

## 2. Architecture principles

**P1 — Kafka is the only thing between areas.** Services do not call each other synchronously except where this document says so (moderation → policy, moderation → LLM). If you are adding a synchronous hop between areas, stop.

**P2 — Fail open, always.** A component that cannot do its job lets the message through. Moderation failing must never stop chat. Every fail-open path increments a counter (§4.5).

**P3 — At-least-once, idempotent effects.** Kafka redelivers. Every consumer must tolerate seeing the same message twice without double-deleting, double-charging, or double-counting.

**P4 — Text is radioactive.** It exists in Kafka and in process memory. It is never written to a database, a log line, a metric label, or an error message. Violating this breaks the retention story in §4.8.

**P5 — Opaque IDs stay opaque.** See §1.4.

---

## 3. Environments

| | Local (Compose) | Target |
| --- | --- | --- |
| Postgres | 1 instance, all schemas | Managed, separate clusters for identity/policy |
| Kafka | 1 broker, 3 partitions/topic | 3+ brokers, partition counts per §4.2 |
| Redis | 1 instance | Redis Cluster, sharded by `hash(author_id, content_id)` |
| Memcached | 1 instance | Managed pool |
| vLLM | 1 instance, small model | GPU fleet behind a load balancer |
| Milvus | Standalone | Distributed |
| ClickHouse | 1 instance | Cluster |
| S3 | MinIO | S3 |

All services must run against the Compose profile with no code changes — topology differences are configuration only.

---

## 4. Shared contracts

> **This section is the interface between the four areas. Changes here require agreement across all four; changes elsewhere do not.**

### 4.1 HTTP conventions

**Base path.** All endpoints are under `/v1`. The version increments only on breaking change; additive fields are not breaking.

**Auth.** Bearer JWT in `Authorization`. Access tokens are short-lived; refresh tokens rotate. There are no API keys, no scopes, and no roles — a creator has full access to their own resources and no access to anyone else's. Authorization is therefore a single rule, applied in every handler:

> Every resource is owned by exactly one `creator_id`. The handler resolves the resource's owner and compares it to the JWT subject. A mismatch returns `404`, never `403` — a creator must not be able to probe for the existence of another creator's resources.

**Content type.** `application/json; charset=utf-8` on request and response. Unknown request fields are rejected with `validation_failed` rather than ignored, so that client bugs surface early.

**Request IDs.** Every request gets an `X-Request-Id` (generated if absent), returned on the response, attached to every log line and propagated to downstream calls.

**Pagination.** Cursor-based, never offset-based:

```
GET /v1/reviews?limit=50&cursor=eyJvIjoxMjM0NX0
```

```json
{
  "items": [ ... ],
  "next_cursor": "eyJvIjoxMjM5NX0"
}
```

`limit` defaults to 50, maximum 200. `next_cursor` is an opaque base64 blob — clients must not decode it. `next_cursor` absent means the end of the collection *at this moment*; for review queues (§7.6) it may become non-empty again later.

**Errors.** One envelope, always:

```json
{
  "error": {
    "code": "validation_failed",
    "message": "restricted_words exceeds maximum of 500 entries",
    "details": { "field": "restricted_words", "limit": 500 },
    "request_id": "01J8XQ..."
  }
}
```

`message` is for developers and is not localised. `details` is optional and structurally free-form per code. Error text must never echo chat message content (P4).

| HTTP | `code` | Meaning |
| --- | --- | --- |
| 400 | `validation_failed` | Malformed body, unknown field, or constraint violated |
| 401 | `unauthenticated` | Missing, expired, or invalid token |
| 404 | `not_found` | Absent, or owned by another creator |
| 409 | `conflict` | Uniqueness violation (e.g. policy already exists at this scope) |
| 409 | `state_conflict` | Operation invalid for current state (e.g. connection already revoked) |
| 422 | `unprocessable` | Well-formed and permitted, but semantically impossible |
| 429 | `too_many_requests` | Reserved. Not issued in v1 — no server-side rate limiting |
| 500 | `internal_error` | Unhandled. Body carries `request_id` only |
| 502 | `upstream_error` | A third party (Stripe, an OAuth provider) failed |
| 503 | `unavailable` | Dependency down; retry with backoff |

**Idempotency.** Any `POST` that creates a resource or moves money accepts an `Idempotency-Key` header. The server stores `(creator_id, key) → response` for 24 h and replays the stored response on repeat. Required on `POST /v1/credits/topup` and `POST /v1/reviews`; accepted and honoured everywhere else.

**Timestamps.** RFC 3339, UTC, always suffixed `_at`. Durations are integer seconds, suffixed `_seconds`.

### 4.2 Kafka topic registry

Four topics. This table is the contract; the schemas follow.

| Topic | Key | Partitions (target) | Retention | Produced by | Consumed by |
| --- | --- | --- | --- | --- | --- |
| `messages.v1` | `hash(author_id, content_id)` | 512 | 24 h | `provider-adapter` | `moderation-service`, `insights-service` |
| `flagged.v1` | `creator_id` | 128 | 7 d | `moderation-service` | `review-service`, `insights-service` |
| `deletions.v1` | `content_id` | 128 | 24 h | `moderation-service`, `review-service` | `provider-adapter` |
| `usage.v1` | `creator_id` | 32 | 7 d | `moderation-service`, `clusters-job` | `credits-service` |

Compression `zstd`. Producer `acks=all`, `enable.idempotence=true`. Local Compose uses 3 partitions for every topic.

**Why the keys differ.** `messages.v1` is keyed by `hash(author_id, content_id)` so that all messages from one sender in one stream land on one partition in order — this is what makes the Redis dedup and rate-limit state race-free without locking. Downstream of the verdict, that ordering no longer matters, so `flagged.v1` is keyed by `creator_id` to make a creator's review queue a single contiguous partition read (§7.6), and `deletions.v1` by `content_id` so the adapter can batch deletions per stream.

**`messages.v1`**

```json
{
  "message_id":  "ytc_01J8XQ7K2M4N",
  "content_id":  "ct_9f2a",
  "author_id":   "sd_3b71",
  "creator_id":  "9d4e...",
  "text":        "…",
  "ingested_at": "2026-08-19T14:02:11.412Z"
}
```

`message_id`, `content_id`, `author_id` are opaque adapter-issued strings (≤64 chars). `creator_id` is a Dabet UUID — the adapter resolves it at ingest so that no downstream consumer needs a lookup. `ingested_at` is set by the adapter at the moment of receipt and is the start of the latency clock (§4.6). There is deliberately **no platform field** — see §1.4.

**`flagged.v1`**

```json
{
  "message_id":   "ytc_01J8XQ7K2M4N",
  "content_id":   "ct_9f2a",
  "author_id":    "sd_3b71",
  "creator_id":   "9d4e...",
  "text":         "…",
  "detector":     "restricted_content",
  "action":       "review",
  "policy_id":    "pol_7a13",
  "flagged_at":   "2026-08-19T14:02:11.914Z"
}
```

`detector` ∈ `rate_limit | duplicate | semantic_spam | restricted_word | restricted_content`. `action` ∈ `auto_delete | review`. Only `restricted_content` can carry `review`; every other detector is always `auto_delete` (§6.4). Text is carried because review needs to show it and nothing else stores it.

**`deletions.v1`**

```json
{
  "message_id": "ytc_01J8XQ7K2M4N",
  "content_id": "ct_9f2a",
  "creator_id": "9d4e...",
  "reason":     "restricted_word",
  "issued_at":  "2026-08-19T14:02:11.916Z"
}
```

No text. The adapter needs only the identifiers to issue a platform delete.

**`usage.v1`**

```json
{
  "creator_id":      "9d4e...",
  "event_type":      "messages_processed",
  "quantity":        1000,
  "window_start":    "2026-08-19T14:00:00Z",
  "window_end":      "2026-08-19T14:01:00Z",
  "idempotency_key": "mod-7-14:00-9d4e"
}
```

`event_type` ∈ `messages_processed | messages_reclustered`. Producers emit **aggregated** events (per creator per minute), not per-message events. `idempotency_key` must be deterministic — derived from producer identity, window, and creator — so that a redelivered event is recognised and discarded rather than double-charged (§5.7). Only `credits-service` knows what an event costs; producers never compute money.

### 4.3 Redis keyspace

Owned by Area C. Listed here because the sharding rule is shared.

| Key | Type | TTL | Purpose |
| --- | --- | --- | --- |
| `seen:{message_id}` | string | 5 min | Redelivery guard (§7.4) |
| `dup:{content_id}:{author_id}` | list of hashes | 5 min | Identical-message dedup |
| `emb:{content_id}:{author_id}` | list of packed vectors | 5 min | Semantic spam comparison |
| `rate:{content_id}:{author_id}` | hash (tokens, ts) | 2× window | Token bucket, rate limit |
| `samp:{content_id}` | hash (tokens, ts) | 5 min | LLM sampling bucket (§7.5) |

All bucket operations are Lua scripts so that read-modify-write is atomic. In cluster mode, keys hash to the same slot for a given `(content_id, author_id)` via hash tags: `dup:{ct_9f2a:sd_3b71}`. Apply hash tags from day one even on the single local instance, so the cluster migration is a config change.

### 4.4 Configuration

Every service reads config from environment variables, no config files. Names are prefixed by concern, not by service, so shared values are literally identical strings across services: `KAFKA_BROKERS`, `REDIS_ADDR`, `POSTGRES_DSN`, `MEMCACHED_ADDRS`, `VLLM_ENDPOINT`, `MILVUS_ADDR`, `CLICKHOUSE_DSN`, `S3_ENDPOINT`.

Tunables that this document assigns numbers to (cache TTLs, sampler thresholds, batch sizes) are all environment-overridable. The documented number is the default, not a constant in code.

Secrets (Postgres password, Stripe key, OAuth client secrets) come from the environment in v1, from a secret manager in the k8s target.

### 4.5 Observability

Prometheus metrics on `:9090/metrics`, OpenTelemetry traces, structured JSON logs to stdout. Developer-facing; there is no creator-facing metrics product.

**Cardinality rule.** Never label a metric with `message_id`, `author_id`, `content_id`, or any text. `creator_id` is permitted only on `credits_*`. Everything else labels by service, platform driver, detector, and outcome.

Every service exposes:

| Metric | Type | Labels |
| --- | --- | --- |
| `http_requests_total` | counter | `route`, `method`, `status` |
| `http_request_duration_seconds` | histogram | `route`, `method` |
| `kafka_consumer_lag_messages` | gauge | `topic`, `partition`, `group` |
| `kafka_messages_consumed_total` | counter | `topic`, `group`, `outcome` |
| `dependency_up` | gauge | `dependency` |
| `fail_open_total` | counter | `component`, `reason` |

`fail_open_total` is the single most important metric in the system. It is the count of messages that went unmoderated because something was broken. It must be alerted on, and it must be zero in steady state.

Area-specific metrics are listed in each area's section.

**Health endpoints.** `/healthz` returns 200 if the process is alive. `/readyz` returns 200 if it can serve. Per P2, a moderation-path service that has lost a dependency is still **ready** — it fails open and keeps consuming. `/readyz` returning 503 would remove it from service and stop chat, which is exactly the wrong outcome.

### 4.6 Latency

**The SLI.** p95 of `flagged_at − ingested_at`, over messages that were flagged. Measured inside `moderation-service` and exported as `moderation_e2e_latency_seconds`.

The clock starts when the message enters Dabet (adapter ingress) and stops when the verdict is published to `flagged.v1`. It deliberately excludes:

- Platform-side delay before the message reaches us (YouTube's polling interval alone can exceed the entire budget, and no design of ours can affect it).
- The provider delete call, which is third-party latency. It is measured separately as `deletion_latency_seconds` and reported, but is not part of the SLI.

**Indicative budget** (not a contract; the target is the p95 aggregate):

| Hop | Budget |
| --- | --- |
| Adapter → `messages.v1` | 50 ms |
| Kafka → moderation consume | 50 ms |
| Policy resolution (cache hit) | 1 ms |
| Redis cascade (rate, dup, semantic) | 10 ms |
| Embedding (semantic spam only) | 100 ms |
| LLM batch (when reached) | 1 000 ms |
| Publish verdict | 20 ms |

The LLM dominates, which is the entire justification for the cascade and the sampler: most messages never incur it.

### 4.7 Failure policy

Fail open, everywhere, without exception. The table is normative.

| Failure | Behaviour | Metric |
| --- | --- | --- |
| `policy-service` down, cache cold | Message passes unmoderated | `fail_open_total{component="policy"}` |
| Redis down | Skip rate/dup/semantic stages, continue to word + LLM stages | `fail_open_total{component="redis"}` |
| Embedding service down | Skip semantic spam, continue | `fail_open_total{component="embedding"}` |
| LLM down or timed out | Message passes unmoderated | `fail_open_total{component="llm"}` |
| Creator has zero credits | Message passes unmoderated | `fail_open_total{reason="no_credits"}` |
| Kafka producer failure (verdict) | Retry with backoff; drop after 30 s | `fail_open_total{component="kafka"}` |
| Provider delete API failing | Retry with backoff, then drop | `deletion_failures_total` |
| Consumer lag growing | **Accept the lag.** Do not shed, do not skip | `kafka_consumer_lag_messages` |

Two consequences to state plainly, because they are intended and a reviewer will otherwise read them as bugs:

1. **Dabet fails toward under-moderation.** A broken dependency means offensive messages reach viewers. The alternative — blocking or delaying chat — is worse for a live stream, and is not available to us anyway since we moderate after the platform has already displayed the message.
2. **Lag is silent.** Under sustained overload the verdict still arrives, just late — possibly minutes late, by which point deleting the message has limited value. Lag is therefore an availability signal, not just a performance one.

### 4.8 Data retention

| Data | Where | Retention |
| --- | --- | --- |
| Message text | `messages.v1` | 24 h, configurable |
| Flagged text | `flagged.v1` | 7 d, configurable — also the review deadline (§7.7) |
| Embeddings | S3 (parquet), Milvus | Indefinite |
| Topic/theme aggregates | ClickHouse | Indefinite |
| Redis moderation state | Redis | ≤ 5 min |
| Creator, connection, policy, credits | Postgres | Life of account |

**Message text is never persisted outside Kafka.** No service writes it to a database, a file, a log, a metric label, or a trace attribute. Deletion is therefore not a workflow — text ages out of Kafka on its own, which is what makes the retention story defensible without building an erasure pipeline.

Embeddings are kept forever and carry `creator_id`, `content_id`, and a timestamp — **never `author_id` and never text**. They are not attributable to an individual sender, which is why keeping them indefinitely is acceptable. Insights processes only messages that were *not* flagged (§8.3): Dabet does not build a profile of who misbehaves.

---

## 5. Area A — Identity, Connections & Credits

**Services:** `user-service`, `credits-service`
**Stores:** Postgres (`identity` schema, `billing` schema), Stripe
**Consumes:** `usage.v1`
**Produces:** nothing

### 5.1 Model

A Creator is a native Dabet account with email and password. Platform accounts are attached afterwards as Connections. A Creator with zero Connections is valid — they simply have nothing to moderate. Multiple accounts on the same platform are permitted (a creator may run several Discord servers, or a personal and a brand YouTube channel).

```
Creator ──1:N──► Connection ──1:N──► Content (opaque, not stored)
   │
   └──1:N──► CreditEntry ──► CreatorBalance (1:1, derived)
```

### 5.2 Schema — `identity`

```sql
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE creators (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email             CITEXT      NOT NULL UNIQUE,
    fullname          VARCHAR(32) NOT NULL,
    password_hash     TEXT        NOT NULL,
    email_verified_at TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TYPE platform_t AS ENUM ('youtube', 'twitch', 'discord');
CREATE TYPE connection_status_t AS ENUM ('active', 'expired', 'revoked');

CREATE TABLE connections (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id        UUID NOT NULL REFERENCES creators(id) ON DELETE CASCADE,
    platform          platform_t NOT NULL,
    provider_user_id  TEXT NOT NULL,
    display_name      TEXT NOT NULL,
    access_token      TEXT NOT NULL,
    refresh_token     TEXT,
    expires_at        TIMESTAMPTZ,
    scopes            TEXT[] NOT NULL DEFAULT '{}',
    status            connection_status_t NOT NULL DEFAULT 'active',
    connected_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One platform account may be actively connected once, globally.
CREATE UNIQUE INDEX connections_active_uniq
    ON connections (platform, provider_user_id)
    WHERE status = 'active';

CREATE INDEX connections_creator_idx ON connections (creator_id) WHERE status = 'active';

CREATE TABLE oauth_states (
    state          TEXT PRIMARY KEY,
    creator_id     UUID NOT NULL REFERENCES creators(id) ON DELETE CASCADE,
    platform       platform_t NOT NULL,
    code_verifier  TEXT NOT NULL,
    redirect_after TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE refresh_tokens (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id  UUID NOT NULL REFERENCES creators(id) ON DELETE CASCADE,
    token_hash  TEXT NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`connections_active_uniq` is a partial unique index: it prevents two creators from claiming the same YouTube channel simultaneously, while still allowing a channel to move between accounts after the first is revoked. **`ASSUMPTION`** — the alternative (letting two creators moderate one channel concurrently) produces double deletions and contested policies, so it is disallowed. An attempt returns `409 conflict`.

Tokens are stored as plaintext columns per explicit decision (no envelope encryption in v1). This is recorded as a known risk in §10.

**Sizing.** A creator row is ~200 B; a connection with tokens is ~1.5 KB. 10M creators averaging 1.5 connections ≈ 2 GB + 22 GB ≈ **~25 GB**, consistent with the original estimate. Single Postgres, no sharding.

### 5.3 Schema — `billing`

```sql
CREATE TABLE credit_entries (
    id              BIGSERIAL PRIMARY KEY,
    creator_id      UUID NOT NULL REFERENCES creators(id) ON DELETE CASCADE,
    delta           BIGINT NOT NULL,           -- credits; positive = topup, negative = usage
    reason          TEXT NOT NULL,             -- 'topup' | 'messages_processed' | 'messages_reclustered' | 'adjustment'
    idempotency_key TEXT NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX credit_entries_idem_uniq ON credit_entries (idempotency_key);
CREATE INDEX credit_entries_creator_idx ON credit_entries (creator_id, created_at DESC);

CREATE TABLE creator_balances (
    creator_id UUID PRIMARY KEY REFERENCES creators(id) ON DELETE CASCADE,
    balance    BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**Balance is maintained on write, not derived on read.** Every mutation is one transaction:

```sql
BEGIN;
  INSERT INTO credit_entries (creator_id, delta, reason, idempotency_key)
  VALUES ($1, $2, $3, $4)
  ON CONFLICT (idempotency_key) DO NOTHING
  RETURNING id;
  -- if no row returned, this is a replay: COMMIT and return the existing balance

  INSERT INTO creator_balances (creator_id, balance) VALUES ($1, $2)
  ON CONFLICT (creator_id) DO UPDATE
    SET balance = creator_balances.balance + EXCLUDED.balance, updated_at = now();
COMMIT;
```

The unique index on `idempotency_key` is what makes billing exactly-once on top of at-least-once Kafka delivery: a redelivered `usage.v1` event inserts nothing and adjusts nothing. This replaces the materialized-view design in the original diagram, which could not support a real-time balance.

**Write volume.** Producers aggregate per creator per minute (§4.2), so the ledger takes at most `active_creators / 60` writes per second, not one per message. At 50K concurrently active creators that is ~800 writes/s — comfortable for a single Postgres.

### 5.4 Registration and login

```
POST /v1/auth/register   { email, fullname, password }        -> 201 { creator_id }
POST /v1/auth/verify     { token }                            -> 204
POST /v1/auth/login      { email, password }                  -> 200 { access_token, refresh_token, expires_in }
POST /v1/auth/refresh    { refresh_token }                    -> 200 { access_token, refresh_token, expires_in }
POST /v1/auth/logout     { refresh_token }                    -> 204
GET  /v1/me                                                   -> 200 { id, email, fullname, email_verified }
```

**`ASSUMPTION`** — details not specified, chosen as follows:

- Passwords hashed with **Argon2id** (64 MB, t=3, p=2). Minimum 12 characters, no composition rules, rejected against a common-password list.
- `fullname` is capped at 32 characters per the original schema.
- Access token: JWT, **15 min**, HS256 in local / RS256 in target, claims `sub` (creator_id), `iat`, `exp`, `jti`.
- Refresh token: opaque 32-byte random, stored hashed, **30 days**, **rotated on every use**. Reuse of a rotated token revokes the entire family — a stolen refresh token is thereby limited to one use before the legitimate client's next refresh trips the alarm.
- **Email verification is required before creating a Connection**, not before login. A creator can log in and look around unverified but cannot attach a platform account.
- Login responses are constant-time and identical for unknown-email and wrong-password (`401 unauthenticated`), so the endpoint is not an account-enumeration oracle. Registration cannot hide the same fact — it must reject duplicate emails — so it returns `409 conflict`.

### 5.5 Connecting a platform

```
POST   /v1/connections/{platform}   -> 200 { authorize_url, state }
GET    /v1/connections/callback     ?code=&state=   -> 302 to app
GET    /v1/connections              -> 200 { items: Connection[] }
DELETE /v1/connections/{id}         -> 204
```

`Connection` never exposes tokens:

```json
{
  "id": "…",
  "platform": "twitch",
  "display_name": "somechannel",
  "status": "active",
  "connected_at": "2026-08-19T14:00:00Z"
}
```

**Flow.**

```
Creator            user-service                    Platform OAuth
   │  POST /connections/twitch │                          │
   │─────────────────────────►│                          │
   │                          │ generate state + PKCE     │
   │                          │ verifier, store in        │
   │                          │ oauth_states (TTL 10 min) │
   │  { authorize_url }       │                          │
   │◄─────────────────────────│                          │
   │  browser redirect ───────────────────────────────►  │
   │                          │        creator consents   │
   │  ◄──── redirect to /connections/callback?code&state  │
   │─────────────────────────►│                          │
   │                          │ look up state, verify     │
   │                          │ not expired, delete it    │
   │                          │ exchange code ──────────► │
   │                          │ ◄──── tokens + user id    │
   │                          │ upsert connection         │
   │  302 to app              │                          │
   │◄─────────────────────────│                          │
```

- `state` is a 32-byte random value, single-use, 10-minute TTL, deleted on redemption. An unknown or expired `state` is `400 validation_failed` — this is the CSRF defence and must not be skipped.
- **PKCE** (S256) where the provider supports it; the verifier lives in `oauth_states`.
- Redirect URI is fixed per platform and registered with the provider; it is never taken from the request.
- Requested scopes must include **moderation permissions**, not merely chat read — Dabet must be able to delete. Insufficient granted scope fails the connection with `422 unprocessable` and a message naming the missing scope, rather than creating a connection that silently cannot moderate.

| Platform | Scopes | **`ASSUMPTION`** |
| --- | --- | --- |
| YouTube | `youtube.force-ssl` | Covers live chat read + message delete |
| Twitch | `moderator:manage:chat_messages`, `user:read:chat`, `moderator:read:chatters` | Delete requires the account to be a moderator of the channel |
| Discord | bot install with `MANAGE_MESSAGES`, message content intent | Discord is a bot install, not a user OAuth — see §7.2 |

Verify current scope names against provider documentation before implementing; these change.

**Disconnect** (`DELETE /v1/connections/{id}`) sets `status='revoked'`, best-effort revokes the token with the provider, and signals the adapter to drop the connection's streams. It does not delete the row — the audit trail of what was connected when is worth keeping, and the partial unique index means a revoked row does not block reconnection.

**Revocation from the platform side** is detected by the adapter when a token refresh fails with an auth error; it sets `status='expired'` and stops attempting. **`ASSUMPTION`** — the creator is notified by email; there is no in-app notification system in v1.

### 5.6 Token refresh

Tokens are refreshed **lazily, on failure**, by `provider-adapter` — not on a schedule by `user-service`. A scheduled refresher across 10M creators would be a large standing workload to keep tokens fresh for accounts that are mostly not live; refreshing on demand means work is proportional to activity.

On a `401` from a provider API, the adapter:

1. Takes a short advisory lock on the connection (`pg_advisory_xact_lock(hashtext(connection_id))`) so that concurrent workers on the same connection refresh once, not N times.
2. Re-reads the row — another worker may have already refreshed.
3. Exchanges the refresh token, updates `access_token`, `refresh_token`, `expires_at`.
4. Retries the original call once.

If refresh fails with an auth error, the connection moves to `expired` and its streams are dropped. If it fails with a transport error, retry with backoff — the connection stays `active`.

### 5.7 Credits

```
GET  /v1/credits              -> 200 { balance, updated_at }
GET  /v1/credits/entries      -> 200 { items: CreditEntry[], next_cursor }
POST /v1/credits/topup        { amount_cents }  -> 200 { client_secret, payment_intent_id }
POST /v1/webhooks/stripe      (Stripe → Dabet)  -> 204
```

**Prepaid only.** Credits are bought up front and consumed by usage. There is no invoicing, no postpaid balance, and no subscription in v1.

**The conversion from usage to credits lives entirely inside `credits-service`.** No other service knows the rate, and no other service computes money. Producers of `usage.v1` emit *quantities of work* (`messages_processed`, `messages_reclustered`) and nothing else. This is what allows pricing to change without touching the moderation or insights pipelines.

**Top-up flow.**

```
Creator ──POST /credits/topup──► credits-service ──create PaymentIntent──► Stripe
        ◄──── client_secret ────                                          
        ──────────── confirms payment in browser ──────────────────────►  
                                 ◄─── webhook: payment_intent.succeeded ──
                                 │
                                 └─► credit_entries INSERT (idempotency_key = payment_intent_id)
                                     creator_balances UPDATE
```

Credits are granted **only** on the webhook, never on the client-side confirmation — the client cannot be trusted to report its own payment. The Stripe `payment_intent_id` is the idempotency key, so redelivered webhooks are no-ops.

Webhook handling:

| Event | Action |
| --- | --- |
| `payment_intent.succeeded` | Grant credits |
| `payment_intent.payment_failed` | No ledger entry; log |
| `charge.refunded` | Negative entry, key `refund:{charge_id}` |
| `charge.dispute.created` | Negative entry, key `dispute:{dispute_id}` |

Signature verification via `Stripe-Signature` is mandatory; an unverified webhook is `400`. The endpoint is unauthenticated in the JWT sense — the signature *is* the authentication.

A refund or dispute can drive a balance negative. This is allowed: the ledger must reflect reality. A negative balance behaves identically to zero (§5.8).

### 5.8 Zero credits

**Moderation continues.** Per §4.7, a creator at zero credits has their messages passed through unmoderated and `fail_open_total{reason="no_credits"}` incremented — moderation is not blocked, because blocking would mean chat stops being processed and the system is supposed to degrade rather than stop.

Enforcement is therefore advisory and eventually consistent: `moderation-service` reads a cached `credits_ok` flag per creator (**`ASSUMPTION`** — TTL 60 s, so a creator may get up to a minute of free processing after running out, which is acceptable). It never blocks on a synchronous credits lookup in the hot path.

**`ASSUMPTION`** — the creator is emailed at 20% and 0% balance. Building notification preferences is out of scope.

### 5.9 Area A metrics

| Metric | Type | Labels |
| --- | --- | --- |
| `auth_logins_total` | counter | `outcome` |
| `connections_active` | gauge | `platform` |
| `connection_refresh_total` | counter | `platform`, `outcome` |
| `credits_topup_cents_total` | counter | — |
| `credits_usage_events_total` | counter | `event_type`, `outcome` (`applied`/`replayed`) |
| `credits_balance_negative` | gauge | — |

---

## 6. Area B — Policy

**Service:** `policy-service`
**Stores:** Postgres (`policy` schema), Memcached
**Consumed by:** `moderation-service` (via `GetPolicy`)

### 6.1 Model

A Policy is a complete moderation configuration attached to exactly one scope:

| Scope | `scope_id` | Meaning |
| --- | --- | --- |
| `creator` | Dabet `creator_id` | Default for everything this creator owns |
| `platform` | `{creator_id}:{platform}` | Override for one platform |
| `content` | opaque `content_id` | Override for one stream/channel/server |

**Exactly one policy per `(scope, scope_id)`.** Creating a second returns `409 conflict`. Editing is `PUT`, not delete-and-recreate.

### 6.2 Resolution — first match wins, whole document

Given a `content_id`, `platform`, and `creator_id`, the resolved policy is the **first** of:

1. Policy at scope `content` with `scope_id = content_id`
2. Policy at scope `platform` with `scope_id = {creator_id}:{platform}`
3. Policy at scope `creator` with `scope_id = creator_id`
4. **None** — the message is not moderated

This is **whole-document replacement, not field merge.** A content-scoped policy that omits `restricted_words` means *no restricted words for this content*, not *inherit the creator's list*. This overrides the "inherit missing policies" note on the original diagram, and it is the single most important thing to get right in this area: field-level merge makes the effective policy for any given stream impossible for a creator to reason about, and impossible to cache as one object.

A creator with no policy at any scope gets **no moderation** — every message passes. Their content is still eligible for Insights.

> **Note for Area C:** `moderation-service` calls `GetPolicy(creator_id, platform, content_id)` and receives one resolved policy or `null`. Resolution happens entirely inside `policy-service`; the caller never sees the three candidates and never merges anything.

### 6.3 Schema

```sql
CREATE TYPE policy_scope_t AS ENUM ('creator', 'platform', 'content');
CREATE TYPE spam_mode_t    AS ENUM ('identical', 'semantic', 'none');
CREATE TYPE rc_action_t    AS ENUM ('auto', 'review');

CREATE TABLE policies (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    creator_id              UUID NOT NULL REFERENCES creators(id) ON DELETE CASCADE,
    scope                   policy_scope_t NOT NULL,
    scope_id                TEXT NOT NULL,

    rate_limit_messages     INT,          -- NULL = no rate limiting
    rate_limit_seconds      INT,
    spam                    spam_mode_t NOT NULL DEFAULT 'none',
    restricted_words        TEXT[] NOT NULL DEFAULT '{}',
    restricted_content      JSONB  NOT NULL DEFAULT '[]',
    restricted_content_action rc_action_t NOT NULL DEFAULT 'auto',

    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT rate_limit_pair CHECK (
        (rate_limit_messages IS NULL) = (rate_limit_seconds IS NULL)
    )
);

CREATE UNIQUE INDEX policies_scope_uniq ON policies (scope, scope_id);
CREATE INDEX policies_creator_idx ON policies (creator_id);
```

`restricted_content` is an array of objects:

```json
[
  {
    "title": "Ticket scalping",
    "description": "Offers to resell event tickets, or requests to buy them.",
    "examples": ["selling 2 tickets for tonight DM me", "anyone got a spare ticket"]
  }
]
```

`title` and `description` are what the LLM is asked to judge against; `examples` are few-shot anchors. See §7.9 for how these become a prompt.

### 6.4 Actions

`restricted_content_action` is the **only** action field, and it applies **only** to the LLM `restricted_content` check:

| Detector | Action | Configurable? |
| --- | --- | --- |
| `rate_limit` | `auto_delete` | No |
| `duplicate` | `auto_delete` | No |
| `semantic_spam` | `auto_delete` | No |
| `restricted_word` | `auto_delete` | No |
| `restricted_content` | `auto_delete` or `review` | **Yes** |

The rationale: the first four detectors are deterministic and mechanical — a creator who sets a rate limit of 5/10s means it, and a human confirming each breach would be absurd at volume. The LLM check is judgement-based, so it is the only place where a creator may reasonably want a human in the loop.

There is no warn, timeout, or ban action in v1, and no escalation ladder. Recorded in §10.

### 6.5 Validation

**`ASSUMPTION`** — no limits were specified; these are chosen to bound both storage and, more importantly, LLM prompt size:

| Field | Constraint |
| --- | --- |
| `rate_limit_messages` | 1–1000 |
| `rate_limit_seconds` | 1–3600 |
| `restricted_words` | ≤ 500 entries, each 1–64 chars, lowercased and deduplicated on write |
| `restricted_content` | ≤ 20 entries |
| `restricted_content[].title` | 1–100 chars |
| `restricted_content[].description` | 1–500 chars |
| `restricted_content[].examples` | ≤ 10 entries, each ≤ 200 chars |

Rate limit fields are all-or-nothing (enforced by `rate_limit_pair`). A violation returns `400 validation_failed` with the offending field and limit in `details`.

The 20 × (500 + 10 × 200) ceiling puts the worst-case policy prompt in the low thousands of tokens, which is what keeps the LLM stage inside its latency budget.

### 6.6 API

```
POST   /v1/policies         { scope, scope_id, ...policy }  -> 201 { Policy }
GET    /v1/policies         ?scope=&scope_id=               -> 200 { items, next_cursor }
GET    /v1/policies/{id}                                    -> 200 { Policy }
PUT    /v1/policies/{id}    { ...policy }                   -> 200 { Policy }
DELETE /v1/policies/{id}                                    -> 204
```

`GET /v1/policies` lists the calling creator's policies, filterable by scope. `PUT` replaces the whole document — consistent with resolution semantics (§6.2), a partial update would reintroduce merge thinking. `scope` and `scope_id` are immutable after creation; changing where a policy applies means deleting and recreating it.

Ownership: for `creator` and `platform` scopes, `scope_id` must derive from the caller's own `creator_id` or the request is `400`. For `content` scope, the `content_id` is opaque and cannot be validated against ownership at write time. **`ASSUMPTION`** — this is accepted: a creator can create a policy for a content ID they do not own, but it will never be evaluated, because resolution is always driven by a message that arrived through *their* connection. It is inert, not a privilege escalation.

Deleting a policy causes resolution to fall back to the next scope up, which may change moderation behaviour immediately for that content. This is intended.

### 6.7 Internal API

```
GetPolicy(creator_id, platform, content_id) -> ResolvedPolicy | null
```

gRPC (**`ASSUMPTION`** — chosen over HTTP for the hot path; it is called on every cache miss). The response carries `policy_id` and `resolved_at` alongside the policy body, so that flagged events can record which policy produced the verdict.

A **negative result is a valid, cacheable answer.** "This content has no policy" must be cached exactly like a positive one, or every message on every unconfigured content becomes a database read — which at 500K msg/s is the single easiest way to destroy this system.

### 6.8 Caching

Two layers, **TTL-only, no invalidation bus.** Policies change rarely; a bus adds a failure mode to the hot path for little benefit.

```
moderation-service (in-process LRU, TTL 60s)
        │ miss
        ▼
policy-service ──► Memcached (TTL 300s)
                        │ miss
                        ▼
                   Postgres
```

- **In-process LRU** in `moderation-service`, keyed `(creator_id, platform, content_id)`, TTL **60 s**, bounded by entry count (**`ASSUMPTION`** — 100K entries, roughly the active-content ceiling per instance).
- **Memcached** in `policy-service`, same key, TTL **300 s**.
- Negative results cached at both layers with the same TTLs.

**Worst-case staleness after a policy write is therefore ~6 minutes.** This is a stated property of the system, not a defect. It must be documented in the product UI ("policy changes take effect within a few minutes"), because a creator who edits a policy mid-stream and sees no change will otherwise report it as a bug.

On Memcached failure, `policy-service` reads through to Postgres and continues. On `policy-service` failure with a cold local cache, `moderation-service` fails open (§4.7).

### 6.9 Area B metrics

| Metric | Type | Labels |
| --- | --- | --- |
| `policy_resolve_total` | counter | `result` (`content`/`platform`/`creator`/`none`) |
| `policy_cache_hits_total` | counter | `layer` (`local`/`memcached`), `hit` |
| `policy_resolve_duration_seconds` | histogram | `layer` |
| `policy_writes_total` | counter | `operation`, `scope` |

---

## 7. Area C — Moderation

**Services:** `provider-adapter`, `moderation-service`, `review-service`
**Stores:** Redis, Postgres (`review` schema — cursors only)
**Produces:** `messages.v1`, `flagged.v1`, `deletions.v1`, `usage.v1`
**Consumes:** `messages.v1`, `flagged.v1`, `deletions.v1`

This is the hot path. Everything in it is written against the assumption that it will see 500K msg/s with a heavily skewed distribution — a handful of enormous streams alongside a very long tail of quiet ones.

### 7.1 Pipeline

```
Platform ──► provider-adapter ──► messages.v1 ──► moderation-service ──┬──► (clean: nothing)
                   ▲                                                    │
                   │                                                    ├──► flagged.v1 (action=review)
                   │                                                    │         │
                   │                                                    │         ▼
                   │                                                    │   review-service ──► creator
                   │                                                    │         │
                   │                                                    │         │ upheld
                   └────────────── deletions.v1 ◄──────────────────────┴─────────┘
                                                     (action=auto_delete)
```

Both `auto_delete` verdicts and upheld reviews converge on `deletions.v1`. **`provider-adapter` consumes only `deletions.v1` and never reads `flagged.v1`** — it has no notion of review at all. This is why `action` rides on the flagged event but is resolved before anything reaches the adapter.

### 7.2 `provider-adapter`

The adapter is the only component that knows a platform exists. Its two jobs are to turn platform-specific chat streams into `messages.v1`, and to turn `deletions.v1` back into platform-specific delete calls.

**Driver interface** — the whole of N3 (extensible platforms) rests on this:

```go
type Driver interface {
    Platform() string
    // Watch streams messages for one connection until ctx is cancelled.
    Watch(ctx context.Context, conn Connection, out chan<- Message) error
    // Delete removes one message. Returns nil if already gone.
    Delete(ctx context.Context, conn Connection, contentID, messageID string) error
    // DiscoverLive reports currently-live content for a connection.
    DiscoverLive(ctx context.Context, conn Connection) ([]ContentRef, error)
}
```

Adding a platform is one new `Driver` implementation and one enum value. Nothing outside `provider-adapter` changes.

**Opaque ID construction.** The adapter mints `content_id`, `author_id`, and `message_id` and is the only component that can resolve them back:

```
content_id = base62(platform_tag || hash(platform_native_channel_id))
```

The platform tag is embedded so the adapter can route a deletion without a lookup, but the value is opaque to every other service and must never be parsed outside the adapter (P5).

**`ASSUMPTION` — connection sharding.** None of this was specified. Adapter instances coordinate through a lightweight assignment mechanism: each active connection is a work item, hashed onto a ring of adapter instances registered in a coordinator (etcd or Kafka group membership). An instance watches only the connections in its ring segment. Instances joining or leaving trigger a rebalance in which only the affected segment's connections reconnect. Each instance holds a bounded number of connections (**`ASSUMPTION`** — 5 000) and horizontal scale is the only lever.

**`ASSUMPTION` — per-platform ingestion.** Each platform's live-chat mechanics differ substantially and provider APIs change; **verify all of the following against current provider documentation before implementing**:

| | YouTube | Twitch | Discord |
| --- | --- | --- | --- |
| Transport | Poll LiveChatMessages | EventSub WebSocket / IRC | Gateway WebSocket |
| Liveness | Poll for active broadcasts | EventSub `stream.online` | Bot is always resident |
| Key constraint | Hard daily quota; polling interval dictated by the API | Subscription cost limits per socket | Shard count scales with guild count |
| Delete | `liveChatMessages.delete` | Helix delete-chat-message | `DELETE /channels/{id}/messages/{id}` |
| Identity | User OAuth | User OAuth, must be channel mod | Bot install with `MANAGE_MESSAGES` |

YouTube's polling model is the reason the latency SLI starts at adapter ingress (§4.6): the platform's own delivery delay can exceed the entire budget and is not ours to fix.

**Deletion consumer.** Reads `deletions.v1` (keyed by `content_id`, so one stream's deletions land together and can be batched per platform where the API allows it).

| Provider response | Handling |
| --- | --- |
| Success | `deletions_total{outcome="ok"}` |
| Not found / already deleted | **Treated as success** — the viewer or another mod got there first |
| Stream ended / content gone | Terminal drop, `outcome="gone"` |
| Rate limited (429) | Backoff with jitter, retry |
| Auth error (401) | Refresh token (§5.6), retry once |
| Other 5xx | Exponential backoff, up to 5 attempts, then drop |

Deletion is best-effort by design: per P2, a failing provider must not stall the pipeline.

### 7.3 `moderation-service`

One consumer group on `messages.v1`. Per-partition ordering plus the `hash(author_id, content_id)` key means all state for a given `(sender, content)` is mutated by exactly one consumer, in order — **no distributed locking is required anywhere in the hot path.** This is the single most valuable property of the partitioning scheme and must not be broken by any future re-keying.

Per message:

```
1. Redelivery guard    seen:{message_id}     → if present, drop
2. Resolve policy      GetPolicy(...)        → null? emit usage, done
3. Rate limit          Redis token bucket    → hit? flag(auto_delete), done
4. Duplicate           Redis recent hashes   → hit? flag(auto_delete), done
5. Semantic spam       embed + Redis vectors → hit? flag(auto_delete), done
6. Restricted words    in-memory matcher     → hit? flag(auto_delete), done
7. Sampler             Redis bucket          → not sampled? done
8. LLM                 batched vLLM call     → hit? flag(action per policy), done
9. Usage               aggregate counter     → flushed to usage.v1 per minute
```

**First hit wins.** Stages are ordered strictly by cost. Stages disabled by policy are skipped. A message that survives all stages is clean and produces nothing except a usage increment.

Steps 1 and 3–5 are skipped entirely if Redis is unavailable; step 2 falls through to no-moderation if policy is unavailable; step 8 is skipped if the LLM is unavailable — all per §4.7, all counted.

### 7.4 Detectors

**Redelivery guard.** `SET seen:{message_id} 1 NX EX 300`. If the key already existed, this is a Kafka redelivery and the message is dropped. Without this, a redelivered message would be caught by the duplicate detector and wrongly flagged as spam — the message would be identical to itself. Any deployment that loses Redis will therefore see a burst of spurious duplicate flags on redelivery; this is an accepted consequence of fail-open.

**Rate limit** (`rate_limit_messages` per `rate_limit_seconds`). Token bucket in Redis, one Lua script, key `rate:{content_id}:{author_id}`, capacity `rate_limit_messages`, refill `messages/seconds` per second. Atomic decrement-or-reject; TTL twice the window so idle senders evict themselves.

**Duplicate** (`spam = identical`). `dup:{content_id}:{author_id}` holds the last N (**`ASSUMPTION`** — 20) message hashes, TTL 5 min. Hash is SHA-256 over the text normalised by lowercasing, collapsing whitespace, and stripping zero-width characters — otherwise trivially defeated by an added space. `LPUSH` + `LTRIM` + membership check in one Lua script.

**Semantic spam** (`spam = semantic`). Embeds the message, compares against the last N embeddings for the same `(content, sender)` stored packed in `emb:{content_id}:{author_id}`, flags on cosine similarity above threshold (**`ASSUMPTION`** — 0.95, deliberately high; this catches reworded repetition from one sender, not two senders coincidentally agreeing). Uses the same embedding service as Insights (§8.4) so a message is embedded at most once and the vector is reused.

This is the only detector before the LLM that costs a network round trip, which is why it sits last among the cheap stages and why it is off by default.

**Restricted words.** Compiled per policy into an Aho–Corasick automaton, cached in-process alongside the policy object and rebuilt on cache refresh. Matching is on the same normalised text as the duplicate hash, and is whole-token, not substring — substring matching produces the Scunthorpe problem and would make the feature unusable.

**Restricted content (LLM).** §7.9.

### 7.5 The sampler

The sampler is what makes the LLM stage affordable. It sits between the cheap detectors and the LLM, and only the LLM stage is subject to it.

Per-content token bucket, `samp:{content_id}`:

- Refill rate: **30 tokens/minute** (**`ASSUMPTION`**)
- Capacity: **30 tokens** (**`ASSUMPTION`**)
- Each message reaching stage 7 consumes one token; no token means the message skips the LLM and is treated as clean.

This is deliberately a flat ceiling rather than a percentage. Consequences:

| Content traffic | LLM coverage |
| --- | --- |
| 100 msg/month | 100% |
| 20 msg/min | 100% |
| 100 msg/min | ~30% |
| 6 000 msg/min | ~0.5% |

Quiet content — where a single violation matters and volume is trivial — is evaluated exhaustively. Firehose content is sampled down to a fixed, predictable cost, which also means **LLM load is bounded by the number of active contents, not by message volume**: 50K concurrent live contents at 30/min is ~25K LLM evaluations/second at the ceiling, and real load is far below that because most contents never approach it.

Both parameters are per-deployment configuration, and the ceiling is the primary lever for trading moderation coverage against GPU spend.

### 7.6 `review-service`

Consumes `flagged.v1` where `action = review` and serves the creator's review queue.

**There is no message database.** `flagged.v1` is keyed by `creator_id`, so every pending review for one creator lives in one partition, in order. The queue is a *position in that partition*, and the only persisted state is a cursor:

```sql
CREATE TABLE review_cursors (
    creator_id  UUID PRIMARY KEY REFERENCES creators(id) ON DELETE CASCADE,
    partition   INT    NOT NULL,
    next_offset BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

A partition carries many creators, so Kafka consumer-group offsets cannot represent per-creator progress — this table does. It is one small row per creator, updated only when a creator actually reviews something.

```
GET  /v1/reviews?limit=50    -> 200 { items: PendingReview[], next_cursor }
POST /v1/reviews             { decisions: [{ message_id, flagged }] } -> 200 { applied, deleted }
```

`PendingReview`:

```json
{
  "message_id": "ytc_01J8XQ7K2M4N",
  "content_id": "ct_9f2a",
  "text": "…",
  "detector": "restricted_content",
  "policy_id": "pol_7a13",
  "flagged_at": "2026-08-19T14:02:11.914Z"
}
```

**Read.** Resolve `creator_id` → partition (`hash(creator_id) % partitions`), seek to `next_offset`, scan forward, return the next `limit` events belonging to this creator with `action=review`, skipping other creators' interleaved events. **The cursor is not advanced by a read** — reading is idempotent and repeatable.

**Write.** `POST /v1/reviews` carries decisions for the batch just read. The service re-seeks to `next_offset`, re-reads the same window, matches decisions by `message_id`, publishes `deletions.v1` for every `flagged: true`, and advances `next_offset` past the window — all in one transaction against the cursor row. Replaying the same batch at the same cursor is a no-op, so the endpoint is naturally idempotent; the `Idempotency-Key` header is additionally honoured.

Decisions referencing messages outside the current window are ignored and reported in the response, rather than failing the batch.

**Consequences to state plainly, all intended:**

1. **Review is sequential.** A creator works the queue in order; they cannot skip ahead, jump to a specific message, or revisit a decision. A UI must not offer those affordances. This is the price of having no message store, and it is the right trade — a review queue is worked front-to-back anyway.
2. **Retention is the review deadline.** `flagged.v1` retains 7 days. A message flagged for review that nobody looks at is **never deleted** — it stays live on the platform permanently. Creators are responsible for their own communities; the system does not delete on their behalf by default.
3. **A cursor falling behind retention loses that window silently.** If a creator ignores their queue for longer than retention, the skipped messages are unreviewable and gone. `review_queue_lag_seconds` exposes this per creator, and the UI should surface it.

### 7.7 State machine

```
                  ┌──────────┐
   message ──────►│  clean   │  (no output)
                  └──────────┘

                  ┌──────────────┐        ┌──────────┐        ┌─────────┐
   message ──────►│ auto_delete  │───────►│ deleting │───────►│ deleted │
                  └──────────────┘        └──────────┘        └─────────┘
                                                │  not found / gone
                                                └──────────────► dropped

                  ┌──────────┐   upheld    ┌──────────┐
   message ──────►│ pending  │────────────►│ deleting │──► …
                  │  review  │  dismissed  └──────────┘
                  └──────────┘────────────► kept
                        │ retention expiry
                        └────────────────► kept (never actioned)
```

Nothing in this machine is persisted as state. Each transition is a topic write, and the current state of any message is implicit in which topics contain it.

### 7.8 Ordering, duplication, and correctness

| Concern | Mechanism |
| --- | --- |
| Redis state races | `hash(author_id, content_id)` partitioning — one consumer owns all state for a sender-content pair |
| Kafka redelivery | `seen:{message_id}` guard (§7.4) |
| Double deletion | Provider "already deleted" treated as success |
| Double billing | Deterministic `idempotency_key` on `usage.v1` + unique index (§5.3) |
| Double review | Cursor advance is transactional; replays are no-ops |

Exactly-once processing is **not** attempted. At-least-once delivery with idempotent effects is the target, and every effect above is idempotent.

### 7.9 LLM integration

vLLM, called over HTTP with continuous batching.

**Batching.** Messages reaching stage 8 are accumulated per policy and dispatched when either **32 messages** or **50 ms** is reached (**`ASSUMPTION`**, both configurable). Batching by policy means the policy text is sent once per batch rather than once per message — a large share of the prompt tokens, since the policy is far longer than a chat message.

**Prompt.** `restricted_content` entries become a numbered rubric; messages become a numbered list; the model returns strict JSON.

```
System: You are a chat moderation classifier. For each message, decide whether it
violates any of the numbered rules. Respond with JSON only.

Rules:
1. Ticket scalping — Offers to resell event tickets, or requests to buy them.
   Examples: "selling 2 tickets for tonight DM me" | "anyone got a spare ticket"
2. …

Messages:
1. <text>
2. <text>

Respond: {"results":[{"i":1,"violates":0},{"i":2,"violates":1}]}
```

`violates` is the rule number, or `0` for none. Structured output is enforced by a JSON schema / guided decoding rather than by parsing prose — an unparseable response is a fail-open, and guided decoding makes that path rare.

**Timeout: 1 000 ms** (**`ASSUMPTION`** — the LLM's share of the latency budget, §4.6). On timeout, incomplete response, or transport error, **the entire batch fails open** and is counted. There is no retry: a retry would blow the budget, and the message has already been live for a second.

**`ASSUMPTION` — model.** A small instruct model (~7–8B, quantised) served on vLLM. The classification task is simple and heavily few-shot anchored by the policy; the constraint is throughput, not reasoning depth. Model choice should be validated against a labelled sample before launch, and the sampler ceiling (§7.5) retuned to whatever the chosen model's throughput supports.

### 7.10 Usage emission

`moderation-service` keeps an in-process counter per `(creator_id, minute)` and flushes to `usage.v1` on minute boundaries:

```
idempotency_key = "mod:{instance_id}:{minute}:{creator_id}"
```

Deterministic per instance and window, so a redelivered flush is discarded by the ledger's unique index (§5.3). Counting is per message *processed*, not per message flagged — a creator pays for throughput, not for violations. What that converts to in credits is `credits-service`'s business alone.

### 7.11 Area C metrics

| Metric | Type | Labels |
| --- | --- | --- |
| `moderation_messages_total` | counter | `outcome` (`clean`/`flagged`/`skipped`) |
| `moderation_detector_hits_total` | counter | `detector`, `action` |
| `moderation_e2e_latency_seconds` | histogram | — |
| `moderation_stage_duration_seconds` | histogram | `stage` |
| `sampler_skipped_total` | counter | — |
| `llm_batch_size` | histogram | — |
| `llm_requests_total` | counter | `outcome` |
| `llm_latency_seconds` | histogram | — |
| `adapter_connections_active` | gauge | `platform` |
| `adapter_ingest_total` | counter | `platform` |
| `deletions_total` | counter | `platform`, `outcome` |
| `review_pending_estimate` | gauge | — |
| `review_queue_lag_seconds` | gauge | — |

---

## 8. Area D — Insights

**Services:** `insights-service`, `clustering-service`, `clusters-job`
**Stores:** Milvus, ClickHouse, S3
**Consumes:** `messages.v1`, `flagged.v1`
**Produces:** `usage.v1`

### 8.1 What Insights is

Insights answers *what is my community talking about*, over time. Everything is inferred — **creators do not define topics or themes**, and there is no endpoint to create them.

| Term | Definition |
| --- | --- |
| **Topic** | A cluster of semantically similar messages within one creator's space. Machine-discovered, LLM-labelled. |
| **Theme** | A sub-cluster within a topic — a more specific strand of the same subject. |

Neither is ever backed by stored message text. A topic is a centroid vector plus a label plus counters.

### 8.2 Pipeline

```
messages.v1 ──┐
              ├──► insights-service ──► embedding ──┬──► S3 (parquet, forever)
flagged.v1 ───┘        (2s buffer)                  │
                                                    └──► clustering-service
                                                              │ nearest centroid
                                                              ├──► Milvus (centroids)
                                                              └──► ClickHouse (counters)

                    clusters-job (periodic + on-demand)
                       reads S3 → HDBSCAN → LLM labels → Milvus + ClickHouse
```

### 8.3 The exclusion buffer

Insights consumes both `messages.v1` and `flagged.v1` and processes **only messages that were never flagged**. Dabet builds a picture of the community, not a dossier on offenders.

The two topics are not synchronised: message M arrives at t=0, and its flag — if any — arrives a few hundred milliseconds later. `insights-service` therefore holds every message in an in-memory buffer for **2 seconds** (**`ASSUMPTION`**, configurable) before embedding, and drops it if a flag for that `message_id` arrives within the window.

The window exceeds the p95 verdict latency (§4.6) but not the tail, so a small fraction of flagged messages — those verdicted slowly — will be embedded anyway. This is accepted: the alternative is delaying the whole pipeline to the moderation tail latency, and a handful of contaminating vectors has no effect on cluster structure. `insights_contamination_estimate_total` counts flags arriving after their message was already embedded.

The buffer is in-memory and lost on restart, which drops up to 2 seconds of messages per instance. Also accepted — Insights is an aggregate product and does not require completeness.

### 8.4 Embedding

One embedding service, shared with semantic spam detection (§7.4), so a message is embedded at most once. **`ASSUMPTION`** — a small sentence-transformer class model, 384 dimensions, served on the same vLLM fleet or a dedicated CPU pool. Chat messages are short; 384 dimensions is ample and keeps Milvus and S3 an order of magnitude smaller than a 1536-dimension alternative.

Sampling applies here too, on the same principle as §7.5: low-volume content is embedded exhaustively, high-volume content is sampled to a per-content ceiling. Topic distribution is a statistical property — a sample of a firehose gives the same answer as the whole of it.

**Records written to S3** (parquet, partitioned `creator_id / date`):

```
creator_id, content_id, embedded_at, vector[384]
```

**No `author_id`. No text.** This is what makes indefinite retention defensible (§4.8) — the corpus is not attributable to individuals and cannot be reversed into messages.

**Volume.** 384 floats at fp16 is 768 B, ~800 B with metadata. At a sustained sampled rate of 50K/s that is ~40 MB/s, ~3.5 TB/day of parquet before compression. **This is the dominant storage cost in the system** and the sampling ceiling is the lever that controls it. Tune it before launch, not after.

### 8.5 Live classification — `clustering-service`

For each embedding, find the nearest existing topic centroid for that creator in Milvus:

- **Similarity ≥ 0.75** (**`ASSUMPTION`**) → assign to that topic, increment ClickHouse counters, then test theme centroids within the topic for a sub-assignment.
- **Below threshold** → **unassigned**. The vector is already in S3 and will be considered at the next reclustering, where it may join an existing cluster or seed a new one.

Unassigned is a normal outcome, not an error. A cold creator whose clusters have never been built has everything unassigned until the first `clusters-job` run.

**Milvus layout.** One collection, partitioned by `creator_id`. Not one collection per creator — 10M collections is not a workable Milvus deployment; partitions are. Only **centroids** are stored in Milvus (topics and themes), never per-message vectors: at a few hundred topics per active creator the index stays small and searches stay fast. The full message corpus lives in S3, which is where batch clustering reads from.

### 8.6 Batch clustering — `clusters-job`

Reads a creator's parquet for a window from S3, runs **HDBSCAN**, labels each resulting cluster with an LLM, and writes centroids to Milvus and aggregates to ClickHouse. HDBSCAN is chosen because it does not require k up front and treats sparse points as noise rather than forcing them into a cluster — the right shape for chat, where most messages belong to a few big conversations and a tail belongs to nothing.

Hierarchy: HDBSCAN at coarse granularity yields **topics**; a second pass within each topic yields **themes**.

**Labelling.** For each cluster, take the N points nearest the centroid (**`ASSUMPTION`** — 20) and ask the LLM for a short label and description. Because message text is not retained, labelling runs on the same job pass that computed the clustering, while the source text is *not* available either — so labels are generated from the cluster's nearest neighbours in embedding space as represented by the topic's own prior label and by any still-in-retention text. **This is a real constraint: once text ages out of Kafka, clusters can only be described relative to existing labels.** First labelling of a topic must therefore happen while its messages are still within Kafka retention, or the topic gets a generic label. See §10.

**Triggers** (from the source design):

| Trigger | Rationale |
| --- | --- |
| First 100 messages for a creator | Bootstrap — nothing to compare against before this |
| Message count doubles since last run | Corpus has changed enough to reshape clusters |
| Unassigned rate exceeds threshold | **`ASSUMPTION`** — >30% unassigned over the last hour means the existing clusters no longer describe the conversation |
| On demand, for windows older than 7 days | Creator explicitly requests a refresh of historical data |

**On-demand reclustering rewrites history for the requesting creator only.** A creator who asks to recluster last month will see last month's numbers change — that is the point of the feature, but it means the dashboard is not immutable. Label it in the UI.

Reclustering emits `usage.v1` with `event_type = messages_reclustered` and the message count, since it is a real compute cost. Only `credits-service` knows what that costs (§5.7).

### 8.7 ClickHouse

```sql
CREATE TABLE topic_counts (
    creator_id   UUID,
    content_id   String,
    topic_id     UUID,
    theme_id     UUID,          -- zero UUID when the assignment is topic-level only
    bucket_hour  DateTime,
    count        UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(bucket_hour)
ORDER BY (creator_id, bucket_hour, topic_id, theme_id, content_id);

CREATE TABLE topics (
    creator_id  UUID,
    topic_id    UUID,
    parent_id   UUID,           -- zero UUID for a topic; topic_id for a theme
    label       String,
    description String,
    version     UInt32,
    updated_at  DateTime
) ENGINE = ReplacingMergeTree(version)
ORDER BY (creator_id, topic_id);
```

`SummingMergeTree` collapses the live increments; `ReplacingMergeTree` on `version` lets a recluster overwrite labels in place. Hourly buckets are the base granularity; days and months are rollups over the same table, so no separate schema is needed as retention grows.

The ordering key leads with `creator_id` because every query is creator-scoped — there is no cross-creator analytics product.

### 8.8 API

```
GET /v1/topics       ?content_id=&from=&to=&granularity=hour|day|month  -> 200 { items: Topic[] }
GET /v1/topics/{id}                                                      -> 200 { Topic }
GET /v1/topics/{id}/themes  ?from=&to=                                   -> 200 { items: Theme[] }
```

```json
{
  "id": "…",
  "label": "Ticket resale",
  "description": "Viewers asking about or offering event tickets.",
  "message_count": 1284,
  "series": [
    { "bucket": "2026-08-19T13:00:00Z", "count": 87 },
    { "bucket": "2026-08-19T14:00:00Z", "count": 142 }
  ]
}
```

Topics are returned ordered by volume over the requested window. `from`/`to` default to the last 24 hours; `granularity` defaults to `hour`.

**There are no sample endpoints and no message retrieval.** A creator cannot see the messages behind a topic, because those messages do not exist anywhere after retention. This is a direct and intended consequence of storing embeddings only, and it must be reflected in the UI — a topic is a shape and a count, not a drill-down.

Removed from the original API design for the same reason: `POST /topics`, `POST /topics/{id}/themes` (nothing is creator-defined), and `GET /topics/{id}/themes/{id}/sample` (no messages to sample).

### 8.9 Area D metrics

| Metric | Type | Labels |
| --- | --- | --- |
| `insights_messages_buffered` | gauge | — |
| `insights_messages_dropped_total` | counter | `reason` (`flagged`/`sampled`/`restart`) |
| `insights_contamination_estimate_total` | counter | — |
| `embedding_requests_total` | counter | `outcome` |
| `embedding_latency_seconds` | histogram | — |
| `clustering_assignments_total` | counter | `result` (`topic`/`theme`/`unassigned`) |
| `clusters_job_runs_total` | counter | `trigger`, `outcome` |
| `clusters_job_duration_seconds` | histogram | `trigger` |
| `s3_embedding_bytes_written_total` | counter | — |

---

## 9. Assumptions register

Every one of these was unspecified and filled with a defensible default. They are the first things to challenge in review.

| # | Area | Assumption |
| --- | --- | --- |
| A1 | A | Argon2id password hashing; 12-char minimum; common-password rejection |
| A2 | A | Access JWT 15 min; refresh token 30 d, rotating, family-revoked on reuse |
| A3 | A | Email verification required before connecting a platform, not before login |
| A4 | A | One platform account may be actively connected by one creator at a time |
| A5 | A | OAuth scopes per platform as tabled in §5.5 — **verify against current provider docs** |
| A6 | A | Platform revocation detected lazily on refresh failure; creator emailed |
| A7 | A | `credits_ok` flag cached 60 s in moderation; up to 60 s of free processing after exhaustion |
| A8 | A | Balance-threshold emails at 20% and 0% |
| A9 | B | Policy validation caps as tabled in §6.5 |
| A10 | B | In-process policy cache TTL 60 s, 100K entries; Memcached TTL 300 s; ~6 min worst-case staleness |
| A11 | B | `GetPolicy` over gRPC |
| A12 | B | Content-scoped policies for unowned content IDs are accepted and inert |
| A13 | C | Adapter connection sharding by consistent hashing over a coordinator; 5 000 connections/instance |
| A14 | C | Per-platform ingestion mechanics as tabled in §7.2 — **the thinnest area; verify first** |
| A15 | C | Duplicate detector keeps last 20 hashes per `(content, sender)`, 5 min TTL |
| A16 | C | Semantic spam threshold 0.95 cosine |
| A17 | C | Sampler: 30 tokens/min, capacity 30, per content |
| A18 | C | LLM batch 32 messages or 50 ms; 1 000 ms timeout; no retry; whole batch fails open |
| A19 | C | ~7–8B quantised instruct model on vLLM, guided JSON decoding |
| A20 | D | Exclusion buffer 2 s; late flags accepted as contamination |
| A21 | D | 384-dimension embeddings, shared with semantic spam |
| A22 | D | Milvus: one collection partitioned by `creator_id`; centroids only |
| A23 | D | Live assignment threshold 0.75 cosine |
| A24 | D | Recluster trigger at >30% unassigned over an hour |
| A25 | D | Cluster labelling from 20 nearest points to the centroid |

---

## 10. Known gaps and deferred work

Things this design knowingly does not do. Listed so that a reviewer can see they were decided rather than missed.

**Accepted risks**

1. **OAuth tokens are stored unencrypted.** A database compromise yields live platform credentials for every connected creator. Envelope encryption via KMS is the standard mitigation and was deliberately deferred as overhead.
2. **Fail-open means under-moderation during incidents.** A prolonged LLM or Redis outage means offensive content reaches viewers with no record that it should not have. `fail_open_total` is the only signal.
3. **Redis is a single logical dependency for the whole hot path.** Cluster mode with hash tags is designed in from day one, but a full Redis outage degrades four of five detectors simultaneously.
4. **Unreviewed flags are never actioned.** A creator who ignores their queue effectively runs unmoderated on the LLM path, permanently.
5. **~6 minutes of policy staleness.** Acceptable for a rarely-changing configuration, surprising to a creator editing mid-stream.
6. **Cluster labels degrade after retention.** Once text ages out of Kafka, new clusters cannot be described from source messages (§8.6). Either label eagerly within the retention window, or accept generic labels on historical reclusters.

**Deferred features**

1. **Cross-sender raid detection.** All spam and rate-limit state is keyed per `(content, sender)`. Fifty accounts each posting one identical message defeat every current detector. This is the most likely first gap in production and would require content-scoped state with a different Redis key design.
2. **Warn / timeout / ban actions and escalation ladders.** Only delete and review exist. The Redis token bucket already supports an "until" semantic that would carry timeouts.
3. **Policy versioning.** Flagged events record `policy_id` but not a version, so a verdict cannot be reconstructed against the policy as it stood at the time.
4. **Policy dry-run.** No way to test a policy before it takes effect, which given §6.8's staleness makes iteration slow.
5. **Roles, teams, and seats.** One creator, one account, full access. Real creators delegate moderation to staff.
6. **Push notification of pending reviews.** The queue is polled. A live-stream review workflow really wants a socket.
7. **Non-English moderation** and **distillation of LLM verdicts into small classifiers.** Both explicitly below the line for v1; note that the system currently logs nothing that could serve as distillation training data, so enabling that later requires a deliberate change to what is retained.
8. **Multi-region.** Single-region, multi-AZ. The Kafka-centric design makes active-active non-trivial.

---

## 11. Build order

A suggested sequence that keeps the four areas unblocked. §4 must be agreed and frozen before any of it starts.

| Phase | A | B | C | D |
| --- | --- | --- | --- | --- |
| 1 | Register, login, JWT | Schema, CRUD | Driver interface, one platform | Skeleton consumer |
| 2 | OAuth, connections | Resolution + caching | messages.v1 → cheap detectors | Embedding + S3 |
| 3 | Credits ledger, Stripe | `GetPolicy` gRPC | Sampler + LLM + flagged.v1 | Milvus + live assignment |
| 4 | Usage consumer | Validation, limits | review-service, deletions.v1 | clusters-job, ClickHouse |
| 5 | — | — | Remaining platform drivers | On-demand recluster |

The critical path runs through Area C. Areas A, B, and D can be developed against fixtures of the §4.2 topic schemas without C existing.
