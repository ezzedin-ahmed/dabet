# dabet

Event-driven moderation engine: sub-second LLM policy enforcement for high-volume chats, with pluggable platform adapters.

The complete design is in **[docs/implementation-reference.md](docs/implementation-reference.md)**. Read §4 (shared contracts) before touching anything shared.

## Layout

Go multi-module workspace (`go.work` at the root pre-declares every module — it is not edited per-feature):

| Path | Module | What |
| --- | --- | --- |
| `pkg/` | `dabet/pkg` | Shared contracts and plumbing (docs §4 as code) |
| `services/user-service/` | `dabet/services/user-service` | Area A — accounts, connections, auth |
| `services/credits-service/` | `dabet/services/credits-service` | Area A — credits ledger, Stripe, usage.v1 consumer |
| `services/policy-service/` | `dabet/services/policy-service` | Area B — policy CRUD, resolution, GetPolicy gRPC |
| `services/provider-adapter/` | `dabet/services/provider-adapter` | Area C — platform drivers, messages.v1 in, deletions.v1 out |
| `services/moderation-service/` | `dabet/services/moderation-service` | Area C — detector cascade, sampler, LLM, verdicts |
| `services/review-service/` | `dabet/services/review-service` | Area C — creator review queue over flagged.v1 |
| `services/insights-service/` | `dabet/services/insights-service` | Area D — exclusion buffer, embeddings to S3 |
| `services/clustering-service/` | `dabet/services/clustering-service` | Area D — live centroid assignment (Milvus, ClickHouse) |
| `services/clusters-job/` | `dabet/services/clusters-job` | Area D — batch HDBSCAN + LLM labelling |
| `tools/` | `dabet/tools` | Deterministic stand-ins for the third parties: `mockllm`, `mockembed`, `mockoauth`, `mockstripe` |
| `test/e2e/` | `dabet/test/e2e` | Build-tagged end-to-end smoke test against the live stack |
| `deploy/compose/` | — | The whole local stack (docker-compose) |

### `pkg/` package map

| Package | Contents |
| --- | --- |
| `contracts` | Kafka topic names, the four event schemas (§4.2), detector/action/event_type enums, partition-key helpers |
| `httpx` | Error envelope + code table (§4.1), strict JSON decoding, request-ID middleware, cursor pagination, JWT auth, idempotency key |
| `config` | Env helpers and the canonical variable names (§4.4) |
| `obs` | Standard Prometheus metrics (§4.5), healthz/readyz, slog JSON logging. Read the package comment for the cardinality rule |
| `service` | Shared service runner: env config, logging, health, metrics, graceful shutdown |
| `kafkax` | franz-go producer (zstd, acks=all, idempotent) and consumer-group wrapper (at-least-once, commit after success) |
| `policyapi` | `GetPolicy` gRPC contract (§6.7). Generated `.pb.go` files are committed; `make proto` regenerates |
| `credits` | Internal `credits_ok` contract (§5.8): client with 60s cache + fail-open, and the server handler |
| `embeddings` | Shared embedding HTTP contract (384-dim) + client |
| `rediskeys` | Redis key builders (§4.3) with cluster hash tags from day one |

## Running the stack

```sh
cp deploy/compose/.env.example deploy/compose/.env   # optional; defaults work

make up        # infrastructure + mocks + 7 services, waits for every healthcheck
make e2e       # end-to-end smoke test against the running stack (~1 min)
make down      # stop everything and delete the volumes

make up-full   # as `make up`, plus etcd + Milvus + clustering-service + clusters-job
make logs      # follow everything
make ps        # what is running
```

`make up` blocks until every container is healthy, so `make up && make e2e` is
race-free. `make down` removes volumes, so each run starts from an empty
Postgres, Kafka, MinIO and ClickHouse.

### Profiles

| Profile | Contents | When |
| --- | --- | --- |
| default (`make up`) | Postgres, Kafka (+topics), Redis, Memcached, ClickHouse, MinIO (+buckets), the four mocks, and seven of the nine services | Everyday development, and everything `make e2e` needs |
| `clustering` (`make up-full`) | Adds etcd, Milvus, `clustering-service`, `clusters-job` | Working on live centroid assignment (§8.5) or batch topic discovery (§8.6) |

Milvus wants several GB on its own, which is why it is opt-in. Insights still
proves its durable leg without it: messages are embedded and rolled to MinIO as
parquet either way (§8.4) — only the *live* nearest-centroid assignment needs
Milvus, and the vectors it misses are picked up by the next `clusters-job` run.
With the profile off, `CLUSTERING_ENDPOINT` is empty and `insights-service`
disables the assignment hop rather than counting a fail-open against a service
that was never deployed.

### Service map and ports

Every service listens on `:8080` (app + `/healthz` + `/readyz`) and `:9090`
(Prometheus `/metrics`) inside the network. The host mappings:

| Service | HTTP | Metrics | Key endpoints |
| --- | --- | --- | --- |
| `user-service` | 8081 | 9081 | `/v1/auth/*`, `/v1/me`, `/v1/connections*` |
| `credits-service` | 8082 | 9082 | `/v1/credits*`, `/v1/webhooks/stripe`, `/internal/v1/credits-ok/{id}` |
| `policy-service` | 8083 | 9083 | `/v1/policies*`; `GetPolicy` gRPC on 7101 |
| `provider-adapter` | 8084 | 9084 | `/mock/messages`, `/mock/deletions` |
| `moderation-service` | 8085 | 9085 | no API — metrics only |
| `review-service` | 8086 | 9086 | `/v1/reviews` |
| `insights-service` | 8087 | 9087 | `/v1/topics*` |
| `clustering-service` *(profile)* | 8088 | 9088 | `/internal/v1/assign` |
| `clusters-job` *(profile)* | 8090 | 9089 | `/v1/topics/recluster` |

| Infrastructure | Host port |
| --- | --- |
| Postgres | 5432 |
| Kafka | 9092 |
| Redis | 6379 |
| Memcached | 11211 |
| ClickHouse | 8123 (HTTP), 9002 (native) |
| MinIO | 9000 (API), 9001 (console) |
| Milvus *(profile)* | 19530, 9091 |
| `mockllm` | 8089 |
| `mockembed` | 8091 |
| `mockoauth` | 9099 |
| `mockstripe` | 9098 |

### The mocks

Everything Dabet does not own is stubbed, and each stub speaks the real
protocol so the code under test is exercised rather than bypassed.

- **`mockllm`** — OpenAI-compatible `/v1/chat/completions`. Flags any listed
  message containing the literal string `FLAGME` as a rule-1 violation.
- **`mockembed`** — `/v1/embed`, deterministic 384-dim vectors from a seeded
  hash of the text.
- **`mockoauth`** — authorization-code + PKCE provider on `/oauth/{authorize,
  token,userinfo,revoke}`. PKCE S256 is genuinely verified, codes and refresh
  tokens are single-use, and every authorization mints a fresh provider user id
  so repeated runs do not collide on `connections_active_uniq` (§5.2). It backs
  the `mock` platform, which `user-service` serves when `OAUTH_MOCK_ENABLED` is
  set.
- **`mockstripe`** — `POST /v1/payment_intents` (form-encoded, honouring
  `Idempotency-Key`) plus a test-only `POST /internal/confirm` that delivers the
  `payment_intent.succeeded` webhook with a real HMAC-SHA256 `Stripe-Signature`.
  Credits are still granted only by the webhook (§5.7).

## End-to-end smoke test

`make e2e` runs `test/e2e` against the live stack. It is behind the `e2e` build
tag, so `make test` never touches the network. It drives one ordered scenario
over the real HTTP surfaces — nothing inside the system under test is stubbed —
and every cross-service assertion polls to a deadline rather than sleeping,
because verdicts cross three services and two Kafka topics and the usage ledger
only moves on a wall-clock minute boundary.

What it proves:

| Step | Assertion |
| --- | --- |
| a | Register → verify email → login → the JWT works on `/v1/me`; an unverified creator cannot connect a platform (422); `/v1/me` without a token is 401 |
| g1 | `POST /v1/credits/topup` reaches Stripe; an unsigned webhook is rejected (400); a signed `payment_intent.succeeded` grants credits; a redelivery is a no-op |
| b | The full OAuth round trip for the `mock` platform — authorize (with an S256 PKCE challenge) → callback → connection listed as active with a display name; the state is single-use; no token field appears anywhere in the response |
| c | A creator-scoped policy with a rate limit, `spam=identical`, a restricted word and a `restricted_content` rule routed to `review`; a second policy at the same scope is 409; the document is read back from storage |
| d | 13 messages injected through `provider-adapter`'s `POST /mock/messages`: one clean, one restricted word, eight rate-limit probes, an identical pair, and one `FLAGME` |
| e | `GET /mock/deletions` contains the restricted word, the rate-limited tail and the duplicate — and **not** the clean message, the in-budget probes, the original of the pair, or the `FLAGME` message (which is queued, not deleted). No message text appears in the deletion records |
| f | The `FLAGME` message is in `GET /v1/reviews` with `detector=restricted_content` and the right `policy_id`; reading does not advance the cursor; upholding it produces a deletion; the item leaves the queue and replaying the batch deletes nothing |
| g2 | `GET /v1/credits/entries` shows a `messages_processed` debit with a positive quantity, after the minute flush |
| h | A parquet object landed under `embeddings/creator_id=…/date=YYYY-MM-DD/`, with exactly the columns `creator_id, content_id, embedded_at, vector` — no `author_id`, no text — and no message text or author id anywhere in the bytes |
| i | `fail_open_total` is **zero on every service that was up**, detector hits were counted per detector, `restricted_content` was actioned as `review`, the LLM stage really ran, and no metric carries a `message_id` / `author_id` / `content_id` label (§4.5 cardinality rule) |

Ports and credentials are overridable via `E2E_*` environment variables (see
`test/e2e/helpers_test.go`) if the stack is published elsewhere.

### Test-only affordances

Two narrow, off-by-default hooks exist so an automated run can drive the system
without a human or a mailbox. Both are documented at their definition and are
enabled only in `deploy/compose/docker-compose.yml`.

- **`DEV_EXPOSE_VERIFICATION_TOKEN`** (`user-service`) — there is no mailer in
  v1, so the email-verification token is otherwise reachable only through a
  debug log line. With this set, `POST /v1/auth/register` returns it in the
  response body. It hands an unauthenticated caller a token that verifies an
  address, so it must never be set outside local development.
- **`POST /mock/messages` / `GET /mock/deletions`** (`provider-adapter`) — the
  mock platform's injection surface, which predates this phase. Deletion
  records now also carry `native_message_id`, resolved through the same
  `Resolver` the real drivers use, so a caller can correlate a deletion with
  its injection without parsing an opaque id (P5).

## Building and testing

```sh
make build   # per-module go build (see the note below)
make vet
make test    # unit tests only; the e2e suite is build-tagged out
make proto   # regenerate pkg/policyapi from policy.proto (installs plugins if missing)
```

`make build`/`vet`/`test` iterate the modules listed in `go.work` and run in
each module directory, because a workspace-wide `go build ./...` at the root
can fail depending on where the checkout lives. Every module also builds
standalone with `GOWORK=off` — that is what the Docker images do.

## Container images

Each service has a multi-stage `services/<name>/Dockerfile`. **The build
context is the repository root**, because the modules carry
`replace dabet/pkg => ../../pkg` and need `pkg/` beside them:

```sh
docker build -f services/user-service/Dockerfile -t dabet/user-service .
```

The build stage sets `GOWORK=off` so it needs only `pkg/` and the one service
module rather than all eleven workspace members. The final stage is
`alpine:3.20` running as an unprivileged user; alpine rather than distroless
only because the compose healthchecks need a `wget`.

## Known gaps

- **`clusters-job` is running but unexercised.** `make up-full` starts it, it
  reaches Milvus and ClickHouse and creates its schema, and its trigger loop
  runs — but the e2e does not force a batch clustering pass. Bootstrapping needs
  100 embeddings for one creator (`CLUSTERS_BOOTSTRAP_MIN`), well past what a
  smoke test injects. Live assignment *is* proven under the profile:
  `clustering_assignments_total{result="unassigned"}` moves, which is the
  correct outcome for a creator whose clusters have never been built (§8.5).
- **`GET /v1/topics` is unexercised** for the same reason — with no clusters
  there are no topics to return.
- **Semantic spam is unexercised.** The e2e policy uses `spam=identical`; the
  semantic path would need `spam=semantic` and two rewordings that the mock
  embedder happens to place within 0.95 cosine of each other, which would be
  asserting on the mock rather than on the detector.
- **Token refresh (§5.6) is unexercised.** `mockoauth` implements the
  `refresh_token` grant and rotates on use, but nothing in the smoke test
  expires an access token to trigger it.
- **One adapter instance only.** Connection sharding across adapter instances
  (A13) is not implemented, so the compose stack runs a single
  `provider-adapter`. `deletion.Group` is also a compile-time constant, so two
  independent adapter deployments cannot share a cluster.
- **`fail_open_total` is absent rather than zero** on a healthy service:
  Prometheus does not export a `CounterVec` series until it is first
  incremented. The e2e treats absent as zero, which is correct, but it means
  the assertion cannot distinguish "zero" from "this metric was never wired
  up". The accompanying positive assertions on `moderation_detector_hits_total`
  are what prove the metrics path is live.

## Contributing

- **Frozen contracts:** `pkg/` is the interface between the four areas. It
  changes only by cross-area agreement (docs §4). Everything else is
  area-local.
- **Testing bar:** every feature branch must include unit tests for its logic;
  `go build ./...`, `go vet ./...`, and `go test ./...` must pass in every
  module. Changes that touch the wiring should also pass `make up && make e2e`.
- **P4 — text is radioactive:** chat message text (and message/author/content
  IDs, for metrics) never goes into logs, metric labels, error messages, or
  databases.
- Conventional commits.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request: gofmt and
`go vet`, then per-module `go build` / `go test` / `go test -race`, a
`GOWORK=off` standalone build of every service (this is what container builds
do, and it catches missing `require`/`replace`/`go.sum` entries that the
workspace masks), and an image build for all nine services.

`.github/workflows/e2e.yml` runs `make up && make e2e` on pushes to `main`,
nightly, on manual dispatch, and on pull requests labelled `e2e`. It is not on
every PR because the stack builds eleven images and waits on nineteen
healthchecks. Compose logs are uploaded as an artifact when it fails.
