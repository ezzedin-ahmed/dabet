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
| `tools/` | `dabet/tools` | `mockllm` and `mockembed` deterministic local mocks |
| `deploy/compose/` | — | Local infra (docker-compose) |

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

## Running local infra

```sh
cp deploy/compose/.env.example deploy/compose/.env   # optional; defaults work
make up        # postgres, kafka (+topics), redis, memcached, clickhouse,
               # minio (+buckets), milvus (+etcd), mockllm, mockembed
make down
make logs
```

The 9 dabet services are not in compose yet (a later phase wires them); run them on the host with the variables from `.env.example`. The mock LLM flags any prompt-listed message containing the literal string `FLAGME` as a rule-1 violation; the mock embedder returns deterministic 384-dim vectors.

## Building and testing

```sh
make build   # go build ./... across the workspace
make vet
make test
make proto   # regenerate pkg/policyapi from policy.proto (installs plugins if missing)
```

## Contributing

- **Frozen contracts:** `pkg/` and `go.work` are the interface between the four areas. They change only by cross-area agreement (docs §4). Everything else is area-local.
- **Testing bar:** every feature branch must include unit tests for its logic; `go build ./...`, `go vet ./...`, and `go test ./...` must pass in every module. Integration/e2e testing lands as a dedicated later phase — do not block feature branches on it, and do not skip unit tests because of it.
- **P4 — text is radioactive:** chat message text (and message/author/content IDs, for metrics) never goes into logs, metric labels, error messages, or databases.
- Conventional commits.
