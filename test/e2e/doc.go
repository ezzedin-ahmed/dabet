// Package e2e drives the whole Dabet stack over its real HTTP surfaces,
// against the real Postgres / Kafka / Redis / Memcached / MinIO brought up
// by deploy/compose/docker-compose.yml. Nothing is stubbed inside the
// system under test: the only fakes are the third parties Dabet does not
// own (the LLM, the embedder, the OAuth provider and Stripe), and each of
// those speaks its real protocol.
//
// The suite lives behind the `e2e` build tag, so a plain `go test ./...`
// never reaches the network. Run it with `make e2e` against a stack
// started by `make up`. This file carries no code — it exists so the
// module has a buildable package when the tag is absent.
package e2e
