COMPOSE := docker compose -f deploy/compose/docker-compose.yml
# Tracing overlay. Base file first: the collector config is bind-mounted from a
# path relative to the first -f file's directory (deploy/compose/).
TRACED  := $(COMPOSE) -f deploy/compose/fragments/observability.yml
# Mail overlay: Mailpit captures everything; UI on :8025.
MAILED  := $(COMPOSE) -f deploy/compose/fragments/mail.yml
# Load overlay: 64 partitions, a latency-injecting LLM, three moderation consumers.
LOADC   := $(COMPOSE) -f deploy/compose/fragments/load.yml
LOAD_METRICS := http://localhost:9085,http://localhost:9185,http://localhost:9285
SCENARIO ?= baseline
RATE     ?= 400
GOBIN   := $(shell go env GOPATH)/bin
MODULES := $(shell go work edit -json | grep DiskPath | cut -d'"' -f4)

.PHONY: build test vet proto up up-full up-traced up-mail up-load load load-selfbench load-drills down logs ps e2e

build:
	@for m in $(MODULES); do (cd $$m && go build ./...) || exit 1; done

test:
	@for m in $(MODULES); do (cd $$m && go test ./...) || exit 1; done

vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done

proto:
	@command -v $(GOBIN)/protoc-gen-go >/dev/null 2>&1 || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@command -v $(GOBIN)/protoc-gen-go-grpc >/dev/null 2>&1 || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	PATH="$(GOBIN):$$PATH" protoc -I pkg/policyapi \
		--go_out=paths=source_relative:pkg/policyapi \
		--go-grpc_out=paths=source_relative:pkg/policyapi \
		pkg/policyapi/policy.proto

# Infrastructure, mocks and all services except the Milvus-backed pair.
# --wait blocks until every healthcheck passes, so `make up && make e2e`
# is a race-free sequence.
up:
	$(COMPOSE) up -d --build --wait

# Adds etcd + Milvus + clustering-service + clusters-job (§8.5, §8.6).
# Milvus alone wants several GB, hence the separate target.
up-full:
	CLUSTERING_ENDPOINT=http://clustering-service:8080 \
	$(COMPOSE) --profile clustering up -d --build --wait

# As `make up`, plus an OTel collector and Jaeger (UI on :16686), with every
# service exporting traces. The base file must come first: the collector config
# is bind-mounted from a path relative to the first file's directory.
up-traced:
	$(TRACED) up -d --build --wait

# As `make up`, plus Mailpit; services send real SMTP to it (UI on :8025).
up-mail:
	$(MAILED) up -d --build --wait

# Stack tuned for load: realistic partitions, slow LLM, three moderation consumers.
up-load:
	$(LOADC) up -d --build --wait

# Generator self-benchmark. Needs no stack; proves the harness is not the bottleneck.
load-selfbench:
	cd test/load && go run ./cmd/dabet-load -scenario selfbench

# One scenario against a running `make up-load`: make load SCENARIO=ramp RATE=2000
load:
	cd test/load && LOAD_MODERATION_METRICS=$(LOAD_METRICS) \
		go run ./cmd/dabet-load -scenario $(SCENARIO) -rate $(RATE) -out results/

# The §4.7 fail-open drills. Drives docker directly; requires the load stack.
load-drills:
	cd test/load && LOAD_MODERATION_METRICS=$(LOAD_METRICS) \
		go run ./cmd/dabet-load -scenario failopen -rate $(RATE) -out results/

# Tears down every overlay, whether or not it was started.
down:
	$(COMPOSE) -f deploy/compose/fragments/observability.yml \
		-f deploy/compose/fragments/mail.yml \
		-f deploy/compose/fragments/load.yml --profile clustering down -v

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) --profile clustering ps

# End-to-end smoke test against the running stack (see test/e2e). It is
# build-tagged, so `make test` never touches the network.
e2e:
	cd test/e2e && go test -tags e2e -count=1 -timeout 20m -v ./...
