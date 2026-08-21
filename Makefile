COMPOSE := docker compose -f deploy/compose/docker-compose.yml
# Tracing overlay. Base file first: the collector config is bind-mounted from a
# path relative to the first -f file's directory (deploy/compose/).
TRACED  := $(COMPOSE) -f deploy/compose/fragments/observability.yml
# Mail overlay: Mailpit captures everything; UI on :8025.
MAILED  := $(COMPOSE) -f deploy/compose/fragments/mail.yml
# Load overlay: 64 partitions, a latency-injecting LLM, three moderation consumers.
LOADC   := $(COMPOSE) -f deploy/compose/fragments/load.yml
LOAD_METRICS := http://localhost:9085,http://localhost:9185,http://localhost:9285
# Sharding overlay: three adapter replicas sharing the connection ring (A13).
SHARDED := $(COMPOSE) -f deploy/compose/fragments/sharding.yml
SCENARIO ?= baseline
RATE     ?= 400
GOBIN   := $(shell go env GOPATH)/bin
MODULES := $(shell go work edit -json | grep DiskPath | cut -d'"' -f4)

.PHONY: build test vet kafka-guard tidy proto k8s-lint k8s-template tf-check up up-full up-traced up-mail up-load up-sharded up-full-e2e e2e-full load load-selfbench load-drills down logs ps e2e

build:
	@for m in $(MODULES); do (cd $$m && go build ./...) || exit 1; done

test:
	@for m in $(MODULES); do (cd $$m && go test ./...) || exit 1; done

# Container builds run with GOWORK=off, where each service resolves pkg via a
# replace directive and needs pkg's transitive deps in its OWN go.sum. A pkg
# change therefore breaks every image build until this is run. The workspace
# hides it; CI's standalone-build job is the backstop.
tidy:
	@for m in $(MODULES); do (cd $$m && GOWORK=off go mod tidy) || exit 1; done
	@for m in $(MODULES); do (cd $$m && GOWORK=off go build ./... >/dev/null) || exit 1; done
	@echo "all modules tidy and standalone-buildable"

vet: kafka-guard
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done

kafka-guard:
	@python3 hack/kafka-guard.py

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

# Three adapter replicas sharing the connection ring. Note `make e2e` injects
# through a load-balanced port, so it is not sharding-aware; see the fragment.
up-sharded:
	$(SHARDED) up -d --build --wait

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
		-f deploy/compose/fragments/load.yml \
		-f deploy/compose/fragments/sharding.yml \
		-f deploy/compose/fragments/e2e-extra.yml --profile clustering down -v

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) --profile clustering ps

# ---- Kubernetes -------------------------------------------------------------
K8S_APP  := deploy/k8s/charts/dabet
K8S_DEPS := deploy/k8s/charts/dabet-deps

# Lint both charts and render every dependency on/off combination. The render
# matrix falls back to render-only when no cluster is reachable, so this is
# safe in CI and on a laptop.
k8s-lint:
	helm lint $(K8S_APP)
	helm lint $(K8S_APP) -f $(K8S_APP)/values-local.yaml
	helm lint $(K8S_APP) -f $(K8S_APP)/values-aws.yaml
	helm lint $(K8S_DEPS)
	bash $(K8S_DEPS)/hack/render-matrix.sh

# Terraform/OpenTofu checks. Needs no AWS credentials and touches no state.
TF := deploy/terraform
tf-check:
	cd $(TF) && tofu fmt -check -recursive .
	cd $(TF) && tofu init -backend=false -input=false >/dev/null && tofu validate
	cd $(TF)/examples/dev  && tofu init -backend=false -input=false >/dev/null && tofu validate
	cd $(TF)/examples/prod && tofu init -backend=false -input=false >/dev/null && tofu validate

# Render one profile to stdout: make k8s-template ENV=aws
ENV ?= local
k8s-template:
	helm template dabet $(K8S_APP) -f $(K8S_APP)/values-$(ENV).yaml

# End-to-end smoke test against the running stack (see test/e2e). It is
# build-tagged, so `make test` never touches the network.
e2e:
	cd test/e2e && go test -tags e2e -count=1 -timeout 20m -v ./...

# The clustering profile with timings tightened enough for a test to observe
# a bootstrap run. Thresholds that decide *whether* a run happens stay at spec.
up-full-e2e:
	CLUSTERING_ENDPOINT=http://clustering-service:8080 \
	$(COMPOSE) -f deploy/compose/fragments/e2e-extra.yml --profile clustering up -d --build --wait

# Adds the Milvus-backed clustering suite. Needs `make up-full-e2e`.
e2e-full:
	cd test/e2e && go test -tags "e2e,e2e_full" -count=1 -timeout 30m -v ./...
