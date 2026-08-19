COMPOSE := docker compose -f deploy/compose/docker-compose.yml
GOBIN   := $(shell go env GOPATH)/bin

.PHONY: build test vet proto up down logs

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

proto:
	@command -v $(GOBIN)/protoc-gen-go >/dev/null 2>&1 || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@command -v $(GOBIN)/protoc-gen-go-grpc >/dev/null 2>&1 || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	PATH="$(GOBIN):$$PATH" protoc -I pkg/policyapi \
		--go_out=paths=source_relative:pkg/policyapi \
		--go-grpc_out=paths=source_relative:pkg/policyapi \
		pkg/policyapi/policy.proto

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f
