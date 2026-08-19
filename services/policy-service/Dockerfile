# syntax=docker/dockerfile:1
# Build context is the REPO ROOT:  docker build -f services/policy-service/Dockerfile .
# The module's `replace dabet/pkg => ../../pkg` needs pkg/ beside it, and
# GOWORK=off keeps the build from demanding the other eight go.work modules.
# Final image is alpine (not distroless) only so that compose healthchecks
# have a wget; it runs as an unprivileged user either way.
FROM golang:1.26-alpine AS build
WORKDIR /src
ENV GOWORK=off CGO_ENABLED=0
COPY pkg/ ./pkg/
COPY services/policy-service/go.mod services/policy-service/go.sum ./services/policy-service/
WORKDIR /src/services/policy-service
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY services/policy-service/ ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/policy-service ./cmd/policy-service

FROM alpine:3.20
RUN adduser -D -u 65532 dabet
COPY --from=build /out/policy-service /usr/local/bin/policy-service
USER dabet
EXPOSE 8080 9090
ENTRYPOINT ["policy-service"]
