# syntax=docker/dockerfile:1
# Build context is the REPO ROOT:  docker build -f services/provider-adapter/Dockerfile .
# The module's `replace dabet/pkg => ../../pkg` needs pkg/ beside it, and
# GOWORK=off keeps the build from demanding the other eight go.work modules.
# Final image is alpine (not distroless) only so that compose healthchecks
# have a wget; it runs as an unprivileged user either way.
FROM golang:1.26-alpine AS build
WORKDIR /src
ENV GOWORK=off CGO_ENABLED=0
COPY pkg/ ./pkg/
COPY services/provider-adapter/go.mod services/provider-adapter/go.sum ./services/provider-adapter/
WORKDIR /src/services/provider-adapter
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY services/provider-adapter/ ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/provider-adapter ./cmd/provider-adapter

FROM alpine:3.20
RUN adduser -D -u 65532 dabet
COPY --from=build /out/provider-adapter /usr/local/bin/provider-adapter
USER dabet
EXPOSE 8080 9090
ENTRYPOINT ["provider-adapter"]
