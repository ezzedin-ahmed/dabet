# syntax=docker/dockerfile:1
# Build context is the REPO ROOT:  docker build -f services/credits-service/Dockerfile .
# The module's `replace dabet/pkg => ../../pkg` needs pkg/ beside it, and
# GOWORK=off keeps the build from demanding the other eight go.work modules.
# Final image is alpine (not distroless) only so that compose healthchecks
# have a wget; it runs as an unprivileged user either way.
FROM golang:1.26-alpine AS build
WORKDIR /src
ENV GOWORK=off CGO_ENABLED=0
COPY pkg/ ./pkg/
COPY services/credits-service/go.mod services/credits-service/go.sum ./services/credits-service/
WORKDIR /src/services/credits-service
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY services/credits-service/ ./
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-s -w" -o /out/credits-service ./cmd/credits-service

FROM alpine:3.20
RUN adduser -D -u 65532 dabet
COPY --from=build /out/credits-service /usr/local/bin/credits-service
USER dabet
EXPOSE 8080 9090
ENTRYPOINT ["credits-service"]
