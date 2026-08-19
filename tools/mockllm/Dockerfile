# Build from the tools/ module root: docker build -f mockllm/Dockerfile .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/mockllm ./mockllm

FROM alpine:3.20
COPY --from=build /out/mockllm /usr/local/bin/mockllm
EXPOSE 8089
ENTRYPOINT ["mockllm"]
