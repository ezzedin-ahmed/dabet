# Build from the tools/ module root: docker build -f mockstripe/Dockerfile .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/mockstripe ./mockstripe

FROM alpine:3.20
COPY --from=build /out/mockstripe /usr/local/bin/mockstripe
EXPOSE 9098
ENTRYPOINT ["mockstripe"]
