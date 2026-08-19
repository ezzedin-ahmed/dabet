# Build from the tools/ module root: docker build -f mockembed/Dockerfile .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/mockembed ./mockembed

FROM alpine:3.20
COPY --from=build /out/mockembed /usr/local/bin/mockembed
EXPOSE 8091
ENTRYPOINT ["mockembed"]
