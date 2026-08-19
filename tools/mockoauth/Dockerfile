# Build from the tools/ module root: docker build -f mockoauth/Dockerfile .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/mockoauth ./mockoauth

FROM alpine:3.20
COPY --from=build /out/mockoauth /usr/local/bin/mockoauth
EXPOSE 9099
ENTRYPOINT ["mockoauth"]
