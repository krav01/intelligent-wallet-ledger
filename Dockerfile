# syntax=docker/dockerfile:1

FROM golang:1.26.8-alpine3.23 AS build

WORKDIR /src
COPY go.mod ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/wallet-api ./cmd/wallet-api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/outbox-publisher ./cmd/outbox-publisher

FROM alpine:3.23

RUN apk add --no-cache ca-certificates \
    && addgroup -S app \
    && adduser -S -G app app

COPY --from=build /out/wallet-api /usr/local/bin/wallet-api
COPY --from=build /out/outbox-publisher /usr/local/bin/outbox-publisher

USER app
EXPOSE 8080
ENTRYPOINT ["wallet-api"]
