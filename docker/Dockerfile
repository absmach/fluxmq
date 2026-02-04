# syntax=docker/dockerfile:1
FROM golang:1.24.3-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags "-s -w" -o /out/fluxmq ./cmd

FROM alpine:3.20

RUN apk add --no-cache ca-certificates \
    && addgroup -S fluxmq \
    && adduser -S -G fluxmq fluxmq \
    && mkdir -p /var/lib/fluxmq \
    && chown -R fluxmq:fluxmq /var/lib/fluxmq

COPY --from=builder /out/fluxmq /usr/local/bin/fluxmq

USER fluxmq
WORKDIR /var/lib/fluxmq

EXPOSE 1883 8080 8081 8083 5672 5682 5683

ENTRYPOINT ["/usr/local/bin/fluxmq"]
