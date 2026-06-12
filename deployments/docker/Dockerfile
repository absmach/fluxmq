# syntax=docker/dockerfile:1
# Multi-arch build: BUILDPLATFORM is the builder's native arch,
# TARGETOS/TARGETARCH are the requested target. buildx supplies both.
FROM --platform=$BUILDPLATFORM golang:1.26.4-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" -o /out/fluxmq ./cmd

FROM alpine:3.24

# wget is provided by busybox in alpine and is used by the HEALTHCHECK.
RUN apk upgrade --no-cache \
    && apk add --no-cache ca-certificates \
    && addgroup -S fluxmq \
    && adduser -S -G fluxmq fluxmq \
    && mkdir -p /var/lib/fluxmq \
    && chown -R fluxmq:fluxmq /var/lib/fluxmq

COPY --from=builder /out/fluxmq /usr/local/bin/fluxmq

USER fluxmq
WORKDIR /var/lib/fluxmq

EXPOSE 1883 1884 8080 8081 8083 8084 5672 5682 5683

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -q -O- http://127.0.0.1:8081/health || exit 1

ENTRYPOINT ["/usr/local/bin/fluxmq"]
