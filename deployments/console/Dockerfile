# Native local build; the release image uses deployments/docker/Dockerfile.
FROM golang:1.27.4-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /fluxmq ./cmd

FROM alpine:3.24
RUN apk add --no-cache ca-certificates \
    && addgroup -S fluxmq && adduser -S -G fluxmq fluxmq \
    && mkdir -p /var/lib/fluxmq && chown fluxmq:fluxmq /var/lib/fluxmq
COPY --from=build /fluxmq /usr/local/bin/fluxmq
USER fluxmq
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=10 \
    CMD wget -q -O- http://127.0.0.1:8081/health || exit 1
ENTRYPOINT ["/usr/local/bin/fluxmq"]
