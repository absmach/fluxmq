---
title: Security
description: Auth callout, TLS/mTLS listeners, inter-broker TLS, and rate limiting
---

# Security Configuration

**Last Updated:** 2026-03-17

## Auth Callout

FluxMQ delegates authentication and authorization to an external service via
gRPC or HTTP callout. When `auth.url` is set, every client connection is
verified against the external service before being accepted.

```yaml
auth:
  url: "auth-service:7016"
  transport: "grpc"     # "grpc" (default) or "http"
  timeout: 5s
```

### Per-Protocol Auth

By default, all protocols require auth when `auth.url` is set. The `protocols`
map lets you selectively enable or disable auth per protocol. This is useful
when some listeners handle internal traffic that doesn't need external auth
(e.g., an AMQP 0.9.1 listener used exclusively for service-to-service event
sourcing).

```yaml
auth:
  url: "auth-service:7016"
  transport: "grpc"
  timeout: 5s
  protocols:
    mqtt: true
    http: true
    coap: true
    amqp: true
    amqp091: false    # internal event store — no auth needed
```

Valid protocol keys: `mqtt`, `amqp`, `amqp091`, `http`, `coap`.

When the `protocols` map is omitted or empty, all protocols require auth
(backward compatible). When the map is present, only protocols set to `true`
get auth; all others allow connections without authentication.

## TLS and mTLS

Listeners share TLS fields across `tls` and `mtls` blocks.

```yaml
server:
  tcp:
    tls:
      addr: ":8883"
      cert_file: "/path/server.crt"
      key_file: "/path/server.key"
    mtls:
      addr: ":8884"
      cert_file: "/path/server.crt"
      key_file: "/path/server.key"
      ca_file: "/path/clients-ca.crt"
      client_auth: "require"
```

## Inter-Broker TLS

```yaml
cluster:
  transport:
    tls_enabled: true
    tls_cert_file: "/path/transport.crt"
    tls_key_file: "/path/transport.key"
    tls_ca_file: "/path/transport-ca.crt"
```

## Rate Limiting

```yaml
ratelimit:
  enabled: true
  connection:
    enabled: true
    rate: 50
    burst: 200
  message:
    enabled: true
    rate: 500
    burst: 2000
```

## Learn More

- [Configuration reference](/docs/reference/configuration-reference)
