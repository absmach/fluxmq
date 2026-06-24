---
title: Security
description: Atom auth, auth callout, TLS/mTLS listeners, inter-broker TLS, and rate limiting
---

# Security Configuration

**Last Updated:** 2026-06-24

## Auth Providers

FluxMQ can delegate authentication and authorization to either a legacy
callout service or Atom over gRPC. When auth is enabled, client credentials are
checked during connect and publish/subscribe permissions are checked before the
broker accepts the operation.

### Atom Provider

Use the Atom provider when FluxMQ should talk directly to Atom without a
`fluxmq-auth` bridge.

```yaml
auth:
  provider: atom
  timeout: 2s
  protocols:
    mqtt: true
    http: true
    coap: true
    amqp: true
    amqp091: true
  identity_cache_size: 50000
  identity_cache_ttl: 1h
  atom:
    grpc_addr: "atom:8081"
    insecure: true
    service_token_env: "FLUXMQ_ATOM_SERVICE_TOKEN"
    topic_format: "magistrala"
    authn_cache_ttl: 30s
    alias_cache_ttl: 5m
    decision_cache_ttl: 0s
    unsupported_topic_policy: "deny"
```

Client passwords, bearer tokens, and bridge token fields are treated as Atom
JWTs or API keys. FluxMQ parses `m/<tenant>/c/<channel>` topics, resolves Atom
aliases when needed, then calls Atom `AuthzService.Check` with action
`publish` or `subscribe` and object kind `resource`.

### Auth Callout

FluxMQ delegates authentication and authorization to an external service via
gRPC or HTTP callout. Empty `provider` plus `auth.url` keeps the legacy callout
behavior.

```yaml
auth:
  provider: callout
  url: "auth-service:7016"
  transport: "grpc"     # "grpc" (default) or "http"
  timeout: 5s
```

### Per-Protocol Auth

By default, all protocols require auth when a provider is configured. The
`protocols` map lets you selectively enable or disable auth per protocol. This is useful
when some listeners handle internal traffic that doesn't need external auth
(e.g., an AMQP 0.9.1 listener used exclusively for service-to-service event
sourcing).

```yaml
auth:
  provider: callout
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

- [Configuration reference](/reference/configuration-reference)
