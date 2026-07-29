---
title: Security
description: External auth, local principals, TLS/mTLS listeners, and rate limiting
---

# Security Configuration

**Last Updated:** 2026-07-29

## External Auth Callout

FluxMQ delegates authentication and authorization to an external service via
gRPC or HTTP callout. When `auth.external.url` is set, client connections on
enabled protocols are
verified against the external service before being accepted.

```yaml
auth:
  external:
    url: "https://auth-service.internal:7016"
    transport: "grpc"     # "grpc" (default) or "http"
    timeout: 5s
```

The former flat keys such as `auth.url` are not accepted. This is an
intentional breaking configuration change; move every external-callout field
under `auth.external` before upgrading.

Authentication requests contain client credentials. Protect the callout hop
with server-authenticated HTTPS or a service mesh that provides mTLS. The
built-in HTTPS client uses the host trust store and has no client-certificate
configuration; deployments that require mTLS must terminate it in a sidecar
or proxy. Plain HTTP is suitable only for a loopback hop already protected by
that mesh, never for cross-host traffic.

### Per-Protocol Auth

By default, all protocols require auth when `auth.external.url` is set. The
`protocols` map lets you selectively enable or disable the external callout per
protocol. Internal AMQP services that need local authentication use
`server.amqp091.internal`; they do not disable authentication on a remote
listener.

```yaml
auth:
  external:
    url: "https://auth-service.internal:7016"
    transport: "grpc"
    timeout: 5s
    protocols:
      mqtt: true
      http: true
      coap: true
      amqp: true
      amqp091: true
```

Valid protocol keys: `mqtt`, `amqp`, `amqp091`, `http`, `coap`.

When the `protocols` map is omitted or empty, all protocols require external
auth. When the map is present, only protocols set to `true` get external auth;
all others allow connections without external authentication. Do not disable
external auth merely to carry internal traffic. Use an internal listener with
a local principal instead.

## Internal AMQP Local Principals

AMQP 0.9.1 can expose a separate mTLS-only listener for tightly scoped service
identities. This listener does not call external auth or blocking hooks and
does not fall back to external auth for unknown identities.

```yaml
server:
  amqp091:
    internal:
      addr: ":5683"
      max_connections: 32
      cert_file: "/run/secrets/fluxmq_server_cert"
      key_file: "/run/secrets/fluxmq_server_key"
      ca_file: "/run/secrets/atom_client_ca"
      client_auth: "require"
      min_version: "TLS1.2"

auth:
  local_principals:
    - name: "atom-audit-publisher"
      certificate_uri_san: "spiffe://absmach/atom/audit-publisher"
      current_secret_file: "/run/secrets/atom_audit_secret_current"
      previous_secret_file: "/run/secrets/atom_audit_secret_previous"
      permissions:
        publish:
          - exchange: ""
            routing_key: "atom-audit"
        subscribe: []
```

The internal listener requires a CA-verified certificate URI SAN, SASL
username, and local secret to match one configured principal. Permissions are
exact-match allowlists. Port `5683` must remain on a private network and must
not be published to the host or Internet.

Publish permissions support only the default exchange (`exchange: ""`) and an
exact, non-empty routing key. Other exchanges and wildcard routing keys are
rejected when the configuration is loaded.

Local principals are publish-only. Subscription ACLs are not implemented, so
`permissions.subscribe` must remain `[]`; every consume or `Basic.Get`
operation is denied.

Local-principal counters are exposed under
`by_protocol.amqp.local_principals` in `GET /api/v1/stats`. They cover active
connections, authentication success/failure, publish and operation denials,
reload success/failure, and forced disconnects. The counters are bounded and
do not use principal names, certificate values, or routing keys as dimensions.

See [Internal AMQP Local Principals](/deployment/internal-amqp-local-principals)
for the complete production configuration, rotation process, tests, and
rollout contract.

## Blocking Hooks

Blocking hooks are optional synchronous callouts for operations that need an
external allow/deny or normalization decision before FluxMQ continues. Hooks
run after authentication and before final authorization of the effective topic
or filter.

```yaml
hooks:
  url: "https://hooks.internal:7017"
  transport: "grpc"
  timeout: "500ms"
  fail_mode: "deny"
  protocols:
    mqtt: true
    amqp: true
    amqp091: true
    http: true
    coap: false
  events:
    auth_on_register: true
    auth_on_publish: true
    auth_on_subscribe: true
    auth_on_unsubscribe: true
```

See [Blocking Hooks](/architecture/hooks) for request and response details.

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
