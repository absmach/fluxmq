---
title: Server
description: Configure listeners, WebSocket path, health checks, and OpenTelemetry
---

# Server Configuration

**Last Updated:** 29th July 2026

`server` controls network listeners and telemetry endpoints. Example:

```yaml
server:
  mqtt:
    tcp:
      v3:
        addr: ":1883"
        protocol: "v3"
      v5:
        addr: ":1884"
        protocol: "v5"
    websocket:
      v3:
        addr: ":8083"
        path: "/mqtt"
      v5:
        addr: ":8084"
        path: "/mqtt"
  http:
    plain:
      addr: ":8080"
  amqp:
    plain:
      addr: ":5672"
  amqp091:
    plain:
      addr: ":5682"
    local:
      addr: ":5683"
      max_connections: 32
      cert_file: "/run/secrets/fluxmq_server_cert"
      key_file: "/run/secrets/fluxmq_server_key"
      ca_file: "/run/secrets/local_client_ca"
      client_auth: "require"

  health_enabled: true
  health_addr: ":8081"

  metrics_enabled: false
  metrics_addr: "localhost:4317"

  otel_metrics_enabled: true
  otel_traces_enabled: false
  otel_trace_sample_rate: 0.1

  admin_api_addr: ":8082"

# server.amqp091.local is meaningless without the principals it serves, so the
# two are always configured together. See the security page for the full model.
auth:
  local_principals:
    - name: "audit-publisher"
      certificate_uri_san: "spiffe://example.org/audit-publisher"
      role: "publisher"
      current_secret_file: "/run/secrets/audit_secret_current"
      permissions:
        publish:
          - exchange: ""
            routing_key_prefix: "audit."
        subscribe: []
```

## Key Fields

- Listener families: `tcp`, `websocket`, `http`, `coap`, `amqp`, `amqp091`.
- `amqp091.local` is a private mTLS listener reserved for
  `auth.local_principals`; it never uses external auth or blocking hooks and
  requires a positive `max_connections` cap. It carries service-to-service
  traffic from a fixed set of statically configured internal producers, such as
  audit or event streams — not general client, device, or tenant connections.
- Listener addresses: `addr` (empty disables the specific listener).
- MQTT parser mode per listener: TCP `v3`/`v5` listeners are protocol-pinned; WebSocket listeners can use `protocol` (`auto`, `v3`, `v5`).
- Listener limits/timeouts: `max_connections`, `read_timeout`, `write_timeout`.
- WebSocket specifics: `path`, `allowed_origins`.
- Health/observability: `health_enabled`, `health_addr`, `metrics_enabled`, `metrics_addr`.
- OpenTelemetry identity/tuning: `otel_service_name`, `otel_service_version`, `otel_metrics_enabled`, `otel_traces_enabled`, `otel_trace_sample_rate`.
- Admin API server: `admin_api_addr` (empty string disables the admin API listener).
- Graceful shutdown: `shutdown_timeout`.

## Learn More

- [Configuration reference](/reference/configuration-reference)
