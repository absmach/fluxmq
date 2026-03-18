---
title: Server
description: Configure listeners, WebSocket path, health checks, and OpenTelemetry
---

# Server Configuration

**Last Updated:** 2026-02-25

`server` controls network listeners and telemetry endpoints. Example:

```yaml
server:
  tcp:
    v3:
      addr: ":1883"
      protocol: "v3"
    v5:
      addr: ":1884"
      protocol: "v5"
  websocket:
    plain:
      addr: ":8083"
      path: "/mqtt"
      protocol: "auto" # auto | v3 | v5
  http:
    plain:
      addr: ":8080"
  amqp:
    plain:
      addr: ":5672"
  amqp091:
    plain:
      addr: ":5682"

  health_enabled: true
  health_addr: ":8081"

  metrics_enabled: false
  metrics_addr: "localhost:4317"

  otel_metrics_enabled: true
  otel_traces_enabled: false
  otel_trace_sample_rate: 0.1

  admin_api_addr: ":8082"
```

## Key Fields

- Listener families: `tcp`, `websocket`, `http`, `coap`, `amqp`, `amqp091`.
- Listener addresses: `addr` (empty disables the specific listener).
- MQTT parser mode per listener: TCP `v3`/`v5` listeners are protocol-pinned; WebSocket listeners can use `protocol` (`auto`, `v3`, `v5`).
- Listener limits/timeouts: `max_connections`, `read_timeout`, `write_timeout`.
- WebSocket specifics: `path`, `allowed_origins`.
- Health/observability: `health_enabled`, `health_addr`, `metrics_enabled`, `metrics_addr`.
- OpenTelemetry identity/tuning: `otel_service_name`, `otel_service_version`, `otel_metrics_enabled`, `otel_traces_enabled`, `otel_trace_sample_rate`.
- Admin API server: `admin_api_addr` (empty string disables the admin API listener).
- Graceful shutdown: `shutdown_timeout`.

## Learn More

- [Configuration reference](/docs/reference/configuration-reference)
