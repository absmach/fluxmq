---
title: Server
description: Configure listeners, WebSocket path, health checks, and OpenTelemetry
---

# Server Configuration

**Last Updated:** 2026-02-18

`server` controls network listeners and telemetry endpoints. Example:

```yaml
server:
  tcp:
    plain:
      addr: ":1883"
  websocket:
    plain:
      addr: ":8083"
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

  health_enabled: true
  health_addr: ":8081"

  metrics_enabled: false
  metrics_addr: "localhost:4317"

  otel_metrics_enabled: true
  otel_traces_enabled: false
  otel_trace_sample_rate: 0.1

  api_enabled: false
  api_addr: ""
```

## Key Fields

- Listener families: `tcp`, `websocket`, `http`, `coap`, `amqp`, `amqp091`.
- Listener addresses: `addr` (empty disables the specific listener).
- Listener limits/timeouts: `max_connections`, `read_timeout`, `write_timeout`.
- WebSocket specifics: `path`, `allowed_origins`.
- Health/observability: `health_enabled`, `health_addr`, `metrics_enabled`, `metrics_addr`.
- OpenTelemetry identity/tuning: `otel_service_name`, `otel_service_version`, `otel_metrics_enabled`, `otel_traces_enabled`, `otel_trace_sample_rate`.
- Queue API server: `api_enabled`, `api_addr`.
- Graceful shutdown: `shutdown_timeout`.

## Learn More

- [Configuration reference](/docs/reference/configuration-reference)
