---
title: Server
description: Configure listeners, WebSocket path, health checks, and OpenTelemetry
---

# Server Configuration

**Last Updated:** 2026-02-05

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
```

## Learn More

- `/docs/reference/configuration-reference`
