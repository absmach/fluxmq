---
title: Hot Reload
description: Change configuration at runtime without restarting the broker
---

# Hot Reload

**Last Updated:** 2026-03-18

FluxMQ supports changing a subset of configuration fields at runtime without restarting the broker. Changes are applied atomically per subsystem with automatic rollback on failure.

## Triggering a Reload

There are two ways to trigger a config reload:

### SIGHUP Signal

Send `SIGHUP` to the FluxMQ process:

```bash
kill -HUP <pid>
```

The broker re-reads the config file, diffs it against the running config, and applies any runtime-safe changes. The process continues running — only `SIGINT` / `SIGTERM` trigger shutdown.

### Admin API

```
POST /api/v1/reload
```

No request body is needed. The endpoint re-reads the config file and applies changes, returning a JSON response:

```json
{
  "version": 2,
  "applied": [
    {"path": "Log.Level", "old_value": "info", "new_value": "debug", "class": 1}
  ],
  "restart_required": [],
  "errors": [],
  "duration": 1200000
}
```

| HTTP Status | Meaning                                                  |
| ----------- | -------------------------------------------------------- |
| `200`       | Reload succeeded (check `applied` for what changed)      |
| `400`       | Config invalid or reload partially failed (see `errors`) |
| `405`       | Method not POST                                          |
| `503`       | Reload not configured or server is shutting down         |

## Runtime-Safe Fields

These fields can be changed without restart:

### Logging

| Field        | Description                                  |
| ------------ | -------------------------------------------- |
| `log.level`  | Log level (`debug`, `info`, `warn`, `error`) |
| `log.format` | Log format (`text`, `json`)                  |

### Rate Limits

All rate limit fields are runtime-safe. Changing any rate limit field replaces the entire rate limiter — existing per-IP/per-client token buckets are reset.

| Field                                    | Description                      |
| ---------------------------------------- | -------------------------------- |
| `ratelimit.enabled`                     | Global rate limit toggle         |
| `ratelimit.connection.enabled`          | Connection rate limit toggle     |
| `ratelimit.connection.rate`             | Connections per second per IP    |
| `ratelimit.connection.burst`            | Connection burst allowance       |
| `ratelimit.connection.cleanup_interval` | Stale entry cleanup interval     |
| `ratelimit.message.enabled`             | Message rate limit toggle        |
| `ratelimit.message.rate`                | Messages per second per client   |
| `ratelimit.message.burst`               | Message burst allowance          |
| `ratelimit.subscribe.enabled`           | Subscribe rate limit toggle      |
| `ratelimit.subscribe.rate`              | Subscribes per second per client |
| `ratelimit.subscribe.burst`             | Subscribe burst allowance        |

### Broker Tuning

| Field                          | Description                    |
| ------------------------------ | ------------------------------ |
| `broker.max_qos`               | Maximum QoS level (0, 1, 2)    |

## Restart-Required Fields

All other fields require a full restart to take effect. These include:

- **Server**: listener addresses, TLS certificates, WebSocket paths
- **Storage**: storage backend configuration
- **Cluster**: cluster topology and transport
- **Session**: session defaults (affects existing connections)
- **Webhook**: webhook worker pool configuration
- **Auth**: authentication and authorization settings
- **AMQP / AMQP 0.9.1**: all protocol-level settings

When restart-required fields are changed in the config file, the reload response lists them in `restart_required` so you know a restart is needed to pick them up.

## How It Works

```
Config file changed
        |
   Read & parse config
        |
   Validate new config
        |
   Diff old vs new (field-by-field)
        |
   Classify each changed field
   (runtime_safe / restart_required)
        |
   Apply runtime-safe changes per subsystem:
     1. Logging   (swap slog handler)
     2. Rate Limit (atomic pointer swap)
     3. Broker    (atomic field store)
        |
   On failure: rollback applied subsystems
               in reverse order
        |
   Update internal config snapshot
   Increment version counter
   Emit audit log
```

Changes are applied in subsystem order (logging, rate limits, broker). If any subsystem fails, all previously applied subsystems in that reload cycle are rolled back to their prior state.

## Audit Logging

Every reload emits a structured log entry:

```
level=INFO msg="config reload completed" version=2 applied_count=1
    restart_required_count=0 error_count=0 duration=1.2ms
    applied_fields=["Log.Level"]
```

Failed reloads log at `ERROR` level with the same fields plus error details.

## New Field Safety

New config fields added to the codebase default to `restart_required`. A field must be explicitly registered as `runtime_safe` in the classification registry (`config/diff.go`) before it can be hot-reloaded. A reflection-based test enforces that all leaf config fields are classified.
