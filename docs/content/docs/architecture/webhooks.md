---
title: Webhook System
description: Comprehensive webhook system for asynchronous event notifications with circuit breaker, retry logic, and flexible filtering
---

# Webhook System

**Last Updated:** 2026-02-05

FluxMQ can emit broker events to external HTTP endpoints using an asynchronous webhook notifier.

## Overview

- Asynchronous event queue with worker pool
- Retry with exponential backoff
- Circuit breaker per endpoint
- Filtering by event type and topic pattern
- HTTP sender only (gRPC sender not implemented)

## Architecture

```
Broker Events
   │
   ▼
Webhook Notifier (queue)
   │  drop_policy: oldest/newest
   ▼
Worker Pool
   │  retry + circuit breaker
   ▼
HTTP Sender
   │
   ▼
External Endpoints
```

## Event Types

Events are defined in `broker/events/events.go`.

- `client.connected`: `client_id`, `protocol`, `clean_start`, `keep_alive`, `remote_addr`
- `client.disconnected`: `client_id`, `reason`, `remote_addr`
- `client.session_takeover`: `client_id`, `from_node`, `to_node`
- `message.published`: `client_id`, `topic`, `qos`, `retained`, `payload_size`, `payload`
- `message.delivered`: `client_id`, `topic`, `qos`, `payload_size`
- `message.retained`: `topic`, `payload_size`, `cleared`
- `subscription.created`: `client_id`, `topic_filter`, `qos`, `subscription_id`
- `subscription.removed`: `client_id`, `topic_filter`
- `auth.success`: `client_id`, `remote_addr`
- `auth.failure`: `client_id`, `reason`, `remote_addr`
- `authz.publish_denied`: `client_id`, `topic`, `reason`
- `authz.subscribe_denied`: `client_id`, `topic_filter`, `reason`

### Payload Notes

The `message.published` payload field is defined in the event schema (base64-encoded when populated), but payload inclusion is not currently wired in the broker. `webhook.include_payload` is accepted in config, yet payloads are sent as empty strings at the moment.

## Event Envelope

```json
{
  "event_type": "message.published",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2026-02-05T12:00:00Z",
  "broker_id": "broker-1",
  "data": {
    "client_id": "publisher-1",
    "topic": "sensors/temperature",
    "qos": 1,
    "retained": false,
    "payload_size": 256
  }
}
```

## Filtering

Each endpoint can filter by:

- `events`: list of event types
- `topic_filters`: MQTT-style patterns (supports `+` and `#`)

## Retry and Circuit Breaker

- Retries use exponential backoff (`initial_interval * multiplier^attempt`), capped by `max_interval`
- Circuit breaker is per endpoint and trips after `failure_threshold` consecutive failures

## Configuration

```yaml
webhook:
  enabled: true
  queue_size: 10000
  drop_policy: "oldest"          # oldest or newest
  workers: 5
  include_payload: false
  shutdown_timeout: "30s"

  defaults:
    timeout: "5s"
    retry:
      max_attempts: 3
      initial_interval: "1s"
      max_interval: "30s"
      multiplier: 2.0
    circuit_breaker:
      failure_threshold: 5
      reset_timeout: "60s"

  endpoints:
    - name: "analytics"
      type: "http"
      url: "https://example.com/webhook"
      events: ["message.published"]
      topic_filters: ["sensors/#"]
      headers:
        Authorization: "Bearer token"
```
