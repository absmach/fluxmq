---
title: Durable Queues
description: Shared queue system for MQTT and AMQP with consumer groups, acknowledgments, stream queues, and append-only log storage
---

# Durable Queues

**Last Updated:** 2026-02-05

FluxMQ provides durable queues shared across MQTT, AMQP 1.0, and AMQP 0.9.1. The queue manager is append-only with consumer groups and supports both classic work-queue semantics and stream-style consumption.

## Overview

- **Queue topics** use the `$queue/` prefix
- **Consumer groups** enable load-balanced processing
- **Ack/Nack/Reject** are supported across protocols
- **Retention** can be configured per queue (time/size/message count)
- **Stream queues** are supported via queue type `stream`
- **DLQ handler exists**, but automatic DLQ routing is not wired yet

## Architecture

```
┌──────────────┐  ┌──────────────┐  ┌───────────────┐
│ MQTT Broker  │  │ AMQP Broker  │  │ AMQP091 Broker│
│ (TCP/WS/     │  │ (AMQP 1.0)   │  │ (AMQP 0.9.1)  │
│ HTTP/CoAP)   │  │              │  │               │
└──────┬───────┘  └───────┬──────┘  └──────┬────────┘
       │                  │                │
       └──────────────────┼────────────────┘
                          ▼
              ┌─────────────────────────┐
              │ Shared Queue Manager    │
              │ - Topic bindings        │
              │ - Consumer groups       │
              │ - Retention loop        │
              └───────────┬─────────────┘
                          ▼
              ┌─────────────────────────┐
              │ Log Storage (AOL)       │
              └─────────────────────────┘
```

## Queue Addressing

Queue topics use `$queue/<queue-name>/...`.

Examples:

- Publish: `$queue/orders`
- Subscribe to a pattern: `$queue/orders/#`
- Ack: `$queue/orders/$ack`
- Nack: `$queue/orders/$nack`
- Reject: `$queue/orders/$reject`

## Consumer Groups

- **MQTT v5**: provide `consumer-group` as a user property on SUBSCRIBE
- **MQTT v3**: consumer group falls back to client ID (acks require MQTT v5)
- **AMQP 1.0**: provide `consumer-group` in attach properties
- **AMQP 0.9.1**: provide `x-consumer-group` in `basic.consume`

## Message Properties

Queue deliveries include these properties:

- `message-id` (required for ack/nack/reject)
- `group-id` (consumer group name)
- `queue` (queue name)
- `offset` (sequence number)

Stream deliveries also include:

- `x-stream-offset`
- `x-stream-timestamp` (unix millis)
- `x-work-committed-offset` (if primary group is configured)
- `x-work-acked` (true when below committed offset)
- `x-work-group` (primary work group name)

## Acknowledgments

### MQTT

Ack/Nack/Reject are implemented by publishing to `$queue/<queue>/$ack|$nack|$reject` with MQTT v5 user properties:

- `message-id`
- `group-id`

MQTT v3 can publish and subscribe to queue topics, but acknowledgments require MQTT v5 user properties.

### AMQP 1.0

AMQP dispositions are mapped to queue acknowledgments:

- Accepted → Ack
- Released → Nack
- Rejected → Reject

### AMQP 0.9.1

- `basic.ack`, `basic.nack`, `basic.reject` map to Ack/Nack/Reject

#### Stream Commit (AMQP 0.9.1)

Stream consumers can explicitly commit offsets by publishing to:

- `$queue/<queue>/$commit`

Headers:

- `x-group-id`
- `x-offset`

## Queue Types

### Classic (Work Queue)

- Ack/Nack/Reject semantics
- Pending entry tracking per consumer group
- Redelivery uses visibility timeouts and work stealing
- Retry backoff settings are accepted in config but not yet enforced in delivery timing

### Stream

- Append-only log semantics
- Cursor-based consumption
- Optional manual commit

## Retention

Retention policies can be configured per queue:

- `max_age` (time-based)
- `max_length_bytes`
- `max_length_messages`

A background retention loop truncates logs to the safe offset based on configured limits.

## DLQ Status

A DLQ handler exists in `queue/consumer/dlq.go`, but the main delivery path does not automatically move rejected or expired messages into a DLQ yet. `Reject` currently removes the message from the pending list without pushing it to a DLQ.

## Configuration

Queues are configured under `queues` in the main config file:

```yaml
queue_manager:
  auto_commit_interval: "5s"

queues:
  - name: "orders"
    topics: ["$queue/orders/#"]
    type: "classic"               # classic or stream
    primary_group: ""             # optional for stream status

    limits:
      max_message_size: 10485760
      max_depth: 100000
      message_ttl: "168h"

    retry:
      max_retries: 10
      initial_backoff: "5s"
      max_backoff: "5m"
      multiplier: 2.0

    dlq:
      enabled: true
      topic: ""                    # optional override

    retention:
      max_age: "168h"
      max_length_bytes: 0
      max_length_messages: 0
```
