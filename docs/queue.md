# Durable Queues (MQTT + AMQP)

> **Status**: Implemented (single-log queues, consumer groups; DLQ wiring pending)
> **Last Updated**: 2026-02-03
> **Compilation Status**: Not verified in this document
> **Test Status**: Not run in this document
> **Storage Engine**: Append-only log with segments and indexes

This document describes the durable queue system shared by the MQTT, AMQP 1.0, and AMQP 0.9.1 brokers. It focuses on the current implementation and calls out planned functionality explicitly when relevant.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Log Storage Engine](#log-storage-engine)
4. [Implementation Details](#implementation-details)
5. [Configuration Guidelines](#configuration-guidelines)
6. [Performance & Scalability](#performance--scalability)
7. [Current Progress](#current-progress)
8. [Missing Features & Next Steps](#missing-features--next-steps)
9. [Quick Start](#quick-start)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Overview

Queues provide durable, at-least-once delivery across protocols:
- Persistent append-only storage per queue
- Consumer groups for load-balanced delivery
- Acknowledgments (`$ack`, `$nack`, `$reject`)
- Redelivery via visibility timeouts and work stealing
- DLQ handler exists but is not wired into the main delivery path
- FIFO order per queue and per consumer group (single cursor)
- Stream queues (RabbitMQ-compatible) for event-log consumption with cursor offsets

Queues are integrated with MQTT and AMQP:
- MQTT uses `$queue/<name>/...` topics
- AMQP brokers map queue operations to the same queue manager

---

## Architecture

```
┌──────────────┐  ┌──────────────┐  ┌───────────────┐
│ MQTT Broker  │  │ AMQP Broker  │  │ AMQP091 Broker│
│ (TCP/WS/     │  │ (AMQP 1.0)   │  │ (AMQP 0.9.1)  │
│ HTTP/CoAP)   │  │              │  │               │
└──────┬───────┘  └──────┬───────┘  └──────┬────────┘
       │                │                │
       └────────────────┼────────────────┘
                        ▼
              ┌─────────────────────────┐
              │ Shared Queue Manager    │
              │ - Topic bindings        │
              │ - Consumer groups       │
              │ - DLQ handler (WIP)     │
              └──────────┬──────────────┘
                         ▼
              ┌─────────────────────────┐
              │ Log Storage (AOL)       │
              │ + Topic Index           │
              └─────────────────────────┘
```

### Topic & Queue Addressing

Standard MQTT topics remain unchanged. Queue routing uses `$queue/<name>` topics:

```
$queue/tasks/image-processing           → Durable queue publish
$queue/tasks/image-processing/$ack      → Acknowledge
$queue/tasks/image-processing/$nack     → Retry
$queue/tasks/image-processing/$reject   → Reject (DLQ wiring is planned)
$dlq/{queue-name}                       → Dead-letter queue
```

Stream queues use RabbitMQ-compatible queue names and arguments:

- Declare with `x-queue-type=stream`
- Consume with `x-stream-offset`
- Retention via `x-max-age`, `x-max-length-bytes`, `x-max-length`

The `$queue/<name>` prefix remains supported for legacy queue clients.

**Acknowledgment requirements**:
- `message-id` and `group-id` must be provided in message properties.
- For MQTT, these are MQTT v5 User Properties.
- For MQTT v3, acknowledgments are not supported (no properties).

**Delivery properties**:
Queue deliveries include properties:
- `message-id`
- `group-id`
- `queue`
- `offset`

Stream deliveries also include:
- `x-stream-offset`
- `x-stream-timestamp`
- `x-primary-group-processed` (based on the primary work group's committed offset)
- `x-work-committed-offset`
- `x-work-group`

### Message Flow (Queue Publish)

```
PUBLISH to $queue/orders/123
  ↓
queueManager.Publish()
  ↓
FindMatchingQueues(topic)
  ↓
Append to queue log
  ↓
Deliver to consumer group (one consumer)
```

### Message Lifecycle State Machine

```
QUEUED → DELIVERED → ACKED → DELETED
         ↓
         TIMEOUT → RETRY → DELIVERED (retry count++)
                   ↓
                   MAX_RETRIES → DLQ
```

Note: `MAX_RETRIES → DLQ` is not wired yet. The delivery count limit is tracked,
but DLQ moves are not currently triggered automatically.

---

## RabbitMQ Stream Compatibility

FluxMQ supports RabbitMQ-style stream queues at the protocol level:

- `queue.declare` with `x-queue-type=stream`
- `basic.consume` with `x-stream-offset`
- `x-max-age`, `x-max-length-bytes`, `x-max-length` for retention

Stream queues are append-only: acks do **not** delete messages. Messages are
removed only when retention policies allow it, and only up to the safe
truncation point for queue-mode consumers.

`x-stream-offset` supports:
- `first`
- `last`
- `next`
- `offset=<n>`
- `timestamp=<unix-seconds|unix-millis>`

FluxMQ extensions for stream consumers:
- `x-primary-group-processed` and `x-work-committed-offset` to report delivery status for the
  configured primary work group.
- `x-work-group` to identify the group used for status.
- Optional `x-consumer-group` on `basic.consume` to persist a shared cursor.
  If omitted, the consumer tag is used as the stream group ID.
- Optional `x-auto-commit=false` to disable automatic offset commits (manual commit mode).

Primary work group is configured per queue (see configuration section) and is
used only for delivery status reporting; it does not affect routing.

### Consumer Group Modes

Consumer groups operate in one of two modes:

| Mode | Behavior |
|------|----------|
| `queue` | Work queue: messages removed on ack, PEL tracking, redelivery on timeout |
| `stream` | Log consumption: messages stay in log, cursor-based, optional auto-commit |

**Mode is immutable.** Once a group is created with a mode, all consumers must use the same mode.

#### Mode Mismatch Error

If a consumer joins a group with a different mode, the broker returns `ErrGroupModeMismatch`.

**Resolution:**
- Use a different consumer group name
- Ensure all consumers use consistent subscription options
- Delete and recreate the group (all consumers must disconnect first)

### Manual Commit for Stream Consumers

By default, stream consumers auto-commit offsets as messages are delivered. For exactly-once processing, disable auto-commit:

```go
autoCommit := false
err := client.SubscribeToStream(&StreamConsumeOptions{
    QueueName:  "events",
    AutoCommit: &autoCommit,
    Offset:     "first",
}, handler)

// After processing, explicitly commit
client.CommitOffset("events", "my-group", lastProcessedOffset)
```

With manual commit:
- Messages are delivered but committed offset doesn't advance automatically
- On reconnect, delivery resumes from last committed offset
- Use `CommitOffset()` to advance the committed position

## Model Alignment

FluxMQ’s queue model aligns with:

- **Kafka**: append-only log + consumer groups + retention.
- **Pulsar**: subscriptions + retention window for replay.
- **NATS JetStream**: queue vs log semantics are defined by consumer mode.

Stream queues provide log-like semantics, while classic queue groups preserve
work-queue behavior.

## Log Storage Engine

Queues are backed by an **append-only log** with segments and sparse indexes.
Each queue has a single log (no partitions).

**Key properties**:
- Sequential writes for high throughput
- Offset-based reads
- Sparse offset and time indexes (`.idx`, `.tix`)
- Consumer state with cursor + PEL for redelivery

### Storage Layout

```
<base-dir>/
  queues/
    <queue-name>/
      segments/
        00000000000000000000.seg
        00000000000000000000.idx
        00000000000000000000.tix
        ...
      consumers/
        <group-id>/
          state.json
          pel.log
  config/
    queues.json
```

### Retention

The queue manager truncates logs to a **safe offset** that respects:

- The minimum committed offset across **queue-mode** consumer groups
- The queue’s retention policy (time/size/message-count)

This means:
- Queue-mode consumers never lose unacked data.
- Stream consumers do not block truncation.
- Retention keeps data available for event-log readers as configured.

---

## Implementation Details

### Package Structure

**Core packages**:
```
queue/                    - Queue manager + delivery loops
queue/consumer/           - Consumer groups, PEL, metrics
queue/storage/            - Queue store interfaces
queue/types/              - Shared types
logstorage/               - Append-only log store and consumer state
queue/raft/               - Optional Raft replication layer
```

**Broker integration**:
```
mqtt/broker/publish.go    - Routes $queue/* topics and acks
mqtt/broker/subscribe.go  - Queue subscriptions + consumer groups
```

### Consumer Group Semantics

- Each message is delivered to exactly one consumer per group.
- Multiple groups can subscribe to the same queue (fan-out by group).
- Group selection:
  - MQTT v5: `consumer-group` User Property on SUBSCRIBE.
  - Default: client ID prefix before the first `-` (e.g., `worker-1` → `worker`).

### Zero-Copy Integration

Queue deliveries currently copy payload bytes into broker messages.
Zero-copy delivery for queue messages is planned.

---

## Configuration Guidelines

Queue bindings live under `queues` in the main config:

```yaml
queues:
  - name: "orders"
    topics:
      - "orders/#"
      - "$queue/orders/#"
    reserved: false
    limits:
      max_message_size: 1048576
      max_depth: 100000
      message_ttl: "168h"
    retry:
      max_retries: 10
      initial_backoff: "5s"
      max_backoff: "5m"
      multiplier: 2.0
    dlq:
      enabled: true
      topic: "$dlq/orders"
```

Notes:
- If no queues are configured, a default reserved queue `mqtt` is created with topic `$queue/#`.
- Auto-created queues are **ephemeral** and expire after the last consumer disconnects.
- `message_ttl` is stored in message metadata; automatic expiration is not enforced yet.
- `limits` and `retry` are parsed into queue configs but not enforced at runtime yet.
- `dlq` configuration is parsed, but reject/DLQ wiring is not active in the main delivery path.

---

## Performance & Scalability

Current scaling model:
- Scale horizontally with more broker nodes.
- Use multiple queues to spread load.
- Increase consumer counts per group to improve parallelism.

There is no partitioning in the current implementation. Planned work includes
partitioned queues and per-queue retention policies.

---

## Current Progress

Implemented:
- Append-only log storage with segment indexing
- Consumer groups and PEL-based redelivery
- Ack/Nack/Reject via `$ack`, `$nack`, `$reject`
- Cluster forwarding of queue publishes to nodes with consumers
- Optional Raft layer for queue storage (leader append only)

Raft notes:
- Only leader appends go through Raft.
- Non-leader nodes currently append locally.
- Ack/Nack/Reject and consumer state are not replicated via Raft yet.

---

## Missing Features & Next Steps

Planned or in-progress:
- Queue partitioning and ordering modes
- Time-based and size-based retention policies
- Enforce queue limits (max depth, max message size) and message TTL expiry
- Wire retry policy and DLQ handling into the delivery path
- Replicate ack/nack/reject and consumer state through Raft
- Queue admin API for create/update/delete (beyond basic Connect endpoints)
- Direct routing mode (consumer connects to partition owner)
- Zero-copy queue delivery

---

## Quick Start

### 1. Configure a Queue

```yaml
queues:
  - name: "tasks"
    topics: ["$queue/tasks/#"]
    limits:
      max_message_size: 1048576
      max_depth: 100000
      message_ttl: "168h"
    retry:
      max_retries: 10
      initial_backoff: "5s"
      max_backoff: "5m"
      multiplier: 2.0
    dlq:
      enabled: true
```

### 2. Subscribe with a Consumer Group (MQTT v5)

Use `consumer-group` User Property on SUBSCRIBE.

```
SUBSCRIBE $queue/tasks/#  (User Property: consumer-group=workers-v1)
```

### 3. Publish a Message

```
PUBLISH $queue/tasks/image-process  payload=...
```

### 4. Acknowledge

Publish to the ack topic using the delivery properties:

```
PUBLISH $queue/tasks/image-process/$ack
  User Properties:
    message-id=<from delivery>
    group-id=<from delivery>
```

Use `$nack` to make the message immediately eligible for redelivery. `$reject`
removes it from the pending list; automatic DLQ routing is planned.

---

## Best Practices

- Use explicit `consumer-group` values for stable group identity.
- Always ACK or NACK to avoid redelivery loops.
- Plan for a DLQ, but note it is not wired into the delivery path yet.
- Limit queue bindings to avoid accidental fan-in.

---

## Troubleshooting

**Queue acks fail with "message-id required"**:
- Ensure you are sending `message-id` and `group-id` as properties.
- MQTT v5 is required for properties.

**Messages not delivered**:
- Confirm the queue is configured and topic bindings match.
- Verify consumers are subscribed with the correct group.
