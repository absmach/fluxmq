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

The queue manager periodically truncates logs to the **minimum committed offset**
across all consumer groups. Time-based or size-based retention policies exist in
`logstorage` (segment manager retention) but are not wired into the runtime
loop or exposed via the main config.

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
