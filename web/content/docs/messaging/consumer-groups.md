---
title: Consumer Groups
description: Configure consumer groups and acknowledgments across MQTT and AMQP
---

# Consumer Groups

**Last Updated:** 2026-02-06

Consumer groups provide load-balanced, fault-tolerant consumption for durable queues.

## Overview

A consumer group is a logical consumer that may be backed by many physical processes:

- **Load balancing**: Each message is delivered to exactly one member of the group
- **Progress tracking**: The group tracks cursor/committed offsets
- **Fault tolerance**: In classic mode, unacknowledged messages are redelivered after timeout
- **Work stealing**: Idle consumers can claim work from overloaded peers

```
┌─────────────────────────────────────────────────────────┐
│                    Queue: orders                        │
│  [msg1] [msg2] [msg3] [msg4] [msg5] [msg6] [msg7] ...  │
└─────────────────────────────────────────────────────────┘
                          │
            ┌─────────────┴─────────────┐
            ▼                           ▼
    ┌───────────────┐           ┌───────────────┐
    │ Group: workers│           │ Group: audit  │
    │  (3 consumers)│           │  (1 consumer) │
    └───────┬───────┘           └───────┬───────┘
            │                           │
    ┌───────┼───────┐                   │
    ▼       ▼       ▼                   ▼
  [C1]    [C2]    [C3]                [C4]

  C1 gets msg1    │                   C4 gets msg1
  C2 gets msg2    │ load-balanced     C4 gets msg2
  C3 gets msg3    │                   C4 gets msg3
  C1 gets msg4    ▼                   ...all messages
```

## Key Concepts

### Multiple Groups = Fan-Out

Multiple consumer groups on the same queue each receive **all messages** independently:

- Group "workers" processes messages for order fulfillment
- Group "audit" logs all messages for compliance
- Group "analytics" aggregates metrics

Each group maintains its own cursor and acknowledgment state.

### Multiple Consumers in a Group = Load Balancing

Multiple consumers within a single group share the workload:

- Messages are distributed round-robin across available consumers
- Each message goes to exactly one consumer in the group
- If a consumer disconnects, its pending messages are redelivered to others

### Pattern-Based Group Isolation

When consumers subscribe with different filter patterns, FluxMQ tracks them as separate logical groups internally:

```
Subscribe: $queue/orders/eu/#     → group: "workers@eu/#"
Subscribe: $queue/orders/us/#     → group: "workers@us/#"
```

Even with the same consumer group name "workers", these are independent because the filter pattern differs.

## Setting a Consumer Group

### MQTT v5

Use the `consumer-group` user property on SUBSCRIBE:

```bash
# Join consumer group "workers"
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 \
  -D subscribe user-property consumer-group workers
```

Multiple clients with the same `consumer-group` value form a load-balanced pool.

### MQTT v3

MQTT v3 does not support user properties. The consumer group defaults to the client ID prefix (everything before the first `-`):

```
client ID: worker-1  → group: "worker"
client ID: worker-2  → group: "worker"  (same group, load balanced)
client ID: audit-1   → group: "audit"   (different group)
```

Note: Acknowledgments require MQTT v5 user properties, so MQTT v3 is limited to auto-ack scenarios.

### AMQP 0.9.1

Use the `x-consumer-group` argument on `basic.consume`:

```go
deliveries, err := ch.Consume(
    "$queue/orders/#",  // queue
    "consumer-tag",     // consumer tag
    false,              // auto-ack
    false,              // exclusive
    false,              // no-local
    false,              // no-wait
    amqp091.Table{
        "x-consumer-group": "workers",
    },
)
```

### AMQP 1.0

Use `consumer-group` in attach properties:

```
attach {
    name: "orders-consumer",
    source: { address: "$queue/orders" },
    properties: { "consumer-group": "workers" }
}
```

## Consumer Group Modes

### Classic Mode (Work Queue)

Default mode optimized for "process once" semantics:

- Messages are **claimed** and must be acknowledged
- `ack` marks the message as processed
- `nack` triggers redelivery
- `reject` removes the message (future: moves to DLQ)
- **Visibility timeout**: Unacknowledged messages are redelivered after timeout
- **Work stealing**: Idle consumers can claim work from overloaded peers

### Stream Mode

Optimized for "replay and observe" semantics:

- Cursor-based consumption (read position in append-only log)
- Messages are read, not claimed - no per-message acknowledgment tracking
- Multiple consumers can read the same messages independently
- Supports seeking to specific offsets or timestamps
- Optional manual commit for checkpoint control

```go
// AMQP 0.9.1: Subscribe to stream with cursor positioning
ch.Consume("my-stream", "", false, false, false, false, amqp091.Table{
    "x-consumer-group": "readers",
    "x-stream-offset":  "first",     // start from beginning
    "x-auto-commit":    false,       // manual commit
})
```

See `/docs/messaging/durable-queues#queue-types` for detailed explanation of how classic and stream modes differ internally (PEL tracking, cursor management, etc.).

## Acknowledgments

Queue deliveries include metadata for acknowledgment:

| Property | Description |
|----------|-------------|
| `message-id` | Unique identifier (format: `queueName:offset`) |
| `group-id` | Consumer group that received the message |
| `queue` | Queue name |
| `offset` | Message sequence number |

### MQTT v5 Acknowledgments

Publish to ack/nack/reject topics with required properties:

```bash
# Acknowledge
mosquitto_pub -p 1883 -t '$queue/orders/$ack' -m '' \
  -D publish user-property message-id "orders:42" \
  -D publish user-property group-id "workers"

# Negative acknowledge (retry)
mosquitto_pub -p 1883 -t '$queue/orders/$nack' -m '' \
  -D publish user-property message-id "orders:42" \
  -D publish user-property group-id "workers"

# Reject (no retry, future: DLQ)
mosquitto_pub -p 1883 -t '$queue/orders/$reject' -m '' \
  -D publish user-property message-id "orders:42" \
  -D publish user-property group-id "workers" \
  -D publish user-property reason "invalid payload"
```

### AMQP 0.9.1 Acknowledgments

Standard AMQP acknowledgment methods are mapped:

```go
// Acknowledge - message processed successfully
delivery.Ack(false)  // multiple=false

// Negative acknowledge - retry the message
delivery.Nack(false, true)  // multiple=false, requeue=true

// Reject - no retry (future: moves to DLQ)
delivery.Reject(false)  // requeue=false
```

### AMQP 1.0 Acknowledgments

AMQP 1.0 dispositions are mapped to queue acknowledgments:

| Disposition | Queue Action |
|-------------|--------------|
| `Accepted` | Ack |
| `Released` | Nack (retry) |
| `Rejected` | Reject (DLQ) |

## Visibility Timeout and Work Stealing

In classic mode, FluxMQ provides automatic fault recovery:

### Visibility Timeout

When a message is delivered, it becomes "invisible" to other consumers for a configurable timeout (default: 30s). If the consumer doesn't acknowledge within this window:

1. The message becomes visible again
2. It can be claimed by any consumer in the group
3. The delivery count is incremented

This handles consumer crashes, network issues, and slow processing.

### Work Stealing

Idle consumers can proactively claim pending messages from overloaded peers:

1. Consumer A has 10 pending messages
2. Consumer B is idle (no pending work)
3. Consumer B "steals" some of A's pending messages
4. This rebalances load without waiting for visibility timeout

Work stealing is enabled by default and runs periodically.

## Examples

### Example 1: Simple Worker Pool

Three workers process orders with load balancing:

```bash
# Terminal 1: Worker 1
mosquitto_sub -p 1883 -i worker-1 -t '$queue/orders/#' -q 1 \
  -D subscribe user-property consumer-group workers

# Terminal 2: Worker 2
mosquitto_sub -p 1883 -i worker-2 -t '$queue/orders/#' -q 1 \
  -D subscribe user-property consumer-group workers

# Terminal 3: Worker 3
mosquitto_sub -p 1883 -i worker-3 -t '$queue/orders/#' -q 1 \
  -D subscribe user-property consumer-group workers

# Terminal 4: Publish orders
mosquitto_pub -p 1883 -t '$queue/orders' -m 'order-1'
mosquitto_pub -p 1883 -t '$queue/orders' -m 'order-2'
mosquitto_pub -p 1883 -t '$queue/orders' -m 'order-3'
# Each worker receives one order
```

### Example 2: Fan-Out to Multiple Groups

Same messages go to different groups for different purposes:

```bash
# Group 1: Process orders
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 \
  -D subscribe user-property consumer-group processors

# Group 2: Audit logging
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 \
  -D subscribe user-property consumer-group audit

# Group 3: Analytics
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 \
  -D subscribe user-property consumer-group analytics

# Publish - all three groups receive this message
mosquitto_pub -p 1883 -t '$queue/orders' -m 'order-1'
```

### Example 3: Filtered Consumption

Different consumers handle different message types:

```bash
# EU order processors
mosquitto_sub -p 1883 -t '$queue/orders/eu/#' -q 1 \
  -D subscribe user-property consumer-group eu-processors

# US order processors
mosquitto_sub -p 1883 -t '$queue/orders/us/#' -q 1 \
  -D subscribe user-property consumer-group us-processors

# Image processors (any region)
mosquitto_sub -p 1883 -t '$queue/orders/+/images/#' -q 1 \
  -D subscribe user-property consumer-group image-processors

# Publish
mosquitto_pub -p 1883 -t '$queue/orders/eu/images/resize' -m 'photo.png'
# → eu-processors receives it (matches eu/#)
# → image-processors receives it (matches +/images/#)
# → us-processors does NOT receive it
```

### Example 4: AMQP 0.9.1 Worker Pool (Go)

```go
package main

import (
    "log"
    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
    conn, _ := amqp.Dial("amqp://localhost:5672/")
    defer conn.Close()

    ch, _ := conn.Channel()
    defer ch.Close()

    // Set prefetch for fair dispatch
    ch.Qos(1, 0, false)

    // Subscribe with consumer group
    deliveries, _ := ch.Consume(
        "$queue/tasks/#",
        "",     // auto-generated tag
        false,  // manual ack
        false,  // not exclusive
        false,  // no-local
        false,  // no-wait
        amqp.Table{
            "x-consumer-group": "workers",
        },
    )

    for d := range deliveries {
        log.Printf("Processing: %s", d.Body)

        // Simulate work
        // ...

        // Acknowledge completion
        d.Ack(false)
    }
}
```

## Configuration

Queue manager settings that affect consumer groups:

```yaml
queue_manager:
  visibility_timeout: "30s"      # Time before unacked message is redelivered
  max_delivery_count: 5          # Max retries before giving up
  auto_commit_interval: "5s"     # Cursor commit frequency (stream mode)
  consumer_timeout: "2m"         # Disconnect stale consumers
  steal_enabled: true            # Enable work stealing
  steal_interval: "5s"           # Work stealing check interval
```

## Best Practices

1. **Use meaningful group names** that reflect the consumer's purpose (e.g., "order-processors", "audit-logger")

2. **Set appropriate prefetch/QoS** to control how many messages each consumer handles concurrently

3. **Keep processing time under visibility timeout** or extend the timeout for long-running tasks

4. **Use filters to partition work** when different consumers need different message subsets

5. **Monitor consumer lag** to detect slow consumers or processing bottlenecks

6. **Handle acknowledgments promptly** - don't batch acks for too long or you risk redelivery

## Learn More

- `/docs/messaging/durable-queues` - Queue configuration and lifecycle
- `/docs/messaging/publishing-messages` - How to publish to queues
- `/docs/messaging/consuming-messages` - Subscription patterns
