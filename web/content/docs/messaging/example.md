---
title: Example
description: Runnable Go example demonstrating MQTT pub/sub, cross-protocol queue interop, and stream consumption
---

# Messaging Example

A single Go program that demonstrates FluxMQ's messaging capabilities across three scenarios:

1. **MQTT pub/sub** — standard topic-based publish and subscribe
2. **MQTT → AMQP 0.9.1 queue** — publish via MQTT, consume via AMQP 0.9.1 on a durable queue
3. **AMQP 0.9.1 stream queue** — declare a stream, publish events, and replay from the beginning

The example uses only third-party MQTT and AMQP client libraries — no FluxMQ-specific SDK is required.

## Prerequisites

- **Go 1.21+** installed
- **FluxMQ running locally** with default ports:
  - MQTT: `1883`
  - AMQP 0.9.1: `5682`

Start FluxMQ with Docker or from source:

```bash
# Docker
docker compose -f docker/compose.yaml up -d

# From source
go run ./cmd/ --config examples/no-cluster.yaml
```

See [Quick Start (Docker)](/docs/getting-started/quick-start-docker) for details.

## Running the Example

```bash
go run ./examples/messaging/
```

Custom addresses:

```bash
go run ./examples/messaging/ \
  -mqtt localhost:1883 \
  -amqp091 localhost:5682
```

## What It Does

### Scenario 1: MQTT Pub/Sub

Standard MQTT messaging — a publisher sends a JSON payload to `demo/sensors/temperature`, and a subscriber on the same topic receives it. This is vanilla MQTT with no queues involved.

```go
// Publisher
pub.Publish("demo/sensors/temperature", 1, false, `{"sensor":"temp-1","value":22.5}`)

// Subscriber
sub.Subscribe("demo/sensors/temperature", 1, func(_ mqtt.Client, msg mqtt.Message) {
    fmt.Printf("Received: %s\n", msg.Payload())
})
```

### Scenario 2: MQTT → AMQP 0.9.1 (Durable Queue)

Demonstrates **cross-protocol interoperability**. An MQTT client publishes orders to the `$queue/demo-orders` topic. An AMQP 0.9.1 consumer reads from the same durable queue — messages traverse the protocol boundary transparently.

The AMQP 0.9.1 consumer joins a consumer group and manually acknowledges each message:

```go
deliveries, _ := ch.Consume(
    "$queue/demo-orders/#",
    "demo-consumer",
    false, false, false, false,
    amqp091.Table{
        "x-consumer-group": "demo-workers",
    },
)

for d := range deliveries {
    fmt.Printf("Received: %s\n", d.Body)
    d.Ack(false)
}
```

Key points:
- The `$queue/` prefix routes messages through the durable queue manager
- `x-consumer-group` sets the consumer group for load balancing
- Manual acks ensure at-least-once delivery

### Scenario 3: AMQP 0.9.1 Stream Queue

Demonstrates **stream consumption** — a replayable, append-only log. The example declares a stream queue, publishes events, then consumes them **from the beginning** using `x-stream-offset: "first"`.

```go
// Declare stream queue
ch.QueueDeclare("demo-events", true, false, false, false, amqp091.Table{
    "x-queue-type": "stream",
    "x-max-age":    "1h",
})

// Publish to stream
ch.PublishWithContext(ctx, "", "demo-events", false, false, amqp091.Publishing{
    Body: []byte(`{"event":"user.action","seq":1}`),
})

// Consume from beginning (replay)
deliveries, _ := ch.Consume("demo-events", "stream-reader", false, false, false, false,
    amqp091.Table{
        "x-consumer-group": "demo-readers",
        "x-stream-offset":  "first",
    },
)
```

Key points:
- Stream queues use the queue name directly — no `$queue/` prefix
- `x-stream-offset` controls where consumption starts: `"first"`, `"last"`, `"next"`, or a specific offset number
- Each delivered message includes an `x-stream-offset` header with its position in the log
- Stream queues retain messages based on `x-max-age`, unlike classic queues which remove messages after acknowledgment

## Expected Output

```
=== Scenario 1: MQTT Pub/Sub ===
  [MQTT sub] Subscribed to demo/sensors/temperature
  [MQTT pub] Publishing to demo/sensors/temperature: {"sensor":"temp-1","value":22.5}
  [MQTT sub] Received on demo/sensors/temperature: {"sensor":"temp-1","value":22.5}
  [OK] MQTT pub/sub round-trip successful

=== Scenario 2: MQTT → AMQP 0.9.1 (Durable Queue) ===
  [AMQP 0.9.1] Consuming from queue 'demo-orders' in group 'demo-workers'
  [MQTT pub] Publishing to $queue/demo-orders: {"order_id":"order-1",...}
  [AMQP 0.9.1] Received: {"order_id":"order-1",...}
  ...
  [OK] All 5 messages published via MQTT, consumed via AMQP 0.9.1

=== Scenario 3: AMQP 0.9.1 Stream Queue ===
  [AMQP 0.9.1] Declared stream queue 'demo-events'
  [AMQP 0.9.1] Published to stream: {"event":"user.action","seq":1}
  ...
  [AMQP 0.9.1] Consuming stream 'demo-events' from offset 'first'
  [AMQP 0.9.1] Stream message (offset=1): {"event":"user.action","seq":1}
  ...
  [OK] Published 5 events, replayed all 5 from stream

All scenarios completed.
```

## Libraries Used

| Library | Protocol | Purpose |
| --- | --- | --- |
| [eclipse/paho.mqtt.golang](https://github.com/eclipse/paho.mqtt.golang) | MQTT 3.1.1 | Publish and subscribe |
| [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) | AMQP 0.9.1 | Queue consume, stream declare/publish/consume |

## Next Steps

- [Durable Queues](/docs/messaging/durable-queues) — queue types, routing keys, retention, and acknowledgment semantics
- [Consumer Groups](/docs/messaging/consumer-groups) — fan-out, load balancing, and group configuration
- [Queue Client Example](https://github.com/absmach/fluxmq/tree/main/examples/queue-client) — a more advanced order-processing pipeline with multiple consumer groups across all three protocols
