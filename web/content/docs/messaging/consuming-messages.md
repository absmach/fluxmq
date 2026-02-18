---
title: Consuming Messages
description: Subscribe with MQTT and receive messages from topics or queues
---

# Consuming Messages

**Last Updated:** 2026-02-18

## MQTT Subscribe

```bash
mosquitto_sub -p 1883 -t "sensors/#" -v
```

Use QoS 1 or 2 when you need delivery guarantees:

```bash
mosquitto_sub -p 1883 -t "sensors/#" -q 1 -v
```

## AMQP 0.9.1 Pub/Sub Consumption

For non-queue pub/sub (`basic.consume` without `$queue/...`), AMQP filters are translated to canonical MQTT form:

- `user.*.created` -> `user/+/created`
- `sensor.#` -> `sensor/#`

This means MQTT and AMQP 0.9.1 local subscribers can match the same published topics.

## Queue Consumption

Queue consumers subscribe to `$queue/<queue>/...` and set a consumer group (protocol-specific).

### Basic Queue Subscription

```bash
# Subscribe to all messages in the "orders" queue
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 -v
```

### Filtered Queue Subscription

Use wildcards to filter messages within a queue:

```bash
# Only receive messages with routing key starting with "images/"
mosquitto_sub -p 1883 -t '$queue/orders/images/#' -q 1 -v

# Only receive messages matching +/images/# (e.g., eu/images/resize, us/images/png)
mosquitto_sub -p 1883 -t '$queue/orders/+/images/#' -q 1 -v
```

### With Consumer Group (MQTT v5)

```bash
# Join consumer group "workers" for load balancing
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 -v \
  -D subscribe user-property consumer-group workers
```

## AMQP 0.9.1 Queue Consumption

### Basic Queue Subscription (Go)

```go
// Subscribe to queue with consumer group
deliveries, err := ch.Consume(
    "$queue/orders/#",  // queue filter
    "",                 // consumer tag (auto-generated)
    false,              // auto-ack (manual ack for durability)
    false,              // exclusive
    false,              // no-local
    false,              // no-wait
    amqp091.Table{
        "x-consumer-group": "workers",
    },
)

for d := range deliveries {
    // Process message
    fmt.Printf("Received: %s\n", d.Body)

    // Acknowledge
    d.Ack(false)
}
```

### Filtered Queue Subscription

```go
// Only receive messages matching +/images/#
deliveries, _ := ch.Consume(
    "$queue/orders/+/images/#",
    "", false, false, false, false,
    amqp091.Table{"x-consumer-group": "image-processors"},
)
```

### Stream Queue Consumption

```go
// Declare stream queue first
ch.QueueDeclare("events", true, false, false, false, amqp091.Table{
    "x-queue-type": "stream",
    "x-max-age":    "24h",
})

// Consume from beginning
deliveries, _ := ch.Consume(
    "events", "", false, false, false, false,
    amqp091.Table{
        "x-consumer-group": "replay-consumer",
        "x-stream-offset":  "first",
    },
)

for d := range deliveries {
    // Access stream metadata from headers
    offset := d.Headers["x-stream-offset"]
    timestamp := d.Headers["x-stream-timestamp"]

    fmt.Printf("Offset %v at %v: %s\n", offset, timestamp, d.Body)
    d.Ack(false)
}
```

### Acknowledgments

```go
// Acknowledge successful processing
d.Ack(false)

// Negative acknowledgment - retry the message
d.Nack(false, true)  // multiple=false, requeue=true

// Reject - send to DLQ (no retry)
d.Reject(false)  // requeue=false
```

See:

- [Consumer groups](/docs/messaging/consumer-groups)
- [Durable queues](/docs/messaging/durable-queues)
