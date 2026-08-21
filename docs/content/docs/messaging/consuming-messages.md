---
title: Consuming Messages
description: Subscribe with MQTT and receive messages from topics or queues
---

# Consuming Messages

**Last Updated:** 21st August 2026

## MQTT Subscribe

```bash
mosquitto_sub -p 1883 -t "sensors/#" -v
```

Use QoS 1 or 2 when you need delivery guarantees:

```bash
mosquitto_sub -p 1883 -t "sensors/#" -q 1 -v
```

## Consuming a queue

A queue holds each delivery until the consumer settles it. How you settle
depends on what your protocol can express.

### MQTT 5.0 and AMQP — explicit settlement

Publish to the queue address with a control suffix, carrying the identifiers the
broker stamped on the delivery:

| Suffix    | Meaning                                              |
| --------- | ---------------------------------------------------- |
| `$ack`    | Processed. The message is removed from the queue.    |
| `$nack`   | Not processed. Redeliver it.                         |
| `$reject` | Do not redeliver. Route to the dead-letter queue.    |

```
publish → $queue/orders/$ack
  properties: message-id=orders:42, group-id=workers
```

The identifiers arrive as message properties on every delivery: `message-id`,
`group-id`, `queue`, `offset`, and `x-source-topic` for a captured message.

**The suffix must be the final level of the address.** `$queue/orders/$ack/42`
is rejected — it would otherwise publish a message *into* the queue you meant to
acknowledge.

AMQP consumers do not use this path at all: AMQP 0.9.1 settles with `basic.ack`
and AMQP 1.0 with a disposition, and the broker correlates those itself.

### MQTT 3.1.1 — settlement on PUBACK

MQTT 3.1.1 has no message properties, so a 3.1.1 consumer never receives those
identifiers and could not send them back. Instead the broker settles the message
when the client acknowledges the packet — the PUBACK of a QoS 1 delivery, or the
PUBCOMP of a QoS 2 one.

```bash
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 -v
```

What this means in practice:

- **Subscribe at QoS 1 or 2.** A QoS 0 subscription to a classic queue is
  refused with a SUBACK failure, because QoS 0 has no acknowledgement to settle
  on and the broker would have to discard your work as it delivered it.
- **Settlement means received, not processed.** The message leaves the queue
  when your client library acknowledges the packet, which is usually before your
  handler has finished. A client that crashes mid-handler has already settled
  that message. This is what QoS 1 means everywhere else in MQTT; if you need
  settlement after processing, use MQTT 5.0 or AMQP.
- **There is no nack or reject.** Both need identifiers 3.1.1 cannot carry. A
  message is either settled by the acknowledgement or redelivered because none
  arrived.
- **A captured message's origin is not recoverable.** The delivery address
  identifies the queue and usually contains the source path, but a 3.1.1
  consumer cannot tell a capture of `orders/eu/new` from one of `eu/new` — both
  arrive as `$queue/orders/eu/new`. The `x-source-topic` property that
  distinguishes them needs MQTT 5.0 or AMQP.

Stream queues behave differently on every protocol: they track a cursor with
auto-commit rather than settling individual messages, so a 3.1.1 consumer of a
stream queue needs none of the above.

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
mosquitto_sub -V mqttv5 -p 1884 -t '$queue/orders/#' -q 1 -v \
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

- [Consumer groups](/messaging/consumer-groups)
- [Durable queues](/messaging/durable-queues)
