---
title: Broker
description: What the broker does in FluxMQ and how protocol brokers fit together
---

# Broker

**Last Updated:** 2026-02-18

FluxMQ runs multiple protocol brokers that share the same queue manager:

- **MQTT broker** for MQTT 3.1.1/5.0 over TCP and WebSocket
- **AMQP 1.0 broker**
- **AMQP 0.9.1 broker**

Each broker owns its protocol state machine, but all queue-capable traffic flows into the shared queue manager. Protocol adapters translate protocol-specific concepts (AMQP exchanges/bindings, MQTT shared subscriptions, AMQP 1.0 link capabilities) into FluxMQ queue primitives without fabricating behavior that the underlying queue type doesn't support.

Delivery semantics depend on the [queue type](/docs/concepts/queues), not the protocol. An MQTT client and an AMQP 0.9.1 client consuming from the same durable queue get the same ack/nack/reject behavior. An AMQP 1.0 client consuming from a stream queue gets cursor-based semantics regardless of the protocol's native disposition model.

## Local MQTT <-> AMQP 0.9.1 Pub/Sub

MQTT and AMQP 0.9.1 pub/sub subscribers share one local topic router. A publish from either protocol can fan out to local subscribers of the other protocol on the same node.

For AMQP 0.9.1 pub/sub (default exchange path), FluxMQ normalizes patterns to MQTT form:

| AMQP 0.9.1 | Canonical MQTT |
| --- | --- |
| `.` | `/` |
| `*` | `+` |
| `#` | `#` |

## What the Broker Handles

- Session lifecycle (connect, resume, takeover)
- Topic routing and shared subscriptions
- Retained messages and wills
- QoS enforcement and retry policies
- Queue integration for `$queue/` topics
- Protocol-to-queue translation (adapting protocol semantics to queue primitives)

## Fan-out Modes

When a QoS 2 publisher completes the four-way handshake (PUBLISH → PUBREC → PUBREL → PUBCOMP), the broker must deliver the message to all matching subscribers before it can respond with PUBCOMP. In high fan-out scenarios (many subscribers per topic), this blocks the publisher's read loop for the duration of all subscriber deliveries and any cross-node cluster routing.

FluxMQ provides two fan-out modes, controlled by `broker.async_fan_out`:

**Synchronous (default, `async_fan_out: false`)**

Fan-out and PUBCOMP happen in the publisher's goroutine. The publisher is blocked until every subscriber has been handed the message and cluster routing has been dispatched. Simpler ordering guarantees; lower throughput when subscriber count is high.

**Async (`async_fan_out: true`)**

PUBCOMP is sent immediately after PUBREL, and subscriber fan-out is dispatched to a bounded worker pool (`fan_out_workers`, default `GOMAXPROCS`). The pool back-pressures the publisher if all workers are busy. This is spec-compliant: [MQTT 5.0 §4.3.3](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901103) requires the broker to commit responsibility for the message before sending PUBCOMP, not to have delivered it to all subscribers.

Use async mode for workloads with many subscribers per topic (fanin). Keep synchronous mode when strict ordering or simpler observability is preferred.

## Inflight Overflow

Each subscriber session maintains an inflight window of at most `session.max_inflight_messages` unacknowledged outbound QoS 1/2 messages. When the window is full, behavior is controlled by `session.inflight_overflow`:

**Backpressure (`inflight_overflow: 0`, default)**

The goroutine delivering to the subscriber blocks until the subscriber ACKs a message and a slot opens. This prevents message loss but slows down any goroutine (publisher or fan-out worker) that delivers to a slow subscriber.

**Pending queue (`inflight_overflow: 1`)**

Overflow messages are buffered in a per-subscriber channel of depth `pending_queue_size`. As ACKs arrive, buffered messages are drained into the inflight window. On disconnect, QoS>0 pending messages spill to the subscriber's offline queue (subject to `max_offline_queue_size`). This decouples slow subscribers from the publisher at the cost of per-subscriber memory.

## Learn More

- [Queue Types](/docs/concepts/queues)
- [Architecture overview](/docs/architecture/overview)
- [Routing internals](/docs/architecture/routing)
- [Configuration reference](/docs/reference/configuration-reference)
