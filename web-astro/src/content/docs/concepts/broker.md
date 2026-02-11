---
title: Broker
description: What the broker does in FluxMQ and how protocol brokers fit together
---

# Broker

**Last Updated:** 2026-02-10

FluxMQ runs multiple protocol brokers that share the same queue manager:

- **MQTT broker** for MQTT 3.1.1/5.0 over TCP and WebSocket
- **AMQP 1.0 broker**
- **AMQP 0.9.1 broker**

Each broker owns its protocol state machine, but all queue-capable traffic flows into the shared queue manager. Protocol adapters translate protocol-specific concepts (AMQP exchanges/bindings, MQTT shared subscriptions, AMQP 1.0 link capabilities) into FluxMQ queue primitives without fabricating behavior that the underlying queue type doesn't support.

Delivery semantics depend on the [queue type](/docs/concepts/queues), not the protocol. An MQTT client and an AMQP 0.9.1 client consuming from the same durable queue get the same ack/nack/reject behavior. An AMQP 1.0 client consuming from a stream queue gets cursor-based semantics regardless of the protocol's native disposition model.

## What the Broker Handles

- Session lifecycle (connect, resume, takeover)
- Topic routing and shared subscriptions
- Retained messages and wills
- QoS enforcement and retry policies
- Queue integration for `$queue/` topics
- Protocol-to-queue translation (adapting protocol semantics to queue primitives)

## Learn More

- [Queue Types](/docs/concepts/queues)
- [Architecture overview](/docs/architecture/overview)
- [Routing internals](/docs/architecture/routing)
