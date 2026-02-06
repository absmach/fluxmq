---
title: Broker
description: What the broker does in FluxMQ and how protocol brokers fit together
---

# Broker

**Last Updated:** 2026-02-05

FluxMQ runs multiple protocol brokers that share the same durable queue manager:

- **MQTT broker** for MQTT 3.1.1/5.0 over TCP and WebSocket
- **AMQP 1.0 broker**
- **AMQP 0.9.1 broker**

Each broker owns its protocol state machine, but all queue-capable traffic flows into the shared queue manager. This lets you mix protocols while keeping one durability and delivery pipeline.

## What the Broker Handles

- Session lifecycle (connect, resume, takeover)
- Topic routing and shared subscriptions
- Retained messages and wills
- QoS enforcement and retry policies
- Queue integration for `$queue/` topics

## Learn More

- `/docs/architecture/overview`
- `/docs/architecture/routing`
