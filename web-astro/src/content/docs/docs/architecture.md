---
title: FluxMQ Architecture
description: Comprehensive system design overview covering layered architecture, protocol adapters, domain logic, and multi-protocol support
---

# FluxMQ Architecture

**Last Updated:** 2026-02-05

## Overview

FluxMQ is a multi-protocol message broker built around a shared queue manager. MQTT transports (TCP, WebSocket, HTTP bridge, CoAP) share one MQTT broker instance, while AMQP 1.0 and AMQP 0.9.1 use dedicated brokers. Durable queues are protocol-agnostic and provide cross-protocol routing and fan-out.

## High-Level View

1. **Server wiring**
   Initializes protocol servers (MQTT TCP/WS/HTTP/CoAP and AMQP), creates the shared queue manager, and wires storage, clustering, metrics, and graceful shutdown.
2. **Protocol entry points**
   MQTT transports feed the MQTT broker, while AMQP 1.0 and AMQP 0.9.1 each feed their own protocol broker.
3. **Broker layer**
   MQTT, AMQP 1.0, and AMQP 0.9.1 brokers process protocol semantics independently.
4. **Queue layer**
   Queue-capable traffic from all brokers converges into the shared **Queue Manager** (durable logs, fan-out, consumer groups).
5. **Persistence layer**
   Queue logs and related durable data are written through the storage backend.

`Server Wiring -> Protocol Servers -> Protocol Brokers -> Queue Manager -> Log Storage`

## Key Components (Code Map)

- **MQTT Broker**: `mqtt/broker/`
  - Session lifecycle, topic routing, retained messages, wills
  - Shared subscriptions (MQTT 5.0)
  - Queue integration for `$queue/` topics and ack topics

- **AMQP Brokers**:
  - AMQP 1.0: `amqp1/broker/`
  - AMQP 0.9.1: `amqp/broker/`
  - Both integrate with the shared queue manager

- **Transports and Bridges**: `server/`
  - `server/tcp`, `server/websocket` for MQTT
  - `server/http` HTTP publish bridge
  - `server/coap` CoAP publish bridge
  - `server/amqp`, `server/amqp1` for AMQP listeners

- **Queue Manager**: `queue/` and `logstorage/`
  - Append-only logs with consumer groups
  - Queue and stream modes
  - Ack/Nack/Reject support and retention policies

- **Storage**: `storage/` (BadgerDB and memory backends)
  - Sessions, subscriptions, retained messages, offline queues

- **Clustering**: `cluster/`
  - Embedded etcd metadata, gRPC transport for routing
  - Session ownership, retained/will coordination

- **Observability**: `server/otel/`
  - OpenTelemetry metrics and tracing setup

- **Webhook Notifier**: `broker/webhook/`
  - Asynchronous event delivery with retries and circuit breaker

- **Queue API (Connect/gRPC)**: `server/api/`, `server/queue/`
  - Programmatic queue operations over HTTP/2 (h2c or TLS)

## Related Docs

- `docs/broker.md`
- `docs/queue.md`
- `docs/configuration.md`
- `docs/clustering.md`
- `docs/webhooks.md`
