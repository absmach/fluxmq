---
title: Overview
description: High-level system design overview covering core components and how they fit together
---

# Architecture Overview

**Last Updated:** 2026-02-18

## Overview

FluxMQ is a multi-protocol message broker built around a shared queue manager. MQTT transports (TCP, WebSocket, HTTP bridge, CoAP) share one MQTT broker instance, while AMQP 1.0 and AMQP 0.9.1 use dedicated brokers. MQTT and AMQP 0.9.1 pub/sub share a local topic router for same-node interoperability. Queues are protocol-agnostic — all protocols route through the same queue manager, and delivery semantics depend on the [queue type](/concepts/queues) (ephemeral, durable, or stream), not the protocol.

## High-Level View

```
┌──────────────────────────────────────────────────────────────┐
│  Server Wiring                                               │
│  • Starts protocol servers (MQTT TCP/WS/HTTP/CoAP, AMQP)     │
│  • Creates shared Queue Manager                              │
│  • Wires storage, cluster, metrics, shutdown                 │
└──────────┬──────────────────┬──────────────────┬─────────────┘
           │                  │                  │
           ▼                  ▼                  ▼
  ┌────────────────┐   ┌─────────────┐   ┌─────────────┐
  │ MQTT Transports│   │ AMQP 1.0    │   │ AMQP 0.9.1  │
  │ (TCP/WS/HTTP/  │   │ Broker      │   │ Broker      │
  │ CoAP)          │   └──────┬──────┘   └──────┬──────┘
  └───────┬────────┘          │                 │
          ▼                   ▼                 ▼
    ┌────────────┐      ┌────────────┐    ┌────────────┐
    │ MQTT Broker│      │ AMQP Broker│    │ AMQP Broker│
    └──────┬─────┘      └──────┬─────┘    └──────┬─────┘
           └──────────────────┬──────────────────┘
                              │ queue-capable traffic
                              ▼
                      ┌────────────────┐
                      │ Queue Manager  │
                      │ (ephemeral,    │
                      │  durable,      │
                      │  stream)       │
                      └──────┬─────────┘
                             ▼
                      ┌────────────────┐
                      │ Log Storage    │
                      └────────────────┘
```

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
  - Three queue types: ephemeral (in-memory, best-effort), durable (persistent work queue with PEL), stream (append-only log with cursor-based consumption)
  - Shared routing layer — topic bindings, fan-out, consumer filters
  - Delivery semantics depend on queue type, not protocol
  - See [Queue Types](/concepts/queues) for the full model

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

## Storage Overview

FluxMQ uses three storage layers, each optimized for a different job:

1. **Broker state storage** (`storage/`): Sessions, subscriptions, retained messages, wills, and offline queues. Backed by BadgerDB or in-memory for single-node mode.
2. **Queue log storage** (`logstorage/`): Append-only durable logs, consumer group state, and PEL tracking for queues.
3. **Cluster metadata** (embedded etcd): Session ownership, subscriptions, queue consumer registry, and hybrid retained/will metadata.

If you are debugging data persistence, start here:

1. `storage/` for MQTT session and retained/will state.
2. `logstorage/` for queue durability and retention behavior.
3. `cluster/etcd.go` for cross-node metadata and routing.

## Related Docs

- [Routing internals](/architecture/routing)
- [Webhooks](/architecture/webhooks)
- [Storage internals](/architecture/storage)
- [Clustering internals](/architecture/clustering)
- [Durable queues](/messaging/durable-queues)
- [Configuration reference](/reference/configuration-reference)
