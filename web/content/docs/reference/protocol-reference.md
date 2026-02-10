---
title: Protocol Reference
description: Supported protocols, transport options, and how each protocol adapter maps to FluxMQ queue semantics
---

# Protocol Reference

**Last Updated:** 2026-02-10

FluxMQ supports multiple protocols and transports. Each protocol adapter translates protocol-specific concepts into FluxMQ's shared queue primitives. Delivery semantics depend on the [queue type](/docs/concepts/queues), not the protocol.

## Supported Protocols

| Protocol       | Transport      | Queue Publish                              | Queue Consume                      | Ack Support                       |
| -------------- | -------------- | ------------------------------------------ | ---------------------------------- | --------------------------------- |
| **MQTT 3.1.1** | TCP, WebSocket | `$queue/` prefix                           | `$queue/` prefix                   | No (no user properties)           |
| **MQTT 5.0**   | TCP, WebSocket | `$queue/` prefix                           | `$queue/` prefix + user properties | Yes (`$ack/$nack/$reject` topics) |
| **AMQP 0.9.1** | TCP            | `$queue/` routing key or exchange bindings | `basic.consume`                    | Yes (`basic.ack/nack/reject`)     |
| **AMQP 1.0**   | TCP            | Link to `$queue/` address                  | Link from `$queue/` address        | Yes (dispositions)                |
| **HTTP**       | HTTP POST      | `/publish` endpoint                        | No                                 | No                                |
| **CoAP**       | UDP            | CoAP POST                                  | No                                 | No                                |

Protocol listeners are configured under `server.*` in the YAML config. See [Server configuration](/docs/configuration/server).

## Protocol Adapter Design

Protocol adapters are **translators, not abstractions**. Each adapter maps its native concepts to FluxMQ queue primitives without fabricating behavior that the underlying queue type doesn't support.

Key principles:

- **Routing is shared.** All protocols route through the same queue manager and topic index. An MQTT publish and an AMQP 0.9.1 publish to the same `$queue/` topic land in the same queue log.
- **Delivery semantics come from the queue type.** A durable queue delivers with PEL tracking and ack/nack/reject regardless of whether the consumer is MQTT or AMQP. A stream queue delivers with cursor semantics regardless of protocol.
- **Protocol-specific features are scoped to the adapter.** AMQP 0.9.1 exchanges exist only as per-connection routing tables. MQTT shared subscriptions use a separate code path from queue consumer groups. AMQP 1.0 link capabilities map to queue properties.

## MQTT (3.1.1 / 5.0)

- Queue traffic is activated by the `$queue/` topic prefix
- Consumer groups are set via `consumer-group` user property on SUBSCRIBE (MQTT v5 only)
- MQTT v3 clients can publish and subscribe to queue topics, but cannot set consumer groups or send acknowledgments (requires user properties)
- Acknowledgments use special topics: `$queue/<queue>/$ack`, `$queue/<queue>/$nack`, `$queue/<queue>/$reject`
- Shared subscriptions (`$share/<group>/<filter>`) are a separate pub/sub feature — they are not queue consumer groups

See [MQTT client](/docs/clients/mqtt) for usage examples.

## AMQP 0.9.1

- Direct queue publish: use `$queue/` prefix as routing key with the default exchange
- Stream queue publish: use queue name as routing key (without `$queue/` prefix) after declaring with `x-queue-type: stream`
- Exchange-based routing: declare an exchange, bind a queue, and publish via the exchange — the channel translates this to `$queue/<queue>/<routing-key>`
- Consumer groups: set via `x-consumer-group` argument on `basic.consume`
- Stream cursor positioning: set via `x-stream-offset` argument on `basic.consume`
- Acknowledgments: `basic.ack`, `basic.nack`, `basic.reject` map directly to queue Ack/Nack/Reject

### Exchanges and Bindings

**Exchanges and bindings are per-connection state.** They are not shared across connections, not visible to other clients, and not persisted — even when declared as `durable`. This is a deliberate design choice: FluxMQ's routing layer is topic-based on queues, and exchanges are a compatibility shim that translates AMQP 0.9.1 routing concepts into FluxMQ topic semantics.

Clients must re-declare exchanges and bindings on every new connection. If your application depends on exchanges being shared across connections, use direct `$queue/` routing keys with the default exchange instead.

See [Durable Queues — AMQP 0.9.1](/docs/messaging/durable-queues#amqp-091) for full details.

## AMQP 1.0

- Queue addressing: link to `$queue/<name>` address, or use `queue` capability in source/target
- Consumer groups: set via `consumer-group` in link properties
- Cursor positioning: set via `cursor` link property (`earliest`, `latest`, or a specific offset)
- Settle modes: settled-on-send (fire-and-forget) vs settled-on-ack (at-least-once)
- Dispositions map to queue acknowledgments:

| AMQP 1.0 Disposition | Queue Action |
| -------------------- | ------------ |
| `Accepted`           | Ack          |
| `Released`           | Nack (retry) |
| `Rejected`           | Reject (DLQ) |

## HTTP Bridge

- Publish-only: `POST /publish` with JSON body (`topic`, `payload`, `qos`, `retain`)
- No subscription or acknowledgment support
- Useful for integrating HTTP services that need to push messages into queues

See [HTTP client](/docs/clients/http) for usage.

## CoAP Bridge

- Publish-only: CoAP POST to topic path
- No subscription or acknowledgment support

## Learn More

- [Queue Types](/docs/concepts/queues)
- [Server configuration](/docs/configuration/server)
- [MQTT client](/docs/clients/mqtt)
- [WebSocket client](/docs/clients/websocket)
- [HTTP client](/docs/clients/http)
