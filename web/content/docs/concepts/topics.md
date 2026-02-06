---
title: Topics
description: Topic names, filters, and wildcard matching rules
---

# Topics

**Last Updated:** 2026-02-05

FluxMQ uses MQTT-style topics across protocols. Topics are hierarchical strings separated by `/`.

## Topic Examples

- `sensors/temperature`
- `orders/created`
- `$queue/orders`
- `$SYS/broker/clients/connected`
- `$share/workers/sensors/#`

## Topic Filters (Subscriptions)

MQTT wildcard rules apply:

- `+` matches a single level (`sensors/+`)
- `#` matches multiple levels (`sensors/#`)

These same patterns are used by queue topic bindings as well.

## Special Namespaces

Some prefixes are reserved for broker features:

| Prefix | Purpose |
| --- | --- |
| `$SYS/...` | Broker stats topics (published periodically, retained). |
| `$queue/<name>/...` | Durable queue traffic (stored in queue logs, delivered via consumer groups). |
| `$share/<group>/<filter>` | Shared subscriptions (MQTT): load-balanced pub/sub delivery. |

## Queue Topics (`$queue/`)

Queue traffic uses the `$queue/` prefix. For example:

- `$queue/orders` (publish)
- `$queue/orders/#` (subscribe)
- `$queue/orders/$ack` (ack a delivery)

FluxMQ treats `$queue/<name>/...` as:

- `name`: the durable queue name
- everything after `name/`: the queue routing key (used for pattern matching inside the queue)

For MQTT, durable queue behavior is triggered by the `$queue/` prefix. A publish to a non-`$queue/` topic uses normal pub/sub routing.

Learn more in [/docs/messaging/durable-queues](/docs/messaging/durable-queues).
