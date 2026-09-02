---
title: Topics
description: Topic names, filters, and wildcard matching rules
---

# Topics

**Last Updated:** 18th February 2026

FluxMQ uses MQTT-style topics across protocols. Topics are hierarchical strings separated by `/`.

For AMQP 0.9.1 pub/sub (default exchange path), filters and routing keys are translated at the protocol boundary:

| AMQP 0.9.1 | MQTT canonical |
| --- | --- |
| `.` | `/` |
| `*` | `+` |
| `#` | `#` |

## Topic Examples

- `sensors/temperature`
- `orders/created`
- `$queue/orders`
- `$SYS/broker/clients/connected`
- `$share/workers/sensors/#`

## Topic Filters (Subscriptions)

MQTT wildcard rules apply:

- `+` matches exactly one level (`sensors/+`)
- `#` matches zero or more remaining levels and **must be the final level**
  (`sensors/#`). Because it matches zero levels, `sensors/#` also matches
  `sensors` itself.

A `#` anywhere but the end — `sensors/#/temp` — is not a valid filter and matches
nothing.

These same patterns are used by queue topic bindings as well.

## Special Namespaces

Some prefixes are reserved for broker features:

| Prefix | Purpose |
| --- | --- |
| `$SYS/...` | Broker stats topics (published periodically, retained). |
| `$queue/<name>/...` | Durable queue traffic (stored in queue logs, delivered via consumer groups). |
| `$share/<group>/<filter>` | [Shared subscriptions](/concepts/shared-subscriptions) (MQTT): each message goes to one member of the group. |

## Queue Topics (`$queue/`)

Queue traffic uses the `$queue/` prefix. For example:

- `$queue/orders` (publish)
- `$queue/orders/#` (subscribe)
- `$queue/orders/$ack` (ack a delivery)

FluxMQ treats `$queue/<name>/...` as:

- `name`: the durable queue name
- everything after `name/`: the queue routing key (used for pattern matching inside the queue)

For MQTT, durable queue behavior is triggered by the `$queue/` prefix. A publish to a non-`$queue/` topic uses normal pub/sub routing.

Learn more in [Durable queues](/messaging/durable-queues).
