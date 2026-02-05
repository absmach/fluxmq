---
title: Queues
description: Durable queues, delivery guarantees, and how FluxMQ stores queue messages
---

# Queues

**Last Updated:** 2026-02-05

Queues in FluxMQ are backed by an append-only log. A single publish can be routed into multiple queues based on bindings, and each queue can serve one or more consumer groups (independent progress per group).

## Key Ideas

- Queue topics use the `$queue/` prefix: `$queue/<name>/...`.
- Each queue has a durable log (single-partition, ordered offsets).
- Consumer groups control how that log is consumed (classic vs stream semantics).
- Retention policies control how long data stays available.

## Durable vs Ephemeral Queues

Queues can be configured explicitly via `queues:` in the broker config. FluxMQ can also auto-create **ephemeral queues** as a fallback when a publish does not match any existing bindings.

Ephemeral queues:

- Are created with a default config and a short expiration window after the last consumer disconnects.
- Help prevent “publish to nowhere” from silently dropping data.
- Are best treated as a development convenience; production deployments should define queues explicitly.

## Classic vs Stream

- Classic queues behave like a work queue: deliver once, track pending deliveries, ack/nack/reject.
- Stream queues behave like a log: sequential consumption with cursor-based progress and optional manual commit.

## Learn More

- `/docs/guides/durable-queues`
- `/docs/architecture/storage`
