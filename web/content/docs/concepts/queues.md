---
title: Queue Types
description: Three queue types with distinct semantics — ephemeral, durable, and stream
---

# Queue Types

**Last Updated:** 2026-02-10

FluxMQ supports three distinct queue types. They share the same routing layer and topic namespace (`$queue/`), but differ in persistence, delivery guarantees, and acknowledgment semantics.

**Do not try to unify them.** Each type exists because it solves a different problem. If a use case needs both task semantics and replay, use two queues — one durable, one stream.

## Overview

|                             | Ephemeral                   | Durable                           | Stream                         |
| --------------------------- | --------------------------- | --------------------------------- | ------------------------------ |
| **Storage**                 | In-memory only              | Persistent log                    | Persistent append-only log     |
| **Delivery model**          | Best-effort push            | Push with ack/nack/reject         | Cursor-based pull              |
| **Message lifecycle**       | Fire-and-forget             | Removed on ack                    | Immutable, retained by policy  |
| **Ack means**               | N/A                         | "Message processed, remove it"    | "Checkpoint my read position"  |
| **Nack means**              | N/A                         | "Retry this message"              | No-op (replay via cursor seek) |
| **Persistence**             | None                        | Yes                               | Yes                            |
| **Replay**                  | No                          | No                                | Yes (seek to any offset)       |
| **Retention**               | None (lost on disconnect)   | Truncate after ack                | Time/size/count-based          |
| **Optimized for**           | Low latency, transient data | Task processing, failure handling | Event streams, replay, audit   |
| **Message loss acceptable** | Yes                         | No                                | No                             |

## Ephemeral Queue

Ephemeral queues are auto-created when a publish targets a `$queue/` topic that doesn't match any configured queue. They are in-memory, best-effort, and cleaned up after the last consumer disconnects (plus a short grace period).

- No persistence — messages are lost on broker restart
- No replay — once delivered, messages are gone
- Useful as a development convenience or for transient data where loss is acceptable
- Production deployments should define queues explicitly in config

Ephemeral queues are a safety net, not a primary design target. They prevent "publish to nowhere" from silently dropping data, but they don't provide durability or delivery guarantees.

## Durable Queue

Durable queues are persistent work queues optimized for task processing:

- Messages are appended to a durable log and delivered to consumer groups
- Consumers **ack** to confirm processing — the message is then eligible for truncation
- Consumers **nack** to request redelivery (with backoff and retry limits)
- Consumers **reject** to discard a message (future: route to dead-letter queue)
- Unacknowledged messages are tracked in a **Pending Entry List (PEL)** with visibility timeouts
- **Work stealing** rebalances pending work from slow consumers to idle ones
- Retention is driven by acknowledgment state — once all groups have acked past an offset, the log can be truncated

Use durable queues when every message must be processed at least once, failures must be retried, and message loss is not acceptable.

See [Durable Queues](/docs/messaging/durable-queues) for configuration, addressing, and protocol-specific usage.

## Stream Queue

Stream queues are persistent append-only logs optimized for event streaming:

- Messages are **immutable** — they are never modified or deleted by consumers
- Consumers track progress with a **cursor** (read position in the log)
- **Ack** means "checkpoint" — advance the committed offset, not delete the message
- **Nack** is a no-op — replay is done by seeking the cursor, not by redelivering individual messages
- Multiple independent consumer groups can read the same stream at different positions
- Consumers can seek to any offset or timestamp to replay history
- Retention is policy-based (time, size, message count) — independent of consumer progress

Use stream queues for event sourcing, audit logs, change data capture, analytics pipelines, and any scenario where you need replay, backfill, or multiple independent readers over the same data.

See [Durable Queues — Stream](/docs/messaging/durable-queues#stream) for configuration, cursor positioning, and commit control.

## Shared Routing Layer

All three queue types use the same routing infrastructure:

- **Topic namespace**: `$queue/<name>/...` with MQTT wildcard matching (`+`, `#`)
- **Fan-out**: A single publish can match multiple queues based on their topic bindings
- **Consumer filters**: Subscribers can apply sub-patterns within a queue for fine-grained routing
- **Protocol-agnostic**: MQTT, AMQP 1.0, and AMQP 0.9.1 all route through the same queue manager

Routing determines **which queue** a message is stored in. Delivery semantics depend on **queue type**, not protocol. An MQTT client and an AMQP client consuming from the same durable queue get the same ack/nack/reject behavior.

See [Topics](/docs/concepts/topics) for topic structure and wildcard rules.

## Design Rules

These rules guide FluxMQ's queue design:

1. **Do not assume streams replace queues.** Streams and durable queues solve different problems. A task processor needs ack-based lifecycle control. An event consumer needs cursor-based replay. Using streams for task processing (or durable queues for event replay) will fight the semantics.

2. **Do not overload ack semantics across queue types.** "Ack" means "processed, remove it" in a durable queue and "checkpoint my position" in a stream. These are fundamentally different operations and the broker treats them accordingly.

3. **Prefer correctness and explicit trade-offs over abstraction purity.** Each queue type has its own code path for delivery, acknowledgment, and retention. This is intentional — collapsing them into a single abstraction would hide important behavioral differences.

4. **If a use case needs both task semantics and replay, use two queues.** Publish the same messages to a durable queue (for processing) and a stream queue (for audit/replay). The shared routing layer makes this a one-line config change via overlapping topic bindings.

## Learn More

- [Durable Queues](/docs/messaging/durable-queues) — configuration, addressing, acknowledgments, and protocol-specific usage
- [Consumer Groups](/docs/concepts/consumer-groups) — group modes, progress tracking, and ack semantics per queue type
- [Topics](/docs/concepts/topics) — topic structure, wildcards, and special namespaces
- [Storage](/docs/concepts/storage) — storage layers for broker state, queue logs, and cluster metadata
