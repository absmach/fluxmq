---
title: Consumer Groups
description: Load-balanced queue consumption and acknowledgment behavior
---

# Consumer Groups

**Last Updated:** 2026-02-05

Consumer groups are how FluxMQ turns a durable queue log into a scalable worker pool.

At a glance:

- One queue can have many groups (each group has independent progress).
- One group can have many consumers (messages are load-balanced within the group).
- Groups track progress using offsets (cursor/committed) and, in classic mode, a pending list (PEL).

## Group Modes: Classic vs Stream

FluxMQ supports two consumer group modes:

- **Classic (work queue)**: deliveries are claimed and tracked in a PEL; acks control safe progress.
- **Stream**: consumers read sequentially from an append-only log and advance a cursor.

## Progress Tracking (Cursor, Committed, PEL)

Each group keeps two offsets:

- `cursor`: the next offset to deliver
- `committed`: the safe floor used for retention/truncation

Classic groups also keep a **PEL** (Pending Entry List): messages that were delivered but not yet acknowledged.

Intuition:

- Claiming work moves the cursor forward and adds entries to the PEL.
- Acknowledging work removes entries from the PEL.
- The committed offset advances when the earliest pending work is cleared.

## Group Names in Logs (`group@pattern`)

FluxMQ stores queue consumer groups internally as:

- `<consumer-group>@<subscription-pattern>` when a pattern exists
- `<consumer-group>` when no pattern exists

This lets one logical group keep separate progress for different filters on the same queue.

Examples:

- Subscribe to `$queue/demo-events/#` with `consumer-group=demo-workers` -> internal group `demo-workers@#`
- Subscribe to `$queue/demo-events/orders/+` with `consumer-group=demo-workers` -> internal group `demo-workers@orders/+`
- Subscribe to `$queue/demo-events` (no trailing filter) with `consumer-group=demo-workers` -> internal group `demo-workers`

So seeing `group=demo-events@#` in logs means:

- effective group id is `demo-events`
- subscription pattern is `#` for that queue

## Acknowledgment Semantics

- **Ack**: confirms processing and allows committed offset to move forward.
- **Nack**: requests redelivery (often after a visibility timeout or via work stealing).
- **Reject**: removes a message from pending tracking (DLQ handling is not fully wired yet).

## Learn More

- [/docs/messaging/consumer-groups](/docs/messaging/consumer-groups)
- [/docs/messaging/durable-queues](/docs/messaging/durable-queues)
