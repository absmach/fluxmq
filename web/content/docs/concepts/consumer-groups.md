---
title: Consumer Groups
description: Load-balanced queue consumption and acknowledgment behavior
---

# Consumer Groups

**Last Updated:** 2026-02-10

Consumer groups are how FluxMQ turns a queue log into a scalable worker pool. They apply to both durable and stream queues (ephemeral queues do not use consumer groups — delivery is best-effort with no progress tracking).

At a glance:

- One queue can have many groups (each group has independent progress).
- One group can have many consumers (messages are load-balanced within the group).
- Groups track progress using offsets (cursor/committed) and, in classic mode, a pending list (PEL).
- The group **mode** (classic or stream) determines acknowledgment semantics — see below.

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

Ack/nack/reject mean different things depending on the queue type. Do not assume they are interchangeable.

### Durable Queue (Classic Mode)

- **Ack**: removes the message from the PEL and advances the committed offset. The message becomes eligible for log truncation.
- **Nack**: resets the PEL entry so the message is redelivered (immediately stealable by other consumers).
- **Reject**: removes from PEL without redelivery. The message is discarded (future: routes to DLQ).

### Stream Queue (Stream Mode)

- **Ack**: advances the committed offset (checkpoint). The message remains in the log — retention is policy-based, not ack-based.
- **Nack**: no-op. Stream consumers replay by seeking the cursor to a previous offset, not by nacking individual messages.
- **Reject**: advances the cursor past the message to prevent an infinite redelivery loop. The message stays in the log.

### Ephemeral Queue

Ephemeral queues do not use consumer groups or acknowledgments. Delivery is best-effort — messages are pushed to connected consumers and not tracked.

## Learn More

- [Consumer groups](/docs/messaging/consumer-groups)
- [Durable queues](/docs/messaging/durable-queues)
