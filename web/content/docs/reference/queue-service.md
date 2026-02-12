---
title: Queue Service API
description: Creating queues, assigning replication policy/groups, and consumer-group heartbeats
---

# Queue Service API

**Last Updated:** 2026-02-12

FluxMQ exposes a `QueueService` API for managing log-based queues (durable and stream) and consumer groups.

This is the API you use when you want to:

- Create or update queues dynamically (instead of only via static `queues:` config).
- Enable replication per queue and assign queues to Raft replication groups.
- Manage consumer groups (join/leave/heartbeat) for real-time consumption clients.

## Create Queue With Replication

When you create a queue, you can attach a replication policy in the queue config.

Replication fields (high-level meaning):

- `replication.enabled`: whether this queue uses Raft replication.
- `replication.group`: which Raft replication group (shard) the queue belongs to (empty means default group).
- `replication.replication_factor`, `replication.min_in_sync_replicas`: durability vs availability tradeoffs.
- `replication.mode` + `replication.ack_timeout`: sync vs async commit behavior.
- Optional timing overrides (`heartbeat_timeout`, `election_timeout`, `snapshot_interval`, `snapshot_threshold`): advanced tuning knobs; most deployments should rely on group defaults.

Behavior:

- If replication is enabled, queue creation and subsequent queue writes are leader-owned and replicated in the queue’s assigned Raft group.
- If `cluster.raft.auto_provision_groups` is enabled, the broker can create the referenced replication group at runtime if it is not explicitly listed in config.

## Updating Replication Policy Later

Changing replication settings on an existing queue is operationally meaningful:

- Changing the queue’s replication group is a migration, not a toggle.
- Treat `replication.group` as stable for a live queue unless you have a migration plan.

## Consumer Group Heartbeats

Consumer groups have a membership concept:

1. A consumer joins a group (receives a `generation_id`).
2. The consumer sends periodic `Heartbeat` calls to stay active.
3. If membership changes, the broker can ask the consumer to rejoin via `should_rejoin=true` (and/or a new `generation_id`).

If a consumer stops heartbeating, the broker considers it dead after its `session_timeout` and can rebalance work away from it.

## Where This Fits With Clustering

- etcd tracks cluster metadata (session owners, subscriptions, queue consumer registry).
- Raft replication groups store durable queue logs and replicated consumer-group state (for replicated queues).
- Inter-node transport routes real-time deliveries and forwards follower writes to the Raft leader when needed.

See:

- [Clustering](/docs/architecture/clustering)
- [Queue Replication Groups (Raft)](/docs/architecture/raft-groups)
- [Configuration reference](/docs/reference/configuration-reference)

