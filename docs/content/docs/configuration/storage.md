---
title: Storage
description: Configure broker storage backend and BadgerDB settings
---

# Storage Configuration

**Last Updated:** 21st August 2026

Broker state (sessions, retained messages, offline queues) is stored in the backend defined by `storage`.

```yaml
storage:
  type: "badger"   # "badger" or "memory"
  badger_dir: "/tmp/fluxmq/data"
  badger_sync_writes: false
  queue_ack_durability: "buffered" # "buffered" or "fsync"
  queue_sync_interval: "1s"
```

## Field Notes

- `type`: storage backend (`memory` or `badger`). It selects the key-value store
  for sessions and retained messages only; queues always use the append-only
  log.
- `badger_dir`: required when `type=badger`.
- `badger_sync_writes`: durability/throughput tradeoff for Badger writes. It does not reach the queue append-only log; queue durability is a separate engine.
- `queue_ack_durability`: default acknowledgement policy for durable queues.
- `queue_sync_interval`: how often the queue log syncs in the background. An
  explicit `"0s"` syncs every append before it returns.

Queue logs are stored under:

```
<storage.badger_dir>/queue
```

## What an acknowledged queue publish guarantees

`queue_ack_durability` decides when the broker tells a publisher its message was
accepted by a **durable** queue. Ephemeral queues never sync: they do not
survive a restart either way.

| Value                | Acknowledged after               | Loses on crash                                       |
| -------------------- | -------------------------------- | ---------------------------------------------------- |
| `buffered` (default) | the write reaches the page cache | up to `queue_sync_interval` of acknowledged messages |
| `fsync`              | the append is on disk            | nothing that was acknowledged                        |

Raft-replicated queues currently support only `buffered`. Synchronous Raft
waits for the operation to be applied, but its queue-log apply path does not use
the per-append fsync barrier. A replicated queue whose effective policy is
`fsync` is therefore rejected at configuration load rather than making a false
durability promise.

**A replicated queue cannot use `fsync`.** Raft apply writes through its own
path and never reaches the queue log's per-append barrier, so the setting would
be accepted and silently ignored. A queue with `replication.enabled: true` and
an effective `fsync` policy — its own or inherited from the broker default — is
rejected at load rather than left to look durable.

Set it per queue where it matters, rather than broker-wide:

```yaml
storage:
  queue_ack_durability: "buffered"

queues:
  - name: audit
    topics: ["audit/#"]
    ack_durability: "fsync"   # this queue only
  - name: telemetry
    topics: ["telemetry/#"]   # takes the broker-wide default
```

**The cost depends on how many publishers share the queue.** The queue log
coalesces durability barriers: one publisher performs the fsync and everyone who
arrived before it started rides the same one. A lone publisher has nobody to
share with and pays the device's full fsync latency; a busy queue amortizes it.
Measured on ext4 over consumer NVMe, 256-byte messages:

| Concurrent publishers | `fsync` | `buffered` |
| ---: | ---: | ---: |
| 1  | ~185 msg/s   | ~125,000 msg/s |
| 16 | ~1,260 msg/s | ~104,000 msg/s |
| 64 | ~3,100 msg/s | ~102,000 msg/s |

Reproduce with `go test ./queue -bench BenchmarkAckDurability -cpu 1,16,64`, on
a real filesystem: `/tmp` is often tmpfs, where fsync costs nothing and the
comparison silently measures nothing.

`buffered` is the default because it is what the broker has always done, so
upgrading does not silently change throughput. Choose `fsync` for the queues
whose records must not be lost, and size the volume accordingly.

## Learn More

- [Storage internals](/architecture/storage)
- [Configuration reference](/reference/configuration-reference)
