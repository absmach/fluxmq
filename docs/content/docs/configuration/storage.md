---
title: Storage
description: Configure broker storage backend and BadgerDB settings
---

# Storage Configuration

**Last Updated:** 18th February 2026

Broker state (sessions, retained messages, offline queues) is stored in the backend defined by `storage`.

```yaml
storage:
  type: "badger"   # "badger" or "memory"
  badger_dir: "/tmp/fluxmq/data"
  badger_sync_writes: false
```

## Field Notes

- `type`: storage backend (`memory` or `badger`).
- `badger_dir`: required when `type=badger`.
- `badger_sync_writes`: durability/throughput tradeoff for Badger writes. It does not reach the queue append-only log; queue durability is a separate engine.

Queue logs are stored under:

```
<storage.badger_dir>/queue
```

## Learn More

- [Storage internals](/architecture/storage)
- [Configuration reference](/reference/configuration-reference)
