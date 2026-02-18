---
title: Storage
description: Configure broker storage backend and BadgerDB settings
---

# Storage Configuration

**Last Updated:** 2026-02-18

Broker state (sessions, retained messages, offline queues) is stored in the backend defined by `storage`.

```yaml
storage:
  type: "badger"   # "badger" or "memory"
  badger_dir: "/tmp/fluxmq/data"
  sync_writes: false
```

## Field Notes

- `type`: storage backend (`memory` or `badger`).
- `badger_dir`: required when `type=badger`.
- `sync_writes`: durability/throughput tradeoff for Badger writes.

Queue logs are stored under:

```
<storage.badger_dir>/queue
```

## Learn More

- [Storage internals](/docs/architecture/storage)
- [Configuration reference](/docs/reference/configuration-reference)
