---
title: Storage
description: Configure broker storage backend and BadgerDB settings
---

# Storage Configuration

**Last Updated:** 2026-02-05

Broker state (sessions, retained messages, offline queues) is stored in the backend defined by `storage`.

```yaml
storage:
  type: "badger"   # "badger" or "memory"
  badger_dir: "/tmp/fluxmq/data"
  sync_writes: false
```

Queue logs are stored under:

```
<storage.badger_dir>/queue
```

## Learn More

- [Storage internals](/docs/architecture/storage)
- [Configuration reference](/docs/reference/configuration-reference)
