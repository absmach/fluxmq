---
title: Storage
description: Storage layers for broker state, queue logs, and cluster metadata
---

# Storage

**Last Updated:** 2026-02-05

FluxMQ separates storage concerns into three layers:

1. **Broker state** (`storage/`): sessions, subscriptions, retained messages, wills, offline queues.
2. **Queue logs** (`logstorage/`): append-only logs for durable queues and consumer groups.
3. **Cluster metadata** (embedded etcd): ownership and routing metadata.

## Learn More

- `/docs/architecture/storage`
- `/docs/architecture/clustering-internals`
