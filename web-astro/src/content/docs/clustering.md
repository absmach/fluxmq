---
title: Clustering
description: Distributed broker clustering with embedded etcd, gRPC transport, session takeover, and high availability architecture
---

# Clustering

**Last Updated:** 2026-02-05

FluxMQ supports optional clustering for high availability and cross-node routing. Clustering is embedded and uses etcd for metadata coordination plus gRPC for inter-broker transport.

## What Clustering Provides

- **Session ownership** across nodes
- **Subscription routing** for cross-node publishes
- **Queue consumer registry** for cross-node queue delivery
- **Retained/will storage** with a hybrid metadata + payload strategy
- **Session takeover** when a client reconnects to a different node

## Core Components

- **Embedded etcd**: stores session ownership, subscriptions, queue consumers, retained metadata
- **Transport (gRPC)**: routes publishes, queue deliveries, retained/will fetches
- **Optional Raft for queues**: configurable replication for queue appends

## Configuration (Minimal)

```yaml
cluster:
  enabled: true
  node_id: "broker-1"

  etcd:
    data_dir: "/tmp/fluxmq/etcd"
    bind_addr: "0.0.0.0:2380"
    client_addr: "0.0.0.0:2379"
    initial_cluster: "broker-1=http://0.0.0.0:2380"
    bootstrap: true

  transport:
    bind_addr: "0.0.0.0:7948"
    peers: {}
```

For full cluster options (TLS, Raft, hybrid retained settings), see `docs/configuration.md`.

## Message Routing (High Level)

- On publish, the broker routes to local subscribers and calls the cluster router to forward to remote nodes with matching subscriptions.
- Retained and will messages are stored in a cluster-aware store with metadata in etcd and payloads stored locally for larger messages.
- When a client reconnects to a different node, the new node requests session takeover from the previous owner.

## Queue Delivery Across Nodes

Queue consumers are registered in the cluster. When a queue publish occurs, the queue manager determines which nodes host matching consumers and forwards delivery to those nodes.

## Notes

- Clustering is optional; single-node mode uses in-memory or BadgerDB storage.
- Queue replication is optional and controlled via `cluster.raft`.
