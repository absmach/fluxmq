---
title: Running Cluster
description: Start a multi-node FluxMQ cluster with embedded etcd and transport peers
---

# Running Cluster

**Last Updated:** 2026-02-05

FluxMQ clustering uses embedded etcd for metadata and gRPC transport for routing. The repo includes working 3-node examples.

## Use the Example Configs

From the repo root:

```bash
./build/fluxmq --config examples/node1.yaml
./build/fluxmq --config examples/node2.yaml
./build/fluxmq --config examples/node3.yaml
```

## Key Cluster Settings

- `cluster.enabled`: turn clustering on
- `cluster.node_id`: unique node identifier
- `cluster.etcd.*`: embedded etcd configuration
- `cluster.transport.*`: gRPC transport for routing
- `cluster.raft.*`: optional queue replication

## Bootstrap Rules

- Set `cluster.etcd.bootstrap: true` only on the first node.
- Other nodes should set `bootstrap: false` and share the same `initial_cluster` map.

## Learn More

- `/docs/architecture/clustering-internals`
- `/docs/reference/configuration-reference`
