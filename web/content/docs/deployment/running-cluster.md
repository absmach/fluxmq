---
title: Running Cluster
description: Start a multi-node FluxMQ cluster with embedded etcd and transport peers
---

# Running Cluster

**Last Updated:** 2026-02-12

FluxMQ clustering uses embedded etcd for metadata and gRPC transport for routing. The repo includes working 3-node examples.

## Quick Start

The repo includes shared 3-node cluster configs in `deployments/cluster/config/`.
Both local processes and Docker use the same YAML files.

### Local processes

```bash
make cluster-up      # build + start 3 nodes, wait for health checks
make cluster-down    # graceful stop
```

### Docker (host networking)

```bash
make docker-up       # start 3-node Docker cluster, wait for health checks
make docker-down     # stop containers
```

### Manual (per node)

```bash
make build
./build/fluxmq -config deployments/cluster/config/node1.yaml &
./build/fluxmq -config deployments/cluster/config/node2.yaml &
./build/fluxmq -config deployments/cluster/config/node3.yaml &
```

## Key Cluster Settings

- `cluster.enabled`: turn clustering on
- `cluster.node_id`: unique node identifier
- `cluster.etcd.*`: embedded etcd configuration
- `cluster.transport.*`: gRPC transport for routing
- `cluster.raft.*`: optional queue replication

### Transport Tuning

The transport batches outbound messages per remote node. Relevant knobs:

- `route_batch_max_size` (default `256`): flush threshold in messages.
- `route_batch_max_delay` (default `5ms`): max wait before flushing a partial batch.
- `route_batch_flush_workers` (default `4`): concurrent flush goroutines per remote node. Increase for high-latency links; set to `1` for strict ordering.

See the [configuration reference](/docs/reference/configuration-reference#transport-batching) for details.

## Bootstrap Rules

- Set `cluster.etcd.bootstrap: true` only on the first node.
- Other nodes should set `bootstrap: false` and share the same `initial_cluster` map.

## Learn More

- [Clustering internals](/docs/architecture/clustering)
- [Configuration reference](/docs/reference/configuration-reference)
