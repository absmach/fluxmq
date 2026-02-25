---
title: Running Cluster
description: Start a multi-node FluxMQ cluster with embedded etcd and transport peers
---

# Running Cluster

**Last Updated:** 2026-02-25

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
make docker-cluster-up       # start 3-node Docker cluster, wait for health checks
make docker-cluster-down     # stop containers
```

### Manual (per node)

```bash
make build
./build/fluxmq -config deployments/cluster/config/node1.yaml &
./build/fluxmq -config deployments/cluster/config/node2.yaml &
./build/fluxmq -config deployments/cluster/config/node3.yaml &
```

## Cluster Port Map

| Service      | Node 1 | Node 2 | Node 3 |
|--------------|--------|--------|--------|
| MQTT v3      | 1883   | 1885   | 1887   |
| MQTT v5      | 1884   | 1886   | 1888   |
| WebSocket    | 8883   | 8884   | 8885   |
| HTTP         | 8090   | 8091   | 8092   |
| AMQP 1.0     | 5672   | 5673   | 5674   |
| AMQP 0.9.1   | 5682   | 5683   | 5684   |
| Health       | 8081   | 8082   | 8083   |
| etcd peer    | 2380   | 2381   | 2382   |
| etcd client  | 2379   | 2389   | 2399   |
| gRPC transport | 7948 | 7949   | 7950   |

Local cluster configs use dedicated listeners on each node:
- `server.tcp.v3` for MQTT 3.1.1
- `server.tcp.v5` for MQTT 5.0

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
