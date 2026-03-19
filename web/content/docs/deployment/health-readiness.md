---
title: Health & Readiness
description: Health, readiness, and cluster status endpoints for monitoring and orchestration
---

# Health & Readiness

FluxMQ exposes HTTP endpoints for liveness probes, readiness probes, and
cluster status queries. These are designed to integrate with Kubernetes,
load-balancers, and monitoring systems.

All endpoints are served by the health server when `server.health_enabled: true`
(default). The listen address is controlled by `server.health_addr` (default
`:8081`).

## Endpoints

| Endpoint          | Method | Purpose                                        |
| ----------------- | ------ | ---------------------------------------------- |
| `/health`         | GET    | Liveness probe — is the process alive?         |
| `/ready`          | GET    | Readiness probe — can the node accept traffic? |
| `/cluster/status` | GET    | Cluster membership and leadership info         |

---

## GET /health

Returns `200 OK` if the process is running. Use this for **liveness probes** —
a failure means the process should be restarted.

```json
{
  "status": "healthy"
}
```

This endpoint performs no dependency checks. If the HTTP server can respond,
the process is alive.

---

## GET /ready

Returns a composite readiness assessment of the node's subsystems. Use this
for **readiness probes** — a failure means the node should be removed from
the load-balancer rotation until it recovers.

### Response fields

| Field     | Type   | Description                                                    |
| --------- | ------ | -------------------------------------------------------------- |
| `status`  | string | `"ready"` or `"not_ready"`                                     |
| `mode`    | string | `"nominal"` or `"degraded"` (present when status is `"ready"`) |
| `details` | string | Human-readable reason (present when status is `"not_ready"`)   |
| `checks`  | object | Per-component check results                                    |

Each entry in `checks` contains:

| Field     | Type   | Description                                    |
| --------- | ------ | ---------------------------------------------- |
| `status`  | string | `"up"` or `"down"`                             |
| `details` | string | Optional detail (e.g. peer reachability count) |

### Components checked

| Component   | Failure →     | Condition                                                              |
| ----------- | ------------- | ---------------------------------------------------------------------- |
| **broker**  | 503 not_ready | Broker not initialized                                                 |
| **storage** | 503 not_ready | `storage.Ping()` returns an error (BadgerDB unreachable, closed, etc.) |
| **cluster** | 503 not_ready | Cluster enabled but NodeID is empty (still initializing)               |
| **cluster** | 200 degraded  | Some peers are unreachable (node can still serve local traffic)        |

### Nominal (all healthy)

HTTP 200:

```json
{
  "status": "ready",
  "mode": "nominal",
  "checks": {
    "broker": { "status": "up" },
    "storage": { "status": "up" },
    "cluster": { "status": "up" }
  }
}
```

### Degraded (partial peer loss)

HTTP 200 — the node is still operational but cross-node routing is impaired:

```json
{
  "status": "ready",
  "mode": "degraded",
  "checks": {
    "broker": { "status": "up" },
    "storage": { "status": "up" },
    "cluster": { "status": "up", "details": "1/2 peers reachable" }
  }
}
```

Load-balancers should **keep routing traffic** to degraded nodes. The node
can still serve local clients, accept new connections, and route messages to
reachable peers.

### Not ready (hard failure)

HTTP 503:

```json
{
  "status": "not_ready",
  "details": "storage unavailable",
  "checks": {
    "broker": { "status": "up" },
    "storage": { "status": "down", "details": "badger: closed" }
  }
}
```

### Single-node mode

When clustering is disabled, the `cluster` check is omitted:

```json
{
  "status": "ready",
  "mode": "nominal",
  "checks": {
    "broker": { "status": "up" },
    "storage": { "status": "up" }
  }
}
```

---

## GET /cluster/status

Returns cluster membership information. Useful for dashboards and debugging
but not intended as a probe target.

### Single-node response

```json
{
  "node_id": "single-node",
  "is_leader": false,
  "cluster_mode": false,
  "sessions": 42
}
```

### Cluster response

```json
{
  "node_id": "node-1",
  "is_leader": true,
  "cluster_mode": true,
  "sessions": 128
}
```

---

## Kubernetes integration

### Pod spec example

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8081
  initialDelaySeconds: 5
  periodSeconds: 10

readinessProbe:
  httpGet:
    path: /ready
    port: 8081
  initialDelaySeconds: 10
  periodSeconds: 5
```

### Interpreting degraded mode

Kubernetes readiness probes only look at the HTTP status code. Since degraded
returns `200`, the pod stays in the ready pool. To alert on degraded state,
scrape `/ready` from your monitoring system and check the `mode` field:

```promql
# Example: alert when any FluxMQ node reports degraded for >5m
fluxmq_ready_mode{mode="degraded"} == 1
```

(This assumes you export the mode as a metric or scrape the JSON endpoint.)

---

## Configuration

```yaml
server:
  health_enabled: true    # Enable health endpoints (default: true)
  health_addr: ":8081"    # Listen address (default: ":8081")
```
