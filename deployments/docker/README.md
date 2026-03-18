# Docker Examples

## Build Images

```bash
make docker
```

## Docker Run

Run from the repo root so the config path resolves.

```bash
docker run --rm \
  -p 1883:1883 \
  -p 1884:1884 \
  -p 8083:8083 \
  -p 8084:8084 \
  -p 8080:8080 \
  -p 8082:8082 \
  -p 5672:5672 \
  -p 5682:5682 \
  -p 8081:8081 \
  -v "$(pwd)/deployments/docker/config.yaml:/etc/fluxmq/config.yaml:ro" \
  -v fluxmq-data:/var/lib/fluxmq \
  ghcr.io/absmach/fluxmq:latest \
  --config /etc/fluxmq/config.yaml
```

## Docker Compose

Use `deployments/docker/compose.yaml`:

```bash
docker compose -f deployments/docker/compose.yaml up -d
```

## 3-Node Cluster

See `deployments/cluster/` directory for cluster configs. Both local and Docker
use the same config files (`deployments/cluster/config/node{1,2,3}.yaml`).

### Cluster port map

| Service      | Node 1 | Node 2 | Node 3 |
|--------------|--------|--------|--------|
| MQTT v3      | 1883   | 1885   | 1887   |
| MQTT v5      | 1884   | 1886   | 1888   |
| WS v3        | 8883   | 8885   | 8887   |
| WS v5        | 8884   | 8886   | 8888   |
| HTTP         | 8090   | 8091   | 8092   |
| Admin API    | 9081   | 9082   | 9083   |
| AMQP 1.0     | 5672   | 5673   | 5674   |
| AMQP 0.9.1   | 5682   | 5683   | 5684   |
| Health       | 8081   | 8082   | 8083   |
| etcd peer    | 2380   | 2381   | 2382   |
| etcd client  | 2379   | 2389   | 2399   |
| gRPC transport | 7948 | 7949   | 7950   |

Cluster configs use dedicated TCP and WebSocket listeners per protocol (`server.tcp.v3`/`server.tcp.v5` and `server.websocket.v3`/`server.websocket.v5`).

```bash
# Local processes
make cluster-up
make cluster-down
make clean-data   # optional: remove /tmp/fluxmq data

# Docker (host networking)
make docker-cluster-up
make docker-cluster-down
make clean-data   # optional: remove /tmp/fluxmq data
```
