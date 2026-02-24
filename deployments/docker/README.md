# Docker Examples

## Build Images

```bash
make docker
make docker-latest
```

`docker-latest` uses `git describe --tags --always --dirty` for the tag.

## Docker Run

Run from the repo root so the config path resolves.

```bash
docker run --rm \
  -p 1883:1883 \
  -p 8083:8083 \
  -p 8080:8080 \
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

```bash
# Local processes
make cluster-up
make cluster-down

# Docker (host networking)
make docker-up
make docker-down
```
