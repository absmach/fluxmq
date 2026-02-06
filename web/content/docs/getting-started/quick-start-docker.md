---
title: Quick Start (Docker)
description: Run FluxMQ with Docker Compose or docker run using the provided config
---

# Quick Start (Docker)

**Last Updated:** 2026-02-05

## Option 1: Docker Compose (Recommended)

Run from the repo root so paths resolve correctly:

```bash
docker compose -f docker/compose.yaml up -d
```

Use a custom config (example):

```bash
FLUXMQ_CONFIG=../examples/no-cluster.yaml \
  docker compose -f docker/compose.yaml up -d
```

## Option 2: Docker Run

```bash
docker run --rm \
  -p 1883:1883 \
  -p 8083:8083 \
  -p 8080:8080 \
  -p 5672:5672 \
  -p 5682:5682 \
  -p 8081:8081 \
  -v "$(pwd)/docker/config.yaml:/etc/fluxmq/config.yaml:ro" \
  -v fluxmq-data:/var/lib/fluxmq \
  ghcr.io/absmach/fluxmq:latest \
  --config /etc/fluxmq/config.yaml
```

## Next Steps

- [First message](/docs/getting-started/first-message)
- [First durable queue](/docs/getting-started/first-durable-queue)
