# FluxMQ Dashboard UI

A Next.js dashboard for real-time monitoring of the FluxMQ message broker. Displays live metrics, session state, cluster topology, and subscription data — all sourced from the FluxMQ Admin API.

## Pages

| Route                      | Description                                                                    |
| -------------------------- | ------------------------------------------------------------------------------ |
| `/dashboard`               | Overview — live metrics, message traffic charts, bandwidth, cluster node table |
| `/dashboard/sessions`      | All sessions — connected and disconnected, filterable, with detail dialog      |
| `/dashboard/subscriptions` | Subscriptions — active topic filters aggregated from connected sessions        |
| `/dashboard/broker-info`   | Broker Info — runtime identity, session counts, error counters                 |

## Environment Setup

Copy the example and edit it to match your environment:

```bash
cp .env.example .env.local
```

`.env.local` is not tracked by git. 

Available variables:

### Single-node setup

```env
FLUXMQ_API_URL=http://localhost:9081
FLUXMQ_NODE_URLS=http://localhost:9081
```

### 3-node cluster

```env
FLUXMQ_API_URL=http://localhost:9081
FLUXMQ_NODE_URLS=http://localhost:9081,http://localhost:9082,http://localhost:9083
```

> `FLUXMQ_API_URL` is the node the dashboard queries for cluster-wide overview data. `FLUXMQ_NODE_URLS` lists all nodes so per-node stats can be fetched in parallel.

## Getting Started

### Prerequisites

- Node.js 18+ or pnpm 10+
- FluxMQ broker running (see `deployments/` for single-node and cluster configs)

### Install dependencies

```bash
pnpm install
```

### Run in development

```bash
pnpm dev
```

Open [http://localhost:3000/dashboard](http://localhost:3000/dashboard).

### Build for production

```bash
pnpm build
pnpm start
```

## Running with Docker

### Build the image

```bash
make docker-dashboard
```

### Run with the full single-node stack

Starts FluxMQ and the dashboard together. Both images must be available locally — build them first if needed:

```bash
make docker
make docker-dashboard
```

```bash
cp deployments/docker/.env.example deployments/docker/.env
docker compose -f deployments/docker/compose.yaml up -d
```

The dashboard is available at [http://localhost:3001/dashboard](http://localhost:3001/dashboard).

Override the broker URL if needed:

```bash
FLUXMQ_API_URL=http://my-broker:8082 \
docker compose -f deployments/docker/compose.yaml up -d
```

### Run with the Docker cluster

```bash
make docker-cluster-up
docker compose -f deployments/cluster/docker-compose.yaml up -d
```

The dashboard connects to node 1 (`http://127.0.0.1:9081`) by default and fans out stats across all three nodes.
