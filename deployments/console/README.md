# Live FluxMQ console

Build the current broker and Svelte console together. These local deployments
use real Admin API data, persistent storage, and no simulated history or clients.
All published ports bind to loopback. The Admin API and console are read-only
from the browser; the protocol listeners accept real publishers and consumers.

From the repository root, start a standalone broker:

```sh
docker compose -f deployments/console/compose.yaml up -d --build --wait
```

Open http://localhost:3002/dashboard. Stop any demo preview using port 3002 first,
or prefix the command with `FLUXMQ_CONSOLE_PORT=3004` to choose another port.

| Service | Local address |
| --- | --- |
| Console | http://localhost:3002/dashboard |
| Broker Admin API | http://localhost:19082 |
| MQTT 3.1.1 | localhost:11883 |
| MQTT 5 | localhost:11884 |
| AMQP 0.9.1 | localhost:15682 |
| AMQP 1.0 | localhost:15672 |

## Three-node cluster

The cluster override runs three real brokers with embedded etcd and separate
persistent volumes. Stop the standalone deployment before switching modes:

```sh
docker compose -f deployments/console/compose.yaml down
docker compose -f deployments/console/compose.yaml -f deployments/console/cluster.yaml up -d --build --wait
```

Node 1 uses the ports above. Nodes 2 and 3 expose Admin APIs on 19083/19084,
MQTT 3.1.1 on 12883/13883, MQTT 5 on 12884/13884, and AMQP 0.9.1 on 16682/17682.
The console polls all three nodes. Queue counters remain node-local; replicated
queue entries are identified by node and queue name.

The development cluster uses an isolated Docker network `192.168.240.0/24` and
fixed peer addresses because embedded etcd requires IP listen addresses. If that
subnet conflicts with your environment, change it and the three node YAML files
together. Internal cluster transport is unencrypted for this local setup.

Use both `-f` options for cluster `stop`, `start`, `logs`, and `down` commands.
`down` preserves broker data. Do not add `--volumes` unless you intend to delete it.

## Existing brokers

No new broker deployment is needed for an existing cluster. Build `ui-svelte`
with `make build`, then start its binary with trusted Admin API origins:

```sh
FLUXMQ_NODE_URLS=http://broker-a:8082,http://broker-b:8082,http://broker-c:8082 \
  ui-svelte/fluxmq-console -listen 127.0.0.1:3002
```

`FLUXMQ_API_URL` is a single-node fallback. Docker deployments must use addresses
reachable from the console container, not host loopback. Keep Admin APIs private;
remote console access belongs behind an authenticated management gateway.

Live charts accumulate observations after startup (two-second collection
interval, up to fifteen minutes in memory). Empty charts and inventories are
normal until real clients publish or subscribe. Reloading the browser preserves
collector history; restarting the console resets it. The Overview page shows
**Publish traffic by protocol**, using broker ingress counters for MQTT TCP, MQTT WebSocket, HTTP, CoAP, AMQP 0.9.1 and AMQP 1.0.
Rates count incoming application publishes (including attempts that may fail),
and byte totals count payloads. MQTT control packets and forwarded deliveries
are excluded. MQTT and WebSocket each combine MQTT 3.1.1 and MQTT 5 traffic.
Session inventory still covers MQTT and AMQP 0.9.1; HTTP and CoAP do not create
MQTT sessions, and AMQP 1.0 session inventory is not exposed. Older brokers
without ingress counters do not show the new panel. Connected MQTT sessions
report TCP or WebSocket as a separate transport in the Sessions table, filters,
summary cards, CSV export and client details. Persisted sessions without a live
connection have no stored transport and display **Not reported**.

## Real integration regression

Use a disposable deployment: the fixture creates sessions, subscriptions,
classic/stream queues, offline messages and pending deliveries. It publishes
real MQTT and AMQP traffic and keeps the connections alive for three minutes.
It does not run automatically with the deployment.

```sh
go run ./tests/integration/console-fixture \
  -mqtt tcp://127.0.0.1:11883 \
  -amqp amqp://guest:guest@127.0.0.1:15682/ \
  -duration 3m
```

While that command runs, from another terminal:

```sh
cd ui-svelte
FLUXMQ_TEST_URL=http://127.0.0.1:3002 node tests/live.cjs
```

Requires `npm ci` and Playwright Chromium (`npx playwright install chromium`),
or run the test in `mcr.microsoft.com/playwright:v1.62.1-noble` with the UI directory
mounted and host networking. Tests cover real pagination beyond 200 filters,
MQTT/AMQP sessions, offline backlog, subscription drill-downs, live SSE/rates,
queue types, pending deliveries, mixed-source message filtering, and browsing
without consuming messages. The fixture leaves durable test data for inspection;
use a fresh disposable volume for each full regression run.
