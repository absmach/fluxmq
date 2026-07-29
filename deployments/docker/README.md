# Docker Examples

## Build Images

```bash
# Broker
make docker

# Dashboard
make docker-dashboard
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

Use `deployments/docker/compose.yaml` to start FluxMQ and the dashboard together:

```bash
cp deployments/docker/.env.example deployments/docker/.env
docker compose -f deployments/docker/compose.yaml up -d
```

The dashboard is available at [http://localhost:3001](http://localhost:3001).

By default the dashboard connects to the `fluxmq` service on port `8082`. To override, edit `deployments/docker/.env` before starting:

```bash
FLUXMQ_API_URL=http://my-broker:8082
FLUXMQ_NODE_URLS=http://my-broker:8082
```

## Dedicated local-principal AMQP listener

`compose.local-principal.yaml` is a production-shaped overlay for an Atom audit
publisher. It keeps remote AMQP over TLS on port `5682`, enables an mTLS-only
internal listener on container port `5683`, explicitly disables FluxMQ's
otherwise-default MQTT, WebSocket, and AMQP 1.0 listeners, mounts local
credentials as Docker secrets, and pre-provisions the `atom-audit` stream.

The overlay requires Docker Compose 2.24.4 or newer because it uses
`!override` to remove the base file's unused host port mappings.

FluxMQ's native admin API is a privileged, unauthenticated control plane. This
overlay deliberately does not publish port `8082` to the host; the inherited
dashboard can still reach `fluxmq:8082` over the default Compose network. Keep
that network trusted. If operators need remote admin access, place the API on
a dedicated management network behind an authenticated mTLS reverse proxy or
equivalent access control. Do not add a public `8082:8082` mapping.

`fluxmq-auth` is a required external dependency for the remote listener. This
example intentionally does not invent or pin an auth-service image. Before
starting FluxMQ, provide the real service at the URL configured in
`config-local-principal.yaml` (the default example name is
`https://fluxmq-auth:8181`). The callout hop must use trusted HTTPS, mTLS, or a
service mesh that supplies equivalent authenticated transport protection. The
built-in callout client uses the container's system trust store for HTTPS and
does not expose client-certificate fields. If mTLS is required, terminate it in
a service-mesh sidecar and point FluxMQ at that protected loopback endpoint.
If `fluxmq-auth` is a container, attach it to the Compose project's default
network with the `fluxmq-auth` alias. Do not carry credentials over
unprotected HTTP between hosts.

Set each variable to an absolute host path:

```bash
export FLUXMQ_SERVER_CERT_FILE=/secure/fluxmq/server.crt
export FLUXMQ_SERVER_KEY_FILE=/secure/fluxmq/server.key
export ATOM_CLIENT_CA_FILE=/secure/atom/clients-ca.crt
export ATOM_AUDIT_SECRET_CURRENT_FILE=/secure/atom/audit-secret-current
export ATOM_AUDIT_SECRET_PREVIOUS_FILE=/secure/atom/audit-secret-previous

docker compose \
  -f deployments/docker/compose.yaml \
  -f deployments/docker/compose.local-principal.yaml \
  up -d
```

The previous-secret file is needed only during a rotation overlap. The Compose
overlay declares it so the container mount is stable across rotations; use an
unprivileged placeholder containing a different high-entropy secret when there
is no active overlap.

Secret files must contain a high-entropy printable value of at least 32
bytes/characters. For example, encode 32 random bytes as hex or base64 rather
than writing raw binary. Do not include embedded CR/LF or NUL characters;
FluxMQ strips one terminal newline.

Port `5683` is intentionally absent from `ports:`. The listener also binds only
to FluxMQ's fixed `172.30.0.2` address on the private `atom-internal` network,
so the dashboard and other containers on the default network cannot reach it.
Attach Atom to the Compose project's `atom-internal` network and connect to
`fluxmq:5683`; the server certificate must be valid for the name the Atom AMQP
client verifies. Do not attach public services to that network. If the subnet
conflicts with your environment, change the Compose subnet, FluxMQ static
address, and `server.amqp091.internal.addr` together.

The internal listener requires all three credentials:

- SASL username `atom-audit-publisher`;
- the secret from `ATOM_AUDIT_SECRET_CURRENT_FILE`;
- a client certificate signed by `ATOM_CLIENT_CA_FILE` with URI SAN
  `spiffe://absmach/atom/audit-publisher`.

It may publish only to the default exchange with routing key `atom-audit`.
Subscriptions and topology operations are denied. Confirmed delivery remains
at least once: Atom must reuse a stable event ID on retries, and consumers must
deduplicate it. See
[the implemented design](../../docs/content/docs/deployment/internal-amqp-local-principals.md)
for rotation, validation, rollout requirements, and the complete manual
`rabtap` smoke procedure.

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
