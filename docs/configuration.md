# Configuration Guide

This document provides a comprehensive guide to configuring the MQTT broker for single-node and clustered deployments.

## Table of Contents

- [Configuration File Structure](#configuration-file-structure)
- [Server Configuration](#server-configuration)
- [Broker Configuration](#broker-configuration)
- [Session Configuration](#session-configuration)
- [Queue Configuration](#queue-configuration)
- [Storage Configuration](#storage-configuration)
- [Cluster Configuration](#cluster-configuration)
- [Rate Limiting Configuration](#rate-limiting-configuration)
- [Webhook Configuration](#webhook-configuration)
- [Logging Configuration](#logging-configuration)
- [Example Configurations](#example-configurations)
- [Best Practices](#best-practices)

## Configuration File Structure

The broker uses YAML configuration files:

```yaml
server:
  # Server transport settings
  tcp:
    plain:
      addr: ":1883"
  websocket:
    plain:
      addr: ":8083"
      path: "/mqtt"
  http:
    plain:
      addr: ":8080"
  amqp:
    plain:
      addr: ":5672"
  amqp091:
    plain:
      addr: ":5682"
  # ...

  health_enabled: true
  health_addr: ":8081"
  metrics_enabled: false
  metrics_addr: "localhost:4317"
  otel_service_name: "fluxmq"
  otel_service_version: "1.0.0"
  otel_metrics_enabled: true
  otel_traces_enabled: false
  otel_trace_sample_rate: 0.1
  api_enabled: false
  api_addr: ":9090"

broker:
  # Broker behavior settings
  max_message_size: 1048576
  max_qos: 2
  # ...

session:
  # Session management settings
  max_sessions: 10000
  # ...

queues:
  # Durable queue bindings
  - name: "orders"
    topics: ["orders/#", "$queue/orders/#"]

storage:
  # Storage backend settings
  type: "badger"
  # ...

cluster:
  # Clustering settings
  enabled: true
  # ...

ratelimit:
  enabled: false

webhook:
  enabled: false

log:
  # Logging settings
  level: "info"
  # ...
```

**Loading Configuration**:

```bash
# Use default configuration
./build/fluxmq

# Load from file
./build/fluxmq --config /path/to/config.yaml
```

## Server Configuration

Controls the transport layer (TCP, WebSocket, HTTP, CoAP). Each protocol has
fixed listener slots (`plain`, `tls`, `mtls`) and a listener is enabled when its
`addr` is set.

```yaml
server:
  # TCP Transport (MQTT over TCP)
  tcp:
    plain:
      addr: ":1883"
      max_connections: 10000
      read_timeout: "60s"
      write_timeout: "60s"
    tls:
      addr: ":8883"
      cert_file: "/path/to/server.crt"
      key_file: "/path/to/server.key"
      min_version: "TLS1.2"
      prefer_server_cipher_suites: true
      cipher_suites:
        - TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
        - TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
        - TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
        - TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
    mtls:
      addr: ":8884"
      cert_file: "/path/to/server.crt"
      key_file: "/path/to/server.key"
      ca_file: "/path/to/ca.crt"
      client_auth: "require"

  # WebSocket Transport (MQTT over WebSocket)
  websocket:
    plain:
      addr: ":8083"
      path: "/mqtt"
      allowed_origins:
        - "https://app.example.com"
        - "*.example.com"
    tls:
      addr: ":8084"
      path: "/mqtt"
      cert_file: "/path/to/server.crt"
      key_file: "/path/to/server.key"
    mtls: {}

  # HTTP-MQTT Bridge
  http:
    plain:
      addr: ":8080"
    tls:
      addr: ":8443"
      cert_file: "/path/to/server.crt"
      key_file: "/path/to/server.key"
    mtls:
      addr: ":8444"
      cert_file: "/path/to/server.crt"
      key_file: "/path/to/server.key"
      ca_file: "/path/to/ca.crt"

  # CoAP Bridge
  coap:
    plain:
      addr: ":5683"
    dtls: {}
    mdtls: {}

  # AMQP 1.0
  amqp:
    plain:
      addr: ":5672"
      max_connections: 10000
    tls:
      addr: ":5671"
      cert_file: "/path/to/server.crt"
      key_file: "/path/to/server.key"
    mtls: {}

  # AMQP 0.9.1
  amqp091:
    plain:
      addr: ":5682"
      max_connections: 10000
    tls:
      addr: ":5681"
      cert_file: "/path/to/server.crt"
      key_file: "/path/to/server.key"
    mtls: {}

  shutdown_timeout: "30s"
  health_enabled: true
  health_addr: ":8081"
  metrics_enabled: false
  metrics_addr: "localhost:4317"
  otel_service_name: "fluxmq"
  otel_service_version: "1.0.0"
  otel_metrics_enabled: true
  otel_traces_enabled: false
  otel_trace_sample_rate: 0.1
  api_enabled: false
  api_addr: ":9090"
```

AMQP listeners use the same schema as TCP (addr, max_connections, and TLS fields).
`server.amqp` configures AMQP 1.0, and `server.amqp091` configures AMQP 0.9.1.

TLS fields (`cert_file`, `key_file`, `ca_file`, `server_ca_file`) are inline
under each listener slot (not nested under `tls:`).

`client_auth` is optional and accepts `none`, `request`, `require_any`,
`verify_if_given`, or `require` (alias for require-and-verify). If `ca_file` is
set and `client_auth` is empty, the server defaults to `require` (RequireAndVerifyClientCert).
Set `client_auth` explicitly to override that default.

`min_version` uses string values like `TLS1.2` or `TLS1.3` and applies only to TLS.
If omitted, Go's default TLS behavior is used.
`cipher_suites` takes TLS cipher suite names; TLS 1.3 suites are not configurable in Go
and will be rejected. The same list is used for DTLS listeners, but DTLS will reject
any suites it doesn't support. If `cipher_suites` is omitted, each library's default
list is used. `prefer_server_cipher_suites` only applies to TLS as well.

### Go TLS Defaults (Go 1.24)

If you omit `min_version`, `cipher_suites`, and `prefer_server_cipher_suites`, Go's
defaults are used. As of Go 1.24:

- **Minimum version**: TLS 1.2 (TLS 1.3 is enabled by default).
- **TLS 1.3 cipher suites** (order prefers AES when hardware support is present):

```text
TLS_AES_128_GCM_SHA256
TLS_AES_256_GCM_SHA384
TLS_CHACHA20_POLY1305_SHA256
```

- **TLS 1.2 cipher suites** (order prefers AES-GCM when hardware support is present):

```text
TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256
TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256
TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305
TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305
TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA
TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA
TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
```

Defaults can change across Go versions and can also be affected by `GODEBUG`
flags like `tlsrsakex=1` and `tls3des=1`. For DTLS defaults, see the Pion
DTLS documentation (`dtls.CipherSuites()`).

### TCP Configuration

**server.tcp.<mode>.addr**:
- Format: `"host:port"` or `":port"`
- Examples:
  - `":1883"` - Listen on all interfaces, port 1883
  - `"127.0.0.1:1883"` - Listen only on localhost
  - `"192.168.1.10:1883"` - Listen on specific IP

**server.tcp.<mode>.max_connections**:
- Maximum concurrent TCP connections
- Default: 10000
- Adjust based on available file descriptors (`ulimit -n`)

**server.tcp.<mode>.read_timeout / server.tcp.<mode>.write_timeout**:
- Duration format: `"60s"`, `"5m"`, `"1h"`
- Prevents hung connections
- Should be longer than maximum expected keep-alive

### WebSocket Configuration

```yaml
server:
  websocket:
    plain:
      addr: ":8083"
      path: "/mqtt"
      allowed_origins:
        - "https://app.example.com"
        - "*.example.com"
```

**Client Connection**:
```javascript
// Browser client
const client = mqtt.connect('ws://localhost:8083/mqtt');
```

**Origin Validation**:
- `allowed_origins` empty: allow all origins (development mode, warns in logs)
- `allowed_origins` set: only listed origins allowed
- `"*"`: explicit wildcard for all origins
- Supports wildcard subdomains like `"*.example.com"`

### HTTP Bridge Configuration

```yaml
server:
  http:
    plain:
      addr: ":8080"
```

**Publishing via HTTP**:
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "sensor/temperature",
    "payload": "eyJ0ZW1wIjogMjIuNX0=",
    "qos": 1,
    "retain": false
  }'
```

### Health and Readiness

```yaml
server:
  health_enabled: true
  health_addr: ":8081"
```

Endpoints:
- `GET /health` liveness
- `GET /ready` readiness
- `GET /cluster/status` cluster status (single node returns `cluster_mode=false`)

### OpenTelemetry / Metrics

OpenTelemetry is enabled when `server.metrics_enabled` is `true`. Metrics and
traces are exported via OTLP/gRPC to `server.metrics_addr`.

```yaml
server:
  metrics_enabled: false
  metrics_addr: "localhost:4317"
  otel_service_name: "fluxmq"
  otel_service_version: "1.0.0"
  otel_metrics_enabled: true
  otel_traces_enabled: false
  otel_trace_sample_rate: 0.1
```

Notes:
- `metrics_enabled` controls provider initialization.
- `otel_metrics_enabled` and `otel_traces_enabled` control what is emitted.
- `otel_trace_sample_rate` must be between 0.0 and 1.0.

### Queue API Server (Connect/gRPC)

The queue API server exposes Connect/HTTP and h2c endpoints for queue management.

```yaml
server:
  api_enabled: false
  api_addr: ":9090"
```

## Broker Configuration

Controls broker behavior and message handling.

```yaml
broker:
  max_message_size: 1048576            # 1MB max payload size
  max_retained_messages: 10000         # Max retained messages
  retry_interval: "20s"                # QoS retry interval
  max_retries: 0                       # 0 = infinite retries
  max_qos: 2                           # Max supported QoS (0-2)
```

### max_message_size

Maximum MQTT payload size in bytes.

**Considerations**:
- Larger values use more memory
- MQTT default is unlimited, but this protects against abuse
- Should match client expectations

**Examples**:
- `1048576` (1MB) - IoT sensors
- `10485760` (10MB) - File transfers
- `104857600` (100MB) - Large data transfers

### max_retained_messages

Maximum number of retained messages stored.

**Behavior**:
- When limit reached, oldest retained messages evicted
- 0 = unlimited (not recommended)
- Applies per-node in clustered mode

**Recommendations**:
- Small deployments: 1000-10000
- Large deployments: 100000+

**Note**: This limit is not enforced in the current broker implementation.

### retry_interval

How often to retry unacknowledged QoS 1/2 messages.

**Considerations**:
- Too short: Network congestion
- Too long: Delayed delivery
- Typical: 10-30 seconds

**Note**: The broker currently uses a fixed 20s retry timeout; config wiring is planned.

### max_retries

Maximum retry attempts before giving up.

**Values**:
- `0`: Infinite retries (recommended for reliability)
- `N`: Give up after N attempts

**Note**: Max retries are not currently enforced.

### max_qos

Maximum QoS level supported by the broker (0, 1, or 2). Higher client QoS
publishes are downgraded to this level.

## Session Configuration

Controls session lifecycle and queuing.

```yaml
session:
  max_sessions: 10000                  # Max concurrent sessions
  default_expiry_interval: 300         # 5 minutes default expiry
  max_offline_queue_size: 1000         # Max queued messages per session
  max_inflight_messages: 100           # Max inflight QoS 1/2 per session
  offline_queue_policy: "evict"        # "evict" or "reject"
```

### max_sessions

Maximum concurrent sessions the broker will manage.

**Calculation**:
```
Memory per session ≈ 1-10KB (depending on activity)
Max sessions = Available Memory / Memory per session
```

**Examples**:
- 1GB RAM → ~100,000 - 1,000,000 sessions
- 4GB RAM → ~500,000 - 4,000,000 sessions

### default_expiry_interval

Default session expiry for clients that don't specify.

**Values** (in seconds):
- `0`: Session expires immediately on disconnect
- `300`: 5 minutes (good default)
- `3600`: 1 hour
- `86400`: 24 hours

**MQTT 5.0**: Clients can override with Session Expiry Interval property.

### max_offline_queue_size

Maximum messages queued for disconnected client.

**Behavior**:
- When limit reached, behavior is controlled by `offline_queue_policy`
- Prevents memory exhaustion from offline clients
- Only applies to QoS > 0 messages

**Recommendations**:
- IoT sensors: 100-1000
- Mobile apps: 1000-10000
- Critical systems: 10000+

### max_inflight_messages

Maximum unacknowledged QoS 1/2 messages per session.

**MQTT Spec**:
- MQTT 3.1.1: Fixed at 65535
- MQTT 5.0: Client specifies "Receive Maximum"

**Typical Values**:
- Conservative: 100
- Standard: 1000
- Aggressive: 10000

### offline_queue_policy

Controls what happens when the offline queue is full.

**Values**:
- `evict`: Drop oldest messages and enqueue new ones
- `reject`: Reject new messages while preserving existing queue

## Queue Configuration

Queues are defined under `queues:` and bind **topic patterns** to durable queues.
Topics use MQTT-style wildcards (`+`, `#`). A publish routed through the queue
manager is enqueued into every queue whose bindings match the topic.

If no queues are configured, a default reserved queue named `mqtt` is created
with topics `["$queue/#"]`, which preserves the `$queue/*` behavior out of the box.

```yaml
queues:
  - name: "orders"
    topics:
      - "orders/#"
      - "$queue/orders/#"   # allow explicit queue publish
    reserved: false
    limits:
      max_message_size: 1048576
      max_depth: 100000
      message_ttl: "168h"
    retry:
      max_retries: 10
      initial_backoff: "5s"
      max_backoff: "5m"
      multiplier: 2.0
    dlq:
      enabled: true
      topic: "$dlq/orders" # default: "$dlq/<queue-name>"
```

### name

Logical queue name (no `$queue/` prefix). Queue names are referenced by
protocol-specific addressing (e.g., `$queue/<name>` in MQTT/AMQP interop).

### topics

List of topic filters that route into this queue. A single topic can match
multiple queues. If you want explicit `$queue/<name>` publishes to feed the
queue, include `$queue/<name>/#` in the list.

### reserved

Marks a queue as system-reserved (cannot be deleted via management APIs).

### limits / retry / dlq

Per-queue limits, retry policy, and dead-letter behavior. If `dlq.topic` is
empty, it defaults to `$dlq/<queue-name>`.

**Note**: Queue limits, retry policy, and DLQ behavior are parsed into queue
configs but are not fully enforced at runtime yet. `message_ttl` is stored on
messages, but automatic expiry is not implemented.

## Storage Configuration

Selects the storage backend for persistence.

```yaml
storage:
  type: "badger"                       # "memory" or "badger"
  badger_dir: "/tmp/fluxmq/data"         # BadgerDB directory
```

### Memory Storage

```yaml
storage:
  type: "memory"
```

**Characteristics**:
- All data in RAM
- Fastest performance
- No persistence
- Lost on restart

**Use Cases**:
- Development/testing
- Ephemeral deployments
- Cache-only scenarios

### BadgerDB Storage

```yaml
storage:
  type: "badger"
  badger_dir: "/var/lib/mqtt/data"
```

**Characteristics**:
- LSM tree embedded database
- Persists to disk
- Fast writes, good reads
- Automatic compaction

**Directory Structure**:
```
/var/lib/mqtt/data/
├── 000000.vlog    # Value log
├── 000001.sst     # SSTables
├── MANIFEST       # Metadata
└── ...
```

**Disk Space**:
- Compaction ratio: ~1.5-3x raw data
- Reserve 2-5x expected data size

**Permissions**:
```bash
mkdir -p /var/lib/mqtt/data
chown mqtt:mqtt /var/lib/mqtt/data
chmod 700 /var/lib/mqtt/data
```

## Cluster Configuration

Enables distributed broker clustering.

```yaml
cluster:
  enabled: true                        # Enable clustering
  node_id: "node1"                     # Unique node identifier

  etcd:
    data_dir: "/tmp/fluxmq/node1/etcd"   # etcd data directory
    bind_addr: "127.0.0.1:2380"        # etcd peer address
    client_addr: "127.0.0.1:2379"      # etcd client address
    initial_cluster: "node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2480,node3=http://127.0.0.1:2580"
    bootstrap: true                    # Bootstrap new cluster
    hybrid_retained_size_threshold: 1024 # Bytes; smaller retained messages replicated via etcd

  transport:
    bind_addr: "127.0.0.1:7948"        # gRPC listen address
    peers:                             # Peer node addresses
      node2: "127.0.0.1:7949"
      node3: "127.0.0.1:7950"
    tls_enabled: false
    tls_cert_file: "/path/to/transport.crt"
    tls_key_file: "/path/to/transport.key"
    tls_ca_file: "/path/to/ca.crt"

  raft:
    enabled: false
    replication_factor: 3
    sync_mode: true
    min_in_sync_replicas: 2
    ack_timeout: "5s"
    bind_addr: "127.0.0.1:7100"
    data_dir: "/tmp/fluxmq/raft"
    peers: {}
    heartbeat_timeout: "1s"
    election_timeout: "3s"
    snapshot_interval: "5m"
    snapshot_threshold: 8192
```

### enabled

Enable or disable clustering.

**Values**:
- `false`: Single-node mode
- `true`: Cluster mode (default in `config.Default()`)

### node_id

Unique identifier for this broker node.

**Requirements**:
- Must be unique across all nodes
- Used in etcd cluster configuration
- Cannot be changed after initialization

**Naming Convention**:
- `node1`, `node2`, `node3`, ...
- `broker-us-east-1a`, `broker-us-east-1b`, ...
- `mqtt-01`, `mqtt-02`, ...

### etcd Configuration

#### data_dir

Directory for etcd's persistent data.

**Requirements**:
- Must be writable
- Should be on fast disk (SSD recommended)
- Backed up regularly

**Size Estimation**:
```
Subscriptions: ~100 bytes each
Sessions: ~200 bytes each
Retained: ~1KB each (including payload)

For 10,000 sessions, 50,000 subscriptions:
~10MB + growth for retained messages
```

#### bind_addr / client_addr

**bind_addr**: Raft peer-to-peer communication
**client_addr**: etcd API for local broker

**Port Allocation**:
| Node | bind_addr | client_addr |
|------|-----------|-------------|
| node1 | 127.0.0.1:2380 | 127.0.0.1:2379 |
| node2 | 127.0.0.1:2480 | 127.0.0.1:2479 |
| node3 | 127.0.0.1:2580 | 127.0.0.1:2579 |

**Production** (different hosts):
```yaml
# node1 (192.168.1.10)
bind_addr: "192.168.1.10:2380"
client_addr: "127.0.0.1:2379"  # Local only

# node2 (192.168.1.11)
bind_addr: "192.168.1.11:2380"
client_addr: "127.0.0.1:2379"
```

#### initial_cluster

Comma-separated list of all nodes in initial cluster.

**Format**: `name=peer_url,name=peer_url,...`

**Example**:
```yaml
initial_cluster: "node1=http://192.168.1.10:2380,node2=http://192.168.1.11:2380,node3=http://192.168.1.12:2380"
```

**Important**:
- All nodes must have identical `initial_cluster` value
- Use HTTP (HTTPS not yet supported)
- URLs must match `bind_addr` values

#### bootstrap

Whether this node is part of initial cluster formation.

**Values**:
- `true`: Node participates in new cluster formation
- `false`: Node joins existing cluster (dynamic membership)

**Initial Cluster** (all nodes start together):
```yaml
# All nodes set bootstrap: true
node1: bootstrap: true
node2: bootstrap: true
node3: bootstrap: true
```

**Add Node to Running Cluster** (advanced):
```yaml
# Existing nodes: bootstrap: true
# New node: bootstrap: false

# 1. Add member to existing cluster (via etcdctl)
# 2. Start new node with bootstrap: false
```

#### hybrid_retained_size_threshold

Threshold in bytes for hybrid retained storage:
- Messages smaller than this are replicated via etcd.
- Larger messages are stored on the owner node and fetched on-demand.

### transport Configuration

#### bind_addr

gRPC server listen address for inter-broker communication.

**Format**: `"host:port"`

**Examples**:
```yaml
# Local testing
bind_addr: "127.0.0.1:7948"

# Production
bind_addr: "0.0.0.0:7948"        # Listen on all interfaces
bind_addr: "192.168.1.10:7948"   # Listen on specific IP
```

#### peers

Map of peer node IDs to their transport addresses.

**Format**:
```yaml
peers:
  nodeID: "host:port"
  ...
```

**Example**:
```yaml
# node1 configuration
transport:
  bind_addr: "127.0.0.1:7948"
  peers:
    node2: "127.0.0.1:7949"
    node3: "127.0.0.1:7950"

# node2 configuration
transport:
  bind_addr: "127.0.0.1:7949"
  peers:
    node1: "127.0.0.1:7948"
    node3: "127.0.0.1:7950"
```

**Production** (different hosts):
```yaml
# node1 (192.168.1.10)
transport:
  bind_addr: "0.0.0.0:7948"
  peers:
    node2: "192.168.1.11:7948"
    node3: "192.168.1.12:7948"
```

#### transport TLS

Enable mTLS for inter-broker transport:

```yaml
transport:
  tls_enabled: true
  tls_cert_file: "/path/to/transport.crt"
  tls_key_file: "/path/to/transport.key"
  tls_ca_file: "/path/to/ca.crt"
```

All three files are required when `tls_enabled` is `true`.

### Raft Configuration

Raft-based replication for **queue appends** is optional and disabled by default.
Consumer state and ack/nack/reject paths are not replicated yet.

```yaml
raft:
  enabled: false
  replication_factor: 3
  sync_mode: true
  min_in_sync_replicas: 2
  ack_timeout: "5s"
  bind_addr: "127.0.0.1:7100"
  data_dir: "/tmp/fluxmq/raft"
  peers:
    node2: "127.0.0.1:7101"
    node3: "127.0.0.1:7102"
  heartbeat_timeout: "1s"
  election_timeout: "3s"
  snapshot_interval: "5m"
  snapshot_threshold: 8192
```

## Rate Limiting Configuration

Rate limiting can be enabled per IP and per client.

```yaml
ratelimit:
  enabled: false
  connection:
    enabled: true
    rate: 1.6667            # connections per second per IP (100/min)
    burst: 20
    cleanup_interval: "5m"
  message:
    enabled: true
    rate: 1000              # messages per second per client
    burst: 100
  subscribe:
    enabled: true
    rate: 100               # subscriptions per second per client
    burst: 10
```

Behavior:
- Connection limits are enforced before MQTT handshake.
- Message limits return MQTT 5 `QuotaExceeded` for QoS > 0 and drop QoS 0.
- Subscribe limits return MQTT 5 `SubAckQuotaExceeded` or MQTT 3 `SubAckFailure`.

## Webhook Configuration

Webhook notifications are optional and disabled by default.

```yaml
webhook:
  enabled: false
  queue_size: 10000
  drop_policy: "oldest"
  workers: 5
  include_payload: false
  shutdown_timeout: "30s"
  defaults:
    timeout: "5s"
    retry:
      max_attempts: 3
      initial_interval: "1s"
      max_interval: "30s"
      multiplier: 2.0
    circuit_breaker:
      failure_threshold: 5
      reset_timeout: "60s"
  endpoints:
    - name: "analytics-service"
      type: "http"
      url: "https://analytics.example.com/mqtt/events"
      events: ["message.published", "client.connected"]
      topic_filters: ["sensors/#"]
      headers:
        Authorization: "Bearer token"
      timeout: "10s"
      retry:
        max_attempts: 5
```

Notes:
- `include_payload` is accepted by config but payload inclusion is not yet wired in the notifier.
- Supported endpoint `type` is `"http"` only.

## Logging Configuration

Controls logging output.

```yaml
log:
  level: "info"                        # debug, info, warn, error
  format: "text"                       # text, json
```

### level

Log verbosity.

**Levels** (from most to least verbose):
- `debug`: Everything (connection details, packet traces)
- `info`: Normal operations (connects, subscribes, publishes)
- `warn`: Warnings (retries, non-critical errors)
- `error`: Errors only

**Recommendations**:
- Development: `debug`
- Production: `info` or `warn`
- High-volume: `error`

### format

Log output format.

**Options**:
- `text`: Human-readable
  ```
  time=2025-12-17T18:00:00Z level=INFO msg=v5_connect client_id=client1
  ```

- `json`: Machine-parseable
  ```json
  {"time":"2025-12-17T18:00:00Z","level":"INFO","msg":"v5_connect","client_id":"client1"}
  ```

**Recommendations**:
- Development: `text`
- Production with log aggregation: `json`

## Example Configurations

### Single Node (No Clustering)

```yaml
# examples/no-cluster.yaml
server:
  tcp:
    plain:
      addr: ":1883"
      max_connections: 10000
      read_timeout: "60s"
      write_timeout: "60s"
    tls: {}
    mtls: {}

  websocket:
    plain: {}
    tls: {}
    mtls: {}

  http:
    plain: {}
    tls: {}
    mtls: {}

  coap:
    plain: {}
    dtls: {}
    mdtls: {}

  shutdown_timeout: "30s"

broker:
  max_message_size: 1048576
  max_retained_messages: 10000
  retry_interval: "20s"
  max_retries: 0

session:
  max_sessions: 10000
  default_expiry_interval: 300
  max_offline_queue_size: 1000
  max_inflight_messages: 100

storage:
  type: "badger"
  badger_dir: "/tmp/fluxmq/data"

cluster:
  enabled: false
  node_id: "broker-1"

log:
  level: "info"
  format: "text"
```

### Single Node Cluster (Testing)

```yaml
# examples/single-node-cluster.yaml
server:
  tcp:
    plain:
      addr: ":1883"
      max_connections: 10000
    tls: {}
    mtls: {}

  websocket:
    plain: {}
    tls: {}
    mtls: {}

  http:
    plain: {}
    tls: {}
    mtls: {}

  coap:
    plain: {}
    dtls: {}
    mdtls: {}

storage:
  type: "badger"
  badger_dir: "/tmp/fluxmq/cluster-data"

cluster:
  enabled: true
  node_id: "node1"

  etcd:
    data_dir: "/tmp/fluxmq/cluster-etcd"
    bind_addr: "127.0.0.1:2380"
    client_addr: "127.0.0.1:2379"
    initial_cluster: "node1=http://127.0.0.1:2380"
    bootstrap: true

  transport:
    bind_addr: "127.0.0.1:7948"

log:
  level: "info"
  format: "text"
```

### 3-Node Cluster (Local)

**Node 1**:
```yaml
# examples/node1.yaml
server:
  tcp:
    plain:
      addr: ":1883"
    tls: {}
    mtls: {}

  websocket:
    plain: {}
    tls: {}
    mtls: {}

  http:
    plain: {}
    tls: {}
    mtls: {}

  coap:
    plain: {}
    dtls: {}
    mdtls: {}

storage:
  type: "badger"
  badger_dir: "/tmp/fluxmq/node1/data"

cluster:
  enabled: true
  node_id: "node1"

  etcd:
    data_dir: "/tmp/fluxmq/node1/etcd"
    bind_addr: "127.0.0.1:2380"
    client_addr: "127.0.0.1:2379"
    initial_cluster: "node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2480,node3=http://127.0.0.1:2580"
    bootstrap: true

  transport:
    bind_addr: "127.0.0.1:7948"
    peers:
      node2: "127.0.0.1:7949"
      node3: "127.0.0.1:7950"

log:
  level: "debug"
```

**Node 2**: Same as Node 1, with:
- `tcp.plain.addr: ":1884"`
- `node_id: "node2"`
- `badger_dir: "/tmp/fluxmq/node2/data"`
- `etcd.data_dir: "/tmp/fluxmq/node2/etcd"`
- `etcd.bind_addr: "127.0.0.1:2480"`
- `etcd.client_addr: "127.0.0.1:2479"`
- `transport.bind_addr: "127.0.0.1:7949"`
- Update peers accordingly

**Node 3**: Same pattern, ports 1885, 2580, 2579, 7950

### Production 3-Node Cluster

**Node 1** (192.168.1.10):
```yaml
server:
  tcp:
    plain:
      addr: "0.0.0.0:1883"
    tls: {}
    mtls: {}

  websocket:
    plain: {}
    tls: {}
    mtls: {}

  http:
    plain: {}
    tls: {}
    mtls: {}

  coap:
    plain: {}
    dtls: {}
    mdtls: {}

storage:
  type: "badger"
  badger_dir: "/var/lib/mqtt/data"

cluster:
  enabled: true
  node_id: "node1"

  etcd:
    data_dir: "/var/lib/mqtt/etcd"
    bind_addr: "192.168.1.10:2380"
    client_addr: "127.0.0.1:2379"
    initial_cluster: "node1=http://192.168.1.10:2380,node2=http://192.168.1.11:2380,node3=http://192.168.1.12:2380"
    bootstrap: true

  transport:
    bind_addr: "0.0.0.0:7948"
    peers:
      node2: "192.168.1.11:7948"
      node3: "192.168.1.12:7948"

log:
  level: "warn"
  format: "json"
```

## Best Practices

### Security

1. **Bind addresses**:
   ```yaml
   # Development: localhost only
   server:
     tcp:
       plain:
         addr: "127.0.0.1:1883"

   # Production: specific interface
   server:
     tcp:
       plain:
         addr: "0.0.0.0:1883"  # With firewall

   # Don't expose etcd client port externally
   client_addr: "127.0.0.1:2379"
   ```

2. **File permissions**:
   ```bash
   chmod 600 config.yaml
   chmod 700 /var/lib/mqtt/data
   chmod 700 /var/lib/mqtt/etcd
   ```

### Performance

1. **Connection limits**:
   ```yaml
   server:
     tcp:
       plain:
         max_connections: 50000  # Based on ulimit

   session:
     max_sessions: 50000
   ```

   System:
   ```bash
   # Increase file descriptor limit
   ulimit -n 100000

   # /etc/security/limits.conf
   mqtt soft nofile 100000
   mqtt hard nofile 100000
   ```

2. **Timeouts**:
   ```yaml
   server:
     tcp:
       plain:
         read_timeout: "120s"   # 2x max keep-alive
         write_timeout: "120s"
   ```

3. **Storage**:
   ```yaml
   storage:
     type: "badger"
     badger_dir: "/mnt/ssd/mqtt/data"  # SSD for best performance
   ```

### High Availability

1. **Cluster size**: Use odd numbers (3, 5, 7) for proper quorum
   - 3 nodes: Tolerates 1 failure
   - 5 nodes: Tolerates 2 failures
   - 7 nodes: Tolerates 3 failures

2. **Data directories**: Use separate disks for etcd and BadgerDB
   ```yaml
   storage:
     badger_dir: "/mnt/data/mqtt/badger"

   cluster:
     etcd:
       data_dir: "/mnt/ssd/mqtt/etcd"  # SSD for etcd
   ```

3. **Backups**: Backup etcd data and BadgerDB regularly
   ```bash
   # etcd backup (via etcdctl)
   etcdctl snapshot save /backup/mqtt-etcd-$(date +%Y%m%d).db

   # BadgerDB backup
   tar czf /backup/mqtt-badger-$(date +%Y%m%d).tar.gz /var/lib/mqtt/data
   ```

### Monitoring

1. **Logging**:
   ```yaml
   log:
     level: "info"
     format: "json"  # For log aggregation
   ```

2. **Metrics** (future):
   - Prometheus exporter
   - Connection count
   - Message throughput
   - QoS retry count

### Resource Planning

**CPU**:
- Single node: 2-4 cores
- Cluster node: 4-8 cores (etcd + broker + gRPC)

**Memory**:
- Base: 512MB - 1GB
- Per 1000 sessions: ~10-100MB
- Per 10000 retained: ~10-100MB

**Disk**:
- etcd: 10-100GB (slow growth)
- BadgerDB: Based on message volume
- I/O: SSD recommended for production

**Network**:
- Client connections: Based on concurrent clients
- Cluster: Low latency preferred (<10ms)
- Bandwidth: Based on message throughput

## Summary

Key configuration areas:

**Server**: Transport layer (TCP, WS, HTTP, CoAP)
**Broker**: Message handling behavior
**Session**: Session lifecycle and queuing
**Storage**: Persistence backend (memory, BadgerDB)
**Cluster**: Distributed coordination (etcd, gRPC)
**Log**: Logging output

For more details, see:
- [Clustering](clustering.md) - Distributed broker design, etcd, gRPC, BadgerDB
- [Broker & Message Routing](broker.md) - Message routing internals
- [Architecture](architecture.md) - Detailed system design
