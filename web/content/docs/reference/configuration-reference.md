---
title: Configuration Reference
description: Comprehensive YAML configuration reference for server, broker, storage, clustering, and operational settings
---

# Configuration Reference

**Last Updated:** 2026-02-18

FluxMQ uses a single YAML configuration file. Start the broker with:

```bash
./build/fluxmq --config /path/to/config.yaml
```

If `--config` is omitted, defaults are used (see `config.Default()` in `config/config.go`).

Looking for a guided walkthrough? See:

- [Server configuration](/docs/configuration/server)
- [Storage configuration](/docs/configuration/storage)
- [Cluster configuration](/docs/configuration/clustering)
- [Security configuration](/docs/configuration/security)

## Configuration Overview

Top-level keys:

- `server`
- `broker`
- `session`
- `queue_manager`
- `queues`
- `storage`
- `cluster`
- `webhook`
- `ratelimit`
- `log`

Durations use Go duration strings like `5s`, `1m`, `24h`.

## Server

`server` controls network listeners and telemetry endpoints.

```yaml
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
    plain:
      addr: ":8083"
      path: "/mqtt"
      allowed_origins: ["https://app.example.com"]
    tls: {}
    mtls: {}

  http:
    plain:
      addr: ":8080"
    tls: {}
    mtls: {}

  coap:
    plain:
      addr: ":5683"
    dtls: {}
    mdtls: {}

  amqp:
    plain:
      addr: ":5672"
      max_connections: 10000
    tls: {}
    mtls: {}

  amqp091:
    plain:
      addr: ":5682"
      max_connections: 10000
    tls: {}
    mtls: {}

  health_enabled: true
  health_addr: ":8081"

  metrics_enabled: false
  metrics_addr: "localhost:4317" # OTLP endpoint

  otel_service_name: "fluxmq"
  otel_service_version: "1.0.0"
  otel_metrics_enabled: true
  otel_traces_enabled: false
  otel_trace_sample_rate: 0.1

  api_enabled: false
  api_addr: "" # Queue API (Connect/gRPC)

  shutdown_timeout: "30s"
```

### Listener Fields

These apply to listener blocks (for example `server.tcp.plain`, `server.amqp091.tls`, and so on).

| Field | Description |
|---|---|
| `addr` | Listener bind address (`"<host>:<port>"` or `":<port>"`). Empty string disables that listener. |
| `max_connections` | Connection cap for that listener (`>= 0`). `0` means no explicit cap. Applies to TCP/AMQP/AMQP091 listeners. |
| `read_timeout` | Read timeout for TCP listeners (`time.Duration`). |
| `write_timeout` | Write timeout for TCP listeners (`time.Duration`). |
| `path` | HTTP path for MQTT-over-WebSocket endpoint. |
| `allowed_origins` | WebSocket origin allow-list. Empty list allows all origins; use explicit origins for production. |

### Server Runtime / Telemetry Fields

| Field | Default | Description |
|---|---|---|
| `health_enabled` | `true` | Enables `/health` endpoint. |
| `health_addr` | `:8081` | Health endpoint bind address. |
| `metrics_enabled` | `false` | Enables OpenTelemetry exporters. |
| `metrics_addr` | `localhost:4317` | OTLP endpoint address (collector target). |
| `otel_service_name` | `fluxmq` | Telemetry service name. |
| `otel_service_version` | `1.0.0` | Telemetry service version tag. |
| `otel_metrics_enabled` | `true` | Enables OTel metrics export. |
| `otel_traces_enabled` | `false` | Enables OTel traces export. |
| `otel_trace_sample_rate` | `0.1` | Trace sampling ratio in `[0.0, 1.0]`. |
| `api_enabled` | `false` | Enables queue API server (Connect/gRPC). |
| `api_addr` | `""` | API bind address when `api_enabled=true`. |
| `shutdown_timeout` | `30s` | Graceful server shutdown timeout. |

### TLS / DTLS Settings

TLS fields are shared across `tls`, `mtls`, `dtls`, and `mdtls` blocks via `pkg/tls` config:

- `cert_file`, `key_file`
- `ca_file` (client CA), `server_ca_file`
- `client_auth` (`none`, `request`, `require_any`, `verify_if_given`, `require`)
- `min_version` (`tls1.0`, `tls1.1`, `tls1.2`, `tls1.3`)
- `cipher_suites`, `prefer_server_cipher_suites`
- `ocsp`, `crl` (advanced verification)

## Broker

```yaml
broker:
  max_message_size: 1048576
  max_retained_messages: 10000
  retry_interval: "20s"
  max_retries: 0
  max_qos: 2
  async_fan_out: false    # true = send PUBCOMP immediately, fan-out in worker pool
  fan_out_workers: 0      # worker pool size; 0 = GOMAXPROCS
```

| Field | Default | Description |
|---|---|---|
| `max_message_size` | `1048576` | Maximum PUBLISH payload size in bytes (`>= 1024`). |
| `max_retained_messages` | `10000` | Cap on retained messages in the store. |
| `retry_interval` | `20s` | QoS 1/2 retry interval for unacknowledged outbound messages (`>= 1s`). |
| `max_retries` | `0` | Maximum retries before dropping; `0` = unlimited. |
| `max_qos` | `2` | Maximum QoS accepted from publishers (`0`, `1`, or `2`). |
| `async_fan_out` | `false` | When `true`, sends PUBCOMP immediately after PUBREL and dispatches fan-out to a worker pool. |
| `fan_out_workers` | `0` | Async fan-out worker count; `0` = `GOMAXPROCS`. |

### Fan-out Modes

- `async_fan_out: false` (default): publisher acknowledgment and subscriber fan-out stay coupled.
- `async_fan_out: true`: publisher PUBCOMP is sent earlier; subscriber fan-out runs in background workers.
- `fan_out_workers`: tune worker pool size for high fan-out workloads.

## Session

```yaml
session:
  max_sessions: 10000
  default_expiry_interval: 300
  max_offline_queue_size: 1000
  max_inflight_messages: 256
  max_send_queue_size: 0         # 0 = synchronous writes, >0 = async buffered sends
  disconnect_on_full: false      # when async queue is full: false=block, true=disconnect client
  offline_queue_policy: "evict" # evict or reject
  inflight_overflow: 0           # 0 = backpressure, 1 = pending queue
  pending_queue_size: 1000       # per-subscriber buffer depth when inflight_overflow=1
```

| Field | Default | Description |
|---|---|---|
| `max_sessions` | `10000` | Maximum concurrent sessions (`>= 1`). |
| `default_expiry_interval` | `300` | Session expiry interval in seconds when client does not set one. |
| `max_offline_queue_size` | `1000` | Maximum QoS 1/2 messages buffered for a disconnected client (`>= 10`). |
| `max_inflight_messages` | `256` | Per-session inflight window size (unacknowledged outbound messages). |
| `max_send_queue_size` | `0` | Per-connection async send queue depth. `0` = synchronous writes. |
| `disconnect_on_full` | `false` | Async send queue full behavior: `false` = block/backpressure, `true` = disconnect client. |
| `offline_queue_policy` | `evict` | `evict` drops oldest when full; `reject` drops newest incoming message. |
| `inflight_overflow` | `0` | Inflight full behavior: `0` = backpressure; `1` = per-subscriber pending queue. |
| `pending_queue_size` | `1000` | Pending queue depth when `inflight_overflow=1` (must be `>= 1`). |

### Inflight Overflow

- `inflight_overflow: 0` (backpressure): delivery waits for ACK window to free.
- `inflight_overflow: 1` (pending queue): overflow is buffered per subscriber and drained as ACKs arrive.

## Queue Manager

```yaml
queue_manager:
  auto_commit_interval: "5s"
```

| Field | Default | Description |
|---|---|---|
| `auto_commit_interval` | `5s` | Stream-group auto-commit cadence. `0` means commit on every delivery batch. |

## Queues

Queue configuration controls durable queues and stream queues.

```yaml
queues:
  - name: "mqtt"
    topics: ["$queue/#"]
    reserved: true
    type: "classic"               # classic or stream
    primary_group: ""             # stream status reporting

    retention:
      max_age: "0s"               # 0 = unlimited
      max_length_bytes: 0          # 0 = unlimited
      max_length_messages: 0       # 0 = unlimited

    limits:
      max_message_size: 10485760
      max_depth: 100000
      message_ttl: "168h"

    retry:
      max_retries: 10
      initial_backoff: "5s"
      max_backoff: "5m"
      multiplier: 2.0

    dlq:
      enabled: true
      topic: ""                    # optional override

    replication:
      enabled: false
      group: ""
      replication_factor: 3
      mode: "sync"                 # sync or async
      min_in_sync_replicas: 2
      ack_timeout: "5s"
      heartbeat_timeout: "0s"      # 0 = inherit group/default
      election_timeout: "0s"       # 0 = inherit group/default
      snapshot_interval: "0s"      # 0 = inherit group/default
      snapshot_threshold: 0         # 0 = inherit group/default
```

### Queue Fields

| Field | Description |
|---|---|
| `name` | Unique queue name. |
| `topics` | Topic filters routed into this queue (must be non-empty). |
| `reserved` | Marks system-managed/builtin queue definitions. |
| `type` | Queue mode: `classic` or `stream`. Empty value falls back to default mode. |
| `primary_group` | For stream queues: consumer group used for status reporting. |

### `queues[].retention`

| Field | Description |
|---|---|
| `max_age` | Time retention limit. `0s` disables age-based retention. |
| `max_length_bytes` | Byte-size retention cap. `0` means unlimited. |
| `max_length_messages` | Message-count retention cap. `0` means unlimited. |

### `queues[].limits`

| Field | Description |
|---|---|
| `max_message_size` | Queue-level max payload size in bytes. |
| `max_depth` | Max queued message count. |
| `message_ttl` | Per-message TTL in queue. |

### `queues[].retry`

| Field | Description |
|---|---|
| `max_retries` | Max delivery retries per message (`>= 0`). |
| `initial_backoff` | Initial retry delay. |
| `max_backoff` | Maximum retry delay. |
| `multiplier` | Exponential backoff multiplier (`>= 1.0`). |

### `queues[].dlq`

| Field | Description |
|---|---|
| `enabled` | Enables dead-letter queue routing for exhausted messages. |
| `topic` | Optional DLQ topic override; empty uses generated/default topic. |

### `queues[].replication`

| Field | Description |
|---|---|
| `enabled` | Enables per-queue Raft replication. |
| `group` | Raft group ID for this queue. Empty means `default`. |
| `replication_factor` | Number of replicas (`1..10` when enabled). |
| `mode` | `sync` or `async`. |
| `min_in_sync_replicas` | Minimum replicas required to ACK (`1..replication_factor`). |
| `ack_timeout` | Timeout for sync replication acknowledgments (`> 0`). |
| `heartbeat_timeout` | Optional per-queue heartbeat override. `0` inherits cluster/group value. |
| `election_timeout` | Optional per-queue election timeout override. `0` inherits cluster/group value. |
| `snapshot_interval` | Optional per-queue snapshot interval override. `0` inherits cluster/group value. |
| `snapshot_threshold` | Optional per-queue snapshot threshold override. `0` inherits cluster/group value. |

## Storage

```yaml
storage:
  type: "badger"      # memory or badger
  badger_dir: "/tmp/fluxmq/data"
  sync_writes: false
```

| Field | Default | Description |
|---|---|---|
| `type` | `badger` | Storage backend: `memory` or `badger`. |
| `badger_dir` | `/tmp/fluxmq/data` | Data directory for Badger backend (required when `type=badger`). |
| `sync_writes` | `false` | If `true`, fsync-like durability on write path; if `false`, better throughput. |

## Cluster

Clustering combines:

- **Embedded etcd** (`cluster.etcd`): metadata coordination (session ownership, subscriptions, queue consumers, retained/will metadata).
- **gRPC transport** (`cluster.transport`): cross-node routing (publishes, queue messages, session takeover, hybrid payload fetch), including delivery to local MQTT, AMQP 1.0, and AMQP 0.9.1 clients.
- **Optional Raft** (`cluster.raft`): replicates durable queue operations.

For a “how it works” deep dive, see [Clustering internals](/docs/architecture/clustering).

```yaml
cluster:
  enabled: true
  node_id: "broker-1"

  etcd:
    data_dir: "/tmp/fluxmq/etcd"
    bind_addr: "0.0.0.0:2380"
    client_addr: "0.0.0.0:2379"
    initial_cluster: "broker-1=http://0.0.0.0:2380"
    bootstrap: true
    hybrid_retained_size_threshold: 1024

  transport:
    bind_addr: "0.0.0.0:7948"
    peers: {}
    route_batch_max_size: 256
    route_batch_max_delay: "5ms"
    route_batch_flush_workers: 4
    route_publish_timeout: "15s"
    tls_enabled: false
    tls_cert_file: ""
    tls_key_file: ""
    tls_ca_file: ""

  raft:
    enabled: false
    auto_provision_groups: true
    replication_factor: 3
    sync_mode: true
    min_in_sync_replicas: 2
    ack_timeout: "5s"
    write_policy: "forward"        # local, reject, forward
    distribution_mode: "replicate" # forward, replicate
    bind_addr: "127.0.0.1:7100"
    data_dir: "/tmp/fluxmq/raft"
    peers: {}
    heartbeat_timeout: "1s"
    election_timeout: "3s"
    snapshot_interval: "5m"
    snapshot_threshold: 8192

    groups:
      default:
        bind_addr: "127.0.0.1:7100"
        data_dir: "/tmp/fluxmq/raft"
        peers: {}
      hot:
        bind_addr: "127.0.0.1:7200"
        data_dir: "/tmp/fluxmq/raft/groups/hot"
        peers: {}
```

### Cluster Root Fields

| Field | Default | Description |
|---|---|---|
| `enabled` | `true` | Enables clustering features. Use `false` for standalone deployments. |
| `node_id` | `broker-1` | Unique node identifier in the cluster. |

### `cluster.etcd`

| Field | Description |
|---|---|
| `data_dir` | Local etcd data directory. |
| `bind_addr` | etcd peer address (`:2380`) for member replication. |
| `client_addr` | etcd client address (`:2379`) used by broker components. |
| `initial_cluster` | Comma-separated cluster map (`name=http://host:2380,...`). |
| `bootstrap` | `true` when bootstrapping new cluster; `false` when joining existing cluster. |
| `hybrid_retained_size_threshold` | Payload size threshold for retained/will hybrid storage strategy. |

### `cluster.transport`

| Field | Default | Description |
|---|---|---|
| `bind_addr` | `0.0.0.0:7948` | Inter-node gRPC transport bind address. |
| `peers` | `{}` | Map of `node_id -> transport address`. |
| `route_batch_max_size` | `256` | Flush batch after this many queued messages (`>= 0`). |
| `route_batch_max_delay` | `5ms` | Flush partial batch after this delay (`>= 0`). |
| `route_batch_flush_workers` | `4` | Concurrent flush workers per remote node (`>= 0`). |
| `route_publish_timeout` | `15s` | Max time for cross-node publish operation (`0` uses default). |
| `tls_enabled` | `false` | Enables mTLS/TLS for transport gRPC channel. |
| `tls_cert_file` | `""` | Required when `tls_enabled=true`. |
| `tls_key_file` | `""` | Required when `tls_enabled=true`. |
| `tls_ca_file` | `""` | Required when `tls_enabled=true`. |

### `cluster.raft`

| Field | Default | Description |
|---|---|---|
| `enabled` | `false` | Enables queue Raft replication engine. |
| `auto_provision_groups` | `true` | Allows dynamic creation of queue-referenced groups not listed under `groups`. |
| `replication_factor` | `3` | Target replica count (`1..10` when enabled). |
| `sync_mode` | `true` | `true` waits for apply/commit path; `false` returns earlier (async path). |
| `min_in_sync_replicas` | `2` | Minimum in-sync replicas required for sync behavior. |
| `ack_timeout` | `5s` | Timeout for sync commit/apply acknowledgments. |
| `write_policy` | `forward` | Follower write behavior: `local`, `reject`, `forward`. |
| `distribution_mode` | `replicate` | Cross-node delivery strategy: `forward` or `replicate`. |
| `bind_addr` | `127.0.0.1:7100` | Base Raft bind address for default group runtime. |
| `data_dir` | `/tmp/fluxmq/raft` | Base Raft data directory. |
| `peers` | `{}` | Map of `node_id -> raft address`. |
| `heartbeat_timeout` | `1s` | Raft heartbeat interval/tick. |
| `election_timeout` | `3s` | Raft election timeout. |
| `snapshot_interval` | `5m` | Snapshot interval. |
| `snapshot_threshold` | `8192` | Snapshot threshold in log entries. |
| `groups` | `{}` | Optional per-group overrides (`default`, `hot`, etc.). |

### `cluster.raft.groups.<group_id>`

| Field | Description |
|---|---|
| `enabled` | Optional group enable switch. If omitted, inherits enabled behavior. |
| `bind_addr` | Group-specific Raft bind address (required for non-default groups). |
| `data_dir` | Group-specific data directory. |
| `peers` | Group-specific peer map (`node_id -> raft address`; required for non-default groups). |
| `replication_factor` | Optional override for this group. `0` inherits base value. |
| `sync_mode` | Optional per-group sync-mode override. |
| `min_in_sync_replicas` | Optional per-group ISR override. `0` inherits base value. |
| `ack_timeout` | Optional per-group ack timeout override. `0` inherits base value. |
| `heartbeat_timeout` | Optional per-group heartbeat override. `0` inherits base value. |
| `election_timeout` | Optional per-group election timeout override. `0` inherits base value. |
| `snapshot_interval` | Optional per-group snapshot interval override. `0` inherits base value. |
| `snapshot_threshold` | Optional per-group snapshot threshold override. `0` inherits base value. |

### Transport Batching

The gRPC transport batches outbound messages per remote node before flushing them over the wire.

| Setting | Default | Description |
|---|---|---|
| `route_batch_max_size` | `256` | Maximum number of messages collected before flush. |
| `route_batch_max_delay` | `5ms` | Maximum wait before flushing partial batch. |
| `route_batch_flush_workers` | `4` | Concurrent flush goroutines per remote node. |
| `route_publish_timeout` | `15s` | Maximum time for cross-cluster publish completion. |

### Raft Behavior (What The Knobs Mean)

Two fields control most queue behavior tradeoffs:

- `cluster.raft.write_policy`: behavior on follower writes.
- `cluster.raft.distribution_mode`: how deliveries are routed across nodes.

Other durability and timing fields:

- `sync_mode`, `ack_timeout`
- `heartbeat_timeout`, `election_timeout`
- `snapshot_interval`, `snapshot_threshold`

Implementation notes:

- FluxMQ supports multiple Raft replication groups; queues choose group via `queues[].replication.group`.
- Group membership comes from peer configuration. `replication_factor` and `min_in_sync_replicas` are validated and used by policy logic, but do not replace Raft quorum mechanics.

## Webhooks

```yaml
webhook:
  enabled: false
  queue_size: 10000
  drop_policy: "oldest" # oldest or newest
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
    - name: "analytics"
      type: "http"
      url: "https://example.com/webhook"
      events: ["message.published"]
      topic_filters: ["sensors/#"]
      headers:
        Authorization: "Bearer token"
      timeout: "10s"
```

Only `http` endpoints are currently supported.

### Webhook Fields

| Field | Default | Description |
|---|---|---|
| `enabled` | `false` | Enables webhook event delivery. |
| `queue_size` | `10000` | In-memory webhook queue depth (`>= 100` when enabled). |
| `drop_policy` | `oldest` | Queue full behavior: `oldest` or `newest`. |
| `workers` | `5` | Concurrent webhook workers (`>= 1`). |
| `include_payload` | `false` | Includes message payload in webhook body. |
| `shutdown_timeout` | `30s` | Graceful drain timeout during shutdown. |
| `defaults` | — | Default delivery settings applied to endpoints. |
| `endpoints` | `[]` | List of webhook endpoint configs. |

### `webhook.defaults`

| Field | Description |
|---|---|
| `timeout` | Default endpoint timeout. |
| `retry` | Retry policy defaults. |
| `circuit_breaker` | Circuit breaker defaults. |

### `webhook.defaults.retry`

| Field | Description |
|---|---|
| `max_attempts` | Max delivery attempts (`>= 1`). |
| `initial_interval` | Initial retry delay. |
| `max_interval` | Max retry delay. |
| `multiplier` | Exponential backoff multiplier (`>= 1.0`). |

### `webhook.defaults.circuit_breaker`

| Field | Description |
|---|---|
| `failure_threshold` | Failures before opening breaker (`>= 1`). |
| `reset_timeout` | Time before half-open probe/reset. |

### `webhook.endpoints[]`

| Field | Description |
|---|---|
| `name` | Unique endpoint identifier. |
| `type` | Endpoint type. Currently only `http` is supported. |
| `url` | Target endpoint URL. |
| `events` | Event-type filter. Empty means all events. |
| `topic_filters` | Topic filter list for message events. Empty means all topics. |
| `headers` | Static headers attached to webhook requests. |
| `timeout` | Optional endpoint-specific timeout override. |
| `retry` | Optional endpoint-specific retry override. |

## Rate Limiting

```yaml
ratelimit:
  enabled: false

  connection:
    enabled: true
    rate: 1.6667           # connections per second per IP
    burst: 20
    cleanup_interval: "5m"

  message:
    enabled: true
    rate: 1000             # messages per second per client
    burst: 100

  subscribe:
    enabled: true
    rate: 100              # subscriptions per second per client
    burst: 10
```

| Field | Description |
|---|---|
| `enabled` | Global rate-limit feature switch. |
| `connection.enabled` | Enables per-IP connection rate limiting. |
| `connection.rate` | Allowed connection attempts per second per IP. |
| `connection.burst` | Token-bucket burst allowance for connection limiter. |
| `connection.cleanup_interval` | Cleanup interval for stale connection limiter entries. |
| `message.enabled` | Enables per-client publish/message limiter. |
| `message.rate` | Allowed messages per second per client. |
| `message.burst` | Token-bucket burst for message limiter. |
| `subscribe.enabled` | Enables per-client subscribe limiter. |
| `subscribe.rate` | Allowed subscribe operations per second per client. |
| `subscribe.burst` | Token-bucket burst for subscribe limiter. |

## Logging

```yaml
log:
  level: "info"   # debug, info, warn, error
  format: "text"  # text or json
```

| Field | Default | Description |
|---|---|---|
| `level` | `info` | Log level: `debug`, `info`, `warn`, `error`. |
| `format` | `text` | Log format: `text` or `json`. |
