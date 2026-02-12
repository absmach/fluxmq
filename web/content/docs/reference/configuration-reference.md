---
title: Configuration Reference
description: Comprehensive YAML configuration reference for server, broker, storage, clustering, and operational settings
---

# Configuration Reference

**Last Updated:** 2026-02-07

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
    tls: {}
    mtls: {}

  amqp091:
    plain:
      addr: ":5682"
    tls: {}
    mtls: {}

  health_enabled: true
  health_addr: ":8081"

  metrics_enabled: false
  metrics_addr: "localhost:4317"   # OTLP endpoint

  otel_service_name: "fluxmq"
  otel_service_version: "dev"
  otel_metrics_enabled: true
  otel_traces_enabled: false
  otel_trace_sample_rate: 0.1

  api_enabled: false
  api_addr: ":9090"                 # Queue API (Connect/gRPC)

  shutdown_timeout: "30s"
```

### TLS/DTLS Settings

TLS fields are shared across `tls`, `mtls`, `dtls`, and `mdtls` blocks via `pkg/tls` config:

- `cert_file`, `key_file`
- `ca_file` (client CA) and `server_ca_file`
- `client_auth` (e.g., `require`, `verify_if_given`)
- `min_version`, `cipher_suites`, `prefer_server_cipher_suites`
- `ocsp`, `crl` (advanced verification)

## Broker

```yaml
broker:
  max_message_size: 1048576
  max_retained_messages: 10000
  retry_interval: "20s"
  max_retries: 0
  max_qos: 2
```

## Session

```yaml
session:
  max_sessions: 10000
  default_expiry_interval: 300
  max_offline_queue_size: 1000
  max_inflight_messages: 100
  offline_queue_policy: "evict"   # evict or reject
```

## Queue Manager

```yaml
queue_manager:
  auto_commit_interval: "5s"      # Stream groups auto-commit cadence
```

## Queues

Queue configuration controls durable queues and stream queues.

```yaml
queues:
  - name: "mqtt"
    topics: ["$queue/#"]
    reserved: true
    type: "classic"               # classic or stream
    primary_group: ""             # stream status reporting

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

    retention:
      max_age: "168h"
      max_length_bytes: 0
      max_length_messages: 0
```

## Storage

```yaml
storage:
  type: "badger"      # memory or badger
  badger_dir: "/tmp/fluxmq/data"
  sync_writes: false
```

## Cluster

Clustering combines:

- **Embedded etcd** (`cluster.etcd`): metadata coordination (session ownership, subscriptions, queue consumers, retained/will metadata).
- **gRPC transport** (`cluster.transport`): cross-node routing (publishes, queue messages, session takeover, hybrid payload fetch), including delivery to local MQTT, AMQP 1.0, and AMQP 0.9.1 clients.
- **Optional Raft** (`cluster.raft`): replicates durable queue operations.

For a “how it works” deep dive, see [Clustering internals](/docs/architecture/clustering).

```yaml
cluster:
  enabled: false
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
    tls_enabled: false
    tls_cert_file: ""
    tls_key_file: ""
    tls_ca_file: ""

  raft:
    enabled: false
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
```

### Raft Behavior (What The Knobs Mean)

Two Raft fields control most real-world behavior for queues:

- `cluster.raft.write_policy`: follower behavior when receiving a queue publish (`forward` is usually the best default).
- `cluster.raft.distribution_mode`: cross-node delivery strategy (`forward` routes deliveries; `replicate` relies on the replicated log).

Other fields affect durability and timing:

- `sync_mode`: if true, a queue publish waits for the Raft apply to complete (bounded by `ack_timeout`).
- `ack_timeout`: how long the leader waits for an apply to finish in sync mode.
- `heartbeat_timeout`, `election_timeout`: Raft stability knobs (failover sensitivity vs churn).
- `snapshot_interval`, `snapshot_threshold`: storage/compaction knobs for the Raft log.

Notes on current implementation:

- FluxMQ supports multiple Raft replication groups. Queues can be assigned to a group via `queues[].replication.group` (empty means the default group).
- Group membership still comes from the configured peer list(s). `replication_factor` and `min_in_sync_replicas` are accepted in config but do not currently limit membership or override Raft quorum rules.

### Raft Groups (Per-Queue Sharding)

Use `cluster.raft.groups` to configure multiple independent replication groups.

Key rules:

- A `default` group is always required when Raft is enabled.
- Non-default groups must define their own `bind_addr` and `peers` (and typically a dedicated `data_dir`).
- If `cluster.raft.auto_provision_groups` is `true`, groups referenced by queues can be created on demand using derived defaults.

## Webhooks

```yaml
webhook:
  enabled: false
  queue_size: 10000
  drop_policy: "oldest"          # oldest or newest
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

Only `http` endpoints are supported at the moment.

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

## Logging

```yaml
log:
  level: "info"   # debug, info, warn, error
  format: "text"  # text or json
```
