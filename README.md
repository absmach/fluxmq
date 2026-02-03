# FluxMQ

[![Go Report Card](https://goreportcard.com/badge/github.com/absmach/fluxmq)](https://goreportcard.com/report/github.com/absmach/fluxmq)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A high-performance, multi-protocol message broker written in Go designed for scalability, extensibility, and protocol diversity. Supports MQTT 3.1.1 and 5.0 over TCP and WebSocket, plus HTTP-MQTT and CoAP bridges for IoT integration.

## Who Is This For

### ✅ Ideal Use Cases

**Event-Driven Architectures**
- **Event backbone for microservices** - Reliable, ordered event distribution between services with at-least-once or exactly-once delivery (QoS 1/2)
- **CQRS systems** - Durable queues for command/event distribution with partition-based ordering per aggregate
- **Asynchronous workflows** - Decouple services with persistent message queues and automatic retries
- **Real-time event processing** - High throughput (300K-500K msg/s per node) with low latency (<10ms local, ~5ms cross-node)

**Why choose this over Kafka for EDA:**
- ✅ Simpler operations - single binary with embedded storage, no Zookeeper/KRaft
- ✅ Multi-protocol - same broker handles MQTT, HTTP, WebSocket, CoAP
- ✅ Partition-based ordering with sequence numbers (perfect for aggregate-based event streams)
- ✅ Configurable retention (hours to days) for event replay during deployments/failures
- ✅ Raft replication with quorum writes ensures no lost events

**IoT & Real-Time Systems**
- **Device communication** - MQTT 3.1.1/5.0 with QoS levels for reliable delivery over unreliable networks
- **Edge computing** - Embedded deployment with low resource footprint
- **Browser clients** - WebSocket transport for real-time web applications
- **Constrained devices** - CoAP bridge for resource-limited IoT hardware

**High-Availability Systems**
- **Clustered deployments** - 3-5 node clusters with automatic failover (sub-100ms session takeover)
- **Geographic distribution** - gRPC-based cross-node routing with embedded etcd coordination
- **Scalability** - Linear scaling (3-node cluster: 1-2M msg/s, 5-node cluster: 2-4M msg/s)

### ⚠️ Not Recommended For

**Long-term Event Storage**
- ❌ Event sourcing as permanent source of truth - storage uses LSM-tree (compaction/deletion allowed)
- ❌ Compliance/audit trails requiring immutability - use purpose-built event stores (EventStoreDB)
- ❌ Time-travel debugging or temporal queries - no time-range indexing

**Complex Event Processing**
- ❌ Advanced queries over events - no indexing beyond partition+sequence
- ❌ Built-in stream processing - no Kafka Streams equivalent (process events in consumers)

**Large Payloads**
- ❌ Multi-megabyte messages - 1MB default limit (configurable, but storage optimized for smaller messages)

### Event-Driven Architecture Pattern

```
┌─────────────┐         ┌──────────────────┐         ┌─────────────┐
│  Service A  │────────>│   MQTT Broker    │────────>│  Service B  │
│ (Producer)  │  events │  (Event Bus)     │ events  │ (Consumer)  │
└─────────────┘         │                  │         └─────────────┘
      │                 │  • Retention: 7d │               │
      │                 │  • Replication:3x│               │
      ▼                 │  • Ordering: Yes │               ▼
┌─────────────┐         └──────────────────┘         ┌─────────────┐
│  Database   │                                      │  Database   │
│  (State)    │         Broker = Durable Pipe        │  (State)    │
└─────────────┘         Database = Source of Truth   └─────────────┘
```

**Recommended configuration for EDA:**
```yaml
queue:
  ordering: partition              # FIFO per aggregate/entity
  partitions: 50-100               # Balance parallelism vs overhead
  retention:
    retention_time: 168h           # 7 days for replay
  replication:
    enabled: true
    replication_factor: 3          # Survive node failures
    mode: sync                     # Don't lose events
    min_in_sync_replicas: 2        # Quorum writes
```

## Features

- **Multi-Protocol Support**
  - **MQTT 3.1.1** - Full support over TCP and WebSocket
  - **MQTT 5.0** - Full support over TCP and WebSocket
  - **HTTP-MQTT Bridge** - RESTful API for publishing messages
  - **WebSocket Transport** - MQTT over WebSocket for browser clients
  - **CoAP Bridge** - UDP and DTLS (mDTLS) support for constrained IoT devices
  - All protocols share the same broker core - messages flow seamlessly across protocols

- **Performance Optimized**
  - Zero-copy packet parsing
  - Object pooling for reduced GC pressure
  - Efficient trie-based topic matching
  - Direct instrumentation (no middleware overhead)
  - Concurrent connection handling
  - 3.3x throughput improvement via buffer pooling

- **Full MQTT Feature Set**
  - QoS 0, 1, and 2 message delivery
  - Retained messages
  - Will messages
  - Session expiry
  - Topic wildcards (`+` and `#`)
  - Session persistence
  - Keep-alive management
  - Shared subscriptions (MQTT 5.0)
  - MaxQoS enforcement (MQTT 5.0)

- **Security**
  - TLS/mTLS for MQTT client connections
  - mTLS for inter-broker gRPC communication
  - DTLS/mDTLS for CoAP connections
  - WebSocket origin validation
  - Rate limiting (per-IP connections, per-client messages/subscriptions)

- **Clustering & High Availability**
  - Embedded etcd for distributed coordination
  - gRPC-based inter-broker communication with mTLS
  - Automatic session ownership management
  - Cross-node message routing
  - Persistent storage with BadgerDB
  - Graceful shutdown with session transfer
  - No external dependencies - all embedded in single binary

- **Durable Queues**
  - Persistent message queues with consumer groups
  - Ack/Nack/Reject message acknowledgment
  - Partitioning with ordered delivery
  - Dead-letter queue support
  - Raft-based replication (3x, automatic failover)
  - Kafka-style retention (time, size, log compaction)

- **Persistent Storage**
  - BadgerDB for session state and offline queues
  - Hybrid storage for retained messages
  - Pluggable storage backends (memory, BadgerDB)

- **Extensible Architecture**
  - Clean layered design: Transport → Protocol → Domain
  - Protocol-agnostic domain logic
  - Easy to add new protocols and transports
  - Dependency injection for logging and metrics

## Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ TCP Server  │  │ WebSocket   │  │ HTTP Bridge │  │ CoAP Bridge │
│   :1883     │  │   :8083     │  │   :8080     │  │   :5683     │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       └────────────────┴────────────────┴────────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │     Protocol Detection        │
                  └───────────────┬───────────────┘
                                  │
                  ┌───────────────┴───────────────┐
                  │                               │
           ┌──────▼──────┐                 ┌──────▼──────┐
           │ V3 Handler  │                 │ V5 Handler  │
           │ (MQTT 3.1.1)│                 │ (MQTT 5.0)  │
           └──────┬──────┘                 └──────┬──────┘
                  └───────────────┬───────────────┘
                                  ▼
    ┌─────────────────────────────────────────────────────────────┐
    │                     Domain Layer                            │
    │                                                             │
    │  Sessions  │  Router (Trie)  │  Pub/Sub  │  Durable Queues  │
    │                                                             │
    │  Built-in: Logging (slog) • Metrics • Instrumentation       │
    └──────────────────────────────┬──────────────────────────────┘
                                   │
    ┌──────────────────────────────┴──────────────────────────────┐
    │                    Infrastructure                           │
    │                                                             │
    │ ┌──────────┐  ┌───────────┐  ┌───────────┐  ┌────────────┐  │
    │ │ Storage  │  │ Cluster   │  │ Session   │  │   Queue    │  │
    │ │ BadgerDB │  │ etcd+gRPC │  │ Cache     │  │  Storage   │  │
    │ └──────────┘  └───────────┘  └───────────┘  └────────────┘  │
    └─────────────────────────────────────────────────────────────┘
```

All protocols share the same broker core - messages flow seamlessly across protocols.

## Quick Start

### Prerequisites

- Go 1.24 or later

### Build & Run

```bash
# Clone and build
git clone https://github.com/absmach/fluxmq.git
cd fluxmq
make build

# Run single node
./build/fluxmq

# Run with configuration
./build/fluxmq --config config.yaml

# Run 3-node cluster
make run-node1  # Terminal 1
make run-node2  # Terminal 2
make run-node3  # Terminal 3
```

### Test

```bash
# Subscribe on one node
mosquitto_sub -p 1884 -t "test/#" -v

# Publish on another node
mosquitto_pub -p 1885 -t "test/hello" -m "Cross-node message"
```

## Configuration

```yaml
server:
  tcp:
    plain:
      addr: ":1883"
      max_connections: 10000
      read_timeout: "60s"
      write_timeout: "60s"
  websocket:
    plain:
      addr: ":8083"
      path: "/mqtt"
  http:
    plain:
      addr: ":8080"

broker:
  max_message_size: 1048576
  max_retained_messages: 10000

storage:
  type: badger
  path: "./data"

log:
  level: info
```

TLS/mTLS listeners use inline certificate fields under the listener mode:

```yaml
server:
  tcp:
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
```

`client_auth` supports `none`, `request`, `require_any`, `verify_if_given`, or `require` (alias for require-and-verify).
If `ca_file` is set and `client_auth` is empty, the server defaults to `require`.
`min_version` and `prefer_server_cipher_suites` apply to TLS only.
If they are omitted, Go's default TLS behavior is used.
`cipher_suites` applies to TLS and DTLS; if omitted, each library's default list is used.
DTLS will reject suites it doesn't support.
HTTP TLS/mTLS uses the same inline fields under `server.http.tls` and `server.http.mtls`.

Go 1.24 default TLS behavior (when you omit these fields):
- Minimum version: TLS 1.2 (TLS 1.3 enabled by default)
- TLS 1.3 suites (order prefers AES when hardware support is present):

```text
TLS_AES_128_GCM_SHA256
TLS_AES_256_GCM_SHA384
TLS_CHACHA20_POLY1305_SHA256
```

- TLS 1.2 suites (order prefers AES-GCM when hardware support is present):

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

Default order can change with hardware support and `GODEBUG` flags like `tlsrsakex` and `tls3des`.

See [Configuration Guide](docs/configuration.md) for complete reference.

## Performance

| Metric                     | Value                    |
| -------------------------- | ------------------------ |
| **Concurrent Connections** | 500K+ per node           |
| **Message Throughput**     | 300K-500K msg/s per node |
| **Latency (local)**        | <10ms                    |
| **Latency (cross-node)**   | ~5ms                     |
| **Session Takeover**       | <100ms                   |

**With clustering and topic sharding:**
- 3-node cluster: 1-2M msg/s
- 5-node cluster: 2-4M msg/s

See [Scaling & Performance](docs/scaling.md) for detailed benchmarks.

## Documentation

| Document                                 | Description                                 |
| ---------------------------------------- | ------------------------------------------- |
| [Architecture](docs/architecture.md)     | Detailed system design                      |
| [Scaling & Performance](docs/scaling.md) | Capacity analysis, benchmarks, optimization |
| [Clustering](docs/clustering.md)         | Distributed broker design                   |
| [Client Library](docs/client.md)         | Go MQTT and AMQP 0.9.1 clients with queue support |
| [Broker Internals](docs/broker.md)       | Message routing, sessions                   |
| [Durable Queues](docs/queue.md)          | Queue configuration, consumer groups        |
| [Configuration](docs/configuration.md)   | Complete config reference                   |
| [Webhooks](docs/webhooks.md)             | Webhook event system                        |
| [Roadmap](docs/roadmap.md)               | Development plan                            |

## Roadmap

### Completed ✅
- MQTT 3.1.1 and 5.0 support
- TCP, WebSocket, HTTP transports
- QoS 0/1/2, retained messages, will messages
- Clustering with embedded etcd
- gRPC inter-broker communication (mTLS supported)
- BadgerDB persistent storage
- Durable queues with consumer groups
- Queue replication with Raft (3x replication, automatic failover)
- Kafka-style retention policies (time, size, log compaction)
- TLS/mTLS for client and inter-broker connections
- WebSocket origin validation
- Shared subscriptions (MQTT 5.0)
- MaxQoS enforcement (MQTT 5.0)
- Performance optimization (3.3x throughput, zero-copy buffers)
- Rate limiting (per-IP connections, per-client messages/subscriptions)
- CoAP with UDP and DTLS/mDTLS support

### In Progress 🚧
- Secure default ACL

### Planned 📋
- Management dashboard
- Prometheus metrics endpoint
- Distributed tracing instrumentation
- Hot configuration reload

See [Roadmap](docs/roadmap.md) for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests (`make test`)
4. Run linter (`make lint`)
5. Open a Pull Request

## License

Apache License 2.0 - see [LICENSE](LICENSE)

## Acknowledgments

- [Eclipse Paho](https://www.eclipse.org/paho/) - MQTT client libraries
- [Mosquitto](https://mosquitto.org/) - Reference MQTT broker
- [MQTT.org](https://mqtt.org/) - MQTT specification
