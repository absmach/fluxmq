# FluxMQ

[![Go Report Card](https://goreportcard.com/badge/github.com/absmach/fluxmq)](https://goreportcard.com/report/github.com/absmach/fluxmq)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> **Status: Experimental** — FluxMQ is not production-ready yet.

A high-performance, multi-protocol message broker written in Go designed for scalability, extensibility, and protocol diversity. MQTT transports share a single broker, AMQP 1.0 and AMQP 0.9.1 run in independent brokers, and durable queues provide cross-protocol routing and fan-out.

## Links

- [Documentation](https://fluxmq.absmach.eu/docs)
- [Website](https://fluxmq.absmach.eu)
- [Professional Support](mailto:support@absmach.eu)
- [Discord](https://discord.gg/HvB5QuzF)

## Who Is This For

### ✅ Ideal Use Cases

**Event-Driven Architectures**
- **Event backbone for microservices** - Reliable, ordered event distribution between services with at-least-once or exactly-once delivery (QoS 1/2)
- **CQRS systems** - Durable queues for command/event distribution with per-queue FIFO ordering
- **Asynchronous workflows** - Decouple services with persistent message queues and ack/nack-based redelivery
- **Real-time event processing** - High throughput (300K-500K msg/s per node) with low latency (<10ms local, ~5ms cross-node)

**Why choose this over Kafka for EDA:**
- ✅ Simpler operations - single binary with embedded storage, no Zookeeper/KRaft
- ✅ Multi-protocol - same broker handles MQTT, HTTP, WebSocket, CoAP
- ✅ Per-queue FIFO ordering (single-log queues)
- ✅ Retention via committed-offset truncation (time/size retention planned)
- ✅ Optional Raft layer for queue appends (WIP)

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
- ❌ Advanced queries over events - no indexing beyond topic and offset
- ❌ Built-in stream processing - no Kafka Streams equivalent (process events in consumers)

**Large Payloads**
- ❌ Multi-megabyte messages - 1MB default limit (configurable, but storage optimized for smaller messages)

### Event-Driven Architecture Pattern

FluxMQ is optimized for event-driven systems that need ordered delivery, durable queues, and lightweight operations. For configuration examples and queue patterns, see `examples/` and `docs/queue.md`.

## Features

- **Multi-Protocol Support**
  - **MQTT 3.1.1** - Full support over TCP and WebSocket
  - **MQTT 5.0** - Full support over TCP and WebSocket
  - **AMQP 1.0** - Dedicated broker with queue integration
  - **AMQP 0.9.1** - Dedicated broker with queue integration
  - **HTTP-MQTT Bridge** - RESTful API for publishing messages
  - **WebSocket Transport** - MQTT over WebSocket for browser clients
  - **CoAP Bridge** - UDP and DTLS (mDTLS) support for constrained IoT devices
  - MQTT transports share a broker; AMQP brokers are independent; queues are the shared durability layer

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
  - FIFO per queue and per consumer group (single cursor)
  - DLQ handler present (delivery path wiring pending)
  - Optional Raft layer for queue appends (WIP)
  - Retention via committed-offset truncation (time/size retention planned)

- **Persistent Storage**
  - BadgerDB for session state and offline queues
  - Hybrid storage for retained messages
  - Pluggable storage backends (memory, BadgerDB)

- **Extensible Architecture**
  - Clean layered design: Transport → Protocol → Domain
  - Protocol-agnostic domain logic and shared queue manager
  - Easy to add new protocols and transports
  - Dependency injection for logging and metrics

## Architecture

```
┌──────────────────┐   ┌──────────────┐   ┌──────────────┐
│ MQTT Transports  │   │ AMQP 1.0     │   │ AMQP 0.9.1   │
│ TCP/WS/HTTP/CoAP │   │ Server       │   │ Server       │
└───────┬──────────┘   └──────┬───────┘   └──────┬───────┘
        ▼                     ▼                  ▼
   MQTT Broker           AMQP Broker        AMQP091 Broker
        └───────────────┬───────────────┬───────────────┘
                        ▼
                  Queue Manager
                        ▼
               Log Storage + Index
```

MQTT transports share one broker; AMQP brokers are independent; queues provide the shared durability and cross-protocol fan-out layer.

## Getting Started

### Quick Single-Instance Service (No Clustering)

```bash
make build
./build/fluxmq --config examples/no-cluster.yaml
```

Defaults in `examples/no-cluster.yaml`:
- MQTT TCP: `:1883`
- AMQP 0.9.1: `:5682`
- Data dir: `/tmp/fluxmq/data`

## Configuration

Configuration is YAML-based. See `examples/` for starter files and `docs/configuration.md` for the full reference.

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
- Raft layer for queue appends (WIP)
- Committed-offset truncation for queues (time/size retention planned)
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
