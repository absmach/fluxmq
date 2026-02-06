# FluxMQ

[![Go Report Card](https://goreportcard.com/badge/github.com/absmach/fluxmq)](https://goreportcard.com/report/github.com/absmach/fluxmq)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

> **Status: Experimental** — FluxMQ is not production-ready yet.

> **Status of this document** — Since the project is still in an early stage, some of the features described here are **not facts, but eventual targets**. Comprehensive benchmarks and tests are not yet available to fully substantiate these claims, and we will update them regularly as the project evolves.

A high-performance, multi-protocol message broker written in Go designed for scalability, extensibility, and protocol diversity. MQTT transports share a single broker, AMQP 0.9.1 and AMQP 1.0 run in independent brokers, and durable queues provide cross-protocol routing and fan-out.

## Links

- [Documentation](https://fluxmq.absmach.eu/docs)
- [Website](https://fluxmq.absmach.eu)
- [Professional Support](mailto:info@absmach.eu)
- [Discord](https://discord.gg/HvB5QuzF)
- [Blogs](https://absmach.eu/blog/?category=fluxmq)

## ✅ Recommended for

**Event-Driven Architectures**
- **Event backbone for microservices** - Reliable, ordered event distribution between services with at-least-once or exactly-once delivery (QoS 1/2)
- **CQRS systems** - Durable queues for command/event distribution with per-queue FIFO ordering
- **Asynchronous workflows** - Decouple services with persistent message queues and ack/nack-based redelivery
- **Real-time event processing** - Low-latency pub/sub with durable queues and ordering

**Why choose this for EDA:**
- ✅ Simple operations - single binary with embedded storage, no Zookeeper/KRaft
- ✅ Multi-protocol - same broker handles MQTT, HTTP, WebSocket, CoAP
- ✅ Per-queue FIFO ordering (single-log queues)
- ✅ Retention policies for queue logs (time/size/message count)
- ✅ Optional Raft layer for queue appends (WIP)

**IoT & Real-Time Systems**
- **Device communication** - MQTT 3.1.1/5.0 with QoS levels for reliable delivery over unreliable networks
- **Edge computing** - Embedded deployment with low resource footprint
- **Browser clients** - WebSocket transport for real-time web applications
- **Constrained devices** - CoAP bridge for resource-limited IoT hardware

**High-Availability Systems**
- **Clustered deployments** - Automatic session takeover with embedded coordination
- **Geographic distribution** - gRPC-based cross-node routing with embedded etcd coordination
- **Scalability** - Horizontal scaling with multi-node clusters

### Event-Driven Architecture Pattern

FluxMQ is optimized for event-driven systems that need ordered delivery, durable queues, and lightweight operations. For configuration examples and queue patterns, see `examples/` and [Durable Queues](https://fluxmq.absmach.eu/docs/messaging/durable-queues).

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
  - Retention policies (time/size/message count)

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
┌──────────────────────────────────────────────────────────────┐
│  • Set up MQTT, AMQP 1.0, AMQP 0.9.1 brokers                 │
│  • Creates shared Queue Manager (bindings + delivery)        │
│  • Wires cluster, storage, metrics, shutdown                 │
└──────────┬──────────────────┬──────────────────┬─────────────┘
           │                  │                  │
           ▼                  ▼                  ▼
  ┌────────────────┐   ┌─────────────┐   ┌─────────────┐
  │ TCP/WS/HTTP/   │   │ AMQP 1.0    │   │ AMQP 0.9.1  │
  │ CoAP Servers   │   │ Server      │   │ Server      │
  └───────┬────────┘   └──────┬──────┘   └───────┬─────┘
          │                   │                  │
          ▼                   ▼                  ▼
    ┌────────────┐      ┌────────────┐     ┌────────────┐
    │ MQTT Broker│      │ AMQP Broker│     │ AMQP Broker│
    │ (protocol  │      │   (1.0)    │     │   (0.9.1)  │
    │ logic/fsm) │      └─────┬──────┘     └─────┬──────┘
    └──────┬─────┘            │                  │
           └──────────────────┬──────────────────┘
                              │ queue-capable traffic
                              ▼
                      ┌────────────────┐
                      │ Queue Manager  │
                      │ (bindings +    │
                      │ delivery)      │
                      └──────┬─────────┘
                             ▼
                      ┌────────────────┐
                      │ Log Storage    │
                      │ + Topic Index  │
                      └────────────────┘
```

MQTT transports share one broker; AMQP brokers are independent; queues provide the shared durability and cross-protocol fan-out layer.

## Getting Started

The simplest way to run the broker is using Docker Compose file with default config:

```bash
docker compose -f docker/compose.yaml up -d
```

To run with the config from `examples/no-cluster.yaml`,
execute the following from the repo root so the file path resolves.

```bash
FLUXMQ_CONFIG=../examples/no-cluster.yaml \
  docker compose -f docker/compose.yaml up -d
```

To run locally, use:

```bash
make build
./build/fluxmq --config examples/no-cluster.yaml
```

Defaults in `examples/no-cluster.yaml`:
- MQTT TCP: `:1883`
- AMQP 0.9.1: `:5682`
- Data dir: `/tmp/fluxmq/data`

## Docker build

- Image: `ghcr.io/absmach/fluxmq`
- Examples: `docker/README.md`, `docker/compose.yaml`, `docker/config.yaml`
- Build local images: `make docker` (latest) or `make docker-latest` (git tag/sha)

## Configuration

Configuration is YAML-based. See `examples/` for starter files and [Configuration Reference](https://fluxmq.absmach.eu/docs/reference/configuration-reference) for the full reference.

## Benchmarks

Benchmark results are workload- and hardware-dependent. For reproducible numbers,
run the benchmark scripts in `benchmarks/` and capture results on your target
hardware. See `benchmarks/README.md` for commands and guidance.

## Documentation

| Document                                                                                 | Description                                       |
| ---------------------------------------------------------------------------------------- | ------------------------------------------------- |
| [Architecture](https://fluxmq.absmach.eu/docs/architecture/overview)                     | Detailed system design                            |
| [Scaling & Performance](https://fluxmq.absmach.eu/docs/deployment/running-in-production) | Benchmarking and tuning guidance                  |
| [Clustering](https://fluxmq.absmach.eu/docs/architecture/clustering)                     | Distributed broker design                         |
| [Client Library](https://fluxmq.absmach.eu/docs/clients/go-client)                       | Go MQTT and AMQP 0.9.1 clients with queue support |
| [Broker Internals](https://fluxmq.absmach.eu/docs/architecture/routing)                  | Message routing, sessions                         |
| [Durable Queues](https://fluxmq.absmach.eu/docs/messaging/durable-queues)                | Queue configuration, consumer groups              |
| [Configuration](https://fluxmq.absmach.eu/docs/reference/configuration-reference)        | Complete config reference                         |
| [Webhooks](https://fluxmq.absmach.eu/docs/architecture/webhooks)                         | Webhook event system                              |
| [Roadmap](https://fluxmq.absmach.eu/docs/roadmap)                                        | Project planning notes                            |

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests (`make test`)
4. Run linter (`make lint`)
5. Open a Pull Request

## License

Apache License 2.0 - see [LICENSE](LICENSE)
