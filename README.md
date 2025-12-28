# Absmach MQTT Broker

[![Go Report Card](https://goreportcard.com/badge/github.com/absmach/mqtt)](https://goreportcard.com/report/github.com/absmach/mqtt)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A high-performance, multi-protocol message broker written in Go. Supports MQTT 3.1.1 and 5.0 over TCP and WebSocket, plus HTTP-MQTT and CoAP bridges for IoT integration. Designed for scalability, extensibility, and protocol diversity.

## Table of Contents

- [Absmach MQTT Broker](#absmach-mqtt-broker)
  - [Table of Contents](#table-of-contents)
  - [Features](#features)
  - [Architecture](#architecture)
    - [Core Design Principles](#core-design-principles)
    - [Architecture Layers](#architecture-layers)
    - [Why This Architecture?](#why-this-architecture)
  - [Project Structure](#project-structure)
  - [Quick Start](#quick-start)
    - [Prerequisites](#prerequisites)
    - [Build](#build)
    - [Run](#run)
  - [Configuration](#configuration)
    - [Configuration File](#configuration-file)
    - [Command-line Flags](#command-line-flags)
  - [Protocol Support](#protocol-support)
    - [MQTT 3.1.1 (over TCP and WebSocket)](#mqtt-311-over-tcp-and-websocket)
    - [MQTT 5.0 (over TCP and WebSocket)](#mqtt-50-over-tcp-and-websocket)
    - [HTTP-MQTT Bridge](#http-mqtt-bridge)
    - [WebSocket Transport](#websocket-transport)
    - [CoAP Bridge](#coap-bridge)
  - [Roadmap](#roadmap)
    - [Completed ✅](#completed-)
    - [In Progress 🚧](#in-progress-)
    - [Planned 📋](#planned-)
  - [Performance](#performance)
  - [Contributing](#contributing)
  - [License](#license)
  - [Acknowledgments](#acknowledgments)

## Features

- **Multi-Protocol Support**
  - **MQTT 3.1.1** - Full support over TCP and WebSocket
  - **MQTT 5.0** - Full support over TCP and WebSocket
  - **HTTP-MQTT Bridge** - RESTful API for publishing messages
  - **WebSocket Transport** - MQTT over WebSocket for browser clients
  - **CoAP Bridge** - Lightweight protocol for constrained IoT devices
  - All protocols share the same broker core - messages flow seamlessly across protocols

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

- **Extensible Architecture**
  - Clean layered design: Transport → Protocol → Domain
  - Protocol-agnostic domain logic
  - Easy to add new protocols and transports
  - Pluggable storage backends (memory, BadgerDB)
  - Dependency injection for logging and metrics

- **Clustering & High Availability**
  - Embedded etcd for distributed coordination
  - gRPC-based inter-broker communication
  - Automatic session ownership management
  - Cross-node message routing
  - Persistent storage with BadgerDB
  - Graceful shutdown with session transfer
  - No external dependencies - all embedded in single binary

## Architecture

The broker implements a **clean layered architecture** with strict separation between transport, protocol, and domain concerns. This design enables high performance, testability, and extensibility.

### Core Design Principles

1. **Domain-Driven Design** - Core `Broker` contains pure business logic (sessions, routing, pub/sub)
2. **Protocol Adapters** - Stateless handlers (`V3Handler`, `V5Handler`) translate MQTT packets to domain operations
3. **Direct Instrumentation** - Logging and metrics embedded at the domain layer, no middleware overhead
4. **Zero Dependencies** - Domain layer has no knowledge of packets or protocols

### Architecture Layers

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ TCP Server   │  │  WebSocket   │  │HTTP Bridge   │  │CoAP Bridge   │
│  :1883       │  │   :8083      │  │  :8080       │  │  :5683       │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │ net.Conn        │ ws.Conn         │                 │
       └─────────┬───────┴─────────────────┘                 │
                 │ core.Connection                           │
                 ▼                                           ▼
       ┌─────────────────────┐                    ┌──────────────────┐
       │ Protocol Detection  │                    │ Direct Domain    │
       │ (connection.go)     │                    │ Calls (Publish)  │
       └─────────┬───────────┘                    └────────┬─────────┘
                 │                                         │
        ┌────────┴────────┐                                │
        │                 │                                │
 ┌──────▼──────┐   ┌──────▼──────┐                         │
 │ V3Handler   │   │ V5Handler   │                         │
 │ (v3/v4.0)   │   │ (v5.0)      │                         │
 └──────┬──────┘   └──────┬──────┘                         │
        │                 │                                │
        │ Domain Ops      │                                │
        └────────┬────────┴────────────────────────────────┘
                 ▼
    ┌────────────────────────────────────────────────────────┐
    │              Domain Layer (broker/broker.go)           │
    │                                                        │
    │  • CreateSession()  • Publish()    • Subscribe()       │
    │  • DeliverToSession()  • Unsubscribe()  • AckMessage() │
    │                                                        │
    │  Built-in instrumentation:                             │
    │  • Logger (slog)  • Metrics (Stats)                    │
    └─────────────────────┬──────────────────────────────────┘
                          │
                          ▼
    ┌────────────────────────────────────────────────────────┐
    │                Infrastructure Layer                    │
    │    ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐     │
    │    │ Router  │ │Sessions │ │ Storage │ │ Stats   │     │
    │    │ (Trie)  │ │ (Cache) │ │(Memory) │ │(Metrics)│     │
    │    └─────────┘ └─────────┘ └─────────┘ └─────────┘     │
    └────────────────────────────────────────────────────────┘

Key Insight: All protocols (MQTT/TCP, MQTT/WS, HTTP, CoAP) share
the same Broker instance. A message published via HTTP appears in
all MQTT subscriptions, and vice versa.
```

### Why This Architecture?

**Performance**
- No decorator/middleware overhead - direct function calls
- Zero allocations for instrumentation
- Compiler can inline logging checks

**Clarity**
- See exactly where operations are logged
- No hidden control flow through middleware chains
- Stack traces are straightforward

**Testability**
- Domain logic completely independent of protocols
- Mock logger/metrics for unit tests
- Protocol handlers test packet translation only

**Extensibility**
- New MQTT versions → new handler (e.g., `V6Handler`)
- New protocols (CoAP, HTTP) → new adapters
- Core domain logic unchanged

For detailed architecture documentation, see:
- [Scaling Quick Reference](docs/scaling-quick-reference.md) - **TL;DR: Can we handle 10M clients? (Answer: Yes, with 6 weeks work)**
- [Architecture Deep Dive](docs/architecture-deep-dive.md) - **etcd vs custom Raft, alternative architectures, bottleneck analysis**
- [Architecture & Capacity Analysis](docs/architecture-capacity.md) - 20-node cluster scaling, throughput analysis
- [Clustering Architecture](docs/clustering-architecture.md) - Distributed broker design
- [Clustering Infrastructure](docs/clustering-infrastructure.md) - etcd, gRPC, BadgerDB deep dive
- [Broker & Routing](docs/broker-routing.md) - Message routing and session management
- [Configuration Guide](docs/configuration.md) - Complete configuration reference

## Project Structure

```
mqtt/
├── cmd/
│   └── broker/          # Application entry point
├── server/              # Transport & Protocol Bridges
│   ├── tcp/             # MQTT over TCP transport
│   ├── websocket/       # MQTT over WebSocket transport
│   ├── http/            # HTTP-MQTT bridge (REST API)
│   └── coap/            # CoAP-MQTT bridge
├── core/                # Core primitives
│   ├── packets/         # MQTT Packet encoding/decoding
│   │   ├── v3/          # MQTT 3.1.1 packets
│   │   └── v5/          # MQTT 5.0 packets
│   ├── codec/           # Packet stream codec
│   └── connection.go    # Connection interfaces
├── broker/              # Core Broker Logic (Protocol-Agnostic)
│   ├── broker.go        # Domain logic (pure business logic)
│   ├── v3_handler.go    # MQTT 3.1.1 protocol adapter
│   ├── v5_handler.go    # MQTT 5.0 protocol adapter
│   ├── connection.go    # Protocol detection & handler creation
│   ├── handler.go       # Handler interface
│   ├── router.go        # Topic matching (Trie)
│   └── stats.go         # Metrics collection
├── session/             # Session Management
│   ├── session.go       # Session state
│   └── cache.go         # Session cache
├── storage/             # Storage Interfaces & Backends
│   ├── storage.go       # Storage interfaces
│   ├── memory/          # In-memory implementation
│   ├── badger/          # BadgerDB persistent storage
│   └── messages/        # Message storage types
├── cluster/             # Clustering & Distribution
│   ├── cluster.go       # Cluster interface
│   ├── noop.go          # Single-node implementation
│   ├── etcd_cluster.go  # Embedded etcd cluster
│   ├── transport.go     # gRPC inter-broker transport
│   ├── broker.proto     # gRPC service definition
│   └── *.pb.go          # Generated protobuf code
├── topics/              # Topic validation & utilities
├── config/              # Configuration loading
├── examples/            # Example configurations
│   ├── no-cluster.yaml         # Single node
│   ├── single-node-cluster.yaml # Testing cluster features
│   ├── node1.yaml              # 3-node cluster: node 1
│   ├── node2.yaml              # 3-node cluster: node 2
│   └── node3.yaml              # 3-node cluster: node 3
└── docs/                # Documentation
    ├── clustering-architecture.md     # Clustering overview
    ├── clustering-infrastructure.md   # etcd, gRPC, BadgerDB
    ├── broker-routing.md              # Routing & sessions
    └── configuration.md               # Configuration guide
```

## Quick Start

### Prerequisites

- Go 1.24 or later
- Make (optional)

### Build

```bash
# Clone the repository
git clone https://github.com/absmach/mqtt.git
cd mqtt

# Build the broker
make build

# Or without Make
go build -o build/mqttd ./cmd/broker
```

### Run

```bash
# Start with default configuration (TCP only on port 1883)
./build/mqttd

# Start with custom configuration file
./build/mqttd --config config.yaml

# Run a 3-node cluster (in separate terminals)
make run-node1  # Terminal 1
make run-node2  # Terminal 2
make run-node3  # Terminal 3

# Test cross-node messaging
mosquitto_sub -p 1884 -t "test/#" -v  # Subscribe on node2
mosquitto_pub -p 1885 -t "test/hello" -m "Cross-node message"  # Publish on node3
```

## Configuration

### Configuration File

Create a `config.yaml` to enable additional protocols:

```yaml
server:
  tcp_addr: ":1883"
  tcp_max_connections: 10000

  # Enable HTTP-MQTT bridge
  http_addr: ":8080"
  http_enabled: true

  # Enable WebSocket transport
  ws_addr: ":8083"
  ws_path: "/mqtt"
  ws_enabled: true

  # Enable CoAP bridge
  coap_addr: ":5683"
  coap_enabled: false

  shutdown_timeout: 30s

broker:
  max_message_size: 1048576  # 1MB
  max_retained_messages: 10000
  retry_interval: 20s

session:
  max_sessions: 10000
  default_expiry_interval: 300  # 5 minutes
  max_offline_queue_size: 1000

log:
  level: info  # debug, info, warn, error
  format: text  # text, json

storage:
  type: memory
```

### Command-line Flags

| Option | Default | Description |
|--------|---------|-------------|
| `--config` | - | Path to YAML configuration file |

## Protocol Support

### MQTT 3.1.1 (over TCP and WebSocket)

Full support for all packet types and flows:
- CONNECT, CONNACK
- PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP
- SUBSCRIBE, SUBACK
- UNSUBSCRIBE, UNSUBACK
- PINGREQ, PINGRESP
- DISCONNECT

### MQTT 5.0 (over TCP and WebSocket)

Comprehensive support including:
- All packet types with properties
- Reason codes
- Session expiry
- Message expiry
- User properties
- Shared subscriptions (Planned)
- Topic aliases (Planned)

### HTTP-MQTT Bridge

RESTful API for publishing messages to the broker:

**Endpoint:** `POST /publish`

**Request:**
```json
{
  "topic": "sensor/temperature",
  "payload": "eyJ0ZW1wIjogMjIuNX0=",
  "qos": 1,
  "retain": false
}
```

**Response:** `{"status": "ok"}`

**Use case:** Publish from web applications, serverless functions, or services without MQTT client libraries.

### WebSocket Transport

MQTT protocol over WebSocket for browser and mobile clients:

- **URL:** `ws://localhost:8083/mqtt` (configurable path)
- **Protocol:** Standard MQTT 3.1.1 or 5.0 packets over WebSocket binary frames
- **Full support:** CONNECT, SUBSCRIBE, PUBLISH, QoS 0/1/2
- **Use case:** Web dashboards, browser-based IoT clients

### CoAP Bridge

Lightweight protocol for constrained IoT devices:

- **Protocol:** CoAP over UDP
- **Endpoint:** `/mqtt/publish/<topic>`
- **Method:** POST with payload
- **Use case:** Low-power sensors, embedded devices
- **Status:** Stub implementation (handlers defined, requires UDP server setup)

## Roadmap

### Completed ✅
- [x] MQTT 3.1.1 and 5.0 packet encoding/decoding
- [x] TCP transport
- [x] WebSocket transport
- [x] HTTP-MQTT bridge
- [x] CoAP bridge (stub)
- [x] Session management
- [x] QoS 0, 1, 2 flows
- [x] Retained messages
- [x] Will messages
- [x] Session expiry
- [x] Topic routing with wildcards
- [x] In-memory storage
- [x] BadgerDB persistent storage
- [x] Direct instrumentation (logging & metrics)
- [x] Multi-protocol support (all share same broker core)
- [x] **Clustering with embedded etcd**
- [x] **gRPC inter-broker communication**
- [x] **Cross-node message routing**
- [x] **Distributed session ownership**
- [x] **Graceful shutdown with session transfer**
- [x] **BadgerDB persistent storage with background GC**

### In Progress 🚧
- [ ] Session state migration (full takeover)
- [ ] Complete CoAP UDP server implementation
- [ ] TLS/SSL support for TCP and WebSocket
- [ ] Authentication & Authorization hooks

### Planned 📋
- [ ] Shared subscriptions (MQTT 5.0)
- [ ] Topic aliases (MQTT 5.0)
- [ ] Message expiry enforcement
- [ ] Cluster auto-discovery
- [ ] Admin REST API for management
- [ ] Prometheus metrics exporter
- [ ] MQTT bridge to other brokers
- [ ] Rate limiting and quotas
- [ ] Message bridge to Kafka/NATS

See [docs/roadmap.md](docs/roadmap.md) for the detailed plan.

## Performance

**20-Node Cluster Capacity (with hybrid storage):**
- **Concurrent Connections**: 1,000,000+ clients (50K per node)
- **Message Throughput**: 200K-500K messages/second cluster-wide
- **Retained Messages**: 10M+ messages
- **Subscriptions**: 20M+ active subscriptions
- **Message Latency**: <10ms local, ~5ms cross-node
- **Session Takeover**: <100ms

**Design optimizations:**
- **Zero-Copy Parsing**: Payloads reference the original buffer where possible
- **Object Pooling**: Extensive use of `sync.Pool` for packets and buffers
- **Efficient Routing**: Trie-based topic matching with local caching
- **Hybrid Storage**: Size-based replication reduces etcd load by 70%
- **Concurrency**: Fine-grained locking and non-blocking I/O patterns

See [Architecture & Capacity Analysis](docs/architecture-capacity.md) for detailed benchmarks.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Write tests for new functionality
4. Ensure all tests pass (`make test`)
5. Run the linter (`make lint`)
6. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Eclipse Paho](https://www.eclipse.org/paho/) - MQTT client libraries for testing
- [Mosquitto](https://mosquitto.org/) - Reference MQTT broker
- [MQTT.org](https://mqtt.org/) - MQTT specification
