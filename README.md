# Absmach MQTT Broker

[![Go Report Card](https://goreportcard.com/badge/github.com/absmach/mqtt)](https://goreportcard.com/report/github.com/absmach/mqtt)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A high-performance, multi-protocol MQTT broker written in Go. Designed for scalability and extensibility, supporting MQTT 3.1.1 and 5.0.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Protocol Support](#protocol-support)
- [Roadmap](#roadmap)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Multi-Protocol Support**
  - **MQTT 3.1.1** (Full support)
  - **MQTT 5.0** (Full support)
  - Pluggable architecture for future adapters (HTTP, WebSocket, CoAP)

- **Performance Optimized**
  - Zero-copy packet parsing
  - Object pooling for reduced GC pressure
  - Efficient trie-based topic matching

- **Full MQTT Feature Set**
  - QoS 0, 1, and 2 message delivery
  - Retained messages
  - Will messages
  - Session expiry
  - Topic wildcards (`+` and `#`)
  - Session persistence
  - Keep-alive management

- **Extensible Architecture**
  - Pluggable storage backends (currently memory-based)
  - Separation of concerns: Transport, Protocol, and Business Logic

## Architecture

The broker implements a **clean layered architecture** with strict separation between transport, protocol, and domain concerns. This design enables high performance, testability, and extensibility.

### Core Design Principles

1. **Domain-Driven Design** - Core `Broker` contains pure business logic (sessions, routing, pub/sub)
2. **Protocol Adapters** - Stateless handlers (`V3Handler`, `V5Handler`) translate MQTT packets to domain operations
3. **Direct Instrumentation** - Logging and metrics embedded at the domain layer, no middleware overhead
4. **Zero Dependencies** - Domain layer has no knowledge of packets or protocols

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                     TCP Server Layer                        │
│                    (server/tcp/server.go)                   │
│  • Connection acceptance and management                     │
│  • TCP optimizations (keepalive, nodelay)                   │
│  • Graceful shutdown                                        │
└──────────────────────────┬──────────────────────────────────┘
                           │ net.Conn
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  Protocol Adapter Layer                     │
│               (broker/connection.go)                        │
│  • Detects MQTT version from CONNECT packet                 │
│  • Creates appropriate protocol handler                     │
└────────────┬──────────────────────────┬─────────────────────┘
             │                          │
      ┌──────▼──────┐            ┌──────▼──────┐
      │ V3Handler   │            │ V5Handler   │
      │ (v3/v4.0)   │            │ (v5.0)      │
      └──────┬──────┘            └──────┬──────┘
             │                          │
             │ Domain Operations        │
             └──────────┬───────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                     Domain Layer                            │
│                  (broker/broker.go)                         │
│  • CreateSession()    • Publish()                           │
│  • Subscribe()        • DeliverToSession()                  │
│  • Unsubscribe()      • AckMessage()                        │
│                                                             │
│  Built-in instrumentation:                                  │
│  • Logger (slog) for operation tracing                      │
│  • Metrics (Stats) for monitoring                           │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   Infrastructure Layer                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  Router  │  │ Sessions │  │ Storage  │  │  Stats   │   │
│  │  (Trie)  │  │  (Cache) │  │ (Memory) │  │(Metrics) │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
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

For detailed architecture documentation, see [docs/architecture.md](docs/architecture.md).

## Project Structure

```
mqtt/
├── cmd/
│   └── broker/          # Application entry point
├── server/
│   └── tcp/             # TCP Transport layer
├── core/                # Core primitives
│   ├── packets/         # MQTT Packet encoding/decoding
│   │   ├── v3/          # MQTT 3.1.1 packets
│   │   └── v5/          # MQTT 5.0 packets
│   ├── codec/           # Packet stream codec
│   └── connection.go    # Connection interfaces
├── broker/              # Core Broker Logic
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
│   ├── memory/          # In-memory implementation
│   └── messages/        # Message storage types
├── topics/              # Topic validation & utilities
├── config/              # Configuration loading
└── docs/                # Documentation
    ├── architecture.md  # Detailed architecture
    └── roadmap.md       # Implementation roadmap
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
# Start the broker (default port 1883)
make run

# Or with custom options
./build/mqttd --addr=:1883 --log=info
```

## Configuration

Command-line flags are available for basic configuration:

| Option | Default | Description |
|--------|---------|-------------|
| `--addr` | `:1883` | TCP address to listen on |
| `--log` | `info` | Log level: `debug`, `info`, `warn`, `error` |

## Protocol Support

### MQTT 3.1.1

Full support for all packet types and flows:
- CONNECT, CONNACK
- PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP
- SUBSCRIBE, SUBACK
- UNSUBSCRIBE, UNSUBACK
- PINGREQ, PINGRESP
- DISCONNECT

### MQTT 5.0

Comprehensive support including:
- Properties on all packets
- Reason codes
- Session expiry
- Message expiry
- Shared subscriptions (Planned)
- Topic aliases (Planned)

## Roadmap

### Completed
- [x] MQTT 3.1.1 and 5.0 packet encoding/decoding
- [x] TCP transport
- [x] Session management
- [x] QoS 0, 1, 2 flows
- [x] Retained messages
- [x] Will messages
- [x] Session expiry
- [x] Topic routing with wildcards
- [x] In-memory storage

### Planned
- [ ] TLS support
- [ ] WebSocket transport
- [ ] Shared subscriptions
- [ ] Topic aliases
- [ ] Authentication / Authorization hooks
- [ ] Persistent storage (PostgreSQL/etcd)
- [ ] Clustering support

See [docs/roadmap.md](docs/roadmap.md) for the detailed plan.

## Performance

Design decisions for high performance:

- **Zero-Copy Parsing**: Payloads reference the original buffer where possible.
- **Object Pooling**: Extensive use of `sync.Pool` for packets and buffers.
- **Efficient Routing**: Radix tree-based topic matching.
- **Concurrency**: Fine-grained locking and non-blocking I/O patterns.

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
