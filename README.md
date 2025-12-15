# MQTT Broker

A high-performance, multi-protocol MQTT broker written in Go. Supports MQTT 3.1.1 and 5.0 with pluggable protocol adapters for HTTP, WebSocket, and CoAP.

## Features

- **Multi-Protocol Support**
  - MQTT 3.1.1 and MQTT 5.0
  - HTTP adapter for REST-to-MQTT bridging
  - WebSocket transport
  - CoAP adapter (planned)

- **Performance Optimized**
  - Zero-copy packet parsing
  - Object pooling for reduced GC pressure
  - Efficient trie-based topic matching

- **Full MQTT Feature Set**
  - QoS 0, 1, and 2 message delivery
  - Retained messages
  - Will messages
  - Topic wildcards (`+` and `#`)
  - Session persistence
  - Keep-alive management

- **Extensible Architecture**
  - Pluggable storage backends (memory, etcd)
  - Pluggable authentication and authorization
  - Protocol adapters for non-MQTT clients

## Quick Start

### Prerequisites

- Go 1.21 or later
- Make (optional, for using Makefile)

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

### Test with mosquitto

```bash
# Terminal 1 - Start the broker with debug logging
make run-debug

# Terminal 2 - Subscribe to a topic
mosquitto_sub -h localhost -p 1883 -t "sensors/#" -v

# Terminal 3 - Publish a message
mosquitto_pub -h localhost -p 1883 -t "sensors/temperature" -m '{"value": 23.5}'
```

## Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--addr` | `:1883` | TCP address to listen on |
| `--log` | `info` | Log level: `debug`, `info`, `warn`, `error` |

### Log Levels

- **debug**: Logs all packets sent/received (useful for development)
- **info**: Connection events and errors
- **warn**: Warnings and recoverable errors
- **error**: Critical errors only

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       TCP Server                            │
└─────────────────────────────┬───────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
  ┌───────────┐        ┌───────────┐        ┌───────────┐
  │   MQTT    │        │   HTTP    │        │ WebSocket │
  │  Adapter  │        │  Adapter  │        │  Adapter  │
  └─────┬─────┘        └─────┬─────┘        └─────┬─────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                             ▼
                 ┌───────────────────────┐
                 │      Broker Core      │
                 │  ┌──────┐ ┌─────────┐ │
                 │  │Router│ │ Session │ │
                 │  │      │ │ Manager │ │
                 │  └──────┘ └─────────┘ │
                 └───────────┬───────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │     Storage     │
                    │ (memory, etcd)  │
                    └─────────────────┘
```

For detailed architecture documentation, see [docs/architecture.md](docs/architecture.md).

## Project Structure

```
mqtt/
├── cmd/
│   └── broker/          # Broker binary
│       └── main.go
├── packets/             # MQTT packet encoding/decoding
│   ├── codec/           # Binary encoding primitives
│   ├── v3/              # MQTT 3.1.1 packets
│   │   └── pool/        # Object pooling
│   └── v5/              # MQTT 5.0 packets
│       └── pool/        # Object pooling
├── broker/              # Broker core logic
│   ├── server.go        # Main server
│   ├── router.go        # Topic routing
│   └── interfaces.go    # Frontend/Connection interfaces
├── handlers/            # Packet handlers
│   ├── handler.go       # Handler interface
│   ├── broker.go        # Handler implementation
│   └── dispatcher.go    # Packet dispatcher
├── session/             # Session management
│   ├── session.go       # Session state
│   ├── manager.go       # Session lifecycle
│   ├── inflight.go      # QoS message tracking
│   └── queue.go         # Offline message queue
├── store/               # Storage backends
│   ├── store.go         # Storage interfaces
│   └── memory/          # In-memory implementation
├── topics/              # Topic handling
│   ├── match.go         # Wildcard matching
│   └── validate.go      # Topic validation
├── transport/           # Network transports
│   ├── tcp.go           # TCP transport
│   └── ws.go            # WebSocket transport
├── adapter/             # Protocol adapters
│   ├── http.go          # HTTP-to-MQTT adapter
│   └── virtual.go       # Virtual connections (testing)
├── client/              # MQTT client (for testing)
├── integration/         # Integration tests
├── docs/                # Documentation
│   ├── architecture.md  # Detailed architecture
│   └── diff.md          # Implementation status
├── build/               # Build output (gitignored)
├── Makefile             # Build automation
└── README.md
```

## Development

### Makefile Targets

```bash
make build        # Build broker to build/mqttd
make run          # Build and run with info logging
make run-debug    # Build and run with debug logging
make test         # Run all tests
make test-cover   # Run tests with coverage report
make lint         # Run golangci-lint
make fmt          # Format code
make clean        # Remove build artifacts
make deps         # Download dependencies
make help         # Show all targets
```

### Running Tests

```bash
# Run all tests
make test

# Run with verbose output
go test -v ./...

# Run specific package tests
go test -v ./handlers/...

# Run with race detector
go test -race ./...

# Generate coverage report
make test-cover
open build/coverage.html
```

### Code Quality

```bash
# Format code
make fmt

# Run linter (requires golangci-lint)
make lint

# Install golangci-lint
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

## HTTP Adapter

The HTTP adapter allows REST clients to publish messages:

```bash
# Publish via HTTP
curl -X POST "http://localhost:8080/publish?topic=sensors/temp" \
  -d '{"value": 23.5}'
```

To enable the HTTP adapter, it must be added to the server configuration (coming soon).

## Protocol Support

### MQTT 3.1.1

Full support for all packet types:
- CONNECT, CONNACK
- PUBLISH, PUBACK, PUBREC, PUBREL, PUBCOMP
- SUBSCRIBE, SUBACK
- UNSUBSCRIBE, UNSUBACK
- PINGREQ, PINGRESP
- DISCONNECT

### MQTT 5.0

Full support including:
- All 3.1.1 packet types
- AUTH packet for enhanced authentication
- Properties on all packets
- Reason codes
- Session expiry
- Topic aliases (planned)
- Shared subscriptions (planned)

## Roadmap

### Completed
- [x] MQTT 3.1.1 and 5.0 packet encoding/decoding
- [x] TCP transport with protocol version detection
- [x] Session management with offline queuing
- [x] QoS 0, 1, 2 message flow
- [x] Topic routing with wildcards
- [x] HTTP adapter
- [x] In-memory storage

### In Progress
- [ ] Retained message delivery on subscribe
- [ ] Will message triggering
- [ ] Session expiry enforcement

### Planned
- [ ] TLS support
- [ ] WebSocket subprotocol handling
- [ ] CoAP adapter
- [ ] Configuration file (YAML)
- [ ] Prometheus metrics
- [ ] Graceful shutdown
- [ ] etcd storage backend
- [ ] Clustering support

See [docs/diff.md](docs/diff.md) for detailed implementation status.

## Performance

Design decisions for high performance:

- **Zero-Copy Parsing**: PUBLISH payloads reference the original buffer without copying
- **Object Pooling**: All packet types use `sync.Pool` to reduce allocations
- **Efficient Routing**: Trie-based topic matching with O(levels) lookup
- **Minimal Locking**: Read-heavy operations use `sync.RWMutex`

### Benchmarks (coming soon)

| Metric | Target |
|--------|--------|
| Connections | 100K+ per node |
| Messages/sec | 100K+ per node |
| Latency (p99) | < 10ms |
| Memory per connection | < 10KB |

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for new functionality
4. Ensure all tests pass (`make test`)
5. Run the linter (`make lint`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [Eclipse Paho](https://www.eclipse.org/paho/) - MQTT client libraries for testing
- [Mosquitto](https://mosquitto.org/) - Reference MQTT broker
- [MQTT.org](https://mqtt.org/) - MQTT specification
