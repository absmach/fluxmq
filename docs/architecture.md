# Current Architecture

This document describes the current state of the MQTT broker implementation.

## Overview

The broker is a multi-protocol MQTT 3.1.1/5.0 broker written in Go. It features automatic protocol version detection, multiple transport support (TCP, WebSocket, HTTP adapter), object pooling for high throughput, and zero-copy packet parsing.

**Current Status**: Single-node broker with partial feature completion (~60% of MQTT spec).

## Package Structure

```
mqtt/
├── packets/              # Protocol layer - packet encoding/decoding
│   ├── packets.go        # Common interfaces, constants, factory
│   ├── codec/            # Low-level binary encoding/decoding
│   │   ├── encode.go     # VBI, string, int encoding
│   │   ├── decode.go     # VBI, string, int decoding
│   │   └── zerocopy.go   # Zero-copy parsing utilities
│   ├── sniffer.go        # Protocol version detection
│   ├── validate.go       # Packet validation
│   ├── v3/               # MQTT 3.1.1 implementation
│   │   ├── types.go      # Packet types, constants
│   │   ├── connect.go    # CONNECT packet
│   │   ├── connack.go    # CONNACK packet
│   │   ├── publish.go    # PUBLISH packet
│   │   ├── puback.go     # PUBACK packet
│   │   ├── pubrec.go     # PUBREC packet
│   │   ├── pubrel.go     # PUBREL packet
│   │   ├── pubcomp.go    # PUBCOMP packet
│   │   ├── subscribe.go  # SUBSCRIBE packet
│   │   ├── suback.go     # SUBACK packet
│   │   ├── unsubscribe.go# UNSUBSCRIBE packet
│   │   ├── unsuback.go   # UNSUBACK packet
│   │   ├── pingreq.go    # PINGREQ packet
│   │   ├── pingresp.go   # PINGRESP packet
│   │   ├── disconnect.go # DISCONNECT packet
│   │   ├── zerocopy.go   # Zero-copy parsing for v3
│   │   └── pool/         # Object pooling
│   │       ├── pool.go   # sync.Pool for all packet types
│   │       └── buffer.go # Buffer pooling
│   └── v5/               # MQTT 5.0 implementation
│       ├── types.go      # Packet types, constants
│       ├── properties.go # MQTT 5.0 properties
│       ├── connect.go    # CONNECT with properties
│       ├── connack.go    # CONNACK with properties
│       ├── publish.go    # PUBLISH with properties
│       ├── puback.go     # PUBACK with reason codes
│       ├── pubrec.go     # PUBREC with reason codes
│       ├── pubrel.go     # PUBREL with reason codes
│       ├── pubcomp.go    # PUBCOMP with reason codes
│       ├── subscribe.go  # SUBSCRIBE with options
│       ├── suback.go     # SUBACK with reason codes
│       ├── unsubscribe.go# UNSUBSCRIBE
│       ├── unsuback.go   # UNSUBACK with reason codes
│       ├── auth.go       # AUTH packet (v5 only)
│       ├── pingreq.go    # PINGREQ
│       ├── pingresp.go   # PINGRESP
│       ├── disconnect.go # DISCONNECT with reason codes
│       ├── zerocopy.go   # Zero-copy parsing for v5
│       └── pool/         # Object pooling for v5
│
├── broker/               # Orchestration layer
│   ├── server.go         # Main Server, connection lifecycle
│   ├── router.go         # Topic-based message routing (trie)
│   └── interfaces.go     # Frontend, Connection interfaces
│
├── session/              # Session management
│   └── session.go        # Session struct, packet handlers
│
├── store/                # Storage abstraction
│   └── store.go          # MessageStore interface, MemoryStore
│
├── topics/               # Topic handling
│   ├── validate.go       # Topic name validation
│   └── match.go          # MQTT wildcard matching
│
├── transport/            # Network layer
│   ├── tcp.go            # TCP listener and connection
│   └── ws.go             # WebSocket support
│
├── adapter/              # Protocol adapters
│   ├── http.go           # HTTP-to-MQTT gateway
│   └── virtual.go        # Virtual connections (testing)
│
├── client/               # MQTT client (for testing)
│   └── client.go         # Basic client implementation
│
├── handlers/             # Packet handlers (empty, planned)
│
└── integration/          # Integration tests
    ├── broker_test.go    # Full PubSub flow tests
    └── http_test.go      # HTTP adapter tests
```

## Core Components

### 1. Packets Package (Protocol Layer)

The protocol layer handles all MQTT packet encoding, decoding, and validation.

#### Core Interfaces

```go
// ControlPacket is implemented by all MQTT packet types
type ControlPacket interface {
    Encode() []byte           // Serialize to bytes
    Pack(io.Writer) error     // Write to stream
    Unpack(io.Reader) error   // Parse from stream
    Type() byte               // Return packet type constant
    String() string           // Human-readable representation
}

// Detailer provides packet metadata for QoS tracking
type Detailer interface {
    Details() Details  // Returns {Type, ID, QoS}
}

// Resetter supports object pooling
type Resetter interface {
    Reset()  // Clear all fields for reuse
}
```

#### Packet Types

| Type | Value | Description |
|------|-------|-------------|
| CONNECT | 1 | Client connection request |
| CONNACK | 2 | Connection acknowledgment |
| PUBLISH | 3 | Publish message |
| PUBACK | 4 | QoS 1 acknowledgment |
| PUBREC | 5 | QoS 2 received |
| PUBREL | 6 | QoS 2 release |
| PUBCOMP | 7 | QoS 2 complete |
| SUBSCRIBE | 8 | Subscribe request |
| SUBACK | 9 | Subscribe acknowledgment |
| UNSUBSCRIBE | 10 | Unsubscribe request |
| UNSUBACK | 11 | Unsubscribe acknowledgment |
| PINGREQ | 12 | Keep-alive ping |
| PINGRESP | 13 | Ping response |
| DISCONNECT | 14 | Disconnect notification |
| AUTH | 15 | Authentication (v5 only) |

#### Protocol Version Support

| Version | Constant | Status |
|---------|----------|--------|
| MQTT 3.1 | V31 (0x03) | Partial |
| MQTT 3.1.1 | V311 (0x04) | Full |
| MQTT 5.0 | V5 (0x05) | Full |

#### Codec Package

Low-level binary encoding/decoding operations:

```go
// Variable Byte Integer (for lengths)
func EncodeVBI(length int) []byte
func DecodeVBI(r io.Reader) (int, error)

// UTF-8 Strings (2-byte length prefix)
func EncodeString(s string) []byte
func DecodeString(r io.Reader) (string, error)

// Integers (big-endian)
func EncodeUint16(v uint16) []byte
func DecodeUint16(r io.Reader) (uint16, error)
func EncodeUint32(v uint32) []byte
func DecodeUint32(r io.Reader) (uint32, error)
```

#### Protocol Sniffer

Automatic version detection without consuming bytes:

```go
func DetectProtocolVersion(r io.Reader) (byte, io.Reader, error)
func DetectPacketType(r io.Reader) (byte, error)
```

#### Zero-Copy Parsing

High-performance parsing that avoids allocations:

```go
// ZeroCopyReader allows parsing without payload copy
type ZeroCopyReader interface {
    UnpackBytes(data []byte) error
}

// Example: Publish.UnpackBytes parses directly from byte slice
// - Topic: string allocated (Go strings immutable)
// - Payload: slice points into original buffer (no copy)
```

#### Object Pooling

sync.Pool-based reuse of packet structs:

```go
// Acquire a packet from pool
pkt := pool.AcquirePublish()
defer pool.ReleasePublish(pkt)

// Pools exist for all packet types
pool.AcquireByType(packets.PUBLISH) // Generic acquisition
```

### 2. Broker Package (Orchestration Layer)

The broker orchestrates connections, sessions, and message routing.

#### Server Structure

```go
type Server struct {
    listeners  []Frontend              // Active listeners
    sessions   map[string]*session.Session  // ClientID -> Session
    sessionsMu sync.RWMutex            // Protects sessions map
    router     *Router                 // Topic subscription routing
}
```

#### Key Responsibilities

1. **Connection Lifecycle**
   - Accept connections from frontends
   - Detect MQTT version
   - Create and manage sessions
   - Clean up on disconnect

2. **Message Distribution**
   - Receive PUBLISH from clients
   - Query router for matching subscriptions
   - Deliver to each subscriber's session

3. **Subscription Management**
   - Register subscriptions in router
   - Handle wildcard filters

#### Frontend Interface

```go
type Frontend interface {
    Serve(handler ConnectionHandler) error
    Close() error
    Addr() net.Addr
}
```

#### Connection Interface

```go
type Connection interface {
    ReadPacket() (packets.ControlPacket, error)
    WritePacket(p packets.ControlPacket) error
    Close() error
    RemoteAddr() net.Addr
}
```

### 3. Router (Topic Matching)

Trie-based data structure for efficient topic matching.

#### Structure

```go
type Router struct {
    mu   sync.RWMutex
    root *node
}

type node struct {
    children map[string]*node  // Level -> child node
    subs     []Subscription    // Subscriptions at this level
}

type Subscription struct {
    SessionID string
    QoS       byte
}
```

#### Wildcard Support

| Pattern | Matches | Description |
|---------|---------|-------------|
| `home/bedroom/temp` | Exact match only | Literal topic |
| `home/+/temp` | `home/*/temp` | Single-level wildcard |
| `home/#` | `home`, `home/a`, `home/a/b` | Multi-level wildcard |
| `#` | Everything | Root wildcard |

### 4. Session Package

Manages per-client connection state.

#### Session Structure

```go
type Session struct {
    Broker  BrokerInterface  // Reference to broker
    Conn    Connection       // Network connection
    ID      string           // Client ID
    Version int              // MQTT version (3 or 5)
}
```

#### Packet Handlers

| Packet | Handler | Action |
|--------|---------|--------|
| PUBLISH | `handlePublish()` | Extract topic/payload, call broker.Distribute() |
| SUBSCRIBE | `handleSubscribe()` | Register with broker, send SUBACK |
| PINGREQ | `handlePingReq()` | Send PINGRESP |
| DISCONNECT | Exit read loop | Clean disconnect |

### 5. Store Package

Storage abstraction for message persistence.

#### Interface

```go
type MessageStore interface {
    Store(key string, payload []byte) error
    Retrieve(key string) ([]byte, error)
    Delete(key string) error
}
```

#### Implementation

**MemoryStore**: Simple in-memory map with sync.RWMutex protection.

```go
type MemoryStore struct {
    mu   sync.RWMutex
    data map[string][]byte
}
```

### 6. Topics Package

Topic validation and wildcard matching.

#### Validation

```go
func ValidateTopic(topic string) error
// - Must not be empty
// - Cannot contain wildcards (+, #)
// - Must be valid UTF-8
// - Cannot contain null characters
```

#### Matching

```go
func TopicMatch(filter, topic string) bool
// Implements MQTT wildcard matching:
// - + matches single level
// - # matches all remaining levels
// - $ prefix handled specially (system topics)
```

### 7. Transport Package

Network transport implementations.

#### TCP Transport

```go
type TCPFrontend struct {
    listener net.Listener
}

type TCPConnection struct {
    net.Conn
    reader  io.Reader
    version int  // Detected MQTT version
}
```

#### WebSocket Transport

```go
type WSFrontend struct {
    addr    string
    server  *http.Server
    handler ConnectionHandler
}

type WSConnection struct {
    *websocket.Conn
    reader  io.Reader
    version int
}
```

### 8. Adapter Package

Protocol adapters for non-MQTT traffic.

#### HTTP Adapter

```go
type HTTPAdapter struct {
    broker BrokerInterface
}

// POST /publish?topic=... with payload in body
// Creates virtual session, publishes message, disconnects
```

## Data Flow

### Connection Flow

```
1. Frontend.Accept()
   ↓
2. Server.HandleConnection(conn)
   ↓
3. DetectProtocolVersion() → version
   ↓
4. ReadPacket() → CONNECT
   ↓
5. Create Session(broker, conn, clientID, version)
   ↓
6. sessions[clientID] = session
   ↓
7. WritePacket(CONNACK)
   ↓
8. session.Start() → blocking read loop
   ↓
9. On disconnect: delete(sessions, clientID)
```

### Publish Flow

```
1. Session reads PUBLISH packet
   ↓
2. handlePublish() extracts topic, payload
   ↓
3. broker.Distribute(topic, payload)
   ↓
4. router.Match(topic) → []Subscription
   ↓
5. For each subscription:
   ↓
6. sessions[sessionID].Deliver(topic, payload)
   ↓
7. Create version-specific PUBLISH packet
   ↓
8. WritePacket(PUBLISH) to subscriber
```

### Subscribe Flow

```
1. Session reads SUBSCRIBE packet
   ↓
2. handleSubscribe() extracts filters
   ↓
3. For each filter:
   ↓
4. broker.Subscribe(filter, sessionID, qos)
   ↓
5. router.Subscribe(filter, sessionID, qos)
   ↓
6. Create SUBACK with reason codes
   ↓
7. WritePacket(SUBACK)
```

## Concurrency Model

### Per-Connection Goroutine

- Each client connection runs in its own goroutine
- `Session.Start()` blocks on `ReadPacket()`
- Sequential packet processing within session
- No goroutine-per-packet overhead

### Shared State Protection

| Resource | Protection | Access Pattern |
|----------|------------|----------------|
| `sessions` map | sync.RWMutex | Read-heavy (distribution) |
| `router` | sync.RWMutex | Read-heavy (matching) |
| `MemoryStore` | sync.RWMutex | Balanced |

### Lock Contention Minimization

- Per-connection read loop (no lock during packet processing)
- RLock for subscription lookup during message distribution
- WLock only for subscribe/unsubscribe operations

## Performance Optimizations

### 1. Object Pooling

All packet types use sync.Pool to reduce GC pressure:

```go
var publishPool = sync.Pool{
    New: func() any { return &Publish{} },
}

func AcquirePublish() *Publish {
    return publishPool.Get().(*Publish)
}

func ReleasePublish(p *Publish) {
    p.Reset()
    publishPool.Put(p)
}
```

### 2. Zero-Copy Parsing

- PUBLISH payload points directly into input buffer
- No intermediate allocations for binary primitives
- Topic string must be copied (Go string immutability)

### 3. Efficient VBI Encoding

Variable Byte Integer encoding inline:

```go
func EncodeVBI(length int) []byte {
    var buf []byte
    for {
        b := byte(length & 0x7F)
        length >>= 7
        if length > 0 {
            b |= 0x80
        }
        buf = append(buf, b)
        if length == 0 {
            break
        }
    }
    return buf
}
```

## Current Limitations

### Protocol Features

| Feature | Status | Notes |
|---------|--------|-------|
| QoS 0 | Implemented | Fire and forget |
| QoS 1 | Partial | No PUBACK handling/retry |
| QoS 2 | Partial | No state machine |
| Retained messages | Interface only | Not implemented |
| Will messages | Parsed | Not triggered |
| Keep-alive | Parsed | Not enforced |
| Authentication | Parsed | Not validated |
| Topic aliases (v5) | Parsed | Not used |
| Shared subscriptions | Not implemented | |
| Session expiry | Not implemented | |

### Production Features

| Feature | Status |
|---------|--------|
| TLS/SSL | Not implemented |
| Configuration file | Not implemented |
| Graceful shutdown | Not implemented |
| Metrics/observability | Not implemented |
| Logging framework | Not implemented |
| Rate limiting | Not implemented |
| Connection limits | Not implemented |

### Distributed Features

| Feature | Status |
|---------|--------|
| Clustering | Not implemented |
| Session persistence | In-memory only |
| etcd backend | Not implemented |
| Cross-node routing | Not implemented |

## Test Coverage

### Unit Tests

| Package | Coverage |
|---------|----------|
| `packets/v3` | All packet types |
| `packets/v5` | All packet types |
| `topics` | Validation, matching |
| `broker/router` | Trie operations |
| `packets/pool` | Pool operations |
| `packets/codec` | Encoding/decoding |
| `packets/sniffer` | Version detection |

### Integration Tests

| Test | Description |
|------|-------------|
| `broker_test.go` | Full PubSub flow (v5) |
| `http_test.go` | HTTP adapter |

### Missing Coverage

- QoS 1/2 acknowledgment flows
- Keep-alive/ping handling
- Session persistence
- Will message triggering
- WebSocket transport
- Concurrent client stress tests
- Error handling edge cases

## Code Statistics

| Metric | Value |
|--------|-------|
| Total Go files | ~76 |
| Total lines | ~10,000 |
| Main packages | 10 |
| Test files | 11 |
| Packet types v3.1.1 | 14 |
| Packet types v5 | 15 |
