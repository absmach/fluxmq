# MQTT Broker Architecture

## Overview

This document describes the architecture of a multi-protocol MQTT broker with pluggable adapters. The broker accepts connections over various transports and protocols, converts them to MQTT messages internally, and routes them to subscribers.

**Design Philosophy:**
1. **TCP Server First** - All network I/O goes through a unified TCP server layer
2. **Pluggable Adapters** - Protocol-specific adapters convert incoming data to MQTT
3. **Core Packets** - Pure MQTT packet encoding/decoding (the essential MQTT implementation)
4. **Separation of Concerns** - Client and Broker are distinct components
5. **Extensibility** - Storage, authentication, and routing are pluggable

## Architecture Diagram

```
              ┌─────────────────────────────────────────────────────────────┐
              │                         TCP Server                          │
              │  (accepts TCP connections on multiple ports/addresses)      │
              └────────────────────────────┬────────────────────────────────┘
                                           │
           ┌───────────────┬───────────────┼───────────────┬───────────────┐
           │               │               │               │               │
           ▼               ▼               ▼               ▼               ▼
    ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
    │    MQTT     │ │    HTTP     │ │  WebSocket  │ │  CoAP/TCP   │ │  CoAP/UDP   │
    │   Adapter   │ │   Adapter   │ │   Adapter   │ │   Adapter   │ │   Adapter   │
    │  (packets/) │ │             │ │             │ │             │ │  (future)   │
    └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
           │               │               │               │               │
           │   ┌───────────┴───────────────┴───────────────┴───────────────┘
           │   │
           ▼   ▼
    ┌─────────────────────────────────────────────────────────────────────────────┐
    │                            MQTT Message Bus                                 │
    │                    (unified internal representation)                        │
    └───────────────────────────────────┬─────────────────────────────────────────┘
                                        │
                 ┌──────────────────────┼──────────────────────┐
                 │                      │                      │
                 ▼                      ▼                      ▼
          ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
          │   Handlers  │       │   Session   │       │   Topic     │
          │  (dispatch) │◄─────►│   Manager   │◄─────►│   Router    │
          └──────┬──────┘       └──────┬──────┘       └──────┬──────┘
                 │                     │                     │
                 └──────────────┬──────┴─────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │        Store          │
                    │  (sessions, retained, │
                    │   subscriptions)      │
                    └───────────────────────┘
```

## Component Layers

### Layer 1: Network I/O (TCP Server)

The TCP Server is the foundation. It:
- Listens on configured addresses/ports
- Accepts raw TCP connections
- Routes connections to the appropriate adapter based on port or protocol detection
- Handles TLS termination (optional)

```go
type TCPServer interface {
    // Listen starts listening on the given address
    Listen(addr string, config ListenerConfig) error

    // Close shuts down all listeners
    Close() error
}

type ListenerConfig struct {
    Protocol    string      // "mqtt", "http", "ws", "coap"
    TLS         *tls.Config // Optional TLS
    MaxConns    int         // Connection limit
    ReadTimeout time.Duration
}
```

### Layer 2: Protocol Adapters

Adapters convert protocol-specific messages to/from MQTT. Each adapter:
- Receives raw bytes from TCP connections
- Parses the protocol (HTTP, CoAP, WebSocket frames, raw MQTT)
- Converts to MQTT Control Packets
- Sends responses in the original protocol format

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Adapter Interface                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  type Adapter interface {                                                   │
│      // Name returns the adapter identifier                                 │
│      Name() string                                                          │
│                                                                             │
│      // HandleConnection processes an incoming connection                   │
│      // Converts protocol messages to MQTT and vice versa                   │
│      HandleConnection(conn net.Conn) error                                  │
│                                                                             │
│      // SetBroker injects the broker for message routing                    │
│      SetBroker(broker Broker)                                               │
│  }                                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### MQTT Adapter (Core)

The `packets/` package IS the MQTT adapter. It's the essential MQTT implementation:
- Reads raw TCP bytes
- Parses MQTT Control Packets (CONNECT, PUBLISH, SUBSCRIBE, etc.)
- Returns MQTT Control Packets
- Supports MQTT 3.1.1 and 5.0

```
TCP bytes ──► packets.Reader ──► ControlPacket ──► Broker
Broker ──► ControlPacket ──► packets.Writer ──► TCP bytes
```

#### HTTP Adapter

Converts HTTP requests to MQTT operations:

| HTTP | MQTT |
|------|------|
| `POST /publish?topic=X` | PUBLISH to topic X |
| `POST /subscribe?topic=X` | SUBSCRIBE + long-poll or SSE |
| `GET /retained?topic=X` | Get retained message |
| `DELETE /session?client=X` | Disconnect client |

#### WebSocket Adapter

WebSocket with MQTT binary subprotocol:
- Upgrades HTTP to WebSocket
- Each WebSocket message = one MQTT packet
- Binary frames, "mqtt" subprotocol

#### CoAP Adapter (TCP)

CoAP-to-MQTT mapping (RFC 7252 over TCP):

| CoAP | MQTT |
|------|------|
| POST | PUBLISH |
| GET (Observe) | SUBSCRIBE |
| DELETE | UNSUBSCRIBE |

#### CoAP/UDP Adapter (Future)

Same as TCP CoAP but over UDP with:
- Message deduplication
- Reliability via CON/ACK
- Block-wise transfers for large payloads

### Layer 3: Core Packets (packets/)

The essential MQTT implementation. Pure packet encoding/decoding with no I/O:

```
packets/
├── packets.go        # Interfaces: ControlPacket, Encoder, Decoder
├── codec/            # Binary encoding primitives
│   ├── encode.go     # VBI, string, uint16/32 encoding
│   ├── decode.go     # VBI, string, uint16/32 decoding
│   └── zerocopy.go   # Zero-allocation parsing
├── sniffer.go        # Protocol version detection
├── validate.go       # Packet validation
├── v3/               # MQTT 3.1.1 packets
│   ├── connect.go
│   ├── publish.go
│   ├── subscribe.go
│   └── pool/         # Object pooling
└── v5/               # MQTT 5.0 packets (with properties)
    ├── connect.go
    ├── publish.go
    ├── properties.go
    └── pool/
```

**Key Interfaces:**

```go
// ControlPacket is implemented by all MQTT packets
type ControlPacket interface {
    Encode() []byte
    Pack(io.Writer) error
    Unpack(io.Reader) error
    Type() byte
}

// Detailer provides packet metadata
type Detailer interface {
    Details() Details // {Type, PacketID, QoS}
}
```

### Layer 4: Broker Core

The broker orchestrates sessions, routing, and storage:

```
broker/
├── server.go         # Main broker, lifecycle management
├── interfaces.go     # Adapter, Connection interfaces
└── router.go         # Topic subscription trie
```

```go
type Broker interface {
    // HandleConnection processes a new client (from any adapter)
    HandleConnection(conn Connection)

    // Publish distributes a message to subscribers
    Publish(topic string, payload []byte, qos byte, retain bool) error

    // Subscribe registers a subscription
    Subscribe(clientID, filter string, qos byte) error

    // Unsubscribe removes a subscription
    Unsubscribe(clientID, filter string) error

    // Close shuts down the broker
    Close() error
}
```

### Layer 5: Handlers

Packet handlers implement MQTT protocol logic:

```
handlers/
├── handler.go        # Handler interface
├── broker.go         # BrokerHandler implementation
├── dispatcher.go     # Routes packets to handlers
└── handlers_test.go
```

```go
type Handler interface {
    HandlePublish(sess *Session, pkt ControlPacket) error
    HandleSubscribe(sess *Session, pkt ControlPacket) error
    HandleUnsubscribe(sess *Session, pkt ControlPacket) error
    HandlePingReq(sess *Session) error
    HandleDisconnect(sess *Session, pkt ControlPacket) error
    // QoS handlers
    HandlePubAck(sess *Session, pkt ControlPacket) error
    HandlePubRec(sess *Session, pkt ControlPacket) error
    HandlePubRel(sess *Session, pkt ControlPacket) error
    HandlePubComp(sess *Session, pkt ControlPacket) error
}
```

### Layer 6: Session Management

Sessions track client state independent of connection:

```
session/
├── session.go        # Session struct with full state
├── manager.go        # SessionManager for lifecycle
├── inflight.go       # QoS 1/2 message tracking
├── queue.go          # Offline message queue
└── errors.go
```

**Session Lifecycle:**

```
                    ┌──────────────┐
         CONNECT    │              │    DISCONNECT
    ─────────────►  │  Connected   │  ◄─────────────
                    │              │
                    └───────┬──────┘
                            │
                            │ connection lost
                            ▼
                    ┌──────────────┐
                    │ Disconnected │──────► Expired (after ExpiryInterval)
                    │  (session    │
                    │   persists)  │
                    └──────────────┘
                            │
                            │ CONNECT (same ClientID)
                            ▼
                    ┌──────────────┐
                    │  Connected   │  (session resumed)
                    └──────────────┘
```

### Layer 7: Topic Router

Subscription matching using a topic trie:

```
topics/
├── trie.go           # Subscription trie
├── match.go          # Wildcard matching
├── validate.go       # Topic validation
└── shared.go         # Shared subscriptions (future)
```

**Wildcard Support:**

| Pattern | Matches |
|---------|---------|
| `sensor/+/temp` | `sensor/room1/temp`, `sensor/room2/temp` |
| `sensor/#` | `sensor`, `sensor/a`, `sensor/a/b/c` |
| `+/+/temp` | `home/room/temp`, `office/floor1/temp` |

### Layer 8: Storage

Pluggable storage backends:

```
store/
├── store.go          # Interfaces
├── memory/           # In-memory (default)
│   ├── store.go
│   ├── session.go
│   ├── retained.go
│   └── subscription.go
├── etcd/             # etcd backend (distributed)
└── redis/            # Redis backend (future)
```

```go
type Store interface {
    Sessions() SessionStore
    Retained() RetainedStore
    Subscriptions() SubscriptionStore
    Close() error
}

type SessionStore interface {
    Get(clientID string) (*Session, error)
    Save(session *Session) error
    Delete(clientID string) error
    // For distributed session takeover
    Lock(clientID string) (unlock func(), err error)
}

type RetainedStore interface {
    Set(topic string, msg *Message) error
    Get(topic string) (*Message, error)
    Delete(topic string) error
    Match(filter string) ([]*Message, error)
}
```

### Layer 9: Client (Separate from Broker)

The client package provides an MQTT client for testing and embedding:

```
client/
├── client.go         # Client struct
├── options.go        # Connection options
└── handlers.go       # Message callbacks
```

```go
type Client interface {
    Connect(ctx context.Context) error
    Disconnect() error
    Publish(topic string, payload []byte, qos byte) error
    Subscribe(filter string, qos byte, handler MessageHandler) error
    Unsubscribe(filter string) error
}
```

## Data Flow Examples

### MQTT Client Publishing

```
1. TCP Server accepts connection on :1883
2. MQTT Adapter reads CONNECT packet
3. Broker creates/resumes Session
4. MQTT Adapter reads PUBLISH packet
5. Handler.HandlePublish() called
6. Router.Match() finds subscribers
7. For each subscriber:
   - If connected: write PUBLISH to their Session
   - If disconnected + QoS > 0: queue in OfflineQueue
8. If retain flag: Store.Retained().Set()
```

### HTTP Client Publishing

```
1. TCP Server accepts connection on :8080
2. HTTP Adapter reads HTTP request
3. POST /publish?topic=sensor/temp&qos=0
4. HTTP Adapter creates virtual MQTT session
5. HTTP Adapter converts to PUBLISH packet
6. Same flow as MQTT from step 5
7. HTTP Adapter returns 200 OK
```

### WebSocket Client Subscribing

```
1. TCP Server accepts connection on :8080
2. HTTP upgrade to WebSocket (subprotocol: mqtt)
3. WebSocket Adapter reads CONNECT (binary frame)
4. Broker creates Session
5. WebSocket Adapter reads SUBSCRIBE
6. Handler.HandleSubscribe() called
7. Router.Subscribe() adds to trie
8. Send retained messages matching filter
9. Return SUBACK
```

## Configuration

```yaml
# Server configuration
server:
  listeners:
    - address: ":1883"
      protocol: mqtt
    - address: ":8883"
      protocol: mqtt
      tls:
        cert: /etc/mqtt/server.crt
        key: /etc/mqtt/server.key
    - address: ":8080"
      protocol: http
    - address: ":8081"
      protocol: ws
    - address: ":5683"
      protocol: coap

# Session configuration
session:
  max_expiry: 86400        # Maximum session expiry (seconds)
  max_offline_queue: 1000  # Max queued messages per session

# Storage configuration
storage:
  type: memory  # memory, etcd, redis

# Limits
limits:
  max_connections: 100000
  max_packet_size: 268435456  # 256MB
  max_subscriptions_per_client: 100
```

## Package Dependencies

```
                    ┌─────────────┐
                    │   packets   │  (no dependencies)
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌─────────┐  ┌─────────┐  ┌─────────┐
        │  store  │  │ topics  │  │ session │
        └────┬────┘  └────┬────┘  └────┬────┘
             │            │            │
             └────────────┼────────────┘
                          │
                          ▼
                   ┌─────────────┐
                   │  handlers   │
                   └──────┬──────┘
                          │
                          ▼
                   ┌─────────────┐
                   │   broker    │
                   └──────┬──────┘
                          │
           ┌──────────────┼──────────────┐
           │              │              │
           ▼              ▼              ▼
     ┌──────────┐   ┌──────────┐   ┌──────────┐
     │  adapter │   │ transport│   │  client  │
     │  (http)  │   │  (tcp)   │   │          │
     └──────────┘   └──────────┘   └──────────┘
```

## Current Package Structure

```
mqtt/
├── packets/              # Core MQTT packet encoding (Layer 3)
│   ├── codec/            # Binary primitives
│   ├── v3/               # MQTT 3.1.1
│   │   └── pool/
│   └── v5/               # MQTT 5.0
│       └── pool/
│
├── broker/               # Broker core (Layer 4)
│   ├── server.go
│   ├── router.go
│   └── interfaces.go
│
├── handlers/             # Packet handlers (Layer 5)
│   ├── handler.go
│   ├── broker.go
│   └── dispatcher.go
│
├── session/              # Session management (Layer 6)
│   ├── session.go
│   ├── manager.go
│   ├── inflight.go
│   └── queue.go
│
├── topics/               # Topic routing (Layer 7)
│   ├── match.go
│   └── validate.go
│
├── store/                # Storage (Layer 8)
│   ├── store.go
│   └── memory/
│
├── adapter/              # Protocol adapters (Layer 2)
│   ├── http.go
│   └── virtual.go
│
├── transport/            # Network (Layer 1)
│   ├── tcp.go
│   └── ws.go
│
├── client/               # MQTT Client (Layer 9)
│   └── client.go
│
└── integration/          # Integration tests
```

## Implementation Phases

### Phase 1: Core Architecture (Current)
- [x] Packet encoding/decoding (v3, v5)
- [x] Basic broker with session map
- [x] Topic router (trie-based)
- [x] TCP transport
- [x] HTTP adapter
- [x] Session management
- [x] Handlers package
- [x] Memory store

### Phase 2: Protocol Completeness
- [ ] Full QoS 1/2 flows with retry
- [ ] Retained message delivery on subscribe
- [ ] Will message triggering
- [ ] Keep-alive enforcement
- [ ] Session expiry
- [ ] Topic aliases (v5)

### Phase 3: Additional Adapters
- [ ] WebSocket adapter (with subprotocol)
- [ ] CoAP/TCP adapter
- [ ] TLS support

### Phase 4: Production Hardening
- [ ] Configuration file (YAML)
- [ ] Structured logging
- [ ] Prometheus metrics
- [ ] Graceful shutdown
- [ ] Connection limits
- [ ] Rate limiting

### Phase 5: Distributed (Future)
- [ ] etcd storage backend
- [ ] Raft consensus for metadata
- [ ] Cross-node message routing
- [ ] Session takeover across nodes
- [ ] In-memory caching layer
