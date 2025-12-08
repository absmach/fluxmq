# Desired Architecture

This document describes the target architecture for the MQTT broker - a modular, production-ready system with clean separation of concerns.

## Design Principles

1. **Modularity**: Each component is self-contained with clear interfaces
2. **Robustness**: Exhaustive validation, comprehensive error handling, production-grade reliability
3. **Performance**: Zero-copy parsing, object pooling, minimal allocations, lock-free where possible
4. **Extensibility**: Adapters for protocol conversion, pluggable storage backends
5. **Testability**: Every component has comprehensive unit and integration tests

## High-Level Architecture

```
                                    ┌─────────────────────────────────────────┐
                                    │              MQTT Broker                │
                                    └─────────────────────────────────────────┘
                                                        │
        ┌───────────────────────────────────────────────┼───────────────────────────────────────────────┐
        │                                               │                                               │
        ▼                                               ▼                                               ▼
┌───────────────┐                             ┌─────────────────┐                             ┌─────────────────┐
│    Server     │                             │     Session     │                             │      Store      │
│   (Adapters)  │                             │   Management    │                             │  (Abstraction)  │
└───────┬───────┘                             └────────┬────────┘                             └────────┬────────┘
        │                                              │                                               │
        │    ┌─────────────────────────────────────────┼─────────────────────────────────────────┐     │
        │    │                                         │                                         │     │
        ▼    ▼                                         ▼                                         ▼     ▼
┌─────────────────┐                           ┌─────────────────┐                         ┌─────────────────┐
│     Client      │                           │      Topic      │                         │      Core       │
│   Management    │                           │   Management    │                         │   (Protocol)    │
└─────────────────┘                           └─────────────────┘                         └─────────────────┘
```

## Package Structure

```
mqtt/
├── core/                    # Protocol packets - encoding, decoding, validation
│   ├── packet.go            # ControlPacket interface, factory
│   ├── types.go             # Packet type constants, protocol versions
│   ├── header.go            # Fixed header encoding/decoding
│   ├── codec/               # Binary encoding primitives
│   │   ├── vbi.go           # Variable Byte Integer
│   │   ├── string.go        # UTF-8 string encoding
│   │   ├── binary.go        # Integer encoding (u16, u32)
│   │   └── codec_test.go
│   ├── sniffer.go           # Protocol version detection
│   ├── sniffer_test.go
│   ├── validate.go          # Packet validation rules
│   ├── validate_test.go
│   ├── v3/                  # MQTT 3.1.1 packets
│   │   ├── connect.go
│   │   ├── connack.go
│   │   ├── publish.go
│   │   ├── puback.go
│   │   ├── pubrec.go
│   │   ├── pubrel.go
│   │   ├── pubcomp.go
│   │   ├── subscribe.go
│   │   ├── suback.go
│   │   ├── unsubscribe.go
│   │   ├── unsuback.go
│   │   ├── pingreq.go
│   │   ├── pingresp.go
│   │   ├── disconnect.go
│   │   ├── zerocopy.go      # Zero-copy parsing
│   │   └── packets_test.go
│   ├── v5/                  # MQTT 5.0 packets
│   │   ├── properties.go    # Property encoding/decoding
│   │   ├── connect.go
│   │   ├── connack.go
│   │   ├── publish.go
│   │   ├── puback.go
│   │   ├── pubrec.go
│   │   ├── pubrel.go
│   │   ├── pubcomp.go
│   │   ├── subscribe.go
│   │   ├── suback.go
│   │   ├── unsubscribe.go
│   │   ├── unsuback.go
│   │   ├── pingreq.go
│   │   ├── pingresp.go
│   │   ├── disconnect.go
│   │   ├── auth.go
│   │   ├── zerocopy.go
│   │   └── packets_test.go
│   └── pool/                # Object pooling for all packet types
│       ├── pool.go
│       ├── buffer.go
│       └── pool_test.go
│
├── store/                   # Storage abstraction
│   ├── store.go             # Storage interfaces
│   ├── message.go           # Message store interface
│   ├── session.go           # Session store interface
│   ├── subscription.go      # Subscription store interface
│   ├── retained.go          # Retained message store interface
│   ├── will.go              # Will message store interface
│   ├── memory/              # In-memory implementation
│   │   ├── store.go         # Composite memory store
│   │   ├── message.go
│   │   ├── session.go
│   │   ├── subscription.go
│   │   ├── retained.go
│   │   ├── will.go
│   │   └── memory_test.go
│   ├── etcd/                # etcd distributed backend
│   │   ├── store.go
│   │   ├── message.go
│   │   ├── session.go
│   │   ├── subscription.go
│   │   ├── retained.go
│   │   ├── will.go
│   │   └── etcd_test.go
│   └── store_test.go        # Interface compliance tests
│
├── session/                 # Session management
│   ├── session.go           # Session struct and lifecycle
│   ├── manager.go           # SessionManager for all sessions
│   ├── state.go             # Session state machine
│   ├── inflight.go          # Inflight message tracking (QoS 1/2)
│   ├── queue.go             # Offline message queue
│   ├── keepalive.go         # Keep-alive timer management
│   ├── will.go              # Will message handling
│   └── session_test.go
│
├── topic/                   # Topic management
│   ├── topic.go             # Topic name type and utilities
│   ├── filter.go            # Topic filter type
│   ├── validate.go          # Topic/filter validation
│   ├── validate_test.go
│   ├── match.go             # Wildcard matching algorithm
│   ├── match_test.go
│   ├── trie.go              # Subscription trie
│   ├── trie_test.go
│   ├── router.go            # Message routing
│   ├── router_test.go
│   ├── alias.go             # Topic alias management (v5)
│   └── shared.go            # Shared subscription support
│
├── client/                  # Client management
│   ├── client.go            # Client struct (connection + session)
│   ├── manager.go           # ClientManager for tracking clients
│   ├── takeover.go          # Session takeover logic
│   ├── auth.go              # Authentication interface
│   ├── acl.go               # Authorization/ACL interface
│   └── client_test.go
│
├── server/                  # Server with adapter pattern
│   ├── server.go            # Main server orchestrator
│   ├── config.go            # Server configuration
│   ├── handler.go           # Packet handler dispatch
│   ├── handlers/            # Individual packet handlers
│   │   ├── connect.go
│   │   ├── publish.go
│   │   ├── subscribe.go
│   │   ├── unsubscribe.go
│   │   ├── ping.go
│   │   ├── disconnect.go
│   │   └── auth.go
│   ├── adapter/             # Protocol adapters
│   │   ├── adapter.go       # Adapter interface
│   │   ├── tcp.go           # Native MQTT over TCP
│   │   ├── tls.go           # MQTT over TLS
│   │   ├── websocket.go     # MQTT over WebSocket
│   │   ├── http.go          # HTTP-to-MQTT bridge
│   │   ├── coap.go          # CoAP-to-MQTT bridge
│   │   ├── opcua.go         # OPC-UA-to-MQTT bridge
│   │   └── adapter_test.go
│   ├── listener.go          # Connection listener abstraction
│   └── server_test.go
│
├── metrics/                 # Observability
│   ├── metrics.go           # Metrics interface
│   ├── prometheus.go        # Prometheus exporter
│   └── noop.go              # No-op implementation
│
├── cmd/                     # Command-line binaries
│   └── mqttd/
│       └── main.go          # Broker entry point
│
├── integration/             # Integration tests
│   ├── pubsub_test.go       # Basic pub/sub flows
│   ├── qos_test.go          # QoS 0/1/2 testing
│   ├── session_test.go      # Session persistence
│   ├── will_test.go         # Will message testing
│   ├── retained_test.go     # Retained message testing
│   ├── adapter_test.go      # Protocol adapter testing
│   └── stress_test.go       # Load/stress testing
│
└── examples/                # Usage examples
    ├── simple/              # Basic broker setup
    ├── custom_auth/         # Custom authentication
    └── custom_adapter/      # Custom protocol adapter
```

## Component Details

### 1. Core Package (Protocol Layer)

The core package handles all MQTT protocol concerns - optimized, lean, and extremely robust.

#### Responsibilities

- Packet encoding/decoding for MQTT 3.1.1 and 5.0
- Binary codec for MQTT primitives (VBI, strings, integers)
- Protocol version sniffing/detection
- Comprehensive packet validation
- Zero-copy parsing for high throughput
- Object pooling for reduced GC pressure

#### Key Interfaces

```go
// ControlPacket represents any MQTT packet
type ControlPacket interface {
    // Encoding
    Encode() []byte
    Pack(w io.Writer) error

    // Decoding
    Unpack(r io.Reader) error
    UnpackBytes(data []byte) error  // Zero-copy

    // Metadata
    Type() byte
    String() string

    // Pooling support
    Reset()
}

// PacketFactory creates packets by type
type PacketFactory interface {
    NewPacket(packetType byte) (ControlPacket, error)
    NewPacketFromHeader(header FixedHeader) (ControlPacket, error)
}

// Sniffer detects protocol version
type Sniffer interface {
    DetectVersion(r io.Reader) (Version, io.Reader, error)
    DetectPacketType(r io.Reader) (byte, error)
}

// Validator validates packets
type Validator interface {
    Validate(p ControlPacket) error
    ValidateTopic(topic string) error
    ValidateFilter(filter string) error
}
```

#### Design Decisions

1. **Separate v3 and v5 packages**: Different packet structures, no runtime version checks
2. **Zero-copy parsing**: Payload points into original buffer, topic copied (string immutability)
3. **Object pooling**: sync.Pool for all packet types, Reset() clears state
4. **Validation layers**: Structure validation during Unpack(), semantic validation via Validator

### 2. Store Package (Storage Abstraction)

Abstraction over storage backends - in-memory for development, distributed KV store for production.

#### Responsibilities

- Message persistence (retained, offline queue)
- Session state persistence
- Subscription persistence
- Will message storage
- Pluggable backend implementations

#### Key Interfaces

```go
// Store is the composite storage interface
type Store interface {
    Messages() MessageStore
    Sessions() SessionStore
    Subscriptions() SubscriptionStore
    Retained() RetainedStore
    Wills() WillStore
    Close() error
}

// MessageStore handles message persistence
type MessageStore interface {
    // Store a message with TTL
    Store(key string, msg *Message, ttl time.Duration) error

    // Retrieve a message
    Get(key string) (*Message, error)

    // Delete a message
    Delete(key string) error

    // List messages by prefix
    List(prefix string) ([]*Message, error)
}

// SessionStore handles session persistence
type SessionStore interface {
    // Get or create session
    Get(clientID string) (*Session, error)

    // Save session state
    Save(session *Session) error

    // Delete session
    Delete(clientID string) error

    // Lock for session takeover
    Lock(clientID string) (unlock func(), err error)

    // Get expired sessions
    GetExpired(before time.Time) ([]string, error)
}

// SubscriptionStore handles subscription persistence
type SubscriptionStore interface {
    // Add subscription
    Add(clientID string, filter string, opts SubscribeOptions) error

    // Remove subscription
    Remove(clientID string, filter string) error

    // Remove all subscriptions for client
    RemoveAll(clientID string) error

    // Get subscriptions for client
    GetForClient(clientID string) ([]Subscription, error)

    // Match subscriptions for topic
    Match(topic string) ([]Subscription, error)
}

// RetainedStore handles retained messages
type RetainedStore interface {
    Set(topic string, msg *Message) error
    Get(topic string) (*Message, error)
    Delete(topic string) error
    Match(filter string) ([]*Message, error)
}

// WillStore handles will messages
type WillStore interface {
    Set(clientID string, will *WillMessage) error
    Get(clientID string) (*WillMessage, error)
    Delete(clientID string) error
    Trigger(clientID string) error  // Publish will message
}
```

#### Backend Implementations

1. **Memory**: In-memory maps with sync.RWMutex, for development and testing
2. **etcd**: Distributed KV store, for production clustering

### 3. Session Package (Session Management)

Manages client session state and lifecycle.

#### Responsibilities

- Session state management (connected, disconnected, expired)
- Inflight message tracking for QoS 1/2
- Offline message queue
- Keep-alive timer management
- Will message handling
- Session expiry

#### Key Types

```go
// Session represents a client session
type Session struct {
    // Identity
    ID        string    // Client ID
    Version   Version   // MQTT version

    // Connection state
    Connected bool
    ConnectedAt time.Time

    // MQTT options
    CleanStart     bool
    ExpiryInterval uint32  // Session expiry (v5)
    ReceiveMaximum uint16  // Max inflight (v5)
    MaxPacketSize  uint32  // Max packet size (v5)

    // QoS state
    Inflight      *InflightTracker  // QoS 1/2 tracking
    OfflineQueue  *MessageQueue     // Messages for offline client
    NextPacketID  uint16            // Packet ID generator

    // Will message
    Will *WillMessage

    // Subscriptions (cached from store)
    Subscriptions map[string]SubscribeOptions
}

// SessionManager manages all sessions
type SessionManager interface {
    // Get or create session
    Get(clientID string) (*Session, error)

    // Destroy session
    Destroy(clientID string) error

    // Connect client to session (may trigger takeover)
    Connect(clientID string, conn Connection, opts ConnectOptions) (*Session, error)

    // Disconnect client from session
    Disconnect(clientID string, graceful bool) error

    // Check session expiry
    ExpireOld(before time.Time) (int, error)
}

// InflightTracker tracks QoS 1/2 messages
type InflightTracker interface {
    // Add message to inflight
    Add(packetID uint16, msg *Message) error

    // Acknowledge message
    Ack(packetID uint16) (*Message, error)

    // Get message by packet ID
    Get(packetID uint16) (*Message, error)

    // Get messages needing retry
    GetExpired(timeout time.Duration) []*Message

    // Count inflight messages
    Count() int
}
```

#### State Machine

```
                    ┌──────────┐
                    │ New      │
                    └────┬─────┘
                         │ CONNECT
                         ▼
    ┌──────────────────────────────────────┐
    │                                      │
    │             Connected                │
    │                                      │
    └──────┬───────────────────────┬───────┘
           │                       │
           │ Clean disconnect      │ Unclean disconnect
           │ (CleanStart=true)     │ (CleanStart=false)
           ▼                       ▼
    ┌──────────────┐       ┌──────────────┐
    │   Deleted    │       │ Disconnected │
    └──────────────┘       └──────┬───────┘
                                  │
                                  │ ExpiryInterval elapsed
                                  ▼
                           ┌──────────────┐
                           │   Expired    │
                           └──────────────┘
```

### 4. Topic Package (Topic Management)

Handles topic parsing, validation, matching, and routing.

#### Responsibilities

- Topic name validation (no wildcards, valid UTF-8)
- Topic filter validation (valid wildcard placement)
- Wildcard matching algorithm
- Subscription trie for efficient lookup
- Topic alias management (v5)
- Shared subscription support (v5)

#### Key Types

```go
// Topic represents a topic name (no wildcards)
type Topic string

// Filter represents a topic filter (may contain wildcards)
type Filter string

// Subscription represents a client's subscription
type Subscription struct {
    ClientID   string
    Filter     Filter
    QoS        byte
    Options    SubscribeOptions  // NoLocal, RetainAsPublished, etc.
}

// SubscribeOptions are MQTT 5.0 subscription options
type SubscribeOptions struct {
    QoS               byte
    NoLocal           bool   // Don't receive own messages
    RetainAsPublished bool   // Keep original retain flag
    RetainHandling    byte   // 0=send, 1=new only, 2=none
}
```

#### Trie Implementation

```go
// Trie is a topic subscription trie
type Trie interface {
    // Subscribe adds a subscription
    Subscribe(clientID string, filter Filter, opts SubscribeOptions) error

    // Unsubscribe removes a subscription
    Unsubscribe(clientID string, filter Filter) error

    // UnsubscribeAll removes all subscriptions for client
    UnsubscribeAll(clientID string)

    // Match finds all matching subscriptions
    Match(topic Topic) []Subscription

    // Count returns total subscription count
    Count() int
}
```

#### Router

```go
// Router handles message routing
type Router interface {
    // Route finds all sessions that should receive a message
    Route(topic Topic) []RoutingEntry

    // Subscribe registers a subscription
    Subscribe(clientID string, filter Filter, opts SubscribeOptions) error

    // Unsubscribe removes a subscription
    Unsubscribe(clientID string, filter Filter) error
}

// RoutingEntry is a routing destination
type RoutingEntry struct {
    ClientID string
    QoS      byte
    Options  SubscribeOptions
}
```

### 5. Client Package (Client Management)

Manages client connections and coordinates with sessions.

#### Responsibilities

- Client connection handling
- Session takeover coordination
- Authentication interface
- Authorization/ACL interface
- Connection limits and rate limiting

#### Key Types

```go
// Client represents a connected MQTT client
type Client struct {
    // Connection
    Conn      Connection

    // Session reference
    Session   *Session

    // State
    Connected bool
    ConnectedAt time.Time

    // Limits
    RateLimiter *RateLimiter
}

// ClientManager manages all clients
type ClientManager interface {
    // Register a new client
    Register(conn Connection, opts ConnectOptions) (*Client, error)

    // Unregister a client
    Unregister(clientID string) error

    // Get client by ID
    Get(clientID string) (*Client, bool)

    // Count connected clients
    Count() int

    // Iterate over clients
    ForEach(fn func(*Client))
}

// Authenticator validates client credentials
type Authenticator interface {
    Authenticate(clientID, username, password string) (bool, error)
}

// Authorizer checks topic permissions
type Authorizer interface {
    CanPublish(clientID string, topic Topic) bool
    CanSubscribe(clientID string, filter Filter) bool
}
```

### 6. Server Package (Adapter Pattern)

Server with modular protocol adapters for TCP, UDP, and protocol conversion.

#### Responsibilities

- Main server orchestration
- Protocol adapter management
- Packet handler dispatch
- Graceful shutdown
- Configuration management

#### Adapter Interface

```go
// Adapter converts incoming traffic to MQTT packets
type Adapter interface {
    // Name returns adapter name (e.g., "tcp", "websocket", "coap")
    Name() string

    // Serve starts accepting connections
    Serve(handler PacketHandler) error

    // Close stops the adapter
    Close() error

    // Addr returns listener address
    Addr() net.Addr
}

// PacketHandler processes MQTT packets
type PacketHandler interface {
    HandleConnect(conn Connection, pkt *ConnectPacket) error
    HandlePublish(client *Client, pkt *PublishPacket) error
    HandleSubscribe(client *Client, pkt *SubscribePacket) error
    HandleUnsubscribe(client *Client, pkt *UnsubscribePacket) error
    HandlePingReq(client *Client) error
    HandleDisconnect(client *Client, pkt *DisconnectPacket) error
    HandleAuth(client *Client, pkt *AuthPacket) error  // v5 only
}
```

#### Adapter Implementations

1. **TCP Adapter**: Native MQTT over TCP (port 1883)
2. **TLS Adapter**: MQTT over TLS (port 8883)
3. **WebSocket Adapter**: MQTT over WebSocket (port 8080)
4. **HTTP Adapter**: HTTP-to-MQTT bridge (REST API)
5. **CoAP Adapter**: CoAP-to-MQTT bridge (IoT protocol)
6. **OPC-UA Adapter**: OPC-UA-to-MQTT bridge (industrial protocol)

#### Server Configuration

```go
// Config is the server configuration
type Config struct {
    // Adapters
    Adapters []AdapterConfig

    // Limits
    MaxConnections   int
    MaxPacketSize    int
    ReceiveMaximum   int

    // Timeouts
    ConnectTimeout   time.Duration
    IdleTimeout      time.Duration
    WriteTimeout     time.Duration

    // Authentication
    Auth AuthConfig

    // Storage
    Store StoreConfig

    // Metrics
    Metrics MetricsConfig
}

// AdapterConfig configures an adapter
type AdapterConfig struct {
    Type    string  // "tcp", "tls", "ws", "http", "coap", "opcua"
    Address string
    TLS     *TLSConfig
    Options map[string]any
}
```

## Protocol Adapter Details

### TCP/TLS Adapters

Native MQTT protocol over TCP or TLS.

```go
type TCPAdapter struct {
    listener net.Listener
    handler  PacketHandler
}

func (a *TCPAdapter) Serve(handler PacketHandler) error {
    a.handler = handler
    for {
        conn, err := a.listener.Accept()
        if err != nil {
            return err
        }
        go a.handleConnection(conn)
    }
}
```

### WebSocket Adapter

MQTT over WebSocket for browser clients.

```go
type WebSocketAdapter struct {
    server  *http.Server
    handler PacketHandler
}

// Upgrades HTTP to WebSocket on /mqtt path
// Uses binary frames for MQTT packets
```

### HTTP Adapter

REST-to-MQTT bridge for HTTP clients.

```go
type HTTPAdapter struct {
    server  *http.Server
    handler PacketHandler
}

// Endpoints:
// POST /publish?topic=... - Publish message
// POST /subscribe?filter=... - Subscribe (SSE or long-poll)
// GET  /retained?topic=... - Get retained message
```

### CoAP Adapter

CoAP-to-MQTT bridge for constrained IoT devices.

```go
type CoAPAdapter struct {
    conn    *net.UDPConn
    handler PacketHandler
}

// CoAP methods mapped to MQTT:
// POST /ps/{topic} - Publish
// GET  /ps/{topic} - Subscribe (observe)
// DELETE /ps/{topic} - Unsubscribe
```

### OPC-UA Adapter

OPC-UA-to-MQTT bridge for industrial systems.

```go
type OPCUAAdapter struct {
    server  *opcua.Server
    handler PacketHandler
}

// OPC-UA nodes mapped to MQTT topics
// Monitored items trigger MQTT publishes
```

## Data Flow

### Message Flow

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Adapter  │────►│ Handler  │────►│  Router  │────►│ Session  │────►│ Client   │
│ (decode) │     │ (process)│     │ (match)  │     │ (queue)  │     │ (send)   │
└──────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘
     ▲                                 │
     │                                 │
     └─────────────────────────────────┘
                (store retained)
```

### QoS 1 Flow

```
Publisher                    Broker                      Subscriber
    │                          │                             │
    │  PUBLISH (QoS 1)         │                             │
    │─────────────────────────►│                             │
    │                          │  PUBLISH (QoS 1)            │
    │                          │────────────────────────────►│
    │                          │                             │
    │                          │           PUBACK            │
    │                          │◄────────────────────────────│
    │         PUBACK           │                             │
    │◄─────────────────────────│                             │
```

### QoS 2 Flow

```
Publisher                    Broker                      Subscriber
    │                          │                             │
    │  PUBLISH (QoS 2)         │                             │
    │─────────────────────────►│                             │
    │                          │  PUBLISH (QoS 2)            │
    │         PUBREC           │────────────────────────────►│
    │◄─────────────────────────│                             │
    │                          │           PUBREC            │
    │         PUBREL           │◄────────────────────────────│
    │─────────────────────────►│                             │
    │                          │           PUBREL            │
    │                          │────────────────────────────►│
    │                          │                             │
    │                          │          PUBCOMP            │
    │        PUBCOMP           │◄────────────────────────────│
    │◄─────────────────────────│                             │
```

## Testing Strategy

### Unit Tests

Every component has comprehensive unit tests:

```
core/
├── codec/codec_test.go         # Binary encoding tests
├── sniffer_test.go             # Version detection tests
├── validate_test.go            # Validation tests
├── v3/packets_test.go          # v3 packet tests
├── v5/packets_test.go          # v5 packet tests
└── pool/pool_test.go           # Pool tests

store/
├── memory/memory_test.go       # Memory store tests
├── etcd/etcd_test.go           # etcd store tests
└── store_test.go               # Interface compliance

session/
└── session_test.go             # Session lifecycle tests

topic/
├── validate_test.go            # Validation tests
├── match_test.go               # Matching tests
├── trie_test.go                # Trie tests
└── router_test.go              # Router tests

client/
└── client_test.go              # Client tests

server/
├── adapter/adapter_test.go     # Adapter tests
└── server_test.go              # Server tests
```

### Integration Tests

End-to-end tests for complete flows:

```
integration/
├── pubsub_test.go              # Basic pub/sub
├── qos_test.go                 # QoS 0/1/2 flows
├── session_test.go             # Session persistence
├── will_test.go                # Will messages
├── retained_test.go            # Retained messages
├── adapter_test.go             # Protocol adapters
└── stress_test.go              # Load testing
```

### Conformance Tests

MQTT specification compliance:

- Eclipse Paho interoperability
- mosquitto_pub/sub compatibility
- MQTT.fx client testing

### Performance Tests

Benchmarks for critical paths:

```go
func BenchmarkPublishEncode(b *testing.B)
func BenchmarkPublishDecode(b *testing.B)
func BenchmarkZeroCopyParse(b *testing.B)
func BenchmarkTrieMatch(b *testing.B)
func BenchmarkRouterRoute(b *testing.B)
```

## Production Features

### Metrics

Prometheus metrics for observability:

```
# Connections
mqtt_connections_total
mqtt_connections_current
mqtt_connection_duration_seconds

# Messages
mqtt_messages_published_total
mqtt_messages_delivered_total
mqtt_messages_retained_total
mqtt_messages_inflight

# QoS
mqtt_qos0_messages_total
mqtt_qos1_messages_total
mqtt_qos2_messages_total
mqtt_qos1_retries_total
mqtt_qos2_retries_total

# Subscriptions
mqtt_subscriptions_total
mqtt_subscriptions_current

# Sessions
mqtt_sessions_total
mqtt_sessions_current
mqtt_sessions_expired_total

# Performance
mqtt_packet_parse_duration_seconds
mqtt_message_route_duration_seconds
```

### Configuration

YAML configuration file:

```yaml
server:
  adapters:
    - type: tcp
      address: :1883
    - type: tls
      address: :8883
      tls:
        cert: /etc/mqtt/server.crt
        key: /etc/mqtt/server.key
    - type: websocket
      address: :8080
    - type: http
      address: :8081

  limits:
    max_connections: 100000
    max_packet_size: 268435456
    receive_maximum: 65535
    connect_timeout: 10s
    idle_timeout: 300s

auth:
  type: password  # none, password, jwt, custom
  users:
    - username: admin
      password: ${ADMIN_PASSWORD}
      acl:
        publish: ["#"]
        subscribe: ["#"]

store:
  type: memory  # memory, etcd
  etcd:
    endpoints:
      - localhost:2379
    dial_timeout: 5s

metrics:
  enabled: true
  address: :9090
```

### Graceful Shutdown

Ordered shutdown sequence:

1. Stop accepting new connections
2. Send DISCONNECT to all clients
3. Wait for inflight messages to complete
4. Persist session state
5. Close storage backends
6. Exit
