# MQTT Broker Architecture

## Overview

This document describes the architecture of the MQTT broker with a **clean separation of concerns** between networking, protocol handling, and business logic.

**Design Philosophy:**
1. **Layered Architecture** - Clear separation: TCP Server → MQTT Codec → Broker Logic
2. **Protocol Agnostic Core** - Broker deals only with MQTT semantics; transport is handled separately
3. **Simple Interfaces** - Minimal, focused interfaces at component boundaries
4. **Unexported Implementations** - Return interfaces from constructors, keep internals hidden
5. **Extensibility** - Storage, authentication, and routing are pluggable

## Architecture Diagram

### High-Level Component View

```
┌─────────────────────────────────────────────────────────┐
│                    cmd/broker/main.go                   │
│   - Load config                                         │
│   - Create broker                                       │
│   - Create TCP server                                   │
│   - Context-based graceful shutdown                     │
└────────────┬────────────────────────────────────────────┘
             │
             │ creates & starts
             │
             ▼
┌─────────────────────────────────────────────────────────┐
│              server/tcp/server.go                       │
│   - Listen on TCP port                                  │
│   - Accept connections                                  │
│   - Connection limiting (semaphore)                     │
│   - TCP optimizations (keepalive, nodelay)              │
│   - Graceful shutdown with timeout                      │
│   - Calls Handler.HandleConnection(net.Conn)            │
└────────────┬────────────────────────────────────────────┘
             │
             │ net.Conn
             │
             ▼
┌─────────────────────────────────────────────────────────┐
│                broker/broker.go                         │
│   - HandleConnection(net.Conn)                          │
│   - Wraps conn with MQTT codec                          │
│   - Session management                                  │
│   - Pub/sub routing                                     │
│   - Storage (retained, will messages)                   │
└────────────┬────────────────────────────────────────────┘
             │
             │ uses
             │
             ▼
┌─────────────────────────────────────────────────────────┐
│            broker/connection.go (MQTT Codec)            │
│   - mqttCodec wraps net.Conn                            │
│   - Protocol version detection (v3.1/v3.1.1/v5)         │
│   - ReadPacket() / WritePacket()                        │
│   - Delegates to packets/v3 or packets/v5               │
└─────────────────────────────────────────────────────────┘
             │
             │ uses
             │
    ┌────────┴────────┐
    ▼                 ▼
┌──────────┐    ┌──────────┐
│packets/v3│    │packets/v5│
│          │    │          │
│MQTT 3.1.1│    │ MQTT 5.0 │
└──────────┘    └──────────┘
```

### Detailed Packet Flow Diagram

```
┌──────────────────────────────────────────────────────────────────┐
│                         TCP Client                               │
│                    (mosquitto, paho, etc.)                       │
└────────────┬─────────────────────────────────────────────────────┘
             │
             │ TCP Connection
             │
             ▼
┌────────────────────────────────────────────────────────────────────┐
│                     server/tcp/Server                              │
│                                                                    │
│  1. Accept()  ──────►  2. configureTCPConn()                       │
│                         - SetKeepAlive(15s)                        │
│                         - SetNoDelay(true)                         │
│                                                                    │
│  3. Go handleConn() ──►  4. handler.HandleConnection(net.Conn)     │
└────────────┬───────────────────────────────────────────────────────┘
             │
             │ Delegates to Broker
             │
             ▼
┌────────────────────────────────────────────────────────────────────┐
│                      broker.Broker                                 │
│                  HandleConnection(net.Conn)                        │
│                                                                    │
│  1. Wrap with codec  ─────►  conn := NewConnection(netConn)        │
│                               - Auto-detect version on CONNECT     │
│                                                                    │
│  2. Read CONNECT ─────────►  pkt, err := conn.ReadPacket()         │
│                               │                                    │
│                               ▼                                    │
│                          ┌─────────────────┐                       │
│                          │ packets/v3 or   │                       │
│                          │ packets/v5      │                       │
│                          │ ReadPacket()    │                       │
│                          └─────────────────┘                       │
│                                                                    │
│  3. Create/Get Session ──►  sessionMgr.GetOrCreate(clientID)       │
│                               - Check clean session flag           │
│                               - Resume or create new               │
│                                                                    │
│  4. Send CONNACK ────────►  conn.WritePacket(connack)              │
│                                                                    │
│  5. Packet Loop ─────────►  for { pkt := conn.ReadPacket() }       │
│         │                                                          │
│         └──────────────►  dispatcher.Dispatch(pkt, session)        │
│                                                                    │
└────────────┬───────────────────────────────────────────────────────┘
             │
             │ Dispatch to Handlers
             │
             ▼
┌───────────────────────────────────────────────────────────────────┐
│                   handlers/Dispatcher                             │
│                                                                   │
│  Switch packet.Type():                                            │
│                                                                   │
│  ┌─ PUBLISH ──►  HandlePublish() ─────────────────────┐           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           Validate topic/payload                   │           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           If retain: store.SetRetained()           │           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           router.Match(topic) ──► Get subscribers  │           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           For each subscriber:                     │           │
│  │             - Get session                          │           │
│  │             - Determine delivery QoS               │           │
│  │             - If online: deliverToSession()        │           │
│  │             - If offline: session.OfflineQueue     │           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           Send PUBACK (if QoS 1)                   │           │
│  │                                                    │           │
│  ├─ SUBSCRIBE ──►  HandleSubscribe() ─────────────────┤           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           For each topic filter:                   │           │
│  │             - router.Subscribe(clientID, filter)   │           │
│  │             - Get retained messages                │           │
│  │             - Send retained to client              │           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           Send SUBACK                              │           │
│  │                                                    │           │
│  ├─ UNSUBSCRIBE ──►  HandleUnsubscribe() ─────────────┤           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           router.Unsubscribe(clientID, filters)    │           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           Send UNSUBACK                            │           │
│  │                                                    │           │
│  ├─ PINGREQ ──►  HandlePing() ────────────────────────┤           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           Send PINGRESP                            │           │
│  │                                                    │           │
│  ├─ DISCONNECT ──►  HandleDisconnect() ───────────────┤           │
│  │                │                                   │           │
│  │                ▼                                   │           │
│  │           Clear will message                       │           │
│  │           Close connection                         │           │
│  │                                                    │           │
│  └─ PUBACK/PUBREC/PUBREL/PUBCOMP ─────────────────────┘           │
│                   │                                               │
│                   ▼                                               │
│              session.Inflight operations                          │
│              - Remove from tracking                               │
│              - Trigger next QoS flow step                         │
│                                                                   │
└────────────┬──────────────────────────────────────────────────────┘
             │
             │ Uses
             │
    ┌────────┴────────────────┬──────────────────┐
    ▼                         ▼                  ▼
┌─────────────┐      ┌─────────────┐    ┌─────────────┐
│  session/   │      │  topics/    │    │   store/    │
│  Manager    │      │  Router     │    │   Store     │
│             │      │             │    │             │
│ Sessions    │      │ Trie for    │    │ Retained    │
│ State       │◄────►│ wildcard    │◄──►│ messages    │
│ Inflight    │      │ matching    │    │             │
│ Offline     │      │             │    │ Will msgs   │
│ Queue       │      │             │    │             │
└─────────────┘      └─────────────┘    └─────────────┘
```

### Message Distribution Flow

```
PUBLISH from Client A to topic "sensors/temp"
│
├─► 1. TCP Server receives packet
│
├─► 2. Broker reads packet via codec
│
├─► 3. Dispatcher.HandlePublish()
│     │
│     ├─► Check if retained
│     │    └─► If yes: store.SetRetained("sensors/temp", msg)
│     │
│     ├─► Router.Match("sensors/temp")
│     │    └─► Returns: [
│     │          {sessionID: "client-b", filter: "sensors/#", qos: 1},
│     │          {sessionID: "client-c", filter: "sensors/+", qos: 0}
│     │        ]
│     │
│     └─► For each subscriber:
│           │
│           ├─► Get session from SessionManager
│           │
│           ├─► Determine delivery QoS = min(pub_qos, sub_qos)
│           │
│           ├─► If session.IsConnected():
│           │    └─► deliverToSession()
│           │         ├─► Create PUBLISH packet
│           │         ├─► If QoS > 0: assign PacketID
│           │         ├─► Add to session.Inflight
│           │         └─► session.WritePacket(publish)
│           │
│           └─► Else (offline):
│                └─► session.OfflineQueue.Enqueue(msg)
│                     └─► Delivered when client reconnects
│
└─► 4. Send PUBACK to Client A (if QoS 1)
```


## Component Layers

### Layer 1: TCP Server (`server/tcp/`)

**Purpose:** Robust TCP connection handling with production-ready features.

```go
// server/tcp/server.go
type Handler interface {
    HandleConnection(conn net.Conn)
}

type Server struct { /* unexported fields */ }

func New(cfg Config, h Handler) *Server
```

**Features:**
- Graceful shutdown with configurable timeout
- Connection limiting via semaphore
- TCP optimizations (keepalive, TCP_NODELAY)
- Context-based lifecycle management
- Structured logging with slog
- Buffer pooling

**Config Options:**
- `Address`, `TLSConfig`
- `ShutdownTimeout`, `MaxConnections`
- `ReadTimeout`, `WriteTimeout`, `IdleTimeout`
- `TCPKeepAlive`, `DisableNoDelay`
- `Logger`

### Layer 2: MQTT Codec (`broker/connection.go`)

**Purpose:** Protocol version detection and packet-level I/O.

```go
// broker/connection.go
type mqttCodec struct {
    conn    net.Conn
    reader  io.Reader
    version int // 0=unknown, 3/4=v3.1/v3.1.1, 5=v5
}

func NewConnection(conn net.Conn) Connection
```

**Features:**
- Auto-detects MQTT protocol version on first packet (CONNECT)
- Wraps `net.Conn` for packet-level I/O
- Delegates to `packets/v3` or `packets/v5` readers
- Implements `Connection` interface (internal to broker)

### Layer 3: Broker Core (`broker/`)

**Purpose:** MQTT business logic - sessions, routing, pub/sub.

```go
// broker/broker.go
type Broker struct {
    mu         sync.RWMutex
    sessionMgr *session.Manager
    router     *topics.Router
    store      *store.Store
}

func NewBroker() *Broker

// Implements tcp.Handler
func (b *Broker) HandleConnection(conn net.Conn)
func (b *Broker) Close() error
```

**Responsibilities:**
- Session lifecycle management
- Topic subscription routing
- Message distribution to subscribers
- Retained message storage
- Will message handling
- QoS flow management

**Key Interfaces:**

```go
// broker/interfaces.go
type Connection interface {
    ReadPacket() (packets.ControlPacket, error)
    WritePacket(p packets.ControlPacket) error
    Close() error
    RemoteAddr() net.Addr
}

type Session interface {
    IsConnected() bool
    NextPacketID() uint16
    WritePacket(p packets.ControlPacket) error
    // ... other session methods
}
```

### Layer 4: Core Packets (`packets/`)

Pure MQTT packet encoding/decoding with no I/O dependencies.

```
packets/
├── packets.go        # ControlPacket interface, version detection
├── v3/               # MQTT 3.1.1 packet implementations
│   ├── connect.go
│   ├── publish.go
│   ├── subscribe.go
│   └── ...
└── v5/               # MQTT 5.0 packet implementations (with properties)
    ├── connect.go
    ├── publish.go
    ├── properties.go
    └── ...
```

**Codec Utilities (`codec/`):**

Low-level encoding/decoding primitives used by packet implementations:

```
codec/
├── encode.go        # VBI, string, uint16/32 encoding
├── decode.go        # VBI, string, uint16/32 decoding
└── zerocopy.go      # Zero-allocation parsing helpers
```

### Layer 5: Handlers (`handlers/`)

Packet-specific MQTT protocol logic.

```go
// handlers/dispatcher.go
type Dispatcher struct {
    broker  BrokerHandler
    session SessionHandler
}

func (d *Dispatcher) Dispatch(pkt packets.ControlPacket) error
```

Handles:
- CONNECT → session creation
- PUBLISH → message routing
- SUBSCRIBE → subscription registration
- DISCONNECT → cleanup
- PINGREQ/PINGRESP → keepalive
- PUBACK/PUBREC/PUBREL/PUBCOMP → QoS flows

### Layer 6: Session Management (`session/`)

Tracks client state independent of connection.

```go
// session/session.go
type Session struct {
    ClientID     string
    Version      int
    Connection   Connection
    Inflight     *Inflight      // QoS 1/2 tracking
    OfflineQueue *Queue         // Messages for offline clients
    // ... other state
}

// session/manager.go
type Manager struct {
    sessions map[string]*Session
    mu       sync.RWMutex
}
```

Features:
- Session persistence (clean/persistent sessions)
- QoS 1/2 message retry
- Offline message queuing
- Packet ID allocation

### Layer 7: Topic Router (`topics/`)

Subscription matching using a trie data structure.

```go
// topics/trie.go
type Router struct {
    root *node
    mu   sync.RWMutex
}

func (r *Router) Subscribe(clientID, sessionID, topicFilter string, qos byte) error
func (r *Router) Match(topic string) []Subscription
```

Supports:
- Wildcard matching (`+`, `#`)
- Efficient subscription lookups
- Per-session subscription tracking

### Layer 8: Storage (`store/`)

Pluggable storage backends for retained messages, will messages, and session state.

```go
// store/store.go
type Store struct {
    retained map[string]*Message
    wills    map[string]*WillMessage
    mu       sync.RWMutex
}
```

Currently in-memory; designed for future persistence (Redis, database, etc.)

## Data Flow: MQTT Client Publishing

1. **TCP Accept**: `server/tcp` accepts new connection
2. **Delegate**: Calls `broker.HandleConnection(net.Conn)`
3. **Wrap**: Broker wraps `net.Conn` with MQTT codec via `NewConnection()`
4. **Version Detection**: Codec reads CONNECT packet, detects protocol version
5. **Session Setup**: Broker creates or retrieves session
6. **Packet Loop**: Broker enters read loop via `conn.ReadPacket()`
7. **Dispatch**: Each packet routed to appropriate handler
8. **PUBLISH Handling**:
   - `handlers.HandlePublish` validates packet
   - Stores retained message if retain flag set
   - `topics.Router.Match(topic)` finds subscribers
   - `broker.Distribute()` sends to matching sessions
9. **QoS Flow**: For QoS > 0, track in `session.Inflight` and wait for ACK
10. **Response**: Send PUBACK/PUBREC/PUBREL/PUBCOMP as needed

## Key Design Patterns

### 1. Accept Interfaces, Return Structs
- `NewBroker()` returns `*Broker` (concrete type)
- Broker implements `tcp.Handler` interface (consumed by server)

### 2. Unexported Implementations
- `mqttCodec` is unexported, returned via `Connection` interface
- Encapsulates protocol details, exposes only necessary methods

### 3. Context-Based Lifecycle
```go
ctx, cancel := signal.NotifyContext(context.Background(), 
    os.Interrupt, syscall.SIGTERM)
defer cancel()

server.Listen(ctx) // Blocks until context cancelled
```

### 4. Graceful Shutdown
- TCP server waits for connections to finish (with timeout)
- Broker cleanly closes sessions
- Flush in-flight messages

## Package Structure

```
mqtt/
├── cmd/
│   └── broker/
│       └── main.go           # Application entry point
├── server/
│   └── tcp/
│       ├── server.go         # TCP server implementation
│       └── server_test.go    # Server tests
├── broker/
│   ├── broker.go             # Broker core
│   ├── connection.go         # MQTT codec wrapper
│   └── interfaces.go         # Core interfaces
├── packets/
│   ├── packets.go            # Packet interface, version detection
│   ├── v3/                   # MQTT 3.1.1 packets
│   └── v5/                   # MQTT 5.0 packets
├── codec/
│   ├── encode.go             # Encoding primitives
│   ├── decode.go             # Decoding primitives
│   └── zerocopy.go           # Zero-copy helpers
├── handlers/
│   ├── dispatcher.go         # Packet routing
│   └── broker.go             # Handler implementations
├── session/
│   ├── session.go            # Session state
│   ├── manager.go            # Session lifecycle
│   ├── inflight.go           # QoS tracking
│   └── queue.go              # Offline queue
├── topics/
│   ├── trie.go               # Subscription trie
│   └── match.go              # Wildcard matching
├── store/
│   └── store.go              # Message storage
└── integration/
    ├── broker_test.go        # Integration tests
    └── features_test.go      # Feature tests
```

## Future Extensibility

### Adding HTTP/CoAP/WebSocket Support

The architecture is designed for easy protocol extension:

1. **Create new server package** (e.g., `server/http`, `server/ws`, `server/coap`)
2. **Implement protocol-specific logic** in the server
3. **Call broker methods** directly or via `HandleConnection(net.Conn)`

**Example for HTTP:**
```go
// server/http/server.go
type HTTPServer struct {
    broker *broker.Broker
}

func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    // Parse HTTP request
    topic := r.URL.Query().Get("topic")
    payload, _ := io.ReadAll(r.Body)
    
    // Inject into broker
    s.broker.Distribute(topic, payload, 0, false, nil)
    w.WriteHeader(http.StatusOK)
}
```

**Example for WebSocket:**
```go
// server/ws/server.go
func (s *WSServer) handleConnection(wsConn *websocket.Conn) {
    // Wrap websocket as net.Conn-like interface
    conn := NewWSConnection(wsConn)
    
    // Delegate to broker
    s.broker.HandleConnection(conn)
}
```

No changes to broker core required - it's protocol-agnostic!

## Performance Considerations

1. **Connection Pooling**: TCP server uses buffer pool for reads/writes
2. **Zero-Copy Parsing**: `zerocopy.go` parses packets without allocations where possible
3. **Concurrent Access**: All shared state protected by `sync.RWMutex`
4. **Lock Granularity**: Fine-grained locks (per-session, per-topic node)
5. **TCP Optimizations**: NoDelay for low latency, keepalive for connection health

## Testing Strategy

1. **Unit Tests**: Each package has comprehensive unit tests
2. **Integration Tests**: Full pub/sub flows in `integration/`
3. **TCP Server Tests**: Lifecycle, graceful shutdown, connection limiting
4. **Feature Tests**: Retained messages, will messages, QoS flows
