# MQTT Broker Architecture

## Overview

This document describes the detailed architecture of the MQTT broker, which implements a **clean layered architecture** with strict separation between transport, protocol handling, and domain logic.

**Design Philosophy:**
1. **Domain-Driven Design** - Pure business logic isolated from protocol concerns
2. **Protocol Adapters** - Stateless handlers translate MQTT packets to domain operations
3. **Direct Instrumentation** - Logging and metrics embedded at the domain layer
4. **Zero Indirection** - No middleware chains, decorators, or hidden control flow
5. **Testability First** - Each layer independently testable

## Architecture Diagram

### High-Level Component View

```
┌─────────────────────────────────────────────────────────────┐
│                    cmd/broker/main.go                       │
│  • Creates Broker with logger & metrics                     │
│  • Creates TCP server                                       │
│  • Manages graceful shutdown                                │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           │ creates
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              server/tcp/Server                              │
│  • Listens on TCP port                                      │
│  • Accepts connections                                      │
│  • Applies TCP optimizations (keepalive, nodelay)           │
│  • Connection limiting via semaphore                        │
│  • Graceful shutdown with timeout                           │
└──────────────────────────┬──────────────────────────────────┘
                           │ net.Conn
                           ▼
┌─────────────────────────────────────────────────────────────┐
│         broker/HandleConnection(broker, conn)               │
│  • Reads first CONNECT packet                               │
│  • Detects protocol version (v3.1.1 or v5.0)                │
│  • Creates appropriate handler                              │
│  • Delegates to handler.HandleConnect()                     │
└────────────┬──────────────────────────┬─────────────────────┘
             │                          │
      ┌──────▼──────┐            ┌──────▼──────┐
      │ V3Handler   │            │ V5Handler   │
      │             │            │             │
      │ Stateless   │            │ Stateless   │
      │ Adapter     │            │ Adapter     │
      └──────┬──────┘            └──────┬──────┘
             │                          │
             │ Domain Operations        │
             └──────────┬───────────────┘
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                     Broker (Domain)                         │
│                                                             │
│  Core domain methods (protocol-agnostic):                   │
│  • CreateSession(clientID, opts)                            │
│  • Publish(msg)                                             │
│  • Subscribe(session, filter, opts)                         │
│  • Unsubscribe(session, filter)                             │
│  • DeliverToSession(session, msg)                           │
│  • AckMessage(session, packetID)                            │
│                                                             │
│  Embedded instrumentation:                                  │
│  • logger *slog.Logger - operation tracing                  │
│  • stats  *Stats       - metrics collection                 │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           │ uses
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   Infrastructure                            │
│                                                             │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐           │
│  │   Router   │  │  Sessions  │  │  Storage   │           │
│  │   (Trie)   │  │  (Cache)   │  │  (Memory)  │           │
│  │            │  │            │  │            │           │
│  │ • Match()  │  │ • Get()    │  │ • Messages │           │
│  │ • Subscribe│  │ • Set()    │  │ • Retained │           │
│  └────────────┘  └────────────┘  │ • Wills    │           │
│                                  └────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

## Layered Architecture Deep Dive

### Layer 1: TCP Server (server/tcp)

**Responsibility**: Network transport and connection management

**Key Files**:
- `server/tcp/server.go` - TCP listener and connection acceptance

**What it does**:
1. Listens on configured TCP address
2. Accepts incoming connections
3. Applies TCP optimizations:
   - `SetKeepAlive(15s)` - Detects dead connections
   - `SetNoDelay(true)` - Disables Nagle's algorithm for low latency
4. Enforces connection limits via semaphore
5. Handles graceful shutdown with configurable timeout
6. Delegates each connection to `broker.HandleConnection(broker, conn)`

**Dependencies**: Only standard library (`net`, `context`)

---

### Layer 2: Protocol Detection & Adapter Creation (broker/connection.go)

**Responsibility**: Detect MQTT protocol version and create appropriate handler

**Key Files**:
- `broker/connection.go` - `HandleConnection()` function

**What it does**:
1. Wraps `net.Conn` in MQTT codec (`core.NewConnection`)
2. Reads first packet (must be CONNECT)
3. Inspects `ProtocolVersion` field:
   - `3` or `4` → Creates `V3Handler`
   - `5` → Creates `V5Handler`
4. Validates protocol version matches packet type
5. Delegates to `handler.HandleConnect(conn, connectPacket)`

**Code Flow**:
```go
func HandleConnection(broker *Broker, conn core.Connection) {
    pkt := conn.ReadPacket()

    if v3Connect, ok := pkt.(*v3.Connect); ok {
        handler := NewV3Handler(broker)
        handler.HandleConnect(conn, v3Connect)
        return
    }

    if v5Connect, ok := pkt.(*v5.Connect); ok {
        handler := NewV5Handler(broker)
        handler.HandleConnect(conn, v5Connect)
        return
    }
}
```

---

### Layer 3: Protocol Adapters (broker/v3_handler.go, broker/v5_handler.go)

**Responsibility**: Translate MQTT packets into domain operations

**Key Characteristics**:
- **Stateless** - No internal state, operates on session passed as parameter
- **One per protocol version** - `V3Handler` for MQTT 3.1.1/4.0, `V5Handler` for MQTT 5.0
- **Implements Handler interface** - Common interface for all protocol versions

**Handler Interface**:
```go
type Handler interface {
    HandleConnect(conn core.Connection, pkt packets.ControlPacket) error
    HandlePublish(s *session.Session, pkt packets.ControlPacket) error
    HandlePubAck(s *session.Session, pkt packets.ControlPacket) error
    HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error
    // ... other packet types
}
```

**What handlers do**:

1. **Type assertion** - Cast `packets.ControlPacket` to specific packet type
2. **Extract packet fields** - Topic, payload, QoS, flags, properties
3. **Protocol-specific validation** - Topic aliases (v5), QoS levels, etc.
4. **Authorization checks** - If auth engine configured
5. **Translate to domain operation** - Call `broker.Publish()`, `broker.Subscribe()`, etc.
6. **Send response packet** - CONNACK, PUBACK, SUBACK, etc.
7. **Log operation** - Protocol-specific logging (e.g., "v5_publish")
8. **Run session loop** - After CONNECT, enter packet read loop

**Example: V5Handler.HandlePublish**:
```go
func (h *V5Handler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
    start := time.Now()
    p := pkt.(*v5.Publish) // Type assertion

    // Extract fields
    topic := p.TopicName
    payload := p.Payload
    qos := p.FixedHeader.QoS

    // Handle v5-specific topic aliases
    if p.Properties.TopicAlias != nil {
        topic = resolveAlias(...)
    }

    // Authorization
    if !h.broker.auth.CanPublish(s.ID, topic) {
        return sendV5PubAck(s, packetID, 0x87, "Not authorized")
    }

    // Domain operation
    msg := Message{Topic: topic, Payload: payload, QoS: qos}
    if err := h.broker.Publish(msg); err != nil {
        return err
    }

    // Response
    h.broker.logger.Debug("v5_publish_complete",
        slog.String("client_id", s.ID),
        slog.Duration("duration", time.Since(start)))

    return sendV5PubAck(s, packetID, 0x00, "")
}
```

**Packet Loop**:

After successful CONNECT, handlers start a packet read loop managed by `broker.runSession()`:

```go
func (b *Broker) runSession(handler Handler, s *session.Session) error {
    for {
        pkt := s.ReadPacket()
        s.Touch() // Update last activity

        // Dispatch to appropriate handler method
        err := dispatchPacket(handler, s, pkt)
        if err == io.EOF { // Clean disconnect
            return nil
        }
    }
}
```

---

### Layer 4: Domain Layer (broker/broker.go)

**Responsibility**: Core MQTT business logic, completely protocol-agnostic

**Key Characteristics**:
- **Pure domain methods** - No knowledge of packets or protocols
- **Direct instrumentation** - Logger and metrics injected via constructor
- **Single Responsibility** - Each method does one thing
- **Infrastructure-agnostic** - Uses interfaces for storage, routing

**Constructor**:
```go
func NewBroker(logger *slog.Logger, stats *Stats) *Broker {
    if logger == nil {
        logger = slog.Default()
    }
    if stats == nil {
        stats = NewStats()
    }

    return &Broker{
        sessionsMap: session.NewMapCache(),
        router:      NewRouter(),
        messages:    storage.Messages(),
        retained:    storage.Retained(),
        logger:      logger,
        stats:       stats,
    }
}
```

**Domain Methods**:

#### CreateSession
Creates or retrieves a session, handles clean start logic.

```go
func (b *Broker) CreateSession(clientID string, opts SessionOptions) (*session.Session, bool, error) {
    b.mu.Lock()
    defer b.mu.Unlock()

    // Clean start: destroy existing session
    if opts.CleanStart && existing := b.sessionsMap.Get(clientID); existing != nil {
        b.destroySessionLocked(existing)
    }

    // Return existing or create new
    if existing := b.sessionsMap.Get(clientID); existing != nil {
        return existing, false, nil
    }

    s := session.New(clientID, opts)
    b.sessionsMap.Set(clientID, s)
    b.sessions.Save(s.Info())

    return s, true, nil
}
```

#### Publish
Handles retained messages and distributes to subscribers.

```go
func (b *Broker) Publish(msg Message) error {
    b.logOp("publish",
        slog.String("topic", msg.Topic),
        slog.Int("qos", int(msg.QoS)))
    b.stats.IncrementPublishReceived()
    b.stats.AddBytesReceived(uint64(len(msg.Payload)))

    // Handle retained
    if msg.Retain {
        if len(msg.Payload) == 0 {
            b.retained.Delete(msg.Topic)
        } else {
            b.retained.Set(msg.Topic, &storage.Message{...})
        }
    }

    // Distribute to subscribers
    return b.distribute(msg.Topic, msg.Payload, msg.QoS, false, msg.Properties)
}
```

#### Subscribe
Adds subscription and delivers retained messages.

```go
func (b *Broker) subscribeInternal(s *session.Session, filter string, opts SubscriptionOptions) error {
    b.logOp("subscribe",
        slog.String("client_id", s.ID),
        slog.String("filter", filter))
    b.stats.IncrementSubscriptions()

    // Add to router
    b.router.Subscribe(s.ID, filter, opts.QoS, opts)

    // Persist
    b.subscriptions.Add(&storage.Subscription{...})

    // Update session
    s.AddSubscription(filter, opts)

    return nil
}
```

**Instrumentation Helpers**:

```go
func (b *Broker) logOp(op string, attrs ...any) {
    b.logger.Debug(op, attrs...)
}

func (b *Broker) logError(op string, err error, attrs ...any) {
    if err != nil {
        allAttrs := append([]any{slog.String("error", err.Error())}, attrs...)
        b.logger.Error(op, allAttrs...)
    }
}
```

**Benefits of Direct Instrumentation**:
1. **Visibility** - See exactly what's logged in the source code
2. **Performance** - No function call overhead, compiler can inline
3. **Flexibility** - Can add context-specific attributes easily
4. **No Bypass** - Can't accidentally skip logging by calling internal methods

---

### Layer 5: Infrastructure (storage/, session/, broker/router.go)

**Responsibility**: Data structures and persistence

**Components**:

#### Router (broker/router.go)
- **Trie-based topic matching** for efficient wildcard subscriptions
- `Subscribe(clientID, filter, qos)` - Add subscription
- `Unsubscribe(clientID, filter)` - Remove subscription
- `Match(topic)` - Find all subscriptions matching a topic

#### Session Cache (session/cache.go)
- **In-memory map** of active sessions
- `Get(clientID)` - Retrieve session
- `Set(clientID, session)` - Store session
- `Delete(clientID)` - Remove session

#### Storage (storage/)
- **Interfaces** for persistence
- **Memory implementation** for messages, retained, wills, subscriptions
- **Pluggable** - Can swap for Redis, PostgreSQL, etc.

---

## Data Flow Examples

### CONNECT Flow (MQTT v5)

```
1. TCP Server accepts connection
   └─> net.Conn

2. HandleConnection(broker, conn)
   ├─> Wrap in MQTT codec
   ├─> ReadPacket() → v5.Connect
   └─> Detect version = 5

3. NewV5Handler(broker)
   └─> handler.HandleConnect(conn, v5.Connect)

4. V5Handler.HandleConnect()
   ├─> Extract clientID, cleanStart, keepAlive
   ├─> broker.CreateSession(clientID, opts)
   │   ├─> broker.logger.Debug("create_session")
   │   ├─> broker.stats.IncrementConnections()
   │   ├─> Check clean start
   │   ├─> Create/retrieve session
   │   └─> Return session
   ├─> session.Connect(conn)
   ├─> Send CONNACK
   ├─> broker.logger.Info("v5_connect_success")
   └─> broker.runSession(handler, session)
       └─> Packet loop
```

### PUBLISH Flow (QoS 1)

```
1. Session reads PUBLISH packet
   └─> v3.Publish or v5.Publish

2. dispatchPacket()
   └─> handler.HandlePublish(session, packet)

3. V3Handler.HandlePublish()
   ├─> Extract topic, payload, qos, packetID
   ├─> Check authorization
   │   └─> broker.auth.CanPublish(clientID, topic)
   ├─> Create domain Message
   └─> broker.Publish(msg)
       ├─> broker.logger.Debug("publish", topic, qos)
       ├─> broker.stats.IncrementPublishReceived()
       ├─> Handle retained (if msg.Retain)
       └─> broker.distribute(topic, payload, qos, ...)
           ├─> router.Match(topic) → subscribers
           ├─> For each subscriber:
           │   ├─> Get session
           │   ├─> Determine delivery QoS
           │   └─> broker.DeliverToSession(session, msg)
           │       ├─> If online: Write PUBLISH packet
           │       └─> If offline: Enqueue to session.OfflineQueue
           └─> Return

4. Send PUBACK
   └─> handler sends v3.PubAck or v5.PubAck
```

### SUBSCRIBE Flow

```
1. handler.HandleSubscribe(session, packet)
   ├─> Extract topics and QoS levels
   ├─> For each topic:
   │   ├─> Check authorization
   │   │   └─> broker.auth.CanSubscribe(clientID, filter)
   │   ├─> broker.subscribeInternal(session, filter, opts)
   │   │   ├─> broker.logger.Debug("subscribe", filter)
   │   │   ├─> broker.stats.IncrementSubscriptions()
   │   │   ├─> router.Subscribe(clientID, filter, qos)
   │   │   ├─> storage.Add(subscription)
   │   │   └─> session.AddSubscription(filter)
   │   └─> Deliver retained messages
   │       ├─> retained.Match(filter)
   │       └─> broker.DeliverToSession() for each
   └─> Send SUBACK
```

## Key Design Decisions

### 1. Why No Middleware/Decorators?

**Problem with middleware**:
- Internal method calls bypass middleware chain
- Performance overhead from function wrapping
- Hidden control flow
- Complex debugging (stack traces through wrappers)

**Our solution**:
- Direct instrumentation at domain layer
- Explicit logging/metrics calls
- Clear, linear control flow
- Zero overhead (compiler can inline)

### 2. Why Stateless Protocol Handlers?

**Benefits**:
- No state to manage or synchronize
- Can create new handler per connection (cheap)
- Easy to test (just translation logic)
- No shared mutable state

**Trade-off**:
- Session is passed as parameter to each method
- Handler needs reference to broker

### 3. Why Separate V3Handler and V5Handler?

**Alternatives considered**:
1. Single handler with version checks → Complex, hard to read
2. Handler per packet type → Too granular, code duplication

**Our choice**: One handler per protocol version
- Each handler focuses on one protocol's quirks
- Easy to understand and maintain
- Clear separation of v3 vs v5 logic
- Future protocols add new handler without touching existing code

### 4. Why Logger and Metrics in Broker Constructor?

**Benefits**:
- Dependency injection principle
- Testable (can inject mock logger/metrics)
- Explicit dependencies
- No global state

**Usage**:
```go
// Production
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
stats := broker.NewStats()
b := broker.NewBroker(logger, stats)

// Testing
b := broker.NewBroker(nil, nil) // Uses defaults
```

## Testing Strategy

### Unit Tests

**Domain Layer** (broker/broker.go):
```go
func TestBroker_Publish(t *testing.T) {
    b := NewBroker(nil, nil) // Default logger/metrics

    msg := Message{Topic: "test/topic", Payload: []byte("data")}
    err := b.Publish(msg)

    assert.NoError(t, err)
}
```

**Protocol Handlers** (broker/v3_handler_test.go):
```go
func TestV3Handler_HandlePublish(t *testing.T) {
    b := NewBroker(nil, nil)
    handler := NewV3Handler(b)
    session := createTestSession()

    pkt := &v3.Publish{
        TopicName: "test/topic",
        Payload:   []byte("data"),
    }

    err := handler.HandlePublish(session, pkt)

    assert.NoError(t, err)
}
```

### Integration Tests

Test full flow from TCP connection to domain operation:

```go
func TestE2E_PublishSubscribe(t *testing.T) {
    // Start broker
    b := NewBroker(nil, nil)
    server := tcp.New(cfg, b)
    go server.Listen(ctx)

    // Connect subscriber
    sub := connectClient(t, "subscriber")
    sub.Subscribe("test/#")

    // Connect publisher
    pub := connectClient(t, "publisher")
    pub.Publish("test/topic", "message")

    // Verify delivery
    msg := sub.ReceiveMessage()
    assert.Equal(t, "message", string(msg.Payload))
}
```

## Performance Characteristics

### Zero Indirection
- Direct function calls, no decorators
- Compiler can inline logging checks
- No allocations for middleware chains

### Efficient Data Structures
- **Trie-based router**: O(m) topic matching, where m = topic depth
- **Session cache**: O(1) lookup by client ID
- **Object pools**: Packet and buffer reuse

### Concurrency
- **Fine-grained locking**: Only lock session map, not entire broker
- **Non-blocking I/O**: Each connection runs in own goroutine
- **No global locks**: Router and storage use internal synchronization

### Memory Efficiency
- **Zero-copy packet parsing** where possible
- **Shared topic strings** in subscriptions
- **Bounded message queues** prevent memory exhaustion

## Extensibility Points

### New MQTT Version (e.g., v6)

1. Create `broker/v6_handler.go`:
```go
type V6Handler struct {
    broker *Broker
}

func (h *V6Handler) HandleConnect(...) { ... }
// Implement Handler interface
```

2. Update `broker/connection.go`:
```go
if v6Connect, ok := pkt.(*v6.Connect); ok {
    handler := NewV6Handler(broker)
    handler.HandleConnect(conn, v6Connect)
}
```

### New Protocol (CoAP, HTTP)

1. Create adapter in `coap/adapter.go`:
```go
type CoAPAdapter struct {
    broker *broker.Broker
}

func (a *CoAPAdapter) HandleCoAPMessage(msg CoAPMessage) {
    // Translate CoAP → MQTT Message
    mqttMsg := Message{Topic: msg.URI, Payload: msg.Payload}
    a.broker.Publish(mqttMsg)
}
```

2. Core broker unchanged - operates on domain `Message` type

### Custom Storage Backend

Implement storage interfaces:

```go
type RedisStore struct {
    client *redis.Client
}

func (r *RedisStore) Save(msg *storage.Message) error {
    return r.client.Set(ctx, msg.Key(), msg, 0).Err()
}

// Pass to broker
b := NewBroker(logger, stats)
b.messages = NewRedisStore(redisClient)
```

## Deployment Considerations

### Logging Levels

**Production**: `info` level
- Connection events
- Errors
- Performance metrics

**Debug**: `debug` level
- Every packet
- Domain operations
- Performance timings

### Metrics Collection

The `Stats` type exposes:
- Connection counts (current, total, disconnects)
- Message rates (received, sent, publish)
- Byte counts (received, sent)
- Error counts (protocol, auth, packet)

Export via Prometheus, StatsD, or custom exporter.

### Configuration

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))

stats := broker.NewStats()

b := broker.NewBroker(logger, stats)
```

## Conclusion

This architecture achieves:
- ✅ **Clean separation** between transport, protocol, and domain
- ✅ **High performance** through direct instrumentation
- ✅ **Testability** via dependency injection
- ✅ **Extensibility** through adapters and interfaces
- ✅ **Maintainability** with clear, single-responsibility components

The key insight: **Separate what changes (protocols) from what stays stable (domain logic)**. Protocol handlers are adapters that translate between the MQTT wire format and the domain model, allowing the core broker to remain simple and focused.
