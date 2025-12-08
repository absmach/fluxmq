# Architecture Gap Analysis & Implementation Plan

This document compares the current architecture with the desired architecture and provides a detailed plan to bridge the gap.

## High-Level Comparison

| Component | Current | Desired | Gap |
|-----------|---------|---------|-----|
| Core/Packets | `packets/` with v3, v5, codec, pool | `core/` with same structure | Rename and reorganize |
| Store | Simple `MessageStore` interface | Composite `Store` with 5 interfaces | Major expansion |
| Session | Basic struct in `session/` | Full lifecycle, inflight, queue | Major expansion |
| Topic | `topics/` with match, validate | `topic/` with trie, router, alias | Moderate expansion |
| Client | Testing client in `client/` | Full client management | New component |
| Server/Broker | `broker/` with Server, Router | `server/` with adapters pattern | Restructure |
| Adapters | `adapter/`, `transport/` separate | Unified under `server/adapter/` | Consolidate and expand |
| Handlers | Empty `handlers/` package | `server/handlers/` with all handlers | New implementation |
| Metrics | None | `metrics/` with Prometheus | New component |

## Detailed Gap Analysis

### 1. Core Package (Packets → Core)

**Current State:**
- `packets/` package with comprehensive v3/v5 implementation
- `codec/` subpackage for binary encoding
- `pool/` subpackages for object pooling
- `sniffer.go` for version detection
- `validate.go` for validation

**Desired State:**
- `core/` package (rename)
- Same structure internally
- Additional validation methods

**Gap: MINIMAL** - Mostly a rename

**Changes Required:**
1. Rename `packets/` → `core/`
2. Update all imports across codebase
3. Add `ValidateFilter()` method to validator
4. Ensure all validation is in `validate.go`

### 2. Store Package

**Current State:**
```go
type MessageStore interface {
    Store(key string, payload []byte) error
    Retrieve(key string) ([]byte, error)
    Delete(key string) error
}

// Only MemoryStore implementation
```

**Desired State:**
```go
type Store interface {
    Messages() MessageStore
    Sessions() SessionStore
    Subscriptions() SubscriptionStore
    Retained() RetainedStore
    Wills() WillStore
    Close() error
}

// Memory and etcd implementations
```

**Gap: MAJOR** - 5 new interfaces, 2 implementations

**Changes Required:**
1. Define 5 storage interfaces (message, session, subscription, retained, will)
2. Create composite `Store` interface
3. Implement `memory/` backend for all interfaces
4. Implement `etcd/` backend for all interfaces
5. Add interface compliance tests
6. Add TTL support to message store
7. Add locking for session takeover

### 3. Session Package

**Current State:**
```go
type Session struct {
    Broker  BrokerInterface
    Conn    Connection
    ID      string
    Version int
}

// Methods: New(), Start(), Close(), Deliver(), handlePublish/Subscribe/PingReq
```

**Desired State:**
```go
type Session struct {
    ID              string
    Version         Version
    Connected       bool
    CleanStart      bool
    ExpiryInterval  uint32
    ReceiveMaximum  uint16
    MaxPacketSize   uint32
    Inflight        *InflightTracker
    OfflineQueue    *MessageQueue
    NextPacketID    uint16
    Will            *WillMessage
    Subscriptions   map[string]SubscribeOptions
}

// SessionManager, InflightTracker, MessageQueue
```

**Gap: MAJOR** - Complete rewrite

**Changes Required:**
1. Redesign `Session` struct with all MQTT options
2. Implement `SessionManager` for lifecycle management
3. Implement `InflightTracker` for QoS 1/2
4. Implement `MessageQueue` for offline messages
5. Add session state machine (connected, disconnected, expired)
6. Implement keep-alive timer management
7. Implement will message handling
8. Implement session takeover logic
9. Add packet ID generation

### 4. Topic Package (topics → topic)

**Current State:**
```go
// topics/validate.go
func ValidateTopic(topic string) error

// topics/match.go
func TopicMatch(filter, topic string) bool

// broker/router.go
type Router struct {
    root *node
}
```

**Desired State:**
```go
// topic/topic.go - Topic type
// topic/filter.go - Filter type
// topic/validate.go - Validation
// topic/match.go - Matching
// topic/trie.go - Subscription trie
// topic/router.go - Message router
// topic/alias.go - Topic aliases
// topic/shared.go - Shared subscriptions
```

**Gap: MODERATE** - Move router, add aliases and shared subs

**Changes Required:**
1. Rename `topics/` → `topic/`
2. Add `Topic` and `Filter` types
3. Move `Router` from `broker/` to `topic/`
4. Add `Trie` interface (separate from Router)
5. Implement topic alias management
6. Implement shared subscription support
7. Add `SubscribeOptions` type

### 5. Client Package

**Current State:**
```go
// client/client.go - Basic testing client
```

**Desired State:**
```go
type Client struct {
    Conn        Connection
    Session     *Session
    Connected   bool
    RateLimiter *RateLimiter
}

type ClientManager interface { ... }
type Authenticator interface { ... }
type Authorizer interface { ... }
```

**Gap: MAJOR** - Complete rewrite

**Changes Required:**
1. Design `Client` struct (connection + session)
2. Implement `ClientManager` for tracking
3. Design `Authenticator` interface
4. Design `Authorizer` interface
5. Implement session takeover coordination
6. Add rate limiting
7. Add connection limits

### 6. Server Package (broker → server)

**Current State:**
```go
// broker/server.go
type Server struct {
    listeners  []Frontend
    sessions   map[string]*session.Session
    router     *Router
}

// broker/interfaces.go
type Frontend interface { ... }
type Connection interface { ... }
```

**Desired State:**
```go
// server/server.go
type Server struct {
    adapters []Adapter
    clients  ClientManager
    sessions SessionManager
    router   Router
    store    Store
    metrics  Metrics
}

// server/adapter/adapter.go
type Adapter interface { ... }
```

**Gap: MAJOR** - Restructure around adapters

**Changes Required:**
1. Rename `broker/` → `server/`
2. Replace `Frontend` with `Adapter` interface
3. Move connection handling to adapters
4. Integrate `ClientManager`, `SessionManager`
5. Integrate `Store` and `Metrics`
6. Add configuration management
7. Implement graceful shutdown

### 7. Adapter Package

**Current State:**
```go
// adapter/http.go - HTTP adapter
// adapter/virtual.go - Virtual connections
// transport/tcp.go - TCP transport
// transport/ws.go - WebSocket transport
```

**Desired State:**
```go
// server/adapter/adapter.go - Interface
// server/adapter/tcp.go - TCP adapter
// server/adapter/tls.go - TLS adapter
// server/adapter/websocket.go - WebSocket adapter
// server/adapter/http.go - HTTP adapter
// server/adapter/coap.go - CoAP adapter
// server/adapter/opcua.go - OPC-UA adapter
```

**Gap: MODERATE** - Consolidate and add new adapters

**Changes Required:**
1. Move `transport/` into `server/adapter/`
2. Move `adapter/` into `server/adapter/`
3. Unify under `Adapter` interface
4. Add TLS adapter
5. Add CoAP adapter
6. Add OPC-UA adapter
7. Update adapter tests

### 8. Handlers Package

**Current State:**
```go
// handlers/ - Empty package
```

**Desired State:**
```go
// server/handlers/connect.go
// server/handlers/publish.go
// server/handlers/subscribe.go
// server/handlers/unsubscribe.go
// server/handlers/ping.go
// server/handlers/disconnect.go
// server/handlers/auth.go
```

**Gap: MAJOR** - New implementation

**Changes Required:**
1. Move from `server/` to `server/handlers/`
2. Design `PacketHandler` interface
3. Implement `ConnectHandler` with auth
4. Implement `PublishHandler` with QoS
5. Implement `SubscribeHandler`
6. Implement `UnsubscribeHandler`
7. Implement `PingHandler`
8. Implement `DisconnectHandler`
9. Implement `AuthHandler` (v5)

### 9. Metrics Package

**Current State:**
None

**Desired State:**
```go
// metrics/metrics.go - Interface
// metrics/prometheus.go - Prometheus exporter
// metrics/noop.go - No-op implementation
```

**Gap: NEW** - Complete implementation

**Changes Required:**
1. Design metrics interface
2. Implement Prometheus exporter
3. Implement no-op for testing
4. Add metrics to all components
5. Add HTTP endpoint for scraping

---

## Implementation Plan

### Phase 1: Package Reorganization (Foundation)

Reorganize packages to match desired structure without changing functionality.

**Tasks:**

1.1. **Rename `packets/` → `core/`**
   - Move all files
   - Update all imports
   - Verify tests pass

1.2. **Rename `topics/` → `topic/`**
   - Move all files
   - Update all imports
   - Add `Topic` and `Filter` types

1.3. **Rename `broker/` → `server/`**
   - Move all files
   - Update all imports

1.4. **Consolidate transports and adapters**
   - Move `transport/` → `server/adapter/`
   - Move `adapter/` → `server/adapter/`
   - Unify under `Adapter` interface

1.5. **Move router to topic package**
   - Move `server/router.go` → `topic/router.go`
   - Update dependencies

**Deliverables:**
- Clean package structure
- All existing tests passing
- No functional changes

### Phase 2: Store Package Expansion

Build comprehensive storage abstraction.

**Tasks:**

2.1. **Design storage interfaces**
   - `MessageStore` interface
   - `SessionStore` interface
   - `SubscriptionStore` interface
   - `RetainedStore` interface
   - `WillStore` interface
   - Composite `Store` interface

2.2. **Implement memory backend**
   - `store/memory/store.go` - Composite
   - `store/memory/message.go`
   - `store/memory/session.go`
   - `store/memory/subscription.go`
   - `store/memory/retained.go`
   - `store/memory/will.go`

2.3. **Add storage tests**
   - Interface compliance tests
   - Unit tests for memory backend
   - Concurrency tests

2.4. **Implement etcd backend (optional, can defer)**
   - `store/etcd/store.go`
   - Individual store implementations
   - Integration tests with etcd

**Deliverables:**
- Complete storage interfaces
- Memory backend implementation
- Comprehensive test suite

### Phase 3: Session Management

Build complete session lifecycle management.

**Tasks:**

3.1. **Redesign Session struct**
   - Add all MQTT options
   - Add state fields
   - Add QoS tracking fields

3.2. **Implement InflightTracker**
   - Track QoS 1/2 messages
   - Handle retries
   - Handle acknowledgments

3.3. **Implement MessageQueue**
   - Queue messages for offline clients
   - Handle queue limits
   - Drain on reconnect

3.4. **Implement SessionManager**
   - Session creation
   - Session lookup
   - Session destruction
   - Session takeover
   - Expiry handling

3.5. **Add keep-alive management**
   - Timer per session
   - Timeout detection
   - Disconnect on timeout

3.6. **Add will message handling**
   - Store will on connect
   - Trigger on unclean disconnect
   - Clear on clean disconnect

3.7. **Session tests**
   - Lifecycle tests
   - QoS flow tests
   - Takeover tests
   - Expiry tests

**Deliverables:**
- Complete session management
- QoS 1/2 support
- Keep-alive enforcement
- Will message support

### Phase 4: Topic Management Enhancement

Enhance topic handling with missing features.

**Tasks:**

4.1. **Add topic alias support**
   - `topic/alias.go`
   - Per-connection alias mapping
   - Alias negotiation in CONNECT

4.2. **Add shared subscription support**
   - `topic/shared.go`
   - Parse `$share/{group}/{filter}` format
   - Load balancing within group

4.3. **Enhance trie with options**
   - `SubscribeOptions` type
   - NoLocal, RetainAsPublished, RetainHandling
   - Store options with subscription

4.4. **Topic tests**
   - Alias tests
   - Shared subscription tests
   - Options tests

**Deliverables:**
- Topic alias support (v5)
- Shared subscriptions (v5)
- Subscription options (v5)

### Phase 5: Client Management

Build client tracking and authentication.

**Tasks:**

5.1. **Design Client struct**
   - Connection wrapper
   - Session reference
   - State tracking

5.2. **Implement ClientManager**
   - Client registration
   - Client lookup
   - Client counting
   - Connection limits

5.3. **Design auth interfaces**
   - `Authenticator` interface
   - `Authorizer` interface
   - Default implementations

5.4. **Implement auth backends**
   - Password file auth
   - JWT auth (optional)
   - ACL file authorization

5.5. **Add rate limiting**
   - Per-client rate limits
   - Configurable limits
   - Rate limit responses

5.6. **Client tests**
   - Manager tests
   - Auth tests
   - Rate limit tests

**Deliverables:**
- Client management
- Authentication framework
- Authorization framework
- Rate limiting

### Phase 6: Packet Handlers

Implement modular packet handlers.

**Tasks:**

6.1. **Design PacketHandler interface**
   - Method per packet type
   - Error handling
   - Context passing

6.2. **Implement ConnectHandler**
   - Version detection
   - Auth validation
   - Session creation/takeover
   - CONNACK response

6.3. **Implement PublishHandler**
   - Topic validation
   - Authorization check
   - QoS handling
   - Message routing
   - Retained message storage

6.4. **Implement SubscribeHandler**
   - Filter validation
   - Authorization check
   - Subscription registration
   - Retained message delivery
   - SUBACK response

6.5. **Implement UnsubscribeHandler**
   - Subscription removal
   - UNSUBACK response

6.6. **Implement PingHandler**
   - PINGRESP response
   - Keep-alive refresh

6.7. **Implement DisconnectHandler**
   - Clean disconnect
   - Session cleanup
   - Will handling

6.8. **Implement AuthHandler (v5)**
   - Multi-step auth
   - Challenge/response

6.9. **Handler tests**
   - Unit tests per handler
   - Integration tests for flows

**Deliverables:**
- All packet handlers
- Complete QoS flows
- Full MQTT compliance

### Phase 7: Adapter Expansion

Add new protocol adapters.

**Tasks:**

7.1. **Implement TLS adapter**
   - Certificate loading
   - TLS configuration
   - mTLS support (optional)

7.2. **Implement CoAP adapter**
   - CoAP server
   - MQTT mapping
   - Observe support

7.3. **Implement OPC-UA adapter**
   - OPC-UA server
   - Node-to-topic mapping
   - Monitored items

7.4. **Adapter tests**
   - Unit tests per adapter
   - Integration tests

**Deliverables:**
- TLS support
- CoAP bridge
- OPC-UA bridge

### Phase 8: Metrics & Observability

Add comprehensive metrics.

**Tasks:**

8.1. **Design metrics interface**
   - Counter, gauge, histogram types
   - Label support
   - Registration API

8.2. **Implement Prometheus exporter**
   - All broker metrics
   - HTTP endpoint
   - Metric naming conventions

8.3. **Add metrics throughout codebase**
   - Connection metrics
   - Message metrics
   - QoS metrics
   - Subscription metrics
   - Session metrics
   - Performance metrics

8.4. **Metrics tests**
   - Exporter tests
   - Value verification

**Deliverables:**
- Complete metrics coverage
- Prometheus endpoint
- Grafana dashboard (optional)

### Phase 9: Configuration & Production Hardening

Add production features.

**Tasks:**

9.1. **Implement configuration**
   - YAML config file
   - Environment variable override
   - Validation

9.2. **Implement graceful shutdown**
   - Signal handling
   - Ordered shutdown
   - Connection draining

9.3. **Add logging framework**
   - Structured logging (slog)
   - Log levels
   - Configurable output

9.4. **Add error handling**
   - Error types
   - Error wrapping
   - User-friendly messages

9.5. **Production tests**
   - Config tests
   - Shutdown tests
   - Error handling tests

**Deliverables:**
- Configuration file support
- Graceful shutdown
- Structured logging
- Robust error handling

### Phase 10: Integration & Stress Testing

Comprehensive testing for production readiness.

**Tasks:**

10.1. **Integration test suite**
   - Full PubSub flows
   - QoS 0/1/2 tests
   - Session persistence tests
   - Will message tests
   - Retained message tests
   - All adapter tests

10.2. **Conformance tests**
   - Eclipse Paho compatibility
   - mosquitto compatibility
   - MQTT.fx compatibility

10.3. **Stress tests**
   - 10K+ concurrent connections
   - 100K+ messages/second
   - Memory usage under load
   - CPU usage under load

10.4. **Chaos tests**
   - Network partitions
   - Client disconnections
   - Server restarts

**Deliverables:**
- Complete test coverage
- Performance benchmarks
- Conformance certification

---

## Migration Path

### Backward Compatibility

During migration, maintain backward compatibility:

1. **Import aliases**: Use aliases when renaming packages
2. **Interface compatibility**: Existing interfaces stay compatible
3. **Feature flags**: Enable new features via config

### Step-by-Step Migration

```
v0.1 (current) → v0.2 (Phase 1-2) → v0.3 (Phase 3-4) → v0.4 (Phase 5-6) → v0.5 (Phase 7-8) → v1.0 (Phase 9-10)
```

### Breaking Changes

| Phase | Breaking Change | Migration |
|-------|-----------------|-----------|
| 1 | Package renames | Update imports |
| 2 | Store interface | Implement new interfaces |
| 3 | Session struct | Update session creation |
| 5 | Client management | Use ClientManager |
| 6 | Handler dispatch | Use new handler system |

---

## Timeline Estimation

Each phase can be worked on independently once Phase 1 is complete.

| Phase | Description | Complexity |
|-------|-------------|------------|
| 1 | Package Reorganization | Low |
| 2 | Store Expansion | Medium |
| 3 | Session Management | High |
| 4 | Topic Enhancement | Medium |
| 5 | Client Management | Medium |
| 6 | Packet Handlers | High |
| 7 | Adapter Expansion | Medium |
| 8 | Metrics | Low |
| 9 | Production Hardening | Medium |
| 10 | Testing | Medium |

**Critical Path**: Phase 1 → Phase 2 → Phase 3 → Phase 6

**Parallelizable**:
- Phase 4 (after Phase 1)
- Phase 5 (after Phase 2)
- Phase 7 (after Phase 1)
- Phase 8 (after Phase 1)

---

## Summary of Changes

### Files to Create

```
# Core (rename only)
core/ (from packets/)

# Store (new)
store/store.go
store/message.go
store/session.go
store/subscription.go
store/retained.go
store/will.go
store/memory/store.go
store/memory/message.go
store/memory/session.go
store/memory/subscription.go
store/memory/retained.go
store/memory/will.go
store/etcd/ (optional)

# Session (expand)
session/manager.go
session/state.go
session/inflight.go
session/queue.go
session/keepalive.go
session/will.go

# Topic (expand)
topic/topic.go
topic/filter.go
topic/trie.go
topic/alias.go
topic/shared.go

# Client (new)
client/client.go
client/manager.go
client/takeover.go
client/auth.go
client/acl.go

# Server (reorganize)
server/server.go
server/config.go
server/handler.go
server/adapter/adapter.go
server/adapter/tcp.go
server/adapter/tls.go
server/adapter/websocket.go
server/adapter/http.go
server/adapter/coap.go
server/adapter/opcua.go
server/handlers/connect.go
server/handlers/publish.go
server/handlers/subscribe.go
server/handlers/unsubscribe.go
server/handlers/ping.go
server/handlers/disconnect.go
server/handlers/auth.go

# Metrics (new)
metrics/metrics.go
metrics/prometheus.go
metrics/noop.go

# Integration tests (expand)
integration/qos_test.go
integration/session_test.go
integration/will_test.go
integration/retained_test.go
integration/adapter_test.go
integration/stress_test.go
```

### Files to Delete/Move

```
# Move
packets/ → core/
topics/ → topic/
broker/ → server/
transport/ → server/adapter/
adapter/ → server/adapter/

# Delete (after migration)
handlers/ (empty)
```

### Test Coverage Requirements

Every new component requires:
1. Unit tests with >80% coverage
2. Integration tests for cross-component flows
3. Benchmarks for performance-critical paths
4. Example usage in `examples/`
