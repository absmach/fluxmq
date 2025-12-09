# Implementation Status & Roadmap

This document tracks what exists, what's missing, and the path forward.

## Current State Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            IMPLEMENTATION STATUS                            │
├──────────────────────┬──────────────┬───────────────────────────────────────┤
│ Component            │ Status       │ Notes                                 │
├──────────────────────┼──────────────┼───────────────────────────────────────┤
│ packets/ (core)      │ ✅ Complete  │ v3 + v5, pooling, zero-copy           │
│ broker/broker        │ ✅ Complete  │ Clean architecture, routing           │
│ broker/connection    │ ✅ Complete  │ MQTT codec with version detection     │
│ broker/router        │ ✅ Complete  │ Trie-based, wildcards                 │
│ handlers/            │ ✅ Complete  │ All packet types, dispatcher          │
│ session/             │ ✅ Complete  │ Manager, inflight, queue              │
│ store/memory         │ ✅ Complete  │ Sessions, retained, subscriptions     │
│ topics/              │ ✅ Complete  │ Validation, matching                  │
│ server/tcp           │ ✅ Complete  │ Robust TCP server w/ graceful shutdown│
│ codec/               │ ✅ Complete  │ Encode/decode/zerocopy utilities      │
│ client/              │ ⚠️ Partial   │ Basic test client                     │
├──────────────────────┴──────────────┴───────────────────────────────────────┤
│ ✅ RECENTLY COMPLETED (Architecture Refactoring)                            │
├──────────────────────┬──────────────┬───────────────────────────────────────┤
│ TCP Server (Phase 1) │ ✅ Done      │ server/tcp with graceful shutdown     │
│ Codec separation     │ ✅ Done      │ broker/connection.go MQTT codec       │
│ Remove Frontend      │ ✅ Done      │ Removed broker.Frontend abstraction   │
│ Remove adapters      │ ✅ Done      │ Deleted adapter/http, adapter/virtual │
│ Context shutdown     │ ✅ Done      │ main.go uses context for lifecycle    │
│ Documentation        │ ✅ Done      │ Updated architecture.md, walkthrough  │
├──────────────────────┴──────────────┴───────────────────────────────────────┤
│ ✅ PHASE 2 COMPLETE (Protocol Completeness)                                 │
├──────────────────────┬──────────────┬───────────────────────────────────────┤
│ QoS 1/2 retry        │ ✅ Complete  │ 20s timeout, DUP flag, session.go     │
│ Retained on sub      │ ✅ Complete  │ handlers/broker.go sendRetained       │
│ Will messages        │ ✅ Complete  │ Background trigger, manager.go        │
│ Session expiry       │ ✅ Complete  │ Background expiry, manager.go         │
├──────────────────────┼──────────────┼───────────────────────────────────────┤
│ WebSocket server     │ ❌ Missing   │ Needs new server/ws implementation    │
│ HTTP server          │ ❌ Missing   │ Needs new server/http for REST API    │
│ Topic aliases        │ ❌ Missing   │ v5 feature                            │
│ Shared subscriptions │ ❌ Missing   │ $share/{group}/{filter}               │
│ CoAP adapter         │ ❌ Missing   │ TCP first, UDP later                  │
│ TLS support          │ ❌ Missing   │ TLS config for server/tcp             │
│ Configuration        │ ✅ Complete  │ YAML config, validation, defaults     │
│ Metrics              │ ❌ Missing   │ Prometheus                            │
│ Distributed          │ ❌ Missing   │ etcd, raft, clustering                │
└──────────────────────┴──────────────┴───────────────────────────────────────┘
```

## What's Working Now

### 1. Core Packets (`packets/`)
- Full MQTT 3.1.1 packet set (14 types)
- Full MQTT 5.0 packet set (15 types including AUTH)
- Properties support for v5
- Object pooling (`sync.Pool`) for all packet types
- Zero-copy parsing for PUBLISH payload
- Protocol version detection (sniffer)
- VBI encoding/decoding

### 2. Broker (`broker/`)
- Server with session management
- Connection lifecycle (CONNECT → session → loop → cleanup)
- Topic router (trie-based subscription matching)
- Message distribution to subscribers
- QoS downgrade based on subscription
- Offline message queuing for QoS > 0

### 3. Handlers (`handlers/`)
- PUBLISH handler (QoS 0/1/2)
- SUBSCRIBE handler with SUBACK
- UNSUBSCRIBE handler with UNSUBACK
- PINGREQ handler
- DISCONNECT handler
- QoS acknowledgment handlers (PUBACK, PUBREC, PUBREL, PUBCOMP)
- Dispatcher routing packets to handlers

### 4. Session (`session/`)
- Full session state (connected, disconnected, expiry)
- InflightTracker for QoS 1/2 messages
- MessageQueue for offline delivery
- Keep-alive timer
- Topic alias management
- Packet ID generation
- Session persistence support

### 5. Store (`store/memory/`)
- SessionStore (get, save, delete)
- RetainedStore (set, get, delete, match)
- SubscriptionStore
- WillStore
- Composite Store interface

### 6. Transport (`transport/`)
- TCP frontend with version detection
- WebSocket frontend (basic)

### 7. Adapters (`adapter/`)
- HTTP adapter (POST /publish)
- Virtual connection for testing

---

## What's Missing

### Priority 1: Protocol Completeness ✅ COMPLETE

All MQTT spec compliance features are now implemented:

#### 1.1 QoS 1/2 Retry Mechanism ✅
**Status:** Implemented in `session/session.go`
**Implementation:**
- `retryLoop()` runs every second checking for expired messages
- 20 second retry timeout
- DUP flag set on retransmitted PUBLISH packets
- Infinite retries until acknowledged
- See `resendMessage()` method

#### 1.2 Retained Message Delivery on Subscribe ✅
**Status:** Implemented in `handlers/broker.go`
**Implementation:**
- `HandleSubscribe()` calls `sendRetainedMessages()` for each subscription
- Queries `retained.Match(filter)` for matching messages
- QoS downgrade: min(publish_qos, subscribe_qos)
- Retain flag preserved on delivery
- Integration test: `TestRetainedMessages`

#### 1.3 Will Message Triggering ✅
**Status:** Implemented in `session/manager.go`
**Implementation:**
- Background `triggerWills()` runs every second
- Will stored on unclean disconnect
- Delay interval support via `GetPending()`
- Callback wired in `broker.NewBroker()`
- Integration test: `TestWillMessage`

#### 1.4 Session Expiry ✅
**Status:** Implemented in `session/manager.go`
**Implementation:**
- Background `expireSessions()` runs every second
- Checks disconnected sessions for expiry
- `ExpiryInterval` from CONNECT packet
- Complete cleanup: subscriptions, messages, storage
- CleanStart sessions with expiry=0 destroyed immediately

### Priority 2: Additional Adapters

#### 2.1 WebSocket Subprotocol
**Current:** Basic WebSocket that wraps raw MQTT.
**Needed:**
- Proper "mqtt" subprotocol negotiation
- Binary message framing
- Ping/pong handling

#### 2.2 CoAP/TCP Adapter
**Location:** `adapter/coap.go`
**Mapping:**
```
CoAP Method → MQTT
───────────────────
POST        → PUBLISH
GET         → (subscribe + single response)
GET+Observe → SUBSCRIBE (streaming)
DELETE      → UNSUBSCRIBE
```

**Implementation steps:**
1. Add go-coap dependency
2. Create CoAP server listening on :5683
3. Map CoAP resources to MQTT topics
4. Handle Observe for subscriptions
5. Map CoAP response codes to MQTT reason codes

#### 2.3 TLS Support
**Location:** `transport/tls.go`
**Needed:**
- Certificate loading
- TLS config options
- mTLS support (client certs)
- Cipher suite configuration

```go
type TLSFrontend struct {
    TCPFrontend
    tlsConfig *tls.Config
}

func NewTLSFrontend(addr string, certFile, keyFile string) (*TLSFrontend, error) {
    cert, err := tls.LoadX509KeyPair(certFile, keyFile)
    // ...
}
```

### Priority 3: Production Features

#### 3.1 Configuration File
**Location:** `config/config.go`
**Format:** YAML

```go
type Config struct {
    Server   ServerConfig   `yaml:"server"`
    Session  SessionConfig  `yaml:"session"`
    Storage  StorageConfig  `yaml:"storage"`
    Limits   LimitsConfig   `yaml:"limits"`
    Logging  LoggingConfig  `yaml:"logging"`
    Metrics  MetricsConfig  `yaml:"metrics"`
}

func LoadConfig(path string) (*Config, error)
```

#### 3.2 Structured Logging
**Location:** `internal/log/log.go`
**Implementation:** Use `log/slog` (Go 1.21+)

```go
var logger *slog.Logger

func Init(cfg LoggingConfig) {
    var handler slog.Handler
    if cfg.Format == "json" {
        handler = slog.NewJSONHandler(os.Stdout, nil)
    } else {
        handler = slog.NewTextHandler(os.Stdout, nil)
    }
    logger = slog.New(handler)
}

func Info(msg string, args ...any) { logger.Info(msg, args...) }
func Error(msg string, args ...any) { logger.Error(msg, args...) }
```

#### 3.3 Prometheus Metrics
**Location:** `metrics/prometheus.go`

```go
var (
    ConnectionsTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mqtt_connections_total",
            Help: "Current number of connections",
        },
        []string{"protocol"}, // mqtt, ws, http
    )

    MessagesPublished = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mqtt_messages_published_total",
            Help: "Total messages published",
        },
        []string{"qos"},
    )

    // ... more metrics
)
```

#### 3.4 Graceful Shutdown
**Location:** `broker/server.go`

```go
func (s *Server) Shutdown(ctx context.Context) error {
    // 1. Stop accepting new connections
    for _, l := range s.listeners {
        l.Close()
    }

    // 2. Send DISCONNECT to all clients (v5)
    // 3. Wait for inflight messages to complete (with timeout)
    // 4. Close all sessions
    // 5. Close storage
}
```

### Priority 4: MQTT v5 Features

#### 4.1 Topic Aliases
**Current:** Fields exist in session but not used.
**Needed:**
- Track aliases per connection direction
- Apply aliases when sending PUBLISH
- Resolve aliases when receiving PUBLISH

#### 4.2 Shared Subscriptions
**Format:** `$share/{ShareName}/{TopicFilter}`
**Needed:**
- Parse shared subscription format
- Group subscriptions by share name
- Load balance delivery within group (round-robin)

```go
// In topics/shared.go:
func ParseSharedSubscription(filter string) (shareName, topicFilter string, isShared bool) {
    if strings.HasPrefix(filter, "$share/") {
        parts := strings.SplitN(filter[7:], "/", 2)
        return parts[0], parts[1], true
    }
    return "", filter, false
}
```

### Priority 5: Distributed (Future)

#### 5.1 etcd Storage Backend
**Location:** `store/etcd/`
**Key Schema:**
```
/mqtt/sessions/{clientID}         → Session JSON
/mqtt/retained/{topic}            → Message
/mqtt/subscriptions/{filter_hash} → []Subscription
/mqtt/locks/{clientID}            → Lease (for takeover)
```

#### 5.2 Cluster Membership
- SWIM protocol for failure detection
- Gossip for subscription propagation
- Consistent hashing for topic ownership

#### 5.3 Cross-Node Routing
- Forward PUBLISH to nodes with matching subscribers
- Session takeover coordination
- Subscription sync

---

## Implementation Roadmap

### Phase 1: Architecture Refactoring ✅ COMPLETED

```
┌──────────────────────────────────────────────────────────────────┐
│  Task                              │ Status     │ Files Modified  │
├────────────────────────────────────┼────────────┼─────────────────┤
│ 1.1 Robust TCP server              │ ✅ Done    │ server/tcp/     │
│     - Graceful shutdown            │            │   server.go     │
│     - Connection limiting          │            │   server_test.go│
│     - TCP optimizations            │            │                 │
├────────────────────────────────────┼────────────┼─────────────────┤
│ 1.2 MQTT codec separation          │ ✅ Done    │ broker/         │
│     - Protocol detection           │            │   connection.go │
│     - Version auto-detect          │            │                 │
├────────────────────────────────────┼────────────┼─────────────────┤
│ 1.3 Remove Frontend abstraction    │ ✅ Done    │ broker/         │
│     - Direct HandleConnection      │            │   interfaces.go │
│     - Broker implements Handler    │            │   broker.go     │
├────────────────────────────────────┼────────────┼─────────────────┤
│ 1.4 Remove old transport/adapter   │ ✅ Done    │ Deleted:        │
│                                    │            │   transport/    │
│                                    │            │   adapter/      │
├────────────────────────────────────┼────────────┼─────────────────┤
│ 1.5 Update main.go                 │ ✅ Done    │ cmd/broker/     │
│     - Context-based shutdown       │            │   main.go       │
│     - slog logging                 │            │                 │
├────────────────────────────────────┼────────────┼─────────────────┤
│ 1.6 Update documentation           │ ✅ Done    │ docs/           │
│                                    │            │   architecture  │
│                                    │            │   walkthrough   │
└────────────────────────────────────┴────────────┴─────────────────┘
```

### Phase 2: Protocol Completeness

```
┌──────────────────────────────────────────────────────────────────┐
│  Task                              │ Files to Modify/Create      │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.1 QoS retry mechanism            │ session/inflight.go         │
│                                    │ session/session.go          │
│                                    │ broker/broker.go            │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.2 Retained on subscribe          │ handlers/broker.go          │
│                                    │ broker/broker.go            │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.3 Will message trigger           │ session/session.go          │
│                                    │ broker/broker.go            │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.4 Session expiry                 │ session/manager.go          │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.5 Integration tests              │ integration/qos_test.go     │
│                                    │ integration/will_test.go    │
│                                    │ integration/retained_test.go│
└────────────────────────────────────┴─────────────────────────────┘
```

### Phase 3: Additional Protocol Servers

```
┌──────────────────────────────────────────────────────────────────┐
│  Task                              │ Files to Create             │
├────────────────────────────────────┼─────────────────────────────┤
│ 3.1 WebSocket server               │ server/ws/server.go         │
│     - MQTT subprotocol negotiation │ server/ws/server_test.go    │
│     - Binary framing               │                             │
├────────────────────────────────────┼─────────────────────────────┤
│ 3.2 HTTP/REST server               │ server/http/server.go       │
│     - POST /publish endpoint       │ server/http/publish.go      │
│     - GET /subscribe (SSE)         │ server/http/subscribe.go    │
│                                    │ server/http/server_test.go  │
├────────────────────────────────────┼─────────────────────────────┤
│ 3.3 CoAP server                    │ server/coap/server.go       │
│     - POST → PUBLISH mapping       │ server/coap/observe.go      │
│     - Observe for subscriptions    │ server/coap/server_test.go  │
├────────────────────────────────────┼─────────────────────────────┤
│ 3.4 TLS support                    │ server/tcp/tls.go           │
│     - TLS config in server/tcp     │ Updated: server/tcp/server.go│
│     - Certificate loading          │                             │
└────────────────────────────────────┴─────────────────────────────┘
```

### Phase 4: Production Hardening

```
┌──────────────────────────────────────────────────────────────────┐
│  Task                              │ Files to Create             │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.1 Configuration expansion        │ config/config.go (update)   │
│                                    │ config/config_test.go       │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.2 Metrics                        │ metrics/metrics.go          │
│                                    │ metrics/prometheus.go       │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.3 Connection limits              │ broker/broker.go (update)   │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.4 Auth/ACL hooks                 │ auth/auth.go                │
│                                    │ auth/acl.go                 │
└────────────────────────────────────┴─────────────────────────────┘
```

### Phase 5: Distributed (Future)

```
┌──────────────────────────────────────────────────────────────────┐
│  Task                              │ Files to Modify/Create      │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.1 QoS retry mechanism            │ session/inflight.go         │
│                                    │ broker/server.go            │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.2 Retained on subscribe          │ handlers/broker.go          │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.3 Will message trigger           │ session/session.go          │
│                                    │ broker/server.go            │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.4 Session expiry                 │ session/manager.go          │
├────────────────────────────────────┼─────────────────────────────┤
│ 2.5 Integration tests              │ integration/qos_test.go     │
│                                    │ integration/will_test.go    │
│                                    │ integration/retained_test.go│
└────────────────────────────────────┴─────────────────────────────┘
```

### Phase 3: Additional Adapters

```
┌──────────────────────────────────────────────────────────────────┐
│  Task                              │ Files to Create             │
├────────────────────────────────────┼─────────────────────────────┤
│ 3.1 WebSocket subprotocol          │ transport/ws.go (update)    │
├────────────────────────────────────┼─────────────────────────────┤
│ 3.2 CoAP/TCP adapter               │ adapter/coap.go             │
│                                    │ adapter/coap_test.go        │
├────────────────────────────────────┼─────────────────────────────┤
│ 3.3 TLS support                    │ transport/tls.go            │
│                                    │ transport/tls_test.go       │
└────────────────────────────────────┴─────────────────────────────┘
```

### Phase 4: Production Hardening

```
┌──────────────────────────────────────────────────────────────────┐
│  Task                              │ Files to Create             │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.1 Configuration                  │ config/config.go            │
│                                    │ config/config_test.go       │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.2 Logging                        │ internal/log/log.go         │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.3 Metrics                        │ metrics/metrics.go          │
│                                    │ metrics/prometheus.go       │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.4 Graceful shutdown              │ broker/server.go (update)   │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.5 Connection limits              │ broker/server.go (update)   │
├────────────────────────────────────┼─────────────────────────────┤
│ 4.6 Main binary                    │ cmd/mqttd/main.go           │
└────────────────────────────────────┴─────────────────────────────┘
```

### Phase 5: Distributed

```
┌──────────────────────────────────────────────────────────────────┐
│  Task                              │ Files to Create             │
├────────────────────────────────────┼─────────────────────────────┤
│ 5.1 etcd store                     │ store/etcd/store.go         │
│                                    │ store/etcd/session.go       │
│                                    │ store/etcd/retained.go      │
├────────────────────────────────────┼─────────────────────────────┤
│ 5.2 Cluster membership             │ cluster/membership.go       │
│                                    │ cluster/swim.go             │
├────────────────────────────────────┼─────────────────────────────┤
│ 5.3 Cross-node routing             │ cluster/router.go           │
│                                    │ cluster/forward.go          │
├────────────────────────────────────┼─────────────────────────────┤
│ 5.4 In-memory cache                │ store/cache/cache.go        │
│                                    │ store/cache/lru.go          │
└────────────────────────────────────┴─────────────────────────────┘
```

---

## Recommended Next Steps

### Immediate (This Week)

1. **Retained message delivery on SUBSCRIBE**
   - Modify `handlers/broker.go` HandleSubscribe
   - Query retained store for matching messages
   - Send with retain flag set
   - Add integration test

2. **Will message triggering**
   - Add callback in session for will trigger
   - Wire up in broker to call Publish
   - Add integration test for unclean disconnect

### Short Term

3. **QoS 1/2 retry mechanism**
   - Add timer to InflightTracker
   - Implement retry with DUP flag
   - Add integration test for retry scenarios

4. **Session expiry**
   - Add background checker to SessionManager
   - Clean up subscriptions on expiry
   - Add integration test

### Medium Term

5. **CoAP/TCP adapter**
   - Start with POST → PUBLISH mapping
   - Add Observe for subscriptions
   - Integration test with CoAP client

6. **Configuration file**
   - Define YAML schema
   - Load and validate config
   - Wire up to server startup

---

## Test Coverage Gaps

| Area | Current | Needed |
|------|---------|--------|
| QoS 1 flow | Basic | Retry, timeout, duplicate |
| QoS 2 flow | Basic | Full state machine |
| Retained | Store only | Delivery on subscribe |
| Will messages | Parsed | Trigger test |
| Session expiry | None | Expiry after interval |
| WebSocket | Basic | Subprotocol, binary frames |
| Stress | None | 10K connections, 100K msg/s |

---

## Dependencies to Add

```
# For CoAP adapter
go get github.com/plgd-dev/go-coap/v3

# For metrics
go get github.com/prometheus/client_golang

# For config (already in stdlib for YAML via encoding)
go get gopkg.in/yaml.v3

# For etcd (future)
go get go.etcd.io/etcd/client/v3
```

---

## Architecture Decisions Still Needed

1. **Adapter registration** - How do adapters register with the server?
   - Option A: Compile-time (import side effects)
   - Option B: Runtime via config
   - Recommendation: Runtime via config

2. **Topic alias scope** - Per-connection or per-session?
   - MQTT spec: Per-connection
   - Our implementation: Already per-connection (cleared on disconnect)

3. **Shared subscription load balancing** - Round-robin or random?
   - Option A: Round-robin (predictable)
   - Option B: Random (simpler)
   - Option C: Configurable
   - Recommendation: Round-robin as default

4. **etcd key prefix** - What namespace?
   - Current thinking: `/mqtt/{cluster_name}/`
   - This allows multiple clusters in same etcd
