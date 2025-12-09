# MQTT Broker Implementation Plan
## Production-Ready Roadmap: Tests → v5 → Storage → Clustering

**Last Updated:** 2025-12-09
**Status:** Phase 0 Complete (Code Quality Fixes)

---

## Executive Summary

This document provides a comprehensive roadmap to transform the current MQTT broker implementation into a production-ready, scalable, and maintainable system. The plan prioritizes:

1. **Comprehensive Testing** - Foundation for reliability and confidence
2. **MQTT 5.0 Completion** - Full protocol compliance
3. **Persistent Storage** - Scalability and data durability
4. **Clustering Support** - Horizontal scaling and high availability
5. **Production Hardening** - Observability, security, performance

### Current State
- ✅ Core MQTT 3.1.1 implementation complete
- ✅ Basic MQTT 5.0 packet support
- ✅ Clean architecture with proper separation of concerns
- ✅ Memory-based storage
- ✅ TCP server with graceful shutdown
- ⚠️ Limited test coverage for edge cases
- ❌ MQTT 5.0 features incomplete (topic aliases, shared subscriptions)
- ❌ No persistent storage backend
- ❌ No clustering support
- ❌ Limited production hardening

---

## Phase 0: Code Quality & Foundation ✅ COMPLETE

**Status:** Completed 2025-12-09
**Objective:** Fix critical issues to establish a solid foundation for future work.

### Completed Tasks

| Task | Status | Impact |
|------|--------|--------|
| Fix race conditions in session state access | ✅ | Critical - Prevents data corruption |
| Replace fmt.Printf with structured slog | ✅ | High - Production observability |
| Fix potential deadlock in Session.Disconnect | ✅ | Critical - Stability |
| Remove unused code (parseLogLevel) | ✅ | Low - Code cleanliness |
| Update to Go 1.18+ idioms (any vs interface{}) | ✅ | Low - Modern Go |
| Add error wrapping with context | ✅ | Medium - Better debugging |

### Test Results
```bash
✓ All unit tests passing
✓ All integration tests passing
✓ Race detector: CLEAN
```

---

## Phase 1: Comprehensive Testing (Priority 1)

**Timeline:** 2-3 weeks
**Objective:** Achieve >85% test coverage with robust edge case testing to ensure reliability and enable confident refactoring.

### Why Tests First?
1. **Foundation for Changes** - Confidence to refactor and add features
2. **Regression Prevention** - Catch bugs before they reach production
3. **Documentation** - Tests serve as executable documentation
4. **Fast Development** - Catch issues immediately, not in production
5. **Maintainability** - Easier to onboard new developers

### 1.1 Unit Test Expansion

#### 1.1.1 Session Package Tests
**File:** `session/session_test.go`, `session/manager_test.go`

**Coverage Gaps:**
- [ ] Session reconnection with different clean start flags
- [ ] Concurrent session updates (version, keepalive, will)
- [ ] Keep-alive timer expiration and renewal
- [ ] Topic alias management (set, get, clear on disconnect)
- [ ] Session state transitions under concurrent operations
- [ ] Inflight message retry edge cases:
  - [ ] Packet ID wraparound
  - [ ] Concurrent ack and retry
  - [ ] Inflight queue at capacity
- [ ] Offline queue edge cases:
  - [ ] Queue overflow behavior
  - [ ] Message ordering guarantees
  - [ ] Drain during concurrent enqueue

**New Test Files:**
```
session/
├── session_test.go          # Existing
├── manager_test.go           # Existing
├── inflight_race_test.go    # NEW: Race condition tests
├── keepalive_test.go        # NEW: Keep-alive timer tests
├── reconnect_test.go        # NEW: Reconnection scenarios
└── bench_test.go            # NEW: Performance benchmarks
```

**Example Tests:**
```go
// session/reconnect_test.go
func TestSessionReconnectRaceCondition(t *testing.T)
func TestSessionTakeoverWhilePublishing(t *testing.T)
func TestSessionUpdateDuringInflightRetry(t *testing.T)
```

#### 1.1.2 Broker Package Tests
**File:** `broker/broker_test.go`, `broker/router_test.go`

**Coverage Gaps:**
- [ ] Concurrent PUBLISH to same topic
- [ ] Subscribe/Unsubscribe during message delivery
- [ ] Router trie edge cases:
  - [ ] Deeply nested topics (100+ levels)
  - [ ] Topic with special characters
  - [ ] Wildcard edge cases (`#`, `+`, combined)
- [ ] Connection handling:
  - [ ] Invalid CONNECT packets
  - [ ] Protocol version detection edge cases
  - [ ] Malformed packets

**New Test Files:**
```
broker/
├── broker_test.go           # Existing
├── router_test.go           # Existing
├── connection_test.go       # NEW: Codec tests
├── concurrent_test.go       # NEW: Concurrency tests
└── malformed_test.go        # NEW: Malformed packet handling
```

#### 1.1.3 Handlers Package Tests
**File:** `handlers/broker_test.go`

**Coverage Gaps:**
- [ ] QoS 2 full flow (PUBLISH → PUBREC → PUBREL → PUBCOMP)
- [ ] Duplicate packet handling
- [ ] Out-of-order acknowledgments
- [ ] Packet validation edge cases
- [ ] Error recovery in handlers

**New Test Files:**
```
handlers/
├── broker_test.go           # Existing
├── qos2_flow_test.go        # NEW: Complete QoS 2 flow
├── validation_test.go       # NEW: Packet validation
└── error_handling_test.go   # NEW: Error scenarios
```

#### 1.1.4 Store Package Tests
**File:** `store/memory/memory_test.go`

**Coverage Gaps:**
- [ ] Concurrent retained message operations
- [ ] Wildcard matching for retained messages
- [ ] Will message delay and triggering
- [ ] Store capacity limits
- [ ] Message expiry

**New Test Files:**
```
store/memory/
├── memory_test.go           # Existing
├── concurrent_test.go       # NEW: Concurrent operations
├── wildcard_test.go         # NEW: Wildcard matching
└── bench_test.go            # NEW: Performance benchmarks
```

### 1.2 Integration Test Expansion

**File:** `integration/broker_test.go`, `integration/features_test.go`

**New Integration Tests:**

```
integration/
├── broker_test.go           # Existing - basic pub/sub
├── features_test.go         # Existing - retained, will
├── qos_flows_test.go        # NEW: All QoS combinations
├── stress_test.go           # NEW: Load and stress tests
├── reconnect_test.go        # NEW: Client reconnection scenarios
├── cluster_test.go          # NEW: Multi-broker tests (future)
└── compliance_test.go       # NEW: MQTT spec compliance
```

#### 1.2.1 QoS Flow Tests (`integration/qos_flows_test.go`)
```go
func TestQoS0PubSub(t *testing.T)                    // At most once
func TestQoS1PubSubWithRetry(t *testing.T)           // At least once + retransmit
func TestQoS2PubSubComplete(t *testing.T)            // Exactly once
func TestQoSDowngrade(t *testing.T)                  // Pub QoS 2, Sub QoS 1
func TestMixedQoSSubscribers(t *testing.T)           // Multiple subs, different QoS
func TestQoS1DuplicateDelivery(t *testing.T)         // Verify DUP flag handling
func TestQoS2DuplicatePrevention(t *testing.T)       // Verify no duplicates
func TestOfflineQoS1Delivery(t *testing.T)           // Queue and deliver on reconnect
```

#### 1.2.2 Stress Tests (`integration/stress_test.go`)
```go
func TestStress1000ConcurrentConnections(t *testing.T)
func TestStress100KMessagesPerSecond(t *testing.T)
func TestStressSubscriberFanout(t *testing.T)        // 1 pub, 1000 subs
func TestStressTopicExplosion(t *testing.T)          // 10K different topics
func TestStressSessionTakeover(t *testing.T)         // Rapid reconnections
func TestStressMemoryStability(t *testing.T)         // Run for 10 mins, check leaks
```

#### 1.2.3 Reconnection Tests (`integration/reconnect_test.go`)
```go
func TestReconnectWithCleanSession(t *testing.T)
func TestReconnectPersistentSession(t *testing.T)
func TestReconnectWithPendingInflight(t *testing.T)
func TestReconnectWithOfflineQueue(t *testing.T)
func TestSessionTakeover(t *testing.T)              // Same client ID, different conn
func TestReconnectDuringPublish(t *testing.T)       // Connection drop mid-publish
```

#### 1.2.4 Compliance Tests (`integration/compliance_test.go`)
```go
// MQTT 3.1.1 Compliance
func TestMQTT311_ClientIDGeneration(t *testing.T)
func TestMQTT311_WillMessageDelay(t *testing.T)
func TestMQTT311_RetainedMessageDelivery(t *testing.T)
func TestMQTT311_SubscriptionOptions(t *testing.T)

// MQTT 5.0 Compliance (when implemented)
func TestMQTT5_TopicAliases(t *testing.T)
func TestMQTT5_SharedSubscriptions(t *testing.T)
func TestMQTT5_MessageExpiry(t *testing.T)
```

### 1.3 Fuzzing Tests

**Objective:** Find edge cases and security vulnerabilities through automated fuzzing.

```
fuzz/
├── fuzz_packets_test.go     # Fuzz packet decoding
├── fuzz_topics_test.go      # Fuzz topic validation
└── fuzz_protocol_test.go    # Fuzz protocol state machine
```

**Example:**
```go
// fuzz/fuzz_packets_test.go
func FuzzV3PublishDecode(f *testing.F) {
    f.Fuzz(func(t *testing.T, data []byte) {
        _, _, _, err := v3.ReadPacket(bytes.NewReader(data))
        // Should never panic, only return error
    })
}
```

### 1.4 Benchmark Suite

**File:** `bench/` directory

```
bench/
├── publish_bench_test.go    # Message throughput
├── subscribe_bench_test.go  # Subscription performance
├── router_bench_test.go     # Topic matching performance
├── session_bench_test.go    # Session operations
└── memory_bench_test.go     # Memory allocations
```

**Key Benchmarks:**
```go
BenchmarkPublishQoS0           // Messages/sec for QoS 0
BenchmarkPublishQoS1           // Messages/sec for QoS 1
BenchmarkPublishQoS2           // Messages/sec for QoS 2
BenchmarkRouterMatch           // Topic matching speed
BenchmarkRouterMatchWildcard   // Wildcard matching speed
BenchmarkSessionInflightOps    // Inflight tracking performance
BenchmarkMemoryAllocation      // Allocations per message
```

### 1.5 Test Infrastructure

#### Test Utilities
```go
// testutil/broker.go
type TestBroker struct {
    *broker.Broker
    Port    int
    Cleanup func()
}

func NewTestBroker(t *testing.T) *TestBroker
func (tb *TestBroker) ConnectClient(clientID string) *TestClient

// testutil/client.go
type TestClient struct {
    ClientID string
    conn     net.Conn
}

func (tc *TestClient) Publish(topic string, qos byte, payload []byte) error
func (tc *TestClient) Subscribe(filter string, qos byte) error
func (tc *TestClient) WaitForMessage(timeout time.Duration) (*Message, error)
```

#### CI/CD Integration
```yaml
# .github/workflows/test.yml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.24'
      - run: go test ./... -v -race -coverprofile=coverage.out
      - run: go test ./... -bench=. -benchmem
      - uses: codecov/codecov-action@v3
        with:
          files: ./coverage.out
```

### 1.6 Deliverables

- [ ] **Test Coverage Report**: Generate with `go test -coverprofile`
  - Target: >85% coverage for core packages
  - Target: >70% coverage overall
- [ ] **Benchmark Baseline**: Document current performance metrics
- [ ] **Test Documentation**: Document testing strategy in `docs/testing.md`
- [ ] **CI/CD Pipeline**: Automated tests on every commit

### 1.7 Success Metrics

| Metric | Target | Current |
|--------|--------|---------|
| Unit test coverage | >85% | ~60% |
| Integration tests | 50+ scenarios | 3 |
| Race detector clean | 100% | 100% ✅ |
| Benchmarks established | Yes | No |
| CI/CD pipeline | Yes | No |
| Fuzzing tests | 10+ | 0 |

---

## Phase 2: MQTT 5.0 Completion (Priority 2)

**Timeline:** 3-4 weeks
**Objective:** Complete MQTT 5.0 protocol support for modern clients and features.

### Current MQTT 5.0 Status
- ✅ Packet structures defined
- ✅ Properties encoding/decoding
- ⚠️ Topic aliases (structure exists, not used)
- ❌ Shared subscriptions
- ❌ Request/Response pattern
- ❌ Message expiry
- ❌ Flow control (receive maximum)
- ❌ User properties
- ❌ AUTH packet support
- ❌ Enhanced authentication

### 2.1 Topic Aliases

**Files to Modify:**
- `session/session.go` - Use existing alias maps
- `handlers/broker.go` - Apply aliases on PUBLISH
- `packets/v5/publish.go` - Handle alias in properties

**Implementation:**
```go
// session/session.go
func (s *Session) ApplyTopicAlias(pub *v5.Publish) error {
    if pub.TopicAlias > 0 {
        if pub.TopicName != "" {
            // Register new alias
            s.SetTopicAlias(pub.TopicName, pub.TopicAlias)
        } else {
            // Resolve existing alias
            topic, ok := s.GetTopicAlias(pub.TopicAlias)
            if !ok {
                return fmt.Errorf("unknown topic alias: %d", pub.TopicAlias)
            }
            pub.TopicName = topic
        }
    }
    return nil
}
```

**Tests:**
```go
func TestTopicAlias_FirstUse(t *testing.T)
func TestTopicAlias_SubsequentUse(t *testing.T)
func TestTopicAlias_MaxExceeded(t *testing.T)
func TestTopicAlias_ClearedOnDisconnect(t *testing.T)
```

### 2.2 Shared Subscriptions

**Format:** `$share/{ShareName}/{TopicFilter}`

**Files to Create/Modify:**
- `topics/shared.go` - Parse shared subscription format
- `broker/router.go` - Group-based routing
- `handlers/broker.go` - Load balancing logic

**Implementation:**
```go
// topics/shared.go
type SharedSubscription struct {
    ShareName   string
    TopicFilter string
    Subscribers []string  // List of client IDs
    lastIndex   int       // For round-robin
}

func ParseShared(filter string) (shareName, topicFilter string, isShared bool)

// broker/router.go
type Router struct {
    normal map[string]*trieNode
    shared map[string]*SharedSubscription  // ShareName+Filter -> subscribers
}

func (r *Router) Subscribe(clientID, filter string, qos byte) error {
    if shareName, topicFilter, isShared := topics.ParseShared(filter); isShared {
        // Add to shared group
        key := shareName + "/" + topicFilter
        r.shared[key].Subscribers = append(r.shared[key].Subscribers, clientID)
    } else {
        // Normal subscription
        r.normal.Subscribe(filter, clientID, qos)
    }
}

func (r *Router) GetNextSharedSubscriber(shareName, topicFilter string) string {
    key := shareName + "/" + topicFilter
    shared := r.shared[key]

    // Round-robin load balancing
    if len(shared.Subscribers) == 0 {
        return ""
    }

    subscriber := shared.Subscribers[shared.lastIndex]
    shared.lastIndex = (shared.lastIndex + 1) % len(shared.Subscribers)
    return subscriber
}
```

**Tests:**
```go
func TestSharedSubscription_Parse(t *testing.T)
func TestSharedSubscription_LoadBalancing(t *testing.T)
func TestSharedSubscription_MixedWithNormal(t *testing.T)
func TestSharedSubscription_SubscriberLeaves(t *testing.T)
```

### 2.3 Request/Response Pattern

**Files to Modify:**
- `packets/v5/publish.go` - Add response topic handling
- `session/session.go` - Track correlation data

**Implementation:**
```go
// Handle PUBLISH with Response Topic property
func (h *BrokerHandler) HandlePublish(sess *session.Session, pkt *v5.Publish) error {
    // ... existing publish logic ...

    // If ResponseTopic is set, enable request/response
    if pkt.ResponseTopic != "" {
        // Deliver to subscribers with response capability
        h.publisher.Distribute(pkt.TopicName, pkt.Payload, pkt.QoS, pkt.Retain, map[string]string{
            "response_topic": pkt.ResponseTopic,
            "correlation_data": string(pkt.CorrelationData),
        })
    }

    return nil
}
```

**Tests:**
```go
func TestRequestResponse_BasicFlow(t *testing.T)
func TestRequestResponse_MultipleResponses(t *testing.T)
func TestRequestResponse_CorrelationData(t *testing.T)
```

### 2.4 Message Expiry

**Files to Modify:**
- `store/store.go` - Add expiry tracking
- `session/queue.go` - Check expiry before delivery

**Implementation:**
```go
// store/message.go
type Message struct {
    Topic      string
    Payload    []byte
    QoS        byte
    Retain     bool
    Expiry     time.Time  // NEW
    Properties map[string]string
}

// session/queue.go
func (q *MessageQueue) Dequeue() *store.Message {
    q.mu.Lock()
    defer q.mu.Unlock()

    for len(q.messages) > 0 {
        msg := q.messages[0]
        q.messages = q.messages[1:]

        // Check if expired
        if !msg.Expiry.IsZero() && time.Now().After(msg.Expiry) {
            continue  // Skip expired message
        }

        return msg
    }
    return nil
}
```

**Tests:**
```go
func TestMessageExpiry_BeforeDelivery(t *testing.T)
func TestMessageExpiry_InQueue(t *testing.T)
func TestMessageExpiry_Retained(t *testing.T)
```

### 2.5 Flow Control (Receive Maximum)

**Files to Modify:**
- `session/session.go` - Enforce receive maximum
- `handlers/broker.go` - Respect client's receive max

**Implementation:**
```go
// session/session.go
type Session struct {
    // ... existing fields ...
    ReceiveMaximum uint16  // Already exists
}

func (s *Session) CanSendQoS1or2() bool {
    return s.Inflight.Count() < int(s.ReceiveMaximum)
}

// handlers/broker.go
func (b *BrokerHandler) deliverToSession(s *session.Session, msg *store.Message) error {
    if msg.QoS > 0 && !s.CanSendQoS1or2() {
        // Queue for later or drop based on policy
        return s.OfflineQueue.Enqueue(msg)
    }

    // ... deliver message ...
}
```

**Tests:**
```go
func TestReceiveMaximum_Enforced(t *testing.T)
func TestReceiveMaximum_Queuing(t *testing.T)
func TestReceiveMaximum_Ack(t *testing.T)
```

### 2.6 Enhanced Authentication (AUTH)

**Files to Create:**
- `auth/auth.go` - Authentication interface
- `auth/scram.go` - SCRAM-SHA-256 implementation
- `handlers/auth.go` - AUTH packet handler

**Implementation:**
```go
// auth/auth.go
type Authenticator interface {
    Start(clientData []byte) (serverData []byte, err error)
    Continue(clientData []byte) (serverData []byte, complete bool, err error)
}

type Manager struct {
    mechanisms map[string]Authenticator
}

func (m *Manager) Authenticate(method string, data []byte) (Authenticator, error)

// handlers/auth.go
func (h *BrokerHandler) HandleAuth(sess *session.Session, pkt *v5.Auth) error {
    auth, err := h.authManager.Authenticate(pkt.AuthMethod, pkt.AuthData)
    if err != nil {
        return err
    }

    resp, complete, err := auth.Continue(pkt.AuthData)
    if err != nil {
        // Send AUTH with reason code
        return sess.WritePacket(&v5.Auth{
            ReasonCode: v5.NotAuthorized,
        })
    }

    if complete {
        // Authentication successful, continue with CONNACK
        return nil
    }

    // Send AUTH continue
    return sess.WritePacket(&v5.Auth{
        ReasonCode: v5.ContinueAuthentication,
        AuthData:   resp,
    })
}
```

**Tests:**
```go
func TestAuth_SCRAM_SHA256(t *testing.T)
func TestAuth_MultiStep(t *testing.T)
func TestAuth_Failure(t *testing.T)
```

### 2.7 User Properties

**Files to Modify:**
- `store/message.go` - Store user properties
- `handlers/broker.go` - Preserve and forward properties

**Implementation:**
```go
// Already supported in packets/v5, just need to preserve through system

func (b *BrokerHandler) HandlePublish(sess *session.Session, pkt *v5.Publish) error {
    msg := &store.Message{
        Topic:   pkt.TopicName,
        Payload: pkt.Payload,
        QoS:     pkt.QoS,
        Retain:  pkt.Retain,
        Properties: map[string]string{
            "content_type":     pkt.ContentType,
            "response_topic":   pkt.ResponseTopic,
            "correlation_data": string(pkt.CorrelationData),
        },
        UserProperties: pkt.UserProperties,  // NEW
    }

    return b.publisher.Distribute(msg.Topic, msg.Payload, msg.QoS, msg.Retain, msg.Properties)
}
```

### 2.8 Deliverables

- [ ] Topic aliases fully functional
- [ ] Shared subscriptions with round-robin load balancing
- [ ] Request/response pattern support
- [ ] Message expiry handling
- [ ] Flow control enforcement
- [ ] Enhanced authentication (SCRAM-SHA-256)
- [ ] User properties preserved
- [ ] **MQTT 5.0 compliance tests passing**
- [ ] **Documentation**: `docs/mqtt5-features.md`

---

## Phase 3: Persistent Storage (Priority 3)

**Timeline:** 4-5 weeks
**Objective:** Add persistent storage backends for durability, scalability, and data recovery.

### Why Persistent Storage?

1. **Durability** - Survive broker restarts
2. **Scalability** - Offload memory pressure
3. **Compliance** - Retain messages for audit/regulatory requirements
4. **Clustering** - Shared state across broker instances

### 3.1 Storage Architecture

**Abstraction Layer:**
```go
// store/store.go
type Backend interface {
    // Sessions
    SaveSession(s *Session) error
    GetSession(clientID string) (*Session, error)
    DeleteSession(clientID string) error

    // Retained Messages
    SetRetained(topic string, msg *Message) error
    GetRetained(topic string) (*Message, error)
    MatchRetained(filter string) ([]*Message, error)
    DeleteRetained(topic string) error

    // Subscriptions
    SaveSubscription(clientID string, filter string, opts SubscribeOptions) error
    GetSubscriptions(clientID string) ([]*Subscription, error)
    DeleteSubscription(clientID, filter string) error

    // Will Messages
    SetWill(clientID string, will *WillMessage) error
    GetPendingWills(before time.Time) ([]*WillMessage, error)
    DeleteWill(clientID string) error

    // Inflight Messages (for QoS 1/2)
    SaveInflight(clientID string, packetID uint16, msg *Message) error
    GetInflight(clientID string) ([]*InflightMessage, error)
    DeleteInflight(clientID string, packetID uint16) error

    // Lifecycle
    Close() error
}
```

### 3.2 PostgreSQL Backend

**Files to Create:**
```
store/postgres/
├── postgres.go          # Main implementation
├── schema.sql           # Database schema
├── migrations/          # Schema migrations
│   ├── 001_initial.sql
│   └── 002_add_indexes.sql
├── sessions.go          # Session operations
├── retained.go          # Retained messages
├── subscriptions.go     # Subscription persistence
└── postgres_test.go     # Integration tests
```

**Schema:**
```sql
-- store/postgres/schema.sql
CREATE TABLE sessions (
    client_id VARCHAR(255) PRIMARY KEY,
    version SMALLINT NOT NULL,
    clean_start BOOLEAN NOT NULL,
    expiry_interval INTEGER NOT NULL,
    connected_at TIMESTAMP,
    disconnected_at TIMESTAMP,
    connected BOOLEAN NOT NULL,
    receive_maximum SMALLINT,
    max_packet_size INTEGER,
    topic_alias_max SMALLINT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE retained_messages (
    topic VARCHAR(1024) PRIMARY KEY,
    payload BYTEA NOT NULL,
    qos SMALLINT NOT NULL,
    properties JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE subscriptions (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    topic_filter VARCHAR(1024) NOT NULL,
    qos SMALLINT NOT NULL,
    options JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(client_id, topic_filter)
);

CREATE TABLE will_messages (
    client_id VARCHAR(255) PRIMARY KEY,
    topic VARCHAR(1024) NOT NULL,
    payload BYTEA NOT NULL,
    qos SMALLINT NOT NULL,
    retain BOOLEAN NOT NULL,
    delay INTEGER NOT NULL,
    properties JSONB,
    trigger_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE inflight_messages (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    packet_id SMALLINT NOT NULL,
    topic VARCHAR(1024) NOT NULL,
    payload BYTEA NOT NULL,
    qos SMALLINT NOT NULL,
    state VARCHAR(50) NOT NULL,
    sent_at TIMESTAMP NOT NULL,
    retries INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(client_id, packet_id)
);

-- Indexes for performance
CREATE INDEX idx_subscriptions_client ON subscriptions(client_id);
CREATE INDEX idx_subscriptions_filter ON subscriptions(topic_filter);
CREATE INDEX idx_retained_topic_prefix ON retained_messages USING gin(to_tsvector('simple', topic));
CREATE INDEX idx_will_trigger ON will_messages(trigger_at) WHERE trigger_at IS NOT NULL;
CREATE INDEX idx_inflight_client ON inflight_messages(client_id);
```

**Implementation:**
```go
// store/postgres/postgres.go
type PostgresBackend struct {
    db     *sql.DB
    cache  *cache.LRU  // Optional caching layer
    logger *slog.Logger
}

func New(dsn string, opts Options) (*PostgresBackend, error) {
    db, err := sql.Open("postgres", dsn)
    if err != nil {
        return nil, fmt.Errorf("open database: %w", err)
    }

    // Configure connection pool
    db.SetMaxOpenConns(opts.MaxConnections)
    db.SetMaxIdleConns(opts.MaxIdleConnections)
    db.SetConnMaxLifetime(opts.ConnMaxLifetime)

    return &PostgresBackend{
        db:     db,
        cache:  cache.NewLRU(opts.CacheSize),
        logger: opts.Logger,
    }, nil
}

func (p *PostgresBackend) SaveSession(s *store.Session) error {
    query := `
        INSERT INTO sessions (client_id, version, clean_start, expiry_interval,
                             connected_at, disconnected_at, connected)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (client_id) DO UPDATE SET
            connected_at = EXCLUDED.connected_at,
            disconnected_at = EXCLUDED.disconnected_at,
            connected = EXCLUDED.connected,
            updated_at = NOW()
    `

    _, err := p.db.Exec(query, s.ClientID, s.Version, s.CleanStart,
        s.ExpiryInterval, s.ConnectedAt, s.DisconnectedAt, s.Connected)

    if err != nil {
        return fmt.Errorf("save session: %w", err)
    }

    // Update cache
    p.cache.Set(s.ClientID, s)

    return nil
}
```

**Configuration:**
```go
// config/config.go
type StorageConfig struct {
    Type     string            `yaml:"type"`      // "memory", "postgres", "redis"
    Postgres PostgresConfig    `yaml:"postgres"`
    Redis    RedisConfig       `yaml:"redis"`
}

type PostgresConfig struct {
    DSN                 string        `yaml:"dsn"`
    MaxConnections      int           `yaml:"max_connections"`
    MaxIdleConnections  int           `yaml:"max_idle_connections"`
    ConnMaxLifetime     time.Duration `yaml:"conn_max_lifetime"`
    CacheSize           int           `yaml:"cache_size"`
}
```

### 3.3 Redis Backend

**Files to Create:**
```
store/redis/
├── redis.go             # Main implementation
├── sessions.go          # Session operations
├── retained.go          # Retained messages
├── pubsub.go            # Pub/Sub for clustering
└── redis_test.go        # Integration tests
```

**Key Schema:**
```
# Sessions
mqtt:session:{clientID}              HASH    {version, clean_start, expiry, ...}
mqtt:session:{clientID}:subs         SET     {filter1, filter2, ...}
mqtt:session:{clientID}:inflight     HASH    {packetID: message}

# Retained Messages
mqtt:retained:{topic}                HASH    {payload, qos, properties}

# Subscriptions (for routing)
mqtt:sub:{topicFilter}               SET     {clientID1, clientID2, ...}

# Will Messages
mqtt:will:{clientID}                 HASH    {topic, payload, qos, delay}
mqtt:will:pending                    ZSET    {clientID: trigger_timestamp}

# Clustering (Pub/Sub)
mqtt:cluster:messages                PUBSUB  Cross-broker message forwarding
mqtt:cluster:sessions                PUBSUB  Session takeover notifications
```

**Implementation:**
```go
// store/redis/redis.go
type RedisBackend struct {
    client redis.UniversalClient
    logger *slog.Logger
}

func New(opts *redis.UniversalOptions, logger *slog.Logger) (*RedisBackend, error) {
    client := redis.NewUniversalClient(opts)

    // Test connection
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := client.Ping(ctx).Err(); err != nil {
        return nil, fmt.Errorf("redis ping: %w", err)
    }

    return &RedisBackend{
        client: client,
        logger: logger,
    }, nil
}

func (r *RedisBackend) SaveSession(s *store.Session) error {
    ctx := context.Background()
    key := fmt.Sprintf("mqtt:session:%s", s.ClientID)

    pipe := r.client.Pipeline()
    pipe.HSet(ctx, key, map[string]interface{}{
        "version":         s.Version,
        "clean_start":     s.CleanStart,
        "expiry_interval": s.ExpiryInterval,
        "connected_at":    s.ConnectedAt.Unix(),
        "disconnected_at": s.DisconnectedAt.Unix(),
        "connected":       s.Connected,
    })

    // Set expiry if configured
    if s.ExpiryInterval > 0 {
        pipe.Expire(ctx, key, time.Duration(s.ExpiryInterval)*time.Second)
    }

    _, err := pipe.Exec(ctx)
    return err
}
```

### 3.4 Hybrid Approach: Write-Through Cache

**Objective:** Combine speed of memory with durability of disk.

```go
// store/hybrid/hybrid.go
type HybridBackend struct {
    memory     *memory.MemoryBackend  // Fast path
    persistent store.Backend           // Durable path (Postgres/Redis)
    writeMode  WriteMode               // Sync or Async
}

type WriteMode int

const (
    WriteSynchronous WriteMode = iota  // Block until persistent write completes
    WriteAsynchronous                  // Return immediately, write in background
)

func (h *HybridBackend) SaveSession(s *store.Session) error {
    // Always update memory immediately
    if err := h.memory.SaveSession(s); err != nil {
        return fmt.Errorf("memory save: %w", err)
    }

    // Persist based on mode
    if h.writeMode == WriteSynchronous {
        return h.persistent.SaveSession(s)
    } else {
        // Async write
        go func() {
            if err := h.persistent.SaveSession(s); err != nil {
                h.logger.Error("async save session failed",
                    slog.String("client_id", s.ClientID),
                    slog.String("error", err.Error()))
            }
        }()
        return nil
    }
}
```

### 3.5 Migration Strategy

**Files to Create:**
```
store/migrate/
├── migrate.go           # Migration logic
└── migrate_test.go      # Migration tests
```

```go
// store/migrate/migrate.go
func Migrate(from, to store.Backend, opts MigrateOptions) error {
    // 1. Migrate sessions
    sessions, err := from.GetAllSessions()
    for _, s := range sessions {
        if err := to.SaveSession(s); err != nil {
            return fmt.Errorf("migrate session %s: %w", s.ClientID, err)
        }
    }

    // 2. Migrate retained messages
    retained, err := from.GetAllRetained()
    for topic, msg := range retained {
        if err := to.SetRetained(topic, msg); err != nil {
            return fmt.Errorf("migrate retained %s: %w", topic, err)
        }
    }

    // 3. Migrate subscriptions
    // ... similar pattern

    return nil
}
```

### 3.6 Storage Benchmarks

```go
// store/bench/storage_bench_test.go
func BenchmarkMemory_SaveSession(b *testing.B)
func BenchmarkPostgres_SaveSession(b *testing.B)
func BenchmarkRedis_SaveSession(b *testing.B)
func BenchmarkHybrid_SaveSession(b *testing.B)

func BenchmarkMemory_GetRetained(b *testing.B)
func BenchmarkPostgres_GetRetained(b *testing.B)
func BenchmarkRedis_GetRetained(b *testing.B)
```

### 3.7 Deliverables

- [ ] PostgreSQL backend implementation
- [ ] Redis backend implementation
- [ ] Hybrid write-through cache
- [ ] Migration tooling
- [ ] Storage benchmarks
- [ ] **Documentation**: `docs/storage.md`
- [ ] **Configuration examples** for each backend

---

## Phase 4: Clustering & High Availability (Priority 4)

**Timeline:** 5-6 weeks
**Objective:** Enable horizontal scaling with multiple broker instances sharing state.

### 4.1 Clustering Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Broker 1   │      │  Broker 2   │      │  Broker 3   │
│  (Leader)   │◄────►│  (Follower) │◄────►│  (Follower) │
└──────┬──────┘      └──────┬──────┘      └──────┬──────┘
       │                    │                    │
       │                    │                    │
       └────────────────────┼────────────────────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │  Shared Storage  │
                  │  (Postgres/Redis)│
                  └──────────────────┘
```

### 4.2 Broker Discovery

**Files to Create:**
```
cluster/
├── discovery.go         # Node discovery
├── membership.go        # Cluster membership
├── gossip.go            # Gossip protocol
├── routing.go           # Message routing between nodes
└── takeover.go          # Session takeover coordination
```

**Implementation Options:**

#### Option A: Static Configuration
```yaml
# config.yaml
cluster:
  enabled: true
  node_id: "broker-1"
  nodes:
    - "broker-1:1883"
    - "broker-2:1883"
    - "broker-3:1883"
```

#### Option B: etcd Discovery
```go
// cluster/discovery.go
type EtcdDiscovery struct {
    client *clientv3.Client
    prefix string
}

func (d *EtcdDiscovery) Register(nodeID string, addr string) error {
    key := fmt.Sprintf("%s/nodes/%s", d.prefix, nodeID)
    lease, _ := d.client.Grant(context.Background(), 10) // 10s TTL

    _, err := d.client.Put(context.Background(), key, addr,
        clientv3.WithLease(lease.ID))
    return err
}

func (d *EtcdDiscovery) Watch() <-chan []Node {
    ch := make(chan []Node)
    go func() {
        watchChan := d.client.Watch(context.Background(),
            d.prefix+"/nodes/", clientv3.WithPrefix())
        for resp := range watchChan {
            nodes := d.listNodes()
            ch <- nodes
        }
    }()
    return ch
}
```

#### Option C: SWIM Protocol (Gossip)
```go
// cluster/gossip.go
type GossipCluster struct {
    memberlist *memberlist.Memberlist
    nodes      map[string]*Node
    mu         sync.RWMutex
}

func (g *GossipCluster) Join(existing []string) error {
    _, err := g.memberlist.Join(existing)
    return err
}

func (g *GossipCluster) NotifyJoin(node *memberlist.Node) {
    g.mu.Lock()
    defer g.mu.Unlock()
    g.nodes[node.Name] = &Node{
        ID:   node.Name,
        Addr: node.Addr.String(),
    }
}
```

### 4.3 Distributed Routing

**Challenge:** Client A on Broker 1 publishes to topic; Client B on Broker 2 is subscribed.

**Solution 1: Subscription Replication**
```go
// cluster/routing.go
type ClusterRouter struct {
    local  *broker.Router          // Local subscriptions
    remote map[string]*RemoteNode  // Remote brokers
}

func (r *ClusterRouter) Subscribe(clientID, filter string, qos byte) error {
    // 1. Subscribe locally
    r.local.Subscribe(clientID, filter, qos)

    // 2. Broadcast to other nodes
    for _, node := range r.remote {
        node.Send(&SubscribeMessage{
            NodeID:   r.nodeID,
            ClientID: clientID,
            Filter:   filter,
            QoS:      qos,
        })
    }

    return nil
}

func (r *ClusterRouter) Match(topic string) []Subscription {
    // 1. Get local subscribers
    local := r.local.Match(topic)

    // 2. Get remote subscribers (cached or queried)
    for nodeID, node := range r.remote {
        remoteSubs := node.Match(topic)
        for _, sub := range remoteSubs {
            // Add remote subscription with forwarding info
            local = append(local, Subscription{
                ClientID: sub.ClientID,
                QoS:      sub.QoS,
                NodeID:   nodeID,  // Forward to this node
            })
        }
    }

    return local
}
```

**Solution 2: Message Forwarding (Pub/Sub)**
```go
// Use Redis Pub/Sub or NATS for inter-broker messaging
type MessageForwarder struct {
    redis redis.UniversalClient
}

func (f *MessageForwarder) PublishLocal(topic string, msg *Message) error {
    // 1. Deliver to local subscribers
    localSubs := f.router.Match(topic)
    for _, sub := range localSubs {
        f.deliver(sub.ClientID, msg)
    }

    // 2. Forward to other brokers
    payload, _ := json.Marshal(msg)
    return f.redis.Publish(context.Background(), "mqtt:cluster:messages", payload).Err()
}

func (f *MessageForwarder) SubscribeCluster() {
    pubsub := f.redis.Subscribe(context.Background(), "mqtt:cluster:messages")
    ch := pubsub.Channel()

    for msg := range ch {
        var message Message
        json.Unmarshal([]byte(msg.Payload), &message)

        // Deliver to local subscribers only
        localSubs := f.router.Match(message.Topic)
        for _, sub := range localSubs {
            f.deliver(sub.ClientID, &message)
        }
    }
}
```

### 4.4 Session Takeover

**Scenario:** Client disconnects from Broker 1, reconnects to Broker 2 with same client ID.

**Implementation:**
```go
// cluster/takeover.go
type SessionCoordinator struct {
    locks  map[string]*etcd.Lock  // client_id -> distributed lock
    etcd   *clientv3.Client
}

func (sc *SessionCoordinator) TakeoverSession(clientID string) (*Session, error) {
    // 1. Acquire distributed lock for this client ID
    lock := sc.getLock(clientID)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    if err := lock.Lock(ctx); err != nil {
        return nil, fmt.Errorf("acquire lock: %w", err)
    }
    defer lock.Unlock(context.Background())

    // 2. Check if session exists on another node
    key := fmt.Sprintf("/mqtt/cluster/sessions/%s", clientID)
    resp, err := sc.etcd.Get(ctx, key)
    if err != nil {
        return nil, err
    }

    if len(resp.Kvs) > 0 {
        var location SessionLocation
        json.Unmarshal(resp.Kvs[0].Value, &location)

        // 3. Notify old broker to disconnect session
        if location.NodeID != sc.currentNodeID {
            sc.notifyDisconnect(location.NodeID, clientID)
        }
    }

    // 4. Register session on this node
    location := SessionLocation{
        NodeID:    sc.currentNodeID,
        ClientID:  clientID,
        Timestamp: time.Now(),
    }
    data, _ := json.Marshal(location)
    sc.etcd.Put(ctx, key, string(data))

    return nil
}
```

### 4.5 Split-Brain Prevention

**Implementation:**
```go
// cluster/leader.go
type LeaderElection struct {
    election *concurrency.Election
    session  *concurrency.Session
}

func (l *LeaderElection) Campaign(ctx context.Context) error {
    return l.election.Campaign(ctx, l.nodeID)
}

func (l *LeaderElection) IsLeader() bool {
    ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
    defer cancel()

    resp, err := l.election.Leader(ctx)
    if err != nil {
        return false
    }

    return string(resp.Kvs[0].Value) == l.nodeID
}

// Use leader for cluster-wide operations:
// - Will message triggering
// - Session expiry cleanup
// - Metrics aggregation
```

### 4.6 Load Balancing

**DNS Round-Robin:**
```
mqtt.example.com → 10.0.1.1  (Broker 1)
                 → 10.0.1.2  (Broker 2)
                 → 10.0.1.3  (Broker 3)
```

**HAProxy Configuration:**
```haproxy
# haproxy.cfg
frontend mqtt_frontend
    bind *:1883
    mode tcp
    option tcplog
    default_backend mqtt_backend

backend mqtt_backend
    mode tcp
    balance leastconn
    option tcp-check
    server broker1 10.0.1.1:1883 check
    server broker2 10.0.1.2:1883 check
    server broker3 10.0.1.3:1883 check
```

### 4.7 Deliverables

- [ ] Node discovery (etcd or gossip)
- [ ] Distributed subscription routing
- [ ] Message forwarding between brokers
- [ ] Session takeover coordination
- [ ] Leader election
- [ ] Split-brain prevention
- [ ] Load balancer configuration examples
- [ ] **Documentation**: `docs/clustering.md`
- [ ] **Runbook**: `docs/ops/clustering-runbook.md`

---

## Phase 5: Production Hardening (Ongoing)

**Timeline:** Continuous
**Objective:** Ensure the broker is production-ready with observability, security, and performance optimizations.

### 5.1 Observability

#### 5.1.1 Metrics (Prometheus)

**Files to Create:**
```
metrics/
├── metrics.go           # Prometheus registry
├── broker.go            # Broker-specific metrics
├── session.go           # Session metrics
└── storage.go           # Storage metrics
```

**Key Metrics:**
```go
// metrics/broker.go
var (
    ConnectionsTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mqtt_connections_total",
            Help: "Current number of MQTT connections",
        },
        []string{"protocol", "version"},
    )

    MessagesPublished = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mqtt_messages_published_total",
            Help: "Total messages published",
        },
        []string{"qos"},
    )

    MessagesDelivered = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mqtt_messages_delivered_total",
            Help: "Total messages delivered to subscribers",
        },
        []string{"qos"},
    )

    SessionsActive = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "mqtt_sessions_active",
            Help: "Number of active sessions",
        },
    )

    SubscriptionsTotal = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "mqtt_subscriptions_total",
            Help: "Total number of active subscriptions",
        },
    )

    RetainedMessagesTotal = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "mqtt_retained_messages_total",
            Help: "Number of retained messages",
        },
    )

    MessageLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "mqtt_message_latency_seconds",
            Help:    "Message delivery latency",
            Buckets: prometheus.ExponentialBuckets(0.001, 2, 10),
        },
        []string{"qos"},
    )

    InflightMessages = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mqtt_inflight_messages",
            Help: "Messages in flight (QoS 1/2)",
        },
        []string{"client_id"},
    )

    OfflineQueueSize = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mqtt_offline_queue_size",
            Help: "Size of offline message queue",
        },
        []string{"client_id"},
    )
)
```

**Metrics Endpoint:**
```go
// cmd/broker/main.go
import "github.com/prometheus/client_golang/prometheus/promhttp"

http.Handle("/metrics", promhttp.Handler())
go http.ListenAndServe(":9090", nil)
```

#### 5.1.2 Distributed Tracing (OpenTelemetry)

**Files to Create:**
```
tracing/
├── tracing.go           # OpenTelemetry setup
└── middleware.go        # Tracing middleware
```

```go
// tracing/tracing.go
func Init(serviceName string) (*trace.TracerProvider, error) {
    exporter, err := otlptracegrpc.New(context.Background(),
        otlptracegrpc.WithEndpoint("localhost:4317"),
        otlptracegrpc.WithInsecure(),
    )
    if err != nil {
        return nil, err
    }

    tp := trace.NewTracerProvider(
        trace.WithBatcher(exporter),
        trace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceNameKey.String(serviceName),
        )),
    )

    otel.SetTracerProvider(tp)
    return tp, nil
}

// Usage in broker
tracer := otel.Tracer("mqtt-broker")
ctx, span := tracer.Start(ctx, "HandlePublish")
defer span.End()

span.SetAttributes(
    attribute.String("client_id", clientID),
    attribute.String("topic", topic),
    attribute.Int("qos", int(qos)),
)
```

#### 5.1.3 Health Checks

**Files to Create:**
```
health/
├── health.go            # Health check handlers
└── health_test.go       # Health check tests
```

```go
// health/health.go
type Checker struct {
    broker  *broker.Broker
    storage store.Backend
}

func (c *Checker) Check(ctx context.Context) error {
    // 1. Check storage connection
    if err := c.storage.Ping(ctx); err != nil {
        return fmt.Errorf("storage unhealthy: %w", err)
    }

    // 2. Check if broker is accepting connections
    if !c.broker.IsRunning() {
        return fmt.Errorf("broker not running")
    }

    return nil
}

// HTTP endpoint
http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    if err := checker.Check(r.Context()); err != nil {
        w.WriteHeader(http.StatusServiceUnavailable)
        json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "error": err.Error()})
        return
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
})
```

### 5.2 Security

#### 5.2.1 TLS Support

**Files to Modify:**
- `server/tcp/server.go` - Add TLS config
- `config/config.go` - TLS configuration

```go
// config/config.go
type TLSConfig struct {
    Enabled  bool   `yaml:"enabled"`
    CertFile string `yaml:"cert_file"`
    KeyFile  string `yaml:"key_file"`
    CAFile   string `yaml:"ca_file"`
    ClientAuth string `yaml:"client_auth"` // "none", "request", "require"
}

// server/tcp/server.go
func (s *Server) configureTLS(cfg TLSConfig) (*tls.Config, error) {
    cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
    if err != nil {
        return nil, err
    }

    tlsConfig := &tls.Config{
        Certificates: []tls.Certificate{cert},
        MinVersion:   tls.VersionTLS12,
    }

    // Client certificate verification
    if cfg.ClientAuth != "none" {
        caCert, err := os.ReadFile(cfg.CAFile)
        if err != nil {
            return nil, err
        }

        caCertPool := x509.NewCertPool()
        caCertPool.AppendCertsFromPEM(caCert)

        tlsConfig.ClientCAs = caCertPool
        if cfg.ClientAuth == "require" {
            tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
        } else {
            tlsConfig.ClientAuth = tls.RequestClientCert
        }
    }

    return tlsConfig, nil
}
```

#### 5.2.2 Authentication & Authorization

**Files to Create:**
```
auth/
├── auth.go              # Authentication interface
├── password.go          # Password-based auth
├── jwt.go               # JWT-based auth
├── acl.go               # Access Control Lists
└── hooks.go             # Auth hooks
```

```go
// auth/auth.go
type Authenticator interface {
    Authenticate(clientID, username, password string) error
}

type Authorizer interface {
    AuthorizePublish(clientID, topic string) error
    AuthorizeSubscribe(clientID, topicFilter string) error
}

// auth/password.go
type PasswordAuth struct {
    users map[string]string  // username -> hashed password
}

func (p *PasswordAuth) Authenticate(clientID, username, password string) error {
    hashedPassword, ok := p.users[username]
    if !ok {
        return fmt.Errorf("unknown user: %s", username)
    }

    if err := bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password)); err != nil {
        return fmt.Errorf("invalid password")
    }

    return nil
}

// auth/acl.go
type ACL struct {
    rules []ACLRule
}

type ACLRule struct {
    ClientPattern string
    TopicPattern  string
    Action        string  // "publish", "subscribe", "both"
    Allow         bool
}

func (a *ACL) AuthorizePublish(clientID, topic string) error {
    for _, rule := range a.rules {
        if matchPattern(rule.ClientPattern, clientID) &&
           matchPattern(rule.TopicPattern, topic) &&
           (rule.Action == "publish" || rule.Action == "both") {
            if rule.Allow {
                return nil
            }
            return fmt.Errorf("publish denied by ACL")
        }
    }
    return fmt.Errorf("no matching ACL rule")
}
```

**Integration:**
```go
// broker/broker.go
func (b *Broker) HandleConnection(netConn net.Conn) {
    conn := NewConnection(netConn)
    pkt, _ := conn.ReadPacket()

    connectPkt := pkt.(*v3.Connect)

    // Authenticate
    if b.auth != nil {
        if err := b.auth.Authenticate(connectPkt.ClientID,
            connectPkt.Username, connectPkt.Password); err != nil {
            // Send CONNACK with auth failure
            return
        }
    }

    // ... continue with session setup
}
```

#### 5.2.3 Rate Limiting

**Files to Create:**
```
ratelimit/
├── ratelimit.go         # Rate limiting logic
└── ratelimit_test.go    # Tests
```

```go
// ratelimit/ratelimit.go
type Limiter struct {
    limiters map[string]*rate.Limiter
    mu       sync.Mutex
}

func (l *Limiter) Allow(clientID string, limit rate.Limit) bool {
    l.mu.Lock()
    defer l.mu.Unlock()

    limiter, ok := l.limiters[clientID]
    if !ok {
        limiter = rate.NewLimiter(limit, int(limit))
        l.limiters[clientID] = limiter
    }

    return limiter.Allow()
}

// Usage in broker
if !b.rateLimiter.Allow(clientID, 100) {  // 100 msg/sec
    return fmt.Errorf("rate limit exceeded")
}
```

### 5.3 Performance Optimizations

#### 5.3.1 Connection Pooling
Already implemented in `server/tcp/server.go`

#### 5.3.2 Message Batching
```go
// handlers/broker.go
type MessageBatcher struct {
    batch    []*Message
    maxSize  int
    maxDelay time.Duration
    timer    *time.Timer
}

func (b *MessageBatcher) Add(msg *Message) {
    b.batch = append(b.batch, msg)

    if len(b.batch) >= b.maxSize {
        b.Flush()
    } else if b.timer == nil {
        b.timer = time.AfterFunc(b.maxDelay, b.Flush)
    }
}

func (b *MessageBatcher) Flush() {
    // Send all messages in batch
    for _, msg := range b.batch {
        b.deliver(msg)
    }
    b.batch = b.batch[:0]

    if b.timer != nil {
        b.timer.Stop()
        b.timer = nil
    }
}
```

#### 5.3.3 Compression
```go
// compression/compression.go
type Compressor interface {
    Compress(data []byte) ([]byte, error)
    Decompress(data []byte) ([]byte, error)
}

type GzipCompressor struct {
    level int
}

func (g *GzipCompressor) Compress(data []byte) ([]byte, error) {
    var buf bytes.Buffer
    w, _ := gzip.NewWriterLevel(&buf, g.level)
    w.Write(data)
    w.Close()
    return buf.Bytes(), nil
}
```

### 5.4 Operational Tools

#### 5.4.1 CLI Tool

**Files to Create:**
```
cmd/mqttctl/
├── main.go              # CLI entry point
├── publish.go           # Publish command
├── subscribe.go         # Subscribe command
├── sessions.go          # Session management
└── topics.go            # Topic management
```

```bash
# Examples
mqttctl publish --topic "sensors/temp" --message "25.5" --qos 1
mqttctl subscribe --topic "sensors/#"
mqttctl sessions list
mqttctl sessions kill --client-id "device-123"
mqttctl topics list
mqttctl topics stats --topic "sensors/temp"
```

#### 5.4.2 Admin API

**Files to Create:**
```
api/
├── admin.go             # Admin HTTP API
├── sessions.go          # Session endpoints
├── topics.go            # Topic endpoints
└── metrics.go           # Metrics endpoints
```

```go
// api/admin.go
type AdminAPI struct {
    broker *broker.Broker
}

// GET /api/v1/sessions
func (a *AdminAPI) ListSessions(w http.ResponseWriter, r *http.Request)

// GET /api/v1/sessions/{client_id}
func (a *AdminAPI) GetSession(w http.ResponseWriter, r *http.Request)

// DELETE /api/v1/sessions/{client_id}
func (a *AdminAPI) KillSession(w http.ResponseWriter, r *http.Request)

// GET /api/v1/topics
func (a *AdminAPI) ListTopics(w http.ResponseWriter, r *http.Request)

// GET /api/v1/topics/{topic}/stats
func (a *AdminAPI) GetTopicStats(w http.ResponseWriter, r *http.Request)
```

### 5.5 Documentation

**Files to Create/Update:**
```
docs/
├── README.md                    # Overview
├── quickstart.md                # Getting started
├── configuration.md             # Configuration reference
├── mqtt5-features.md            # MQTT 5.0 features
├── storage.md                   # Storage backends
├── clustering.md                # Clustering guide
├── testing.md                   # Testing guide
├── security.md                  # Security best practices
├── performance.md               # Performance tuning
└── ops/
    ├── deployment.md            # Deployment guide
    ├── monitoring.md            # Monitoring & alerting
    ├── troubleshooting.md       # Common issues
    └── runbooks/
        ├── clustering.md        # Clustering runbook
        ├── failover.md          # Failover procedures
        └── backup.md            # Backup & restore
```

### 5.6 Deliverables

- [ ] Prometheus metrics
- [ ] OpenTelemetry tracing
- [ ] Health check endpoints
- [ ] TLS support
- [ ] Authentication (password, JWT)
- [ ] Authorization (ACL)
- [ ] Rate limiting
- [ ] Message batching
- [ ] Compression support
- [ ] CLI tool (mqttctl)
- [ ] Admin API
- [ ] Comprehensive documentation
- [ ] Deployment guides

---

## Success Criteria

### Phase 1: Testing
- ✅ Test coverage >85% for core packages
- ✅ 50+ integration test scenarios
- ✅ Fuzzing tests implemented
- ✅ Benchmark baseline established
- ✅ CI/CD pipeline operational

### Phase 2: MQTT 5.0
- ✅ Topic aliases functional
- ✅ Shared subscriptions with load balancing
- ✅ Request/response pattern
- ✅ Message expiry
- ✅ Flow control enforcement
- ✅ Enhanced authentication
- ✅ MQTT 5.0 compliance tests passing

### Phase 3: Storage
- ✅ PostgreSQL backend production-ready
- ✅ Redis backend production-ready
- ✅ Hybrid cache operational
- ✅ Migration tooling functional
- ✅ Storage benchmarks documented

### Phase 4: Clustering
- ✅ Multi-broker deployment working
- ✅ Session takeover functional
- ✅ Message routing across brokers
- ✅ Split-brain prevention
- ✅ Load balancer integration
- ✅ Cluster health monitoring

### Phase 5: Production Hardening
- ✅ Metrics exposed via Prometheus
- ✅ Distributed tracing operational
- ✅ TLS encryption enabled
- ✅ Authentication/authorization working
- ✅ Rate limiting functional
- ✅ Admin API operational
- ✅ Documentation complete
- ✅ Deployment runbooks ready

---

## Timeline Summary

| Phase | Duration | Effort (person-weeks) |
|-------|----------|----------------------|
| Phase 0: Code Quality | ✅ Done | 1 week |
| Phase 1: Testing | 2-3 weeks | 3 weeks |
| Phase 2: MQTT 5.0 | 3-4 weeks | 4 weeks |
| Phase 3: Storage | 4-5 weeks | 5 weeks |
| Phase 4: Clustering | 5-6 weeks | 6 weeks |
| Phase 5: Production Hardening | Ongoing | 3 weeks |
| **Total** | **~5 months** | **22 weeks** |

---

## Next Steps (Immediate Actions)

1. **Review this plan** - Get team alignment on priorities and timeline
2. **Set up CI/CD** - GitHub Actions for automated testing
3. **Start Phase 1** - Begin with unit test expansion
4. **Create milestones** - Track progress in GitHub Issues/Projects

---

**Document Version:** 1.0
**Last Review:** 2025-12-09
**Next Review:** After Phase 1 completion
