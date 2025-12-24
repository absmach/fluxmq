# MQTT Broker Development Roadmap
**Production-Ready Implementation Plan**

**Last Updated:** 2025-12-24
**Current Status:** Core features complete, testing & production hardening in progress

---

## Executive Summary

This roadmap outlines the path to a production-ready, scalable MQTT broker. Tasks are sorted by priority with detailed implementation guidance to minimize context and back-and-forth during development.

### Overall Progress
- ✅ **Foundation (100%)** - Core MQTT 3.1.1/5.0, multi-protocol, clean architecture
- ✅ **Clustering (95%)** - Session takeover, message routing, BadgerDB, etcd coordination
- ✅ **Multi-Protocol (100%)** - TCP, WebSocket, HTTP bridge, CoAP stub
- ⏳ **Testing (40%)** - Unit tests for storage, integration tests for clustering
- ⏳ **Production Hardening (25%)** - Health checks done, metrics/TLS/auth pending
- ❌ **MQTT 5.0 Advanced (0%)** - Topic aliases, shared subscriptions, message expiry

---

## Current Implementation Status

### ✅ Completed Features

#### Core MQTT Implementation
- MQTT 3.1.1 full compliance
- MQTT 5.0 packet structures and basic properties
- QoS 0, 1, 2 message flows
- Session management with clean start
- Retained messages
- Will messages
- Keep-alive handling
- Topic routing with wildcard support (+, #)

#### Multi-Protocol Support
- **MQTT over TCP** - Port 1883, full protocol support
- **MQTT over WebSocket** - Port 8083, browser/mobile clients
- **HTTP-MQTT Bridge** - Port 8080, REST API for publishing
- **CoAP Bridge Stub** - Port 5683, awaiting full implementation
- All protocols share single Broker instance

#### Clustering & Distribution
- **Embedded etcd** - Distributed coordination, no external dependencies
- **gRPC Transport** - Inter-broker communication
- **Session Takeover** - Full state transfer (subscriptions, inflight, offline queue)
- **Message Routing** - Cross-node publish/subscribe with local cache
- **Leadership Election** - Leader-only background tasks (expiry, will triggering)
- **Graceful Shutdown** - Session drain and transfer before shutdown

#### Persistent Storage
- **BadgerDB** - All 6 store implementations complete:
  - Session persistence
  - Inflight messages (QoS 1/2)
  - Offline queue
  - Subscriptions
  - Retained messages
  - Will messages
- Background GC with graceful shutdown
- 91% test coverage (73 tests)

#### Operational Features
- Health check endpoints (`/health`, `/ready`, `/cluster/status`)
- Webhook system for event notifications (12 event types)
- Structured logging with slog
- Graceful shutdown with signal handling
- Configuration via YAML

### ⏳ In Progress

- Cross-node messaging tests (4/5 tests passing, QoS 2 investigation needed)
- Cluster formation and failover testing
- Retained message delivery optimization
- Will message cluster-wide validation

### ❌ Not Started

- MQTT 5.0 advanced features (topic aliases, shared subscriptions)
- Prometheus metrics endpoint
- TLS/SSL support
- Authentication & authorization
- Rate limiting
- Message expiry enforcement
- Request/response pattern

---

## Priority 1: HIGH - Production Readiness

### Task 1.1: QoS 2 Cross-Node Routing Investigation
**Priority:** HIGH | **Effort:** 2-3 hours | **Blocking:** MQTT compliance

**Context:**
- QoS 0/1 cross-node routing works
- QoS 2 messages not delivered across nodes (test times out)
- Test client QoS 2 flow is fixed (packet IDs, PUBREC/PUBREL/PUBCOMP)
- Issue likely in `broker.distribute()` or `Cluster.RoutePublish()`

**Files:**
- `broker/broker.go` - distribute() method
- `cluster/transport.go` - RoutePublish RPC
- `cluster/cross_node_test.go:93` - failing test

**Investigation Steps:**
1. Add debug logging to `broker.distribute()` for QoS 2 messages
2. Check if `Cluster.RoutePublish()` is called for QoS 2
3. Verify gRPC `RoutePublish` RPC handles QoS 2 correctly
4. Check if `DeliverToClient()` on remote node processes QoS 2
5. Compare QoS 1 vs QoS 2 code paths

**Test:** `go test -v -run TestCrossNode_QoS2 ./cluster/...`

**Success Criteria:**
- QoS 2 messages delivered across cluster nodes
- Test `TestCrossNode_QoS2` passes
- No message duplication

---

### Task 1.2: 3-Node Cluster Formation Test
**Priority:** HIGH | **Effort:** 3-4 hours | **Blocking:** Cluster validation

**Context:**
- Clustering code complete but needs end-to-end validation
- Need to verify all nodes join, elect leader, replicate data

**New File:** `cluster/formation_test.go`

**Test Cases:**
```go
func TestClusterFormation_ThreeNodes(t *testing.T) {
    // 1. Start 3 brokers with etcd cluster config
    // 2. Verify all nodes see each other via etcd membership
    // 3. Wait for leader election (within 10 seconds)
    // 4. Verify etcd replication (write to one, read from another)
}

func TestClusterFormation_LeaderElection(t *testing.T) {
    // 1. Start 3 nodes
    // 2. Identify leader
    // 3. Kill leader node
    // 4. Verify new leader elected within 30 seconds
    // 5. Verify background tasks continue (session expiry, will trigger)
}

func TestClusterFormation_DataReplication(t *testing.T) {
    // 1. Client subscribes on node-1
    // 2. Verify subscription visible on node-2 via etcd
    // 3. Client publishes retained message on node-2
    // 4. New client on node-3 subscribes, receives retained
}
```

**Implementation:**
- Use `testutil.NewTestCluster(t, 3)` pattern
- Add helper: `cluster.WaitForLeaderElection(timeout)`
- Add helper: `cluster.VerifyReplication(key, value, nodes)`

**Success Criteria:**
- All 3 nodes join cluster successfully
- Leader elected within 10 seconds
- etcd replication confirmed
- Leader failover works

---

### Task 1.3: Session Persistence Across Restart
**Priority:** HIGH | **Effort:** 4-5 hours | **Blocking:** Durability guarantee

**Context:**
- BadgerDB implemented and tested at unit level
- Need integration test proving restart durability
- Critical for production deployment

**New File:** `storage/badger/integration_test.go`

**Test Scenario:**
```go
func TestBadgerDB_SessionPersistence(t *testing.T) {
    // Setup
    tmpDir := t.TempDir()

    // 1. Start broker with BadgerDB
    cfg := &config.Config{
        Storage: config.StorageConfig{
            Type: "badger",
            Badger: config.BadgerConfig{Dir: tmpDir},
        },
    }
    broker := broker.New(cfg)
    broker.Start()

    // 2. Client connects, subscribes to "test/#"
    client := mqtt.Connect("test-client", broker.Addr())
    client.Subscribe("test/#", 1)

    // 3. Publish QoS 1 messages while client is offline
    client.Disconnect()
    broker.Publish("test/topic", []byte("msg1"), 1, false)
    broker.Publish("test/topic", []byte("msg2"), 1, false)

    // 4. Restart broker (close and reopen BadgerDB)
    broker.Shutdown()

    broker2 := broker.New(cfg)  // Same BadgerDB dir
    broker2.Start()

    // 5. Client reconnects
    client.Reconnect(broker2.Addr())

    // 6. Verify: subscriptions restored, offline queue delivered
    msgs := client.WaitForMessages(2, 5*time.Second)
    assert.Len(t, msgs, 2)
    assert.Equal(t, "msg1", string(msgs[0].Payload))
    assert.Equal(t, "msg2", string(msgs[1].Payload))
}
```

**Key Checks:**
- Session expiry timestamp preserved
- Inflight messages restored
- Offline queue intact
- Subscriptions active
- Will message persisted

**Success Criteria:**
- Test passes with broker restart
- No data loss
- Session restored in <1 second

---

### Task 1.4: Prometheus Metrics Endpoint
**Priority:** HIGH | **Effort:** 2-3 hours | **Blocking:** Production observability

**Context:**
- `broker.Stats` already tracks all metrics
- Need to expose as Prometheus `/metrics` endpoint
- Copy health server pattern

**New Files:**
- `server/metrics/server.go` - HTTP server for `/metrics`
- `server/metrics/prometheus.go` - Prometheus collectors

**Implementation:**
```go
// server/metrics/prometheus.go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/absmach/mqtt/broker"
)

var (
    ConnectionsTotal = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mqtt_connections_total",
            Help: "Current number of MQTT connections",
        },
        []string{"protocol", "version"},
    )

    MessagesReceived = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mqtt_messages_received_total",
            Help: "Total messages received from clients",
        },
        []string{"qos"},
    )

    MessagesSent = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mqtt_messages_sent_total",
            Help: "Total messages sent to clients",
        },
        []string{"qos"},
    )

    BytesReceived = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "mqtt_bytes_received_total",
            Help: "Total bytes received",
        },
    )

    BytesSent = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "mqtt_bytes_sent_total",
            Help: "Total bytes sent",
        },
    )

    SubscriptionsActive = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "mqtt_subscriptions_active",
            Help: "Number of active subscriptions",
        },
    )

    ErrorsTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mqtt_errors_total",
            Help: "Total errors by type",
        },
        []string{"type"},  // protocol, auth, packet
    )
)

func init() {
    prometheus.MustRegister(ConnectionsTotal)
    prometheus.MustRegister(MessagesReceived)
    prometheus.MustRegister(MessagesSent)
    prometheus.MustRegister(BytesReceived)
    prometheus.MustRegister(BytesSent)
    prometheus.MustRegister(SubscriptionsActive)
    prometheus.MustRegister(ErrorsTotal)
}

type Collector struct {
    broker *broker.Broker
}

func NewCollector(b *broker.Broker) *Collector {
    return &Collector{broker: b}
}

func (c *Collector) Update() {
    stats := c.broker.Stats()

    ConnectionsTotal.WithLabelValues("tcp", "3.1.1").Set(float64(stats.GetCurrentConnections()))
    MessagesReceived.WithLabelValues("0").Add(float64(stats.MessagesReceived))
    MessagesSent.WithLabelValues("0").Add(float64(stats.MessagesSent))
    BytesReceived.Add(float64(stats.BytesReceived))
    BytesSent.Add(float64(stats.BytesSent))
    SubscriptionsActive.Set(float64(stats.SubscriptionsActive))
    ErrorsTotal.WithLabelValues("protocol").Add(float64(stats.ProtocolErrors))
    ErrorsTotal.WithLabelValues("auth").Add(float64(stats.AuthErrors))
    ErrorsTotal.WithLabelValues("packet").Add(float64(stats.PacketErrors))
}

// server/metrics/server.go
package metrics

import (
    "context"
    "net/http"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/absmach/mqtt/broker"
)

type Server struct {
    addr      string
    server    *http.Server
    collector *Collector
}

func NewServer(addr string, broker *broker.Broker) *Server {
    collector := NewCollector(broker)

    mux := http.NewServeMux()
    mux.Handle("/metrics", promhttp.Handler())

    return &Server{
        addr:      addr,
        collector: collector,
        server: &http.Server{
            Addr:    addr,
            Handler: mux,
        },
    }
}

func (s *Server) Start() error {
    // Update metrics every 10 seconds
    go func() {
        ticker := time.NewTicker(10 * time.Second)
        defer ticker.Stop()

        for range ticker.C {
            s.collector.Update()
        }
    }()

    return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
    return s.server.Shutdown(ctx)
}
```

**Config Addition:**
```yaml
server:
  metrics_addr: ":9090"
  metrics_enabled: true
```

**Integration in `cmd/broker/main.go`:**
```go
if cfg.Server.MetricsEnabled {
    metricsServer := metrics.NewServer(cfg.Server.MetricsAddr, broker)
    go func() {
        if err := metricsServer.Start(); err != nil {
            logger.Error("metrics server failed", slog.String("error", err.Error()))
        }
    }()
}
```

**Success Criteria:**
- `/metrics` endpoint accessible on port 9090
- All key metrics exposed
- Metrics update every 10 seconds
- Prometheus scraping works

---

### Task 1.5: TLS/SSL Support
**Priority:** HIGH | **Effort:** 3-4 hours | **Blocking:** Secure production deployment

**Context:**
- Production deployments require encrypted connections
- Need both server and client certificate support

**Files to Modify:**
- `config/config.go` - Add TLS config
- `server/tcp/server.go` - Wrap listener with TLS
- `server/websocket/server.go` - Add TLS to HTTP server

**Implementation:**
```go
// config/config.go
type TLSConfig struct {
    Enabled    bool   `yaml:"enabled"`
    CertFile   string `yaml:"cert_file"`
    KeyFile    string `yaml:"key_file"`
    CAFile     string `yaml:"ca_file"`
    ClientAuth string `yaml:"client_auth"`  // none, request, require
}

type ServerConfig struct {
    TCPAddr    string    `yaml:"tcp_addr"`
    TLS        TLSConfig `yaml:"tls"`
    // ... existing fields
}

// server/tcp/server.go
func (s *Server) Start() error {
    listener, err := net.Listen("tcp", s.addr)
    if err != nil {
        return err
    }

    // Wrap with TLS if enabled
    if s.tlsConfig.Enabled {
        tlsConf, err := s.buildTLSConfig()
        if err != nil {
            return fmt.Errorf("tls config: %w", err)
        }
        listener = tls.NewListener(listener, tlsConf)
        s.logger.Info("TLS enabled", slog.String("addr", s.addr))
    }

    s.listener = listener
    // ... rest of Start()
}

func (s *Server) buildTLSConfig() (*tls.Config, error) {
    cert, err := tls.LoadX509KeyPair(s.tlsConfig.CertFile, s.tlsConfig.KeyFile)
    if err != nil {
        return nil, fmt.Errorf("load key pair: %w", err)
    }

    tlsConfig := &tls.Config{
        Certificates: []tls.Certificate{cert},
        MinVersion:   tls.VersionTLS12,
    }

    // Client certificate verification
    if s.tlsConfig.ClientAuth != "none" && s.tlsConfig.CAFile != "" {
        caCert, err := os.ReadFile(s.tlsConfig.CAFile)
        if err != nil {
            return nil, fmt.Errorf("read ca file: %w", err)
        }

        caCertPool := x509.NewCertPool()
        if !caCertPool.AppendCertsFromPEM(caCert) {
            return nil, fmt.Errorf("invalid ca certificate")
        }

        tlsConfig.ClientCAs = caCertPool

        switch s.tlsConfig.ClientAuth {
        case "require":
            tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
        case "request":
            tlsConfig.ClientAuth = tls.RequestClientCert
        default:
            tlsConfig.ClientAuth = tls.NoClientCert
        }
    }

    return tlsConfig, nil
}
```

**Config Example:**
```yaml
server:
  tcp_addr: ":8883"  # Standard MQTT TLS port
  tls:
    enabled: true
    cert_file: "/etc/mqtt/certs/server.crt"
    key_file: "/etc/mqtt/certs/server.key"
    ca_file: "/etc/mqtt/certs/ca.crt"
    client_auth: "none"  # or "request" or "require"
```

**Test File:** `server/tcp/tls_test.go`
```go
func TestTLS_ServerClientAuth(t *testing.T)
func TestTLS_RequireClientCert(t *testing.T)
func TestTLS_InvalidCert(t *testing.T)
```

**Success Criteria:**
- TLS handshake succeeds
- MQTT traffic encrypted
- Client certificate validation works
- Test coverage >80%

---

## Priority 2: MEDIUM - Feature Completion

### Task 2.1: Optimize Retained Message Matching
**Priority:** MEDIUM | **Effort:** 2-3 hours | **Impact:** Performance

**Context:**
- Currently scans ALL retained messages in etcd with prefix scan
- Inefficient for large retained message sets
- Should use local cache pattern like subscriptions

**File:** `cluster/etcd.go` - `GetRetainedMatching()` method

**Implementation:**
```go
// cluster/etcd.go
type EtcdCluster struct {
    // ... existing fields
    retainedCache map[string]*storage.Message
    retainedMu    sync.RWMutex
}

func (e *EtcdCluster) Start() error {
    // ... existing startup

    // Load retained cache
    if err := e.loadRetainedCache(); err != nil {
        return fmt.Errorf("load retained cache: %w", err)
    }

    // Watch for retained changes
    go e.watchRetained()

    return nil
}

func (e *EtcdCluster) loadRetainedCache() error {
    ctx := context.Background()
    resp, err := e.client.Get(ctx, "/retained/", clientv3.WithPrefix())
    if err != nil {
        return err
    }

    e.retainedMu.Lock()
    defer e.retainedMu.Unlock()

    for _, kv := range resp.Kvs {
        topic := strings.TrimPrefix(string(kv.Key), "/retained/")
        var msg storage.Message
        if err := json.Unmarshal(kv.Value, &msg); err != nil {
            continue
        }
        e.retainedCache[topic] = &msg
    }

    return nil
}

func (e *EtcdCluster) watchRetained() {
    watchChan := e.client.Watch(context.Background(), "/retained/", clientv3.WithPrefix())

    for resp := range watchChan {
        for _, ev := range resp.Events {
            topic := strings.TrimPrefix(string(ev.Kv.Key), "/retained/")

            e.retainedMu.Lock()
            switch ev.Type {
            case clientv3.EventTypePut:
                var msg storage.Message
                if err := json.Unmarshal(ev.Kv.Value, &msg); err == nil {
                    e.retainedCache[topic] = &msg
                }
            case clientv3.EventTypeDelete:
                delete(e.retainedCache, topic)
            }
            e.retainedMu.Unlock()
        }
    }
}

func (e *EtcdCluster) GetRetainedMatching(filter string) ([]*storage.Message, error) {
    e.retainedMu.RLock()
    defer e.retainedMu.RUnlock()

    var matches []*storage.Message

    for topic, msg := range e.retainedCache {
        if topics.Match(filter, topic) {
            matches = append(matches, msg)
        }
    }

    return matches, nil
}
```

**Success Criteria:**
- GetRetainedMatching() uses local cache
- Cache updates via etcd watch
- Performance: O(N) scan of cache vs O(N) etcd RPCs
- Benchmark shows >10x improvement

---

### Task 2.2: MQTT 5.0 Topic Aliases
**Priority:** MEDIUM | **Effort:** 2-3 hours | **Impact:** Bandwidth optimization

**Context:**
- Packet structures exist but not used
- Session has alias maps but not applied
- Reduces bandwidth for frequent topics

**Files:**
- `session/session.go` - Already has alias methods
- `handlers/broker.go` - Apply aliases on PUBLISH
- `packets/v5/publish.go` - Handle alias in properties

**Implementation:**
```go
// handlers/broker.go
func (h *BrokerHandler) HandlePublish(sess *session.Session, pkt *v5.Publish) error {
    // Apply topic alias if present
    if pkt.TopicAlias > 0 {
        if pkt.TopicName != "" {
            // Register new alias
            sess.SetTopicAlias(pkt.TopicName, pkt.TopicAlias)
        } else {
            // Resolve existing alias
            topic, ok := sess.GetTopicAlias(pkt.TopicAlias)
            if !ok {
                return h.sendError(sess, v5.ProtocolError, "unknown topic alias")
            }
            pkt.TopicName = topic
        }
    }

    // ... rest of HandlePublish
}
```

**Tests:**
```go
func TestTopicAlias_FirstUse(t *testing.T)      // Alias assignment
func TestTopicAlias_SubsequentUse(t *testing.T) // Alias resolution
func TestTopicAlias_MaxExceeded(t *testing.T)   // Error handling
func TestTopicAlias_ClearedOnDisconnect(t *testing.T)
```

**Success Criteria:**
- Clients can assign topic aliases
- Aliases resolved correctly
- Aliases cleared on disconnect
- Max alias enforcement

---

### Task 2.3: MQTT 5.0 Shared Subscriptions
**Priority:** MEDIUM | **Effort:** 4-6 hours | **Impact:** Load balancing

**Context:**
- Load balancing across multiple subscribers
- Format: `$share/{ShareName}/{TopicFilter}`
- Round-robin distribution within share group

**Files:**
- `topics/shared.go` - NEW: Parse shared subscription format
- `broker/router.go` - Modify for group-based routing
- `storage/storage.go` - Add ShareName field to Subscription

**Implementation:**
```go
// topics/shared.go
package topics

import "strings"

type SharedSubscription struct {
    ShareName   string
    TopicFilter string
}

func ParseShared(filter string) (shareName, topicFilter string, isShared bool) {
    if !strings.HasPrefix(filter, "$share/") {
        return "", filter, false
    }

    parts := strings.SplitN(filter[7:], "/", 2)
    if len(parts) != 2 {
        return "", filter, false
    }

    return parts[0], parts[1], true
}

// broker/router.go
type Router struct {
    normal map[string]*trieNode
    shared map[string]*SharedGroup  // ShareName+Filter -> group
    mu     sync.RWMutex
}

type SharedGroup struct {
    ShareName   string
    TopicFilter string
    Subscribers []string  // Client IDs
    lastIndex   int       // For round-robin
}

func (r *Router) Subscribe(clientID, filter string, qos byte) error {
    r.mu.Lock()
    defer r.mu.Unlock()

    if shareName, topicFilter, isShared := topics.ParseShared(filter); isShared {
        // Add to shared group
        key := shareName + "/" + topicFilter
        group, ok := r.shared[key]
        if !ok {
            group = &SharedGroup{
                ShareName:   shareName,
                TopicFilter: topicFilter,
                Subscribers: []string{},
            }
            r.shared[key] = group
        }

        // Add subscriber if not already present
        for _, sub := range group.Subscribers {
            if sub == clientID {
                return nil
            }
        }
        group.Subscribers = append(group.Subscribers, clientID)
    } else {
        // Normal subscription
        return r.addSubscription(clientID, filter, qos)
    }

    return nil
}

func (r *Router) Match(topic string) []Subscription {
    r.mu.RLock()
    defer r.mu.RUnlock()

    var subs []Subscription

    // 1. Get normal subscribers
    subs = append(subs, r.matchNormal(topic)...)

    // 2. Get shared subscribers (one per group)
    for _, group := range r.shared {
        if topics.Match(group.TopicFilter, topic) {
            if len(group.Subscribers) > 0 {
                // Round-robin
                clientID := group.Subscribers[group.lastIndex]
                group.lastIndex = (group.lastIndex + 1) % len(group.Subscribers)

                subs = append(subs, Subscription{
                    ClientID: clientID,
                    Filter:   group.TopicFilter,
                    QoS:      1,  // Default QoS
                })
            }
        }
    }

    return subs
}
```

**Tests:**
```go
func TestSharedSubscription_Parse(t *testing.T)
func TestSharedSubscription_LoadBalancing(t *testing.T)
func TestSharedSubscription_MixedWithNormal(t *testing.T)
func TestSharedSubscription_SubscriberLeaves(t *testing.T)
func TestSharedSubscription_MultipleGroups(t *testing.T)
```

**Success Criteria:**
- Shared subscriptions parsed correctly
- Round-robin distribution works
- Normal and shared subs coexist
- Subscriber removal handled

---

### Task 2.4: Message Expiry Enforcement
**Priority:** MEDIUM | **Effort:** 3-4 hours | **Impact:** MQTT 5.0 compliance

**Context:**
- MQTT 5.0 Message Expiry Interval property
- Property stored but not enforced
- Need background cleanup + delivery-time check

**Files:**
- `storage/storage.go` - Add ExpiresAt field to Message
- `session/queue.go` - Check expiry before delivery
- `broker/broker.go` - Add background expiry cleanup

**Implementation:**
```go
// storage/storage.go
type Message struct {
    Topic      string
    Payload    []byte
    QoS        byte
    Retain     bool
    ExpiresAt  time.Time  // NEW
    Properties map[string]string
}

// session/queue.go
func (q *MessageQueue) Dequeue() *storage.Message {
    q.mu.Lock()
    defer q.mu.Unlock()

    for len(q.messages) > 0 {
        msg := q.messages[0]
        q.messages = q.messages[1:]

        // Check if expired
        if !msg.ExpiresAt.IsZero() && time.Now().After(msg.ExpiresAt) {
            continue  // Skip expired message
        }

        return msg
    }
    return nil
}

// broker/broker.go
func (b *Broker) startExpiryCleanup() {
    ticker := time.NewTicker(5 * time.Minute)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            b.cleanupExpiredMessages()
        case <-b.done:
            return
        }
    }
}

func (b *Broker) cleanupExpiredMessages() {
    now := time.Now()

    // Cleanup retained messages
    retained, _ := b.retainedStore.GetAll()
    for topic, msg := range retained {
        if !msg.ExpiresAt.IsZero() && now.After(msg.ExpiresAt) {
            b.retainedStore.Delete(topic)
        }
    }

    // Cleanup offline queues
    sessions := b.sessionManager.GetAll()
    for _, sess := range sessions {
        if !sess.Connected {
            sess.OfflineQueue.RemoveExpired(now)
        }
    }
}

// handlers/broker.go - Set expiry on PUBLISH
func (h *BrokerHandler) HandlePublish(sess *session.Session, pkt *v5.Publish) error {
    msg := &storage.Message{
        Topic:   pkt.TopicName,
        Payload: pkt.Payload,
        QoS:     pkt.QoS,
        Retain:  pkt.Retain,
    }

    // Set expiry if present
    if pkt.MessageExpiryInterval > 0 {
        msg.ExpiresAt = time.Now().Add(time.Duration(pkt.MessageExpiryInterval) * time.Second)
    }

    return h.broker.Publish(msg)
}
```

**Tests:**
```go
func TestMessageExpiry_BeforeDelivery(t *testing.T)
func TestMessageExpiry_InQueue(t *testing.T)
func TestMessageExpiry_Retained(t *testing.T)
func TestMessageExpiry_BackgroundCleanup(t *testing.T)
```

**Success Criteria:**
- Expired messages not delivered
- Retained messages cleaned up
- Offline queue expiry works
- Background cleanup runs every 5 minutes

---

## Priority 3: LOW - Advanced Features

### Task 3.1: Authentication & Authorization
**Priority:** LOW | **Effort:** 6-8 hours | **Impact:** Security

**Context:**
- Production deployments need auth
- Support password and JWT-based auth
- ACL for publish/subscribe authorization

**New Files:**
- `auth/auth.go` - Authentication interface
- `auth/password.go` - Password-based auth with bcrypt
- `auth/jwt.go` - JWT-based auth
- `auth/acl.go` - Access Control Lists

**Implementation:** See detailed spec in original roadmap Phase 5.2.2

**Success Criteria:**
- Password auth with bcrypt
- JWT token validation
- ACL rules enforced
- Auth failures logged

---

### Task 3.2: Rate Limiting
**Priority:** LOW | **Effort:** 2-3 hours | **Impact:** DoS protection

**Context:**
- Prevent abusive clients
- Per-client message rate limits
- Configurable thresholds

**Implementation:** See detailed spec in original roadmap Phase 5.2.3

**Success Criteria:**
- Per-client rate limiting
- Configurable limits
- Graceful handling (drop vs disconnect)

---

### Task 3.3: Admin API
**Priority:** LOW | **Effort:** 4-6 hours | **Impact:** Operations

**Context:**
- REST API for management
- Session management, topic stats, metrics

**Endpoints:**
- `GET /api/v1/sessions` - List sessions
- `GET /api/v1/sessions/{client_id}` - Get session details
- `DELETE /api/v1/sessions/{client_id}` - Kill session
- `GET /api/v1/topics` - List topics
- `GET /api/v1/topics/{topic}/stats` - Topic statistics

**Implementation:** See detailed spec in original roadmap Phase 5.4.2

**Success Criteria:**
- All endpoints functional
- Authentication required
- JSON responses
- Error handling

---

## Testing Strategy

### Unit Tests (Target: >85% coverage)
- Session package (reconnection, inflight, keepalive)
- Broker package (concurrent publish, router edge cases)
- Handlers package (QoS 2 flow, validation)
- Storage packages (concurrent ops, wildcard matching)

### Integration Tests (Target: 50+ scenarios)
- Cross-node messaging (all QoS levels)
- Session takeover scenarios
- Cluster formation and failover
- Session persistence across restarts
- MQTT 5.0 feature compliance

### Performance Tests
- Benchmark: 100K messages/sec throughput
- Benchmark: 1000 concurrent connections
- Benchmark: 10K topics with wildcards
- Benchmark: Session takeover latency <200ms

---

## Configuration Reference

### Complete Production Config
```yaml
server:
  tcp_addr: ":1883"
  tcp_max_connections: 100000

  # TLS
  tls:
    enabled: true
    cert_file: "/etc/mqtt/certs/server.crt"
    key_file: "/etc/mqtt/certs/server.key"
    ca_file: "/etc/mqtt/certs/ca.crt"
    client_auth: "none"

  # WebSocket
  ws_addr: ":8083"
  ws_path: "/mqtt"
  ws_enabled: true

  # HTTP Bridge
  http_addr: ":8080"
  http_enabled: true

  # Health Checks
  health_addr: ":8081"
  health_enabled: true

  # Metrics
  metrics_addr: ":9090"
  metrics_enabled: true

cluster:
  enabled: true
  node_id: "broker-1"

  etcd:
    data_dir: "/var/lib/mqtt/etcd"
    bind_addr: "0.0.0.0:2380"
    client_addr: "0.0.0.0:2379"
    advertise_addr: "192.168.1.10:2380"
    initial_cluster: "broker-1=http://192.168.1.10:2380,broker-2=http://192.168.1.11:2380,broker-3=http://192.168.1.12:2380"
    bootstrap: false

  transport:
    bind_addr: "0.0.0.0:7948"
    peers:
      broker-2: "192.168.1.11:7948"
      broker-3: "192.168.1.12:7948"

  takeover_timeout: 5s
  rpc_timeout: 3s
  lease_ttl: 30s

storage:
  type: "badger"
  badger:
    dir: "/var/lib/mqtt/data"
    value_log_file_size: 1073741824  # 1GB
    num_versions_to_keep: 1

log:
  level: "info"
  format: "json"

webhooks:
  enabled: true
  endpoints:
    - url: "http://webhook-service:8080/events"
      events:
        - "client.connected"
        - "client.disconnected"
        - "message.published"
      filters:
        topics:
          - "sensors/#"
          - "devices/#"
```

---

## Timeline Estimate

| Priority | Tasks | Effort | Duration |
|----------|-------|--------|----------|
| HIGH (Production Readiness) | 1.1-1.5 | 15-19 hours | 2 weeks |
| MEDIUM (Features) | 2.1-2.4 | 11-16 hours | 1.5 weeks |
| LOW (Advanced) | 3.1-3.3 | 12-17 hours | 1.5 weeks |
| **Total** | **12 tasks** | **38-52 hours** | **5 weeks** |

---

## Next Actions (Immediate)

1. **QoS 2 Investigation** (Task 1.1) - Unblock MQTT compliance
2. **3-Node Cluster Test** (Task 1.2) - Validate clustering works
3. **Prometheus Metrics** (Task 1.4) - Enable production monitoring
4. **Session Persistence Test** (Task 1.3) - Prove durability
5. **TLS Support** (Task 1.5) - Enable secure deployment

**Recommended Order:** 1.1 → 1.2 → 1.4 → 1.5 → 1.3 → 2.1 → 2.2

---

**Document Version:** 2.0
**Last Updated:** 2025-12-24
**Next Review:** After Priority 1 completion
