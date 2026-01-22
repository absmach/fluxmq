# MQTT Broker Development Roadmap

**Last Updated:** 2026-01-19
**Current Phase:** Phase Q - Queue Architecture Redesign (TOP PRIORITY)

---

## Overview

Production-ready MQTT broker with focus on high performance, durability, and scalability.

### Current Status

- 🚨 **Phase Q: Queue Architecture Redesign** - TOP PRIORITY (Log-based model with work stealing)
- ⏸️ **Phase 0: Production Hardening** - PAUSED (Resume after Phase Q)
- ✅ **Phase 1: Performance Optimization** - COMPLETE (3.27x faster)
- ✅ **Phase 2: Queue Replication** - COMPLETE (~98%)
  - ✅ Phase 2.1: Raft Infrastructure - COMPLETE
  - ✅ Phase 2.2: Queue Integration - COMPLETE
  - ✅ Phase 2.3: Testing & Optimization - COMPLETE (core tests done, benchmarks done)
  - ✅ Phase 2.4: Retention Policies - COMPLETE (time/size retention + log compaction)
  - 📋 Phase 2.5: Observability & Migration - PLANNED (deferred)
- ⏳ **Phase 3: E2E Cluster Testing** - PLANNED (after Phase 2)
- 📋 **Phase 4: Custom Raft** - FUTURE (50M+ clients only)

---

## 🎯 Immediate Next Steps

**Current Sprint Focus: Phase Q - Queue Architecture Redesign (TOP PRIORITY)**

Phase Q redesigns the queue system from delete-on-ack to a log-based model inspired by Kafka and Redis Streams. This enables wildcard queue subscriptions, work stealing, and better performance.

**Next: Phase 0 - Production Hardening (After Phase Q)**

Phase 0 must be completed before any production deployment. These are critical security vulnerabilities and operational gaps identified during code audit.

**Completed (2026-01-13):**
- ✅ Fixed Raft storage initialization (BadgerStableStore contract)
- ✅ Fixed partition port conflicts (per-partition bind addresses)
- ✅ Fixed message pool corruption (copy message for Raft FSM)
- ✅ Fixed MessageTTL=0 causing immediate expiration
- ✅ `TestReplication_BasicEnqueueDequeue` now passing
- ✅ Created comprehensive performance benchmarks (`replication_bench_test.go`)
- ✅ **Phase 2.3 Core Tests Complete:**
  - ✅ `TestFailover_LeaderElection` - Leader failover <5s (2.4s achieved)
  - ✅ `TestFailover_MessageDurability` - Messages survive leader failure
  - ✅ `TestReplication_ISRTracking` - ISR tracking and quorum maintenance
  - ✅ `TestFailover_FollowerCatchup` - Follower lag behavior (100 msg delta)
- ✅ **Phase 2.4 Retention Policies - COMPLETE:**
  - ✅ RetentionPolicy schema added to QueueConfig
  - ✅ RetentionManager core logic (400+ lines)
  - ✅ Raft OpRetentionDelete operation
  - ✅ MessageStore interface extended (5 new methods including ListAllMessages)
  - ✅ Full retention implementations in memory and badger stores
  - ✅ Manager integration (async size checks on enqueue)
  - ✅ Partition worker integration (background time-based cleanup)
  - ✅ Leader-only execution for replicated queues
  - ✅ **Retention unit tests** (`retention_test.go`, `retention_manager_test.go`)
  - ✅ **Retention integration tests** (`retention_integration_test.go`)
  - ✅ **Log compaction implementation** (Kafka-style, keeps latest per key)
  - ✅ **Compaction unit tests** (4 tests: basic, lag, no-key, not-configured)
  - ✅ **Compaction integration tests** (2 tests: replication, leader-only)

**Completed (2026-01-16):**
- ✅ **Rate Limiting - COMPLETE:**
  - ✅ Per-IP connection rate limiting (TCP, WebSocket)
  - ✅ Per-client message rate limiting (V3/V5 handlers)
  - ✅ Per-client subscription rate limiting
  - ✅ Configurable rates, bursts, cleanup intervals
  - ✅ 12 unit tests (`ratelimit/ratelimit_test.go`)
- ✅ **CoAP with DTLS/mDTLS - COMPLETE:**
  - ✅ Full UDP CoAP implementation using go-coap v3
  - ✅ DTLS support with pion/dtls v3
  - ✅ Mutual TLS (mDTLS) client verification
  - ✅ 5 unit tests (`server/coap/server_test.go`)

**Next: Phase 0 - Production Hardening**

Remaining production hardening tasks:

1. **Secure Default ACL (P0 Critical)** - Change default to deny-all
2. **Distributed Tracing (P2)** - Instrument message paths with spans
3. **Prometheus Endpoint (P2)** - Native `/metrics` endpoint

**Deferred: Phase 2.5 Observability & Migration**
- Will be implemented after Phase 0
- Metrics, health checks, and migration tooling
- Can be done in parallel with production usage

**See Phase 0 section below for complete details.**

---

## Phase Q: Queue Architecture Redesign 🚨 TOP PRIORITY

**Status:** PLANNING
**Goal:** Redesign queue system to log-based model with cursors, PEL, and work stealing

This phase fundamentally changes how queues work, moving from a delete-on-ack model to an append-only log model inspired by Kafka and Redis Streams. This enables wildcard queue subscriptions and improves performance and reliability.

---

### Motivation

**Current Model Problems:**
- Each `$queue/a/b/c` is a separate queue with independent storage
- Wildcard subscriptions (`$queue/tasks/+`) don't work - would create literal queue named `$queue/tasks/+`
- Delete-on-ack requires random I/O and causes fragmentation
- Partition assignment is sticky - slow consumer blocks partition

**New Model Benefits:**
- Wildcard queue subscriptions work naturally (single log, filter at read time)
- Sequential append-only writes (high throughput)
- Work stealing enables self-healing (claim from slow consumer's PEL)
- Simpler recovery (just load cursors, no rebuild)

---

### Architecture: Log-Based Model

#### Core Concepts

```
Queue: $queue/tasks (single append-only log)
├── Partition 0: [offset 0] [offset 1] [offset 2] ... [offset N]
├── Partition 1: [offset 0] [offset 1] [offset 2] ... [offset M]
└── Partition 2: [offset 0] [offset 1] [offset 2] ... [offset K]

Each message has:
- Offset (position in partition log)
- Routing key (subtopic after queue name)
- Payload, properties, timestamp
```

#### Consumer Groups with Cursors

```
Consumer Group "workers" for $queue/tasks/#
├── Partition 0:
│   ├── cursor: 100 (next offset to deliver)
│   ├── committed: 95 (oldest unacked)
│   └── PEL: {consumer-a: [95, 97], consumer-b: [96, 98, 99]}
├── Partition 1:
│   ├── cursor: 50
│   ├── committed: 48
│   └── PEL: {consumer-a: [48, 49]}
└── Partition 2: ...
```

#### Key Differences from Current Model

| Aspect | Current | New (Log-Based) |
|--------|---------|-----------------|
| Storage | Delete on ack | Append-only, truncate by retention |
| Consumer state | Partition assignment (sticky) | Cursor + PEL per partition |
| Delivery | Push to assigned consumer | Consumer claims from any partition |
| Ack | Delete message | Advance committed offset, remove from PEL |
| Failure handling | Redeliver after timeout | Work stealing (claim from other's PEL) |
| Wildcards | Not supported | Supported via routing key filtering |

---

### Topic Model: Queue Root + Routing Key

**Convention:** `$queue/{name}/{routing-key...}`

```
Publish to $queue/tasks/images    → queue="$queue/tasks", routing_key="images"
Publish to $queue/tasks/videos    → queue="$queue/tasks", routing_key="videos"
Publish to $queue/tasks/us/images → queue="$queue/tasks", routing_key="us/images"
```

**Subscription Patterns:**

| Pattern | Behavior |
|---------|----------|
| `$queue/tasks` | All messages (no filter) |
| `$queue/tasks/#` | All messages (explicit wildcard) |
| `$queue/tasks/+` | All messages (single-level wildcard) |
| `$queue/tasks/images` | Filter: routing_key == "images" |
| `$queue/tasks/+/png` | Filter: routing_key matches "*/png" |

**Consumer Groups per Subscription Pattern:**

```
Queue: $queue/tasks

Group "all-workers" @ "$queue/tasks/#"
├── Reads all messages
└── Cursor: p0=100, p1=95, p2=80

Group "image-workers" @ "$queue/tasks/images"
├── Reads only routing_key="images"
└── Cursor: p0=50, p1=48, p2=40 (lower - skips non-matching)
```

Same message delivered to both groups if routing key matches both patterns.

---

### Work Stealing

When a consumer is slow or crashes, other consumers can steal its pending work.

**Flow:**

1. Consumer A claims message at offset 100 → added to A's PEL with timestamp
2. Consumer A crashes or is slow
3. Consumer B requests work, cursor is at log tail (no new messages)
4. Consumer B scans PEL for entries older than visibility timeout
5. Consumer B claims offset 100 from A's PEL → moves to B's PEL
6. Consumer B processes and acks → removed from PEL, committed advances

**Data Structures:**

```go
type PendingEntry struct {
    Offset        uint64
    ClaimedAt     time.Time
    DeliveryCount int
    ConsumerID    string
}

type PartitionState struct {
    Cursor    uint64                    // Next offset to deliver
    Committed uint64                    // Min unacked offset (safe to truncate)
    PEL       map[string][]PendingEntry // ConsumerID -> pending entries
}

type ConsumerGroup struct {
    Name          string
    Pattern       string                      // Subscription pattern
    Filter        func(routingKey string) bool // Compiled filter
    Partitions    map[int]*PartitionState
}
```

**Claiming Algorithm:**

```go
func (g *ConsumerGroup) Claim(ctx context.Context, consumerID string, partition int) (*Message, error) {
    state := g.Partitions[partition]

    // 1. Try to get next from cursor
    if state.Cursor < log.Tail(partition) {
        offset := state.Cursor
        state.Cursor++

        msg := log.Read(partition, offset)
        if g.Filter(msg.RoutingKey) {
            state.PEL[consumerID] = append(state.PEL[consumerID], PendingEntry{
                Offset:    offset,
                ClaimedAt: time.Now(),
                ConsumerID: consumerID,
            })
            return msg, nil
        }
        // Skip non-matching, try next
        return g.Claim(ctx, consumerID, partition)
    }

    // 2. Cursor at tail - try work stealing
    return g.StealWork(ctx, consumerID, partition)
}

func (g *ConsumerGroup) StealWork(ctx context.Context, consumerID string, partition int) (*Message, error) {
    state := g.Partitions[partition]
    now := time.Now()

    for otherConsumer, entries := range state.PEL {
        if otherConsumer == consumerID {
            continue
        }
        for i, entry := range entries {
            if now.Sub(entry.ClaimedAt) > visibilityTimeout {
                // Steal this entry
                state.PEL[otherConsumer] = slices.Delete(entries, i, i+1)
                entry.ClaimedAt = now
                entry.DeliveryCount++
                entry.ConsumerID = consumerID
                state.PEL[consumerID] = append(state.PEL[consumerID], entry)

                return log.Read(partition, entry.Offset), nil
            }
        }
    }

    return nil, ErrNoMessages
}
```

---

### Ack and Committed Offset

**Ack advances committed offset (not cursor):**

```
Before:
  cursor=10, committed=5, PEL=[5,6,7,8,9]

Ack offset 5:
  cursor=10, committed=6, PEL=[6,7,8,9]

Ack offset 7 (out of order):
  cursor=10, committed=6, PEL=[6,8,9]  ← committed stays (gap at 6)

Ack offset 6:
  cursor=10, committed=8, PEL=[8,9]    ← committed jumps past filled gap
```

**Committed offset determines retention truncation:**

```go
func (g *ConsumerGroup) Ack(ctx context.Context, consumerID string, partition int, offset uint64) error {
    state := g.Partitions[partition]

    // Remove from PEL
    entries := state.PEL[consumerID]
    for i, e := range entries {
        if e.Offset == offset {
            state.PEL[consumerID] = slices.Delete(entries, i, i+1)
            break
        }
    }

    // Advance committed if possible
    state.advanceCommitted()

    return nil
}

func (state *PartitionState) advanceCommitted() {
    // Find minimum offset across all PEL entries
    minPending := uint64(math.MaxUint64)
    for _, entries := range state.PEL {
        for _, e := range entries {
            if e.Offset < minPending {
                minPending = e.Offset
            }
        }
    }

    if minPending > state.Committed {
        state.Committed = minPending
    }
}
```

---

### Implementation Plan

#### Q.1: Storage Layer Changes

**Goal:** Append-only log storage with offset-based access

**Tasks:**
- [ ] Define `LogStore` interface (append, read by offset, truncate)
- [ ] Implement memory-based log store (ring buffer with offset tracking)
- [ ] Implement BadgerDB-based log store (offset as key prefix)
- [ ] Add offset-based iteration (read range of offsets)
- [ ] Implement retention truncation (by offset, not message ID)
- [ ] Update hybrid store for log semantics
- [ ] Migrate existing tests to new interface

**Interface:**

```go
type LogStore interface {
    // Append adds message to partition, returns assigned offset
    Append(ctx context.Context, queue string, partition int, msg *Message) (uint64, error)

    // Read gets message at specific offset
    Read(ctx context.Context, queue string, partition int, offset uint64) (*Message, error)

    // ReadBatch reads messages from startOffset (inclusive) up to limit
    ReadBatch(ctx context.Context, queue string, partition int, startOffset uint64, limit int) ([]*Message, error)

    // Tail returns the next offset that will be assigned (log length)
    Tail(ctx context.Context, queue string, partition int) (uint64, error)

    // Truncate removes messages with offset < minOffset
    Truncate(ctx context.Context, queue string, partition int, minOffset uint64) error
}
```

#### Q.2: Consumer Group Redesign

**Goal:** Cursor-based consumer groups with PEL tracking

**Tasks:**
- [ ] Define new `ConsumerGroup` struct with cursor and PEL per partition
- [ ] Implement pattern-based subscription matching
- [ ] Add routing key filter compilation (wildcard patterns)
- [ ] Implement cursor advancement logic
- [ ] Implement PEL management (add, remove, query)
- [ ] Add committed offset tracking and advancement
- [ ] Persist consumer group state (cursors, PEL) to storage
- [ ] Recovery: reload consumer group state on startup

#### Q.3: Work Stealing Implementation

**Goal:** Automatic work redistribution from slow/dead consumers

**Tasks:**
- [ ] Add visibility timeout configuration
- [ ] Implement PEL scanning for timed-out entries
- [ ] Implement claim transfer between consumers
- [ ] Add delivery count tracking for DLQ routing
- [ ] Optimize with min-heap for O(1) oldest pending lookup
- [ ] Add metrics: steal count, steal latency, PEL depth

#### Q.4: Wildcard Subscription Support

**Goal:** Support `$queue/tasks/+` and `$queue/tasks/#` patterns

**Tasks:**
- [ ] Parse queue subscription patterns (extract queue name + filter)
- [ ] Create consumer group per unique (queue, pattern) combination
- [ ] Implement routing key extraction from publish topic
- [ ] Implement filter matching during claim
- [ ] Handle multiple groups receiving same message
- [ ] Update broker subscribe/unsubscribe handlers

#### Q.5: Delivery Integration

**Goal:** Integrate new model with message delivery system

**Tasks:**
- [ ] Update delivery workers to use claim-based model
- [ ] Implement batch claiming for efficiency
- [ ] Update ack/nack/reject handlers for new model
- [ ] Handle multi-group ack (message acked when all groups ack)
- [ ] Integrate with existing Raft replication (replicate cursor/PEL state)
- [ ] Update partition workers for log-based delivery

#### Q.6: Migration and Testing

**Goal:** Migrate existing queues and validate new model

**Tasks:**
- [ ] Create migration path for existing queue data
- [ ] Update all queue unit tests for new model
- [ ] Add cursor/PEL persistence tests
- [ ] Add work stealing tests (timeout, claim, concurrent)
- [ ] Add wildcard subscription tests
- [ ] Add multi-group delivery tests
- [ ] Performance benchmarks (compare to current model)
- [ ] Integration tests with real MQTT clients

---

### Success Criteria

| Criteria | Target |
|----------|--------|
| Wildcard subscriptions work | `$queue/tasks/+` receives from `$queue/tasks/a`, `$queue/tasks/b` |
| Work stealing functional | Pending messages claimed after visibility timeout |
| No message loss | All messages delivered at least once |
| Throughput improvement | ≥ current throughput (sequential I/O benefit) |
| Recovery time | Cursor reload < 1 second |
| PEL accuracy | No orphaned entries, no duplicate delivery |

---

### Configuration

```yaml
queue:
  model: log  # "log" (new) or "legacy" (current, deprecated)

  # Log storage settings
  log:
    segment_size: 1073741824    # 1GB per segment file
    index_interval: 4096        # Index every N messages

  # Consumer group settings
  consumer:
    visibility_timeout: 30s     # Time before message eligible for stealing
    max_delivery_count: 5       # Max deliveries before DLQ
    cursor_checkpoint_interval: 1s  # How often to persist cursors

  # Work stealing settings
  stealing:
    enabled: true
    scan_interval: 5s           # How often to scan for stealable work
    batch_size: 10              # Max messages to steal per scan

  # Retention (unchanged from Phase 2.4)
  retention:
    time: 168h
    bytes: 10737418240
```

---

### Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Breaking change for existing queues | Provide migration tool, support legacy mode |
| PEL memory growth | Limit PEL size, oldest entries go to DLQ |
| Work stealing contention | Randomize steal targets, backoff on conflict |
| Complex Raft integration | Reuse existing FSM patterns, incremental changes |

---

### Estimated Effort

| Sub-phase | Effort |
|-----------|--------|
| Q.1: Storage Layer | 1-2 weeks |
| Q.2: Consumer Groups | 1-2 weeks |
| Q.3: Work Stealing | 1 week |
| Q.4: Wildcards | 1 week |
| Q.5: Delivery Integration | 1-2 weeks |
| Q.6: Migration & Testing | 1-2 weeks |
| **Total** | **6-10 weeks** |

---

## Phase 0: Production Hardening ⏸️ PAUSED

**Status:** PAUSED - Resume after Phase Q
**Goal:** Address critical security vulnerabilities and operational gaps

This phase was identified through comprehensive code audit comparing against NATS, RabbitMQ, Kafka, HiveMQ, and EMQX.

---

### 0.1: Critical Security Fixes 🔴 CRITICAL

**Priority:** P0 - Block production deployment

**0.1.1 Secure Inter-Broker Communication** ✅ COMPLETE
- **File:** `cluster/transport.go`
- **Issue:** Uses `insecure.NewCredentials()` - cluster traffic is unencrypted
- **Risk:** Man-in-the-middle attacks, data interception between nodes
- **Fix:** Implemented mTLS for gRPC connections

**Configuration:**
```yaml
cluster:
  transport:
    tls_enabled: true
    tls_cert_file: "/path/to/server.crt"
    tls_key_file: "/path/to/server.key"
    tls_ca_file: "/path/to/ca.crt"
```

**Implementation:**
- [x] Add cluster TLS configuration options (`config/config.go`)
- [x] Load certificates for inter-broker mTLS auth (`cluster/transport.go`)
- [x] Server uses `tls.RequireAndVerifyClientCert` for mutual TLS
- [x] Client connections use same cert for peer authentication
- [x] Warning logged when TLS disabled (development mode)
- [ ] Implement certificate rotation support (future enhancement)
- [ ] Add cluster TLS validation tests (future enhancement)

**0.1.2 WebSocket Origin Validation** ✅ COMPLETE
- **File:** `server/websocket/server.go`
- **Issue:** `CheckOrigin` always returns `true` - accepts all origins
- **Risk:** Cross-Site WebSocket Hijacking (CSWSH), CSRF attacks
- **Fix:** Implemented configurable origin allowlist

**Configuration:**
```yaml
server:
  websocket:
    plain:
      allowed_origins:
        - "https://example.com"
        - "https://app.example.com"
        - "*.example.com"  # Wildcard subdomain support
```

**Implementation:**
- [x] Add `websocket.*.allowed_origins` configuration option (`config/config.go`)
- [x] Implement origin validation logic (`server/websocket/server.go`)
- [x] Support exact match origins
- [x] Support wildcard subdomain patterns (e.g., `*.example.com`)
- [x] Warning logged when origins not configured (development mode)
- [x] Requests without Origin header allowed (same-origin or non-browser)
- [ ] Add WebSocket security tests (future enhancement)

**0.1.3 Secure Default ACL**
- **File:** `broker/auth.go`
- **Issue:** Default allows all when no authorizer configured
- **Risk:** Unauthorized access to all topics and operations
- **Fix:** Change default to deny-all, require explicit authorization
- [ ] Change default ACL policy to deny-all
- [ ] Add explicit `development_mode: true` flag for permissive mode
- [ ] Log warnings when running without authorizer
- [ ] Document security configuration requirements

---

### 0.2: Rate Limiting ✅ COMPLETE

**Priority:** P1 - Required for production
**Status:** Completed 2026-01-16

**Implementation:**
- ✅ `ratelimit/ratelimit.go` - IPRateLimiter, ClientRateLimiter, Manager (~280 lines)
- ✅ Per-IP connection rate limiting in TCP server (`server/tcp/server.go`)
- ✅ Per-IP connection rate limiting in WebSocket server (`server/websocket/server.go`)
- ✅ Per-client message rate limiting in V5/V3 handlers (returns QuotaExceeded for V5)
- ✅ Per-client subscription rate limiting in V5/V3 handlers
- ✅ Rate limit configuration in `config/config.go`
- ✅ Rate limiter initialization in `cmd/broker/main.go`
- ✅ 12 unit tests (`ratelimit/ratelimit_test.go`)

**Configuration:**
```yaml
ratelimit:
  enabled: true
  connection:
    enabled: true
    rate: 1.67              # ~100 connections per minute per IP
    burst: 20
    cleanup_interval: 5m
  message:
    enabled: true
    rate: 1000              # messages per second per client
    burst: 100
  subscribe:
    enabled: true
    rate: 100               # subscriptions per second per client
    burst: 10
```

**Behavior:**
- Connection rate limiting: Closes connection immediately if exceeded (before MQTT handshake)
- Message rate limiting: Returns MQTT 5.0 `QuotaExceeded` (0x97) for QoS > 0, silently drops QoS 0
- Subscribe rate limiting: Returns `SubAckQuotaExceeded` for V5, `SubAckFailure` for V3

---

### 0.3: Observability Completion 🟡 MEDIUM

**Priority:** P2 - Required for production operations

**0.3.1 Distributed Tracing Instrumentation**
- **File:** `server/otel/tracer.go`
- **Issue:** Tracer created but never used in message paths
- **Risk:** No visibility into message flow, debugging blind spots
- [ ] Add spans to CONNECT/DISCONNECT handlers
- [ ] Add spans to PUBLISH/SUBSCRIBE/UNSUBSCRIBE handlers
- [ ] Add spans to message routing and delivery
- [ ] Propagate trace context through cluster forwarding
- [ ] Add span attributes (client_id, topic, qos, etc.)

**0.3.2 Prometheus Metrics Endpoint**
- **Issue:** Only OTLP export, no native Prometheus format
- **Risk:** Incompatible with most monitoring stacks
- [ ] Add `/metrics` endpoint with Prometheus format
- [ ] Expose all existing OTLP metrics in Prometheus format
- [ ] Add Prometheus configuration documentation

---

### 0.4: Protocol Compliance ✅ COMPLETE

**Priority:** P2 - Required for MQTT spec compliance

**0.4.1 MaxQoS Enforcement** ✅ COMPLETE
- **MQTT 5.0 Spec 3.2.2.1.4:** Server MUST announce MaxQoS in CONNACK
- **MQTT 5.0 Spec 3.3.2-4:** Server MUST downgrade inbound publish QoS

**Implementation (2026-01-14):**
- ✅ Added `MaxQoS` field to Broker struct with getter/setter (`broker/broker.go`)
- ✅ Added `max_qos` config option (`config/config.go`, default: 2)
- ✅ CONNACK now includes MaxQoS property (`broker/v5_handler.go`)
- ✅ Inbound publish QoS downgraded in both V5 and V3 handlers
- ✅ 6 unit tests covering config, setter, and downgrade behavior (`broker/maxqos_test.go`)

**Configuration:**
```yaml
broker:
  max_qos: 2  # Maximum QoS level (0, 1, or 2)
```

**0.4.2 Shared Subscriptions** ✅ COMPLETE
- **Status:** Fully implemented with comprehensive tests
- **Files:**
  - `topics/shared.go` - `$share/{group}/topic` parsing
  - `broker/shared_subscriptions.go` - Manager with round-robin
  - `broker/subscribe.go` - Subscription routing (lines 38-51)
  - `broker/publish.go` - Delivery with group selection (lines 192-236)
- **Tests:** 15+ tests in `shared_subscriptions_test.go` and `shared_test.go`
- **Features:**
  - ✅ `$share/{ShareName}/{TopicFilter}` format parsing
  - ✅ Round-robin load balancing
  - ✅ Single message delivery per publish to group
  - ✅ QoS downgrade to subscription level
  - ✅ Retained flag cleared for shared subs (per spec)
  - ✅ Session termination cleanup

---

### 0.5: Management Dashboard 🟢 ENHANCEMENT

**Priority:** P3 - Improves operational experience

**Goal:** Modern web UI for broker management and monitoring

**Dashboard Features:**
- **Overview Page**
  - Connection count (current, peak, 24h graph)
  - Message throughput (publish/subscribe rates)
  - Cluster health status
  - Active topics and subscriptions count

- **Clients Page**
  - Client list with search/filter
  - Per-client details (session info, subscriptions, stats)
  - Disconnect client action
  - Connection history

- **Topics Page**
  - Topic tree visualization
  - Per-topic subscriber count
  - Message rate per topic
  - Retained message management

- **Queues Page**
  - Queue list with depth, consumer count
  - Per-queue message rate graphs
  - Partition distribution across nodes
  - Retention policy status

- **Cluster Page**
  - Node list with health status
  - Leader/follower distribution
  - Raft group status per queue
  - Network topology visualization

- **Settings Page**
  - Configuration viewer
  - Log level adjustment (runtime)
  - Rate limit configuration

**Technical Stack:**
- Backend: REST API in Go (extend existing `/api/` routes)
- Frontend: React + TypeScript + Tailwind CSS
- Charts: Recharts or Chart.js
- Real-time: WebSocket for live metrics
- Build: Embedded in binary via `go:embed`

**Implementation Tasks:**
- [ ] Create `dashboard/` directory for frontend code
- [ ] Implement REST API endpoints for management operations
- [ ] Build React frontend with modern UI
- [ ] Add WebSocket endpoint for real-time metrics
- [ ] Implement client list/disconnect API
- [ ] Implement topic inspection API
- [ ] Implement queue management API
- [ ] Add cluster status API
- [ ] Embed frontend assets in Go binary
- [ ] Add dashboard configuration options
- [ ] Document dashboard usage

**API Endpoints:**
```
GET  /api/v1/overview          - System overview metrics
GET  /api/v1/clients           - List connected clients
GET  /api/v1/clients/:id       - Client details
DELETE /api/v1/clients/:id     - Disconnect client
GET  /api/v1/topics            - List topics
GET  /api/v1/topics/:name      - Topic details
GET  /api/v1/subscriptions     - List subscriptions
GET  /api/v1/queues            - List queues
GET  /api/v1/queues/:name      - Queue details
GET  /api/v1/cluster/nodes     - List cluster nodes
GET  /api/v1/cluster/status    - Cluster health status
WS   /api/v1/metrics/stream    - Real-time metrics stream
```

---

### 0.6: Operational Readiness 🟢 ENHANCEMENT

**Priority:** P3 - Improves production operations

**0.6.1 Hot Configuration Reload**

**Goal:** Enable configuration changes without broker restart

**Reloadable Configuration:**
- TLS certificates (rotation without connection drops)
- Log level (debug/info/warn/error)
- Rate limits (connections, messages, subscriptions)
- WebSocket allowed origins
- Webhook endpoints and settings
- Session expiry defaults

**Non-Reloadable (Requires Restart):**
- Listen addresses (TCP, WebSocket, CoAP, HTTP)
- Storage backend type
- Cluster node ID and etcd configuration
- Maximum message size

**Implementation:**
```go
// Signal handler for SIGHUP
func (b *Broker) setupSignalHandler() {
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGHUP)

    go func() {
        for range sigCh {
            if err := b.ReloadConfig(); err != nil {
                b.logger.Error("config reload failed", slog.String("error", err.Error()))
            } else {
                b.logger.Info("configuration reloaded successfully")
            }
        }
    }()
}
```

**Tasks:**
- [ ] Add `ReloadConfig()` method to Broker
- [ ] Implement SIGHUP handler in main.go
- [ ] Add TLS certificate watcher for auto-rotation
- [ ] Add `/api/v1/config/reload` HTTP endpoint
- [ ] Add config diff logging (show what changed)
- [ ] Implement atomic config swapping (no race conditions)
- [ ] Add reload metrics (count, last reload time, failures)
- [ ] Document which settings are hot-reloadable

**0.6.2 Graceful Shutdown**
- [ ] Drain connections before shutdown
- [ ] Wait for inflight messages to complete
- [ ] Transfer sessions to other nodes (clustered mode)
- [ ] Add shutdown timeout configuration

---

### Phase 0 Success Criteria

| Task | Priority | Status |
|------|----------|--------|
| Inter-broker TLS | P0 Critical | ✅ Complete |
| WebSocket origin validation | P0 Critical | ✅ Complete |
| Secure default ACL | P0 Critical | 📋 Planned |
| Rate limiting | P1 High | ✅ Complete |
| CoAP with DTLS/mDTLS | P1 High | ✅ Complete |
| Distributed tracing | P2 Medium | 📋 Planned |
| Prometheus endpoint | P2 Medium | 📋 Planned |
| MaxQoS enforcement | P2 Medium | ✅ Complete |
| Shared subscriptions | P2 Medium | ✅ Complete |
| Management dashboard | P3 Enhancement | 📋 Planned |
| Hot config reload | P3 Enhancement | 📋 Planned |
| Graceful shutdown | P3 Enhancement | 📋 Planned |

**Blocking Production:**
- P0 tasks MUST be complete before production deployment
- P1 tasks SHOULD be complete before production deployment
- P2/P3 tasks can be deployed incrementally

---

## Phase 1: Performance Optimization ✅ COMPLETE

**Status:** Completed 2026-01-04 | **Results:** 3.27x faster, 2,456x less memory

### Summary

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Latency | 220 μs/op | 67 μs/op | 3.27x faster |
| Memory | 424 KB/op | 177 B/op | 2,456x less |
| Throughput | 4.6K msg/s | 14.9K msg/s | 3.24x more |

**Key optimizations:** Object pooling for messages/packets, router match pooling, zero-copy buffers.

📖 **Details:** [Scaling & Performance Guide](scaling.md#performance-optimizations)

---

## Phase 2: Queue Replication ✅ COMPLETE

**Status:** Completed 2026-01-14 | **Progress:** 98% (Phase 2.5 deferred)

### Summary

Raft-based per-partition replication with automatic failover:
- Each queue partition = independent Raft group (leader + replicas)
- Configurable replication factor (default: 3), sync/async modes
- ISR (In-Sync Replicas) tracking, leader-only delivery
- Kafka-style retention policies (time, size, log compaction)

### Benchmark Results

| Metric | Result | Target |
|--------|--------|--------|
| Sync mode throughput | >5K enqueues/sec | ✅ Met |
| Async mode throughput | >50K enqueues/sec | ✅ Met |
| Leader failover | 2.4s | ✅ <5s |
| P99 latency (sync) | <50ms | ✅ Met |
| P99 latency (async) | <10ms | ✅ Met |

### Configuration Example

```yaml
queue:
  replication:
    enabled: true
    replication_factor: 3
    mode: sync                    # "sync" or "async"
    min_in_sync_replicas: 2
  retention:
    time: 168h                    # 7 days
    bytes: 10737418240            # 10GB max
    compaction_enabled: false
    compaction_key: "entity_id"
```

### Sub-Phase Summary

| Phase | Status | Key Deliverable |
|-------|--------|-----------------|
| 2.1: Raft Infrastructure | ✅ | Core Raft + FSM + BadgerDB storage |
| 2.2: Queue Integration | ✅ | Replicated enqueue, leader-only delivery |
| 2.3: Testing | ✅ | Failover tests, benchmarks |
| 2.4: Retention | ✅ | Time/size retention, log compaction |
| 2.5: Observability | 📋 | Deferred (metrics, migration tooling) |

📖 **Details:** [Scaling & Performance Guide](scaling.md#queue-replication-benchmarks)

---

## Phase 3: E2E Cluster Testing ⏳ PLANNED

**Goal:** Comprehensive cluster failure testing and validation

**Status:** Planned to start after Phase 2 completion

**Prerequisites:**
- Phase 2.3 queue replication tests complete
- Phase 2.4 observability metrics in place

### Scope

Extensive end-to-end testing of cluster behavior under various failure scenarios. This phase focuses on **multi-component cluster testing** (pub-sub + queues + sessions), while Phase 2.3 focuses on queue replication specifically.

### Test Scenarios

1. **Network Partitions**
   - Brain-split scenarios (network partition)
   - Asymmetric partitions (A can reach B, but B cannot reach A)
   - Healing after partition

2. **Node Failures**
   - Single node failure
   - Multiple simultaneous node failures
   - Cascading failures
   - Leader node failure

3. **Session Takeover**
   - Client reconnect to different node
   - Session state transfer validation
   - Inflight message recovery
   - Subscription preservation

4. **Queue Replication** (builds on Phase 2.3 tests)
   - Multi-node queue operations with network chaos
   - Combined pub-sub + queue load testing
   - Cross-feature interactions under failure
   - End-to-end durability validation

5. **Load Testing**
   - Sustained high throughput (1M+ msgs/sec)
   - Connection churn (10K connects/sec)
   - Large fanout (1000+ subscribers per topic)
   - Mixed workloads (pub-sub + queues)

### Test Infrastructure

- Docker Compose for multi-node clusters
- Network chaos tools (Pumba, Toxiproxy)
- Load generation tools (custom MQTT load tester)
- Automated test suites (Go tests + shell scripts)
- Metrics collection (Prometheus + Grafana)

### Implementation Tasks

- [ ] Create multi-node test infrastructure
- [ ] Implement network partition tests
- [ ] Add node failure scenarios
- [ ] Test session takeover under load
- [ ] Validate queue replication durability
- [ ] Load testing suite (throughput, latency, connections)
- [ ] Chaos engineering scenarios
- [ ] Automated regression suite for CI

### Success Criteria

- ✅ All failure scenarios handled gracefully
- ✅ Zero message loss in replicated queues
- ✅ Session takeover < 100ms P99
- ✅ Cluster survives any single-node failure
- ✅ Automated tests run in CI on every commit

### Estimated Timeline

**Effort:** 2-3 weeks
**Complexity:** Medium-High

---

## Phase 3.5: Performance & Concurrency Overhaul 📋 PLANNED

**Goal:** Refactor core components to significantly improve throughput and reduce latency under high concurrent load by addressing lock contention bottlenecks.

**Status:** Planned to start after E2E Cluster Testing validates the current architecture's stability.

### Motivation

While the current `queue/storage/log` implementation is durable and feature-rich, analysis shows that its coarse-grained locking strategy will become a bottleneck on multi-core systems. This phase focuses on targeted refactoring to unlock greater performance.

### 3.5.1: Implement Segment Write Buffer

*   **Goal:** Reduce `write` syscalls to improve I/O efficiency.
*   **Tasks:**
    *   [ ] Activate and utilize the `writeBuf` in the `Segment` struct.
    *   [ ] Modify `Segment.Append` to write to the in-memory buffer.
    *   [ ] Flush the buffer to disk when it's full or when `Sync` is called.
    *   [ ] Add benchmarks to validate throughput improvement.

### 3.5.2: Improve `Store` Lock Granularity

*   **Goal:** Eliminate the global lock on the main `Store` to allow concurrent operations on different queues.
*   **Tasks:**
    *   [ ] Analyze trade-offs between `sync.Map` and a custom sharded map with per-shard locks.
    *   [ ] Replace the single `RWMutex` in `store.go` with a sharded map (`[256]struct { sync.RWMutex; ... }`).
    *   [ ] Update all access to `store.queues` and `store.consumers` to use the sharded lock mechanism.
    *   [ ] Add concurrency tests to verify thread safety and measure contention reduction.

### 3.5.3: Decouple Appends from Disk I/O

*   **Goal:** Drastically reduce append latency by making disk writes asynchronous from the client's perspective.
*   **Tasks:**
    *   [ ] Introduce a buffered channel for append requests in `SegmentManager`.
    *   [ ] Create a dedicated writer goroutine that reads from the channel and performs batched writes to the segment file.
    *   [ ] Modify the public `Append` method to be a lightweight, non-blocking send to the channel.
    *   [ ] Update latency benchmarks to measure p99 append latency reduction.

### 3.5.4: Implement Zero-Copy Writes

*   **Goal:** Reduce memory allocations and GC pressure during high-throughput writes.
*   **Tasks:**
    *   [ ] Introduce a `sync.Pool` for byte buffers used in batch encoding.
    *   [ ] Modify `batch.Encode()` and `segment.Append()` to use pooled buffers.
    *   [ ] Profile memory allocations under load to confirm reduction.

---

## Phase 4: Custom Raft Implementation 📋 FUTURE

**Status:** LOW PRIORITY - Only needed at 50M+ client scale

### Context

Current architecture uses **etcd** for cluster coordination. This works well up to 5-10M clients, but has limitations:

- etcd write limit: ~5K writes/sec
- Storage size: 8GB recommended max
- Operational overhead: Separate etcd cluster

Custom Raft implementation would replace etcd with a purpose-built coordination layer optimized for MQTT workloads.

### Expected Benefits

- **10-50x write throughput** (50K-250K writes/sec vs 5K)
- **No storage limits** (BadgerDB-backed, scales to TBs)
- **Lower latency** (no network hop to separate etcd)
- **Simplified deployment** (no external dependencies)

### Architecture

Use **hashicorp/raft** library + **BadgerDB** storage:
- Raft consensus library (battle-tested, used in Consul/Nomad/Vault)
- BadgerDB for Raft log and snapshots
- Custom FSM (Finite State Machine) for MQTT operations
- Batched writes for high throughput

### When to Consider Custom Raft

**DO NOT implement unless:**
- Single cluster exceeds 5-10M clients
- etcd becomes proven bottleneck (profiling data required)
- Write throughput consistently exceeds 5K writes/sec
- Storage requirements exceed 8GB coordinated state

**Better alternatives first:**
- Geographic sharding (multiple 3-5 node clusters)
- Topic-based sharding (95% local routing)
- Hybrid storage optimization (already implemented)

### Implementation Estimate

**Effort:** 20 weeks (5 months)
**Risk:** Medium-High (distributed systems complexity)
**ROI:** Low until 50M+ scale

### Reference Documentation

- [`docs/scaling.md#custom-raft-implementation-plan`](scaling.md#custom-raft-implementation-plan) - Detailed design
- [`docs/scaling.md#etcd-deep-dive--alternatives`](scaling.md#etcd-deep-dive--alternatives) - etcd analysis and alternatives

---

## Current Architecture

### Cluster Design (3-5 Nodes Recommended)

**Components:**
- **Embedded etcd** - Distributed coordination (3-5 member cluster)
- **BadgerDB** - Local persistent storage (500GB-1TB per node)
- **gRPC Transport** - Inter-broker communication
- **Hybrid Storage** - Size-based replication vs on-demand fetching
- **Zero-Copy Buffers** - Reference-counted payload sharing

**Capacity (5-Node Cluster):**
- Concurrent connections: 250K-500K clients
- Message throughput: 2-4M msgs/sec (with topic sharding)
- Retained messages: 5M-10M messages
- Subscriptions: 5M-15M active subscriptions
- Storage: 2.5TB distributed (BadgerDB) + <8GB coordinated (etcd)

### Performance Characteristics

- Session takeover: <100ms
- Message delivery (local): <5ms
- Message delivery (cross-node): ~5ms
- Buffer pool hit rate: >99% under load
- Router throughput: 33.8M matches/sec (not a bottleneck)

---

## Testing Strategy

### Unit Tests
- Target: >85% code coverage
- All core packages (broker, router, queue, storage)
- Fast execution (<1 minute total)

### Integration Tests
- Multi-node cluster scenarios
- Session persistence and recovery
- Cross-node messaging (all QoS levels)
- Failure injection and recovery

### Performance Tests
- Benchmark suite: `benchmarks/`
- CPU/memory profiling
- Regression testing in CI
- Load testing for capacity planning

### E2E Tests
- Real MQTT client integration
- Network partition scenarios
- Chaos engineering (node failures, network issues)
- Production-like workloads

---

## Production Deployment

### Recommended Configuration

**3-Node Cluster (Most Common):**
- Connections: 150K-300K clients
- Throughput: 1-2M msgs/sec (with topic sharding)
- Cost: $1,200-1,500/month (cloud)

**5-Node Cluster (High Scale):**
- Connections: 250K-500K clients
- Throughput: 2-4M msgs/sec (with topic sharding)
- Cost: $2,000-2,500/month (cloud)

### Deployment Checklist

- [ ] TLS/SSL enabled for all connections
- [ ] etcd cluster properly configured (3 or 5 nodes)
- [ ] BadgerDB storage provisioned (500GB-1TB per node)
- [ ] Health checks configured (`/health`, `/ready`, `/cluster/status`)
- [ ] Metrics collection (OpenTelemetry → Prometheus/Grafana)
- [ ] Log aggregation (structured JSON logs)
- [ ] Backup strategy (BadgerDB snapshots + etcd backups)
- [ ] Monitoring alerts (connection limits, message latency, error rates)
- [ ] Load balancer configured (HAProxy/Nginx with ClientID sharding)

---

## Key Documentation

### Architecture
- [`docs/architecture.md`](architecture.md) - System design overview
- [`docs/clustering.md`](clustering.md) - Cluster coordination
- [`docs/queue.md`](queue.md) - Durable queue system
- [`docs/broker.md`](broker.md) - Core broker implementation

### Performance & Scaling
- [`docs/scaling.md`](scaling.md) - Comprehensive scaling & performance guide
- [`benchmarks/README.md`](../benchmarks/README.md) - Benchmark guide
- [`docs/client.md`](client.md) - Go client library with queue support

### Operations
- [`docs/configuration.md`](configuration.md) - Configuration reference
- [`docs/webhooks.md`](webhooks.md) - Event notification system

---

## Overall Progress Summary

| Phase | Duration | Completion | Status |
|-------|----------|------------|--------|
| **Phase Q: Queue Architecture Redesign** | 6-10 weeks | 0% | 🚨 **TOP PRIORITY** |
| └─ Q.1: Storage Layer Changes | 1-2 weeks | 0% | 📋 Planned |
| └─ Q.2: Consumer Group Redesign | 1-2 weeks | 0% | 📋 Planned |
| └─ Q.3: Work Stealing | 1 week | 0% | 📋 Planned |
| └─ Q.4: Wildcard Subscriptions | 1 week | 0% | 📋 Planned |
| └─ Q.5: Delivery Integration | 1-2 weeks | 0% | 📋 Planned |
| └─ Q.6: Migration & Testing | 1-2 weeks | 0% | 📋 Planned |
| **Phase 0: Production Hardening** | 3-4 weeks | 50% | ⏸️ PAUSED |
| └─ 0.1: Critical Security Fixes | 1 week | 67% | ⏸️ Paused (2/3 complete) |
| └─ 0.2: Rate Limiting | 1 week | 100% | ✅ Complete |
| └─ 0.3: Observability Completion | 3-5 days | 0% | 📋 Planned (P2) |
| └─ 0.4: Protocol Compliance | 3-5 days | 100% | ✅ Complete |
| └─ 0.5: Management Dashboard | 2-3 weeks | 0% | 📋 Planned (P3) |
| └─ 0.6: Operational Readiness | 1 week | 0% | 📋 Planned (P3) |
| **Phase 1: Performance Optimization** | 2 weeks | 100% | ✅ Complete |
| **Phase 2: Queue Replication** | 6 weeks | 98% | ✅ **COMPLETE** |
| └─ 2.1: Raft Infrastructure | 1 week | 100% | ✅ Complete |
| └─ 2.2: Queue Integration | 1 week | 100% | ✅ Complete |
| └─ 2.3: Testing & Optimization | 1 week | 100% | ✅ Complete |
| └─ 2.4: Retention Policies | 2 weeks | 100% | ✅ **COMPLETE** |
| └─ 2.5: Observability & Migration | 2-3 weeks | 0% | 📋 Planned (deferred) |
| **Phase 3: E2E Cluster Testing** | 2-3 weeks | 0% | ⏳ Planned |
| **Phase 4: Custom Raft** | 20 weeks | N/A | 📋 Future (50M+ only) |

**Current Sprint:** Phase Q - Queue Architecture Redesign (TOP PRIORITY)

**Key Metrics:**
- ✅ 2,800+ lines of queue replication code complete
- ✅ 17+ unit tests passing (retention + compaction)
- ✅ 4/4 core failover tests passing (leader election, durability, ISR, catch-up)
- ✅ Performance benchmarks complete (sync/async modes, latency, concurrency)
- ✅ **Phase 0.2 Rate Limiting - COMPLETE:**
  - ✅ Per-IP connection rate limiting (TCP, WebSocket)
  - ✅ Per-client message/subscription rate limiting
  - ✅ 12 unit tests (`ratelimit/ratelimit_test.go`)
- ✅ **CoAP with DTLS/mDTLS - COMPLETE:**
  - ✅ Full UDP CoAP implementation (go-coap v3)
  - ✅ DTLS with mutual TLS support (pion/dtls v3)
  - ✅ 5 unit tests (`server/coap/server_test.go`)
- ✅ **Phase 2.4 Retention Policies - COMPLETE:**
  - ✅ Retention infrastructure complete (schema, manager, Raft ops)
  - ✅ Full storage implementations (memory & badger stores)
  - ✅ Manager & partition worker integration complete
  - ✅ Leader-only execution & async size checks
  - ✅ Retention unit & integration tests (11 tests)
  - ✅ Log compaction (Kafka-style, keeps latest per key)
  - ✅ Compaction unit & integration tests (6 tests)

---

## Contributing

When working on this roadmap:

1. **Phase 2.3 (Current Sprint)** - Testing & Optimization
   - Write integration tests first (TDD approach)
   - Test failure scenarios thoroughly
   - Benchmark before optimizing
   - Fix known issues as you encounter them

2. **Phase 2.4** - Observability & Migration
   - Add metrics as features are completed
   - Document configuration extensively
   - Create migration guides with examples

3. **Phase 3** - E2E Testing (After Phase 2)
   - Build test infrastructure first
   - Automate all scenarios
   - Run in CI for regression prevention

4. **Phase 4** - Custom Raft (Future)
   - Requires clear business justification (>10M clients)
   - Proof that etcd is bottleneck (profiling required)
   - Consider geographic sharding first (better ROI)

---

## Notes

- This roadmap consolidates all planning information previously in separate plan files
- All implementation details, design decisions, and progress tracking are maintained here
- For detailed architecture diagrams and analysis, see [`docs/scaling.md`](scaling.md)

---

**Next Milestone:** Phase Q.1 - Storage Layer Changes (Log-based storage interface)
**Final Goal:** Phase Q Complete - Log-based queue model with wildcards and work stealing

---

## What's Next: Recommended Priority Order

### Current Priority: Phase Q - Queue Architecture Redesign

The queue system needs architectural redesign to support wildcard subscriptions and work stealing. This is now the top priority.

**Why Phase Q First:**
- Current model doesn't support wildcard queue subscriptions
- Delete-on-ack model has performance limitations
- Work stealing enables better fault tolerance
- This is a foundational change that other features build upon

**Recommended Order within Phase Q:**
1. **Q.1: Storage Layer** - Foundation for everything else
2. **Q.2: Consumer Groups** - Core cursor/PEL mechanics
3. **Q.3: Work Stealing** - Fault tolerance
4. **Q.4: Wildcards** - User-facing feature
5. **Q.5: Delivery Integration** - Wire it all together
6. **Q.6: Testing** - Validate and benchmark

### After Phase Q

1. **Phase 0: Production Hardening** - Resume security/operational fixes
2. **Phase 3: E2E Cluster Testing** - Validate full system
3. **Phase 2.5: Observability** - Metrics and monitoring
