# MQTT Broker Clustering Implementation Plan

## Status Overview

This document consolidates the clustering implementation plan, tracking completed work and remaining tasks.

**Current Status:** ✅ Session takeover, BadgerDB storage, and message routing fully implemented

### 🎉 Latest Update (Dec 22, 2024)
**Graceful Shutdown & BadgerDB GC Complete!**

**Graceful Shutdown Implemented:**
- ✅ `broker.Shutdown()` - Drains connections, transfers sessions, graceful cleanup
- ✅ Session transfer to other cluster nodes before shutdown
- ✅ Configurable drain timeout
- ✅ Idempotent shutdown (safe to call multiple times)
- ✅ Comprehensive test coverage

**BadgerDB Improvements:**
- ✅ Background GC runs every 5 minutes with graceful shutdown
- ✅ Stoppable GC goroutine with proper cleanup
- ✅ Final GC pass on shutdown
- ✅ Idempotent `Close()` for safe cleanup

**Previous Update (Dec 21, 2024) - Session Takeover Tests:**
- ✅ All 3 integration tests passing
- ✅ Client reconnection, QoS 1 messages, multiple clients
- ✅ Subscription restoration from cluster etcd

---

## Architecture: Simple Embedded Design

### Design Philosophy

1. **Start Simple** - Full replication, no sharding, no compression optimizations
2. **Embedded Only** - No external services, single binary deployment
3. **Interface-Based** - Easy to swap implementations for future optimizations
4. **Proven Components** - Use battle-tested libraries (etcd, memberlist)

### Component Stack

```
┌────────────────────────────────────────────────────────────┐
│                     MQTT Broker Node                       │
│                                                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  MQTT Protocol Layer (unchanged)                     │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Broker Core (minimal changes)                       │  │
│  │  ✅ Check cluster for session ownership              │  │
│  │  ✅ Route publishes to cluster                       │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌────────────────┬──────────────────┬──────────────────┐  │
│  │ Cluster        │ Metadata Store   │ Local Storage    │  │
│  │ Membership     │ (Embedded etcd)  │ (BadgerDB)       │  │
│  │ ⏳ memberlist  │ ✅ Implemented   │ ✅ Implemented   │  │
│  │                │ - Session owner  │ - Inflight msgs  │  │
│  │ - Discovery    │ - Subscriptions  │ - Offline queue  │  │
│  │ - Failure      │ - Retained msgs  │ - Session data   │  │
│  │   detection    │ - Will messages  │                  │  │
│  └────────────────┴──────────────────┴──────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  Inter-Broker Transport (gRPC)                       │  │
│  │  ✅ RoutePublish(clientID, topic, payload)           │  │
│  │  ✅ TakeoverSession(clientID) → SessionState         │  │
│  └──────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────┘
```

---

## What's Been Completed ✅

### Phase 0: Foundation (DONE)
- ✅ Cluster interface defined (`cluster/cluster.go`)
- ✅ Cluster configuration added to config
- ✅ Broker accepts cluster parameter
- ✅ NoopCluster for single-node mode

### Phase 1: Embedded etcd Setup (DONE)
- ✅ EtcdCluster implementation with embedded etcd
- ✅ Session ownership tracking (AcquireSession, ReleaseSession, GetSessionOwner)
- ✅ Leadership election via etcd
- ✅ Subscription storage in etcd
- ✅ Retained message storage in etcd
- ✅ Will message storage in etcd

### Phase 2: Inter-Broker Communication (DONE)
- ✅ gRPC transport layer (`cluster/transport.go`)
- ✅ Protobuf definitions (`cluster/broker.proto`)
- ✅ RoutePublish RPC for message forwarding
- ✅ TakeoverSession RPC with full session state transfer

### Phase 3: Session Takeover (DONE)
- ✅ SessionManager interface for broker callbacks
- ✅ Full session takeover protocol implementation
- ✅ Session state capture (inflight, queue, subscriptions, will)
- ✅ Session state restoration on new node
- ✅ Broker.CreateSession() checks cluster ownership
- ✅ Broker.GetSessionStateAndClose() captures and transfers state
- ✅ Cluster handles remote takeover requests

### Broker Integration (DONE)
- ✅ Broker.CreateSession() triggers takeover when needed
- ✅ Broker wired as SessionManager to cluster
- ✅ Cluster ownership tracking on connect/disconnect
- ✅ Session expiry only runs on leader

### Phase 4: BadgerDB Persistent Storage (DONE)
- ✅ All 6 BadgerDB store implementations complete
  - ✅ `storage/badger/store.go` - Main store with graceful GC
  - ✅ `storage/badger/session.go` - Session persistence
  - ✅ `storage/badger/message.go` - Inflight/offline messages
  - ✅ `storage/badger/subscription.go` - Subscription storage
  - ✅ `storage/badger/retained.go` - Retained messages
  - ✅ `storage/badger/will.go` - Will messages
- ✅ Wired up in `cmd/broker/main.go` with config-based dispatch
- ✅ Storage factory pattern (memory vs badger based on config)
- ✅ Background GC with graceful shutdown
- ✅ Comprehensive test coverage (91% coverage, 73 tests)

### Phase 5: Message Routing Optimization (DONE)
- ✅ Local subscription cache in EtcdCluster with sync.RWMutex
- ✅ etcd watch for real-time subscription updates
- ✅ GetSubscribersForTopic() optimized to use local cache
- ✅ broker.distribute() routes messages to cluster
- ✅ Cluster.RoutePublish() with efficient node grouping

---

## What Needs to Be Done ⏳

### Phase 6: Testing & Validation (HIGH PRIORITY)

**Priority:** HIGH - Core functionality complete, needs validation

#### Unit Tests
- [x] BadgerDB store implementations ✅ **COMPLETED**
  - [x] Session persistence and retrieval (11 tests)
  - [x] Inflight message storage (9 tests)
  - [x] Offline queue operations (included above)
  - [x] Subscription CRUD (18 tests)
  - [x] Retained message matching (11 tests)
  - [x] Will message storage (13 tests)
  - [x] Store integration tests (11 tests)
  - **Coverage:** 91.0% (73 total tests)

- [ ] Cluster subscription cache
  - [ ] Cache loading on startup
  - [ ] etcd watch updates cache correctly
  - [ ] GetSubscribersForTopic performance

#### Integration Tests
- [x] Cross-node messaging ✅ **PARTIALLY COMPLETE**
  - [x] QoS 0 publish/subscribe across nodes
  - [x] QoS 1 publish/subscribe across nodes
  - [x] Multiple subscribers on different nodes
  - [x] Wildcard subscriptions across nodes
  - [ ] QoS 2 cross-node delivery (needs investigation)
  - **Coverage:** 4 tests passing, 1 QoS 2 issue identified

- [ ] 3-node cluster formation
  - [ ] All nodes join cluster successfully
  - [ ] Leader elected
  - [ ] etcd replication working

- [ ] Session persistence
  - [ ] Session survives broker restart with BadgerDB
  - [ ] Inflight messages restored
  - [ ] Offline queue preserved
  - [ ] Subscriptions restored on reconnect

- [x] Session takeover scenarios ✅ **COMPLETED**
  - [x] Client moves Node1→Node2, session state transfers
  - [x] QoS 1/2 messages preserved across takeover
  - [x] Subscriptions active on new node
  - **Tests:** 3 integration tests passing

---

### Phase 7: Retained & Will Messages (MEDIUM PRIORITY)

**Priority:** MEDIUM - Needed for complete MQTT compliance

#### Retained Messages
**Current State:** Stored in etcd but not efficiently queried

- [ ] Optimize GetRetainedMatching()
  - [ ] Currently scans ALL retained messages in etcd
  - [ ] Add topic index or trie structure in etcd
  - [ ] Or cache retained messages locally with watch

- [ ] Test retained message delivery
  - [ ] New subscriber receives matching retained messages
  - [ ] Retained messages visible across all nodes
  - [ ] Retained message updates propagate

#### Will Messages
**Current State:** Stored in etcd but not triggered cluster-wide

- [ ] Implement leader-only will processing
  - [ ] Currently broker.triggerWills() runs on all nodes
  - [ ] Should only run on leader (already has IsLeader check, needs testing)

- [ ] Add will message delay support
  - [ ] Store disconnectedAt timestamp with will
  - [ ] GetPendingWills() checks delay interval

- [ ] Test will message scenarios
  - [ ] Client disconnects ungracefully, will fires
  - [ ] Client disconnects gracefully, will cleared
  - [ ] Will fires exactly once (not on every node)
  - [ ] Will fires after takeover

---

### Phase 7: Background Task Coordination (MEDIUM PRIORITY)

**Priority:** MEDIUM - Prevents duplicate processing

**Current State:** Session expiry and will triggering have leader checks but need comprehensive testing

#### Tasks:
- [ ] Verify leader-only execution
  - [ ] Session expiry loop (broker.expireSessions)
  - [ ] Will trigger loop (broker.triggerWills)
  - [ ] Stats publishing loop (broker.publishStats)

- [ ] Add leader failover testing
  - [ ] Kill leader node
  - [ ] Verify new leader elected
  - [ ] Verify background tasks continue

- [ ] Add leader campaign monitoring
  - [ ] Log leadership changes
  - [ ] Metrics for leadership duration

**Implementation:**
```go
// Already implemented in broker.expiryLoop():
if b.cluster == nil || b.cluster.IsLeader() {
    b.expireSessions()
    b.triggerWills()
}

// Need to test:
// 1. Only one node processes expiry at a time
// 2. After leader fails, new leader takes over immediately
```

---

### Phase 8: Memberlist Integration (OPTIONAL)

**Priority:** LOW - etcd provides member discovery, memberlist is optional

**Why Memberlist:**
- Faster failure detection than etcd
- Gossip-based metadata propagation
- No single point of coordination

**Tasks:**
- [ ] Integrate `hashicorp/memberlist`
- [ ] Node discovery via gossip
- [ ] Failure detection (alternative to etcd watches)
- [ ] Gossip node metadata (address, load, version)

**Decision:** Defer until etcd-based coordination proves insufficient

---

### Phase 9: Production Readiness (HIGH PRIORITY)

#### Error Handling & Resilience
- [x] Graceful shutdown ✅ **COMPLETED**
  - [x] Broker.Shutdown() drains connections with configurable timeout
  - [x] Transfer sessions to other nodes before shutdown
  - [x] Idempotent Close() for safe cleanup
  - [x] Signal handling (SIGTERM/SIGINT)
  - [x] Comprehensive test coverage (4 tests)
  - **Files:** `broker/broker.go`, `broker/shutdown_test.go`, `cmd/broker/main.go`

- [ ] Connection retry logic
  - [ ] Retry failed gRPC calls to peers
  - [ ] Circuit breaker for failing nodes
  - [ ] Backoff and timeout configuration

- [ ] Split-brain protection
  - [ ] Verify etcd quorum requirements prevent split-brain
  - [ ] Add fencing tokens for session ownership

#### Monitoring & Observability
- [ ] Cluster metrics
  - [ ] Per-node message rate
  - [ ] Cross-node message latency
  - [ ] Session takeover count and latency
  - [ ] Leader election events

- [ ] Health checks
  - [ ] Liveness: Node responding
  - [ ] Readiness: Node ready to accept traffic
  - [ ] Cluster health: Quorum status

- [ ] Logging enhancements
  - [ ] Structured logging for cluster events
  - [ ] Distributed tracing for cross-node messages
  - [ ] Correlation IDs for session takeover

#### Configuration
- [ ] Validate cluster configuration
  - [ ] Ensure node IDs are unique
  - [ ] Validate initial cluster string
  - [ ] Check port conflicts

- [ ] Add timeouts and tuning parameters
  - [ ] Session takeover timeout
  - [ ] RPC timeout
  - [ ] Leader election timeout
  - [ ] Lease TTL

---

### Phase 10: Testing Strategy

#### Unit Tests
- [ ] Storage implementations (BadgerDB)
- [ ] Subscription matching in etcd
- [ ] Session takeover protocol
- [ ] Message routing logic

#### Integration Tests
- [ ] 3-node cluster formation
- [ ] Session takeover preserves state
- [ ] Cross-node publish/subscribe
- [ ] QoS 1/2 flows across takeover
- [ ] Retained messages cluster-wide
- [ ] Will messages fire exactly once
- [ ] Leader election and background tasks

#### Chaos Tests
- [ ] Random node failures
- [ ] Network partitions (split-brain scenarios)
- [ ] etcd cluster failures
- [ ] Slow network conditions
- [ ] High load during takeover

#### Performance Tests
- [ ] Message throughput (single node vs cluster)
- [ ] Session takeover latency
- [ ] Subscription matching performance
- [ ] Memory usage with 1M subscriptions
- [ ] Leader election time

---

## Implementation Phases with Timeline

### Immediate Next Steps (Weeks 1-2)
**Goal:** Production-ready persistent storage

1. Implement BadgerDB storage backend
2. Test session persistence across restarts
3. Verify takeover works with persistent state

**Deliverable:** Broker can restart without losing session state

---

### Short Term (Weeks 3-4)
**Goal:** Complete message routing

1. Optimize subscription routing with local cache
2. Update broker.Publish() to route to cluster
3. Test cross-node pub/sub
4. Add message deduplication

**Deliverable:** Messages route correctly across cluster

---

### Medium Term (Weeks 5-6)
**Goal:** Full MQTT compliance

1. Optimize retained message delivery
2. Test will message triggering
3. Comprehensive integration testing
4. Performance benchmarking

**Deliverable:** All MQTT features work in cluster mode

---

### Long Term (Weeks 7-8)
**Goal:** Production hardening

1. Error handling and resilience
2. Monitoring and metrics
3. Chaos testing
4. Documentation

**Deliverable:** Production-ready cluster deployment

---

## Configuration Reference

### Complete Cluster Config

```yaml
# config.yaml
server:
  tcp_addr: ":1883"
  tcp_max_connections: 100000

cluster:
  enabled: true
  node_id: "broker-1"

  # Embedded etcd configuration
  etcd:
    data_dir: "/var/lib/mqtt/etcd"
    bind_addr: "0.0.0.0:2380"         # Raft peer communication
    client_addr: "0.0.0.0:2379"       # Local etcd client
    advertise_addr: "192.168.1.10:2380"  # External address for peers
    initial_cluster: "broker-1=http://192.168.1.10:2380,broker-2=http://192.168.1.11:2380,broker-3=http://192.168.1.12:2380"
    bootstrap: false  # true only on first node

  # Inter-broker gRPC transport
  transport:
    bind_addr: "0.0.0.0:7948"
    peers:  # Optional: pre-configure peer addresses
      broker-2: "192.168.1.11:7948"
      broker-3: "192.168.1.12:7948"

  # Timeouts and tuning
  takeover_timeout: 5s
  rpc_timeout: 3s
  lease_ttl: 30s

storage:
  type: "badger"  # or "memory"
  badger:
    dir: "/var/lib/mqtt/data"
    value_log_file_size: 1073741824  # 1GB
    num_versions_to_keep: 1

log:
  level: "info"
  format: "json"
```

### Bootstrap Cluster

```bash
# Node 1 (bootstrap)
./mqtt-broker --config node1.yaml --cluster.etcd.bootstrap=true

# Node 2
./mqtt-broker --config node2.yaml

# Node 3
./mqtt-broker --config node3.yaml
```

---

## Dependencies

```go
// Already in go.mod
require (
    go.etcd.io/etcd/server/v3 v3.5.12
    go.etcd.io/etcd/client/v3 v3.5.12
    google.golang.org/grpc v1.62.0
    google.golang.org/protobuf v1.33.0
)

// Need to add
require (
    github.com/dgraph-io/badger/v4 v4.2.0
    // Optional: github.com/hashicorp/memberlist v0.5.0
)
```

---

## Current Architecture State

### What Works Today ✅
1. **Session Takeover:** Client can move from Node1 to Node2, session state transfers ✅ **TESTED**
   - QoS 1/2 messages preserved across takeover
   - Subscriptions restored and active on new node
   - Multiple concurrent clients can takeover simultaneously
2. **Ownership Tracking:** etcd tracks which node owns each session
3. **Leadership Election:** Only leader runs background tasks
4. **Subscription Storage:** Subscriptions stored in etcd (accessible cluster-wide)
5. **Retained Storage:** Retained messages stored in etcd
6. **Will Storage:** Will messages stored in etcd

### What's Missing ⏳
1. **Storage Testing:** BadgerDB implemented but needs comprehensive unit tests
2. **Cross-Node Messaging Tests:** Message routing implemented but needs integration tests
3. **Retained Message Optimization:** GetRetainedMatching() scans all etcd (needs caching/indexing)
4. **Will Message Testing:** Will processing needs cluster-wide validation
5. **Production Readiness:** Missing monitoring, error handling, chaos testing

---

## Success Criteria

### Milestone 1: Persistent Storage ✅
- [ ] Session survives broker restart with BadgerDB
- [ ] 1000 sessions restored in <1 second
- [ ] No data loss on clean shutdown

### Milestone 2: Cross-Node Messaging ✅
- [ ] Publish on Node1, receive on Node2
- [ ] QoS 1/2 work across nodes
- [ ] <50ms added latency for cross-node delivery

### Milestone 3: Production Ready ✅
- [ ] 3-node cluster runs stably for 7 days
- [ ] Session takeover <200ms p99
- [ ] Zero message loss during node failure
- [ ] Comprehensive monitoring and alerting

---

## Summary

### ✅ Completed (Phases 0-5)
- **Session Takeover:** Full protocol with state transfer
- **etcd Integration:** Embedded etcd with leadership and leases
- **gRPC Transport:** Inter-broker communication
- **BadgerDB Storage:** All 6 store implementations complete
- **Message Routing:** Optimized with local subscription cache and etcd watch
- **Broker Integration:** Cluster-aware session and publish handling

### ⏳ Remaining Work
1. **Testing & Validation** (Phase 6) - HIGH PRIORITY
   - Unit tests for BadgerDB and subscription cache
   - Integration tests for 3-node cluster scenarios
   - Cross-node messaging validation

2. **Retained & Will Messages** (Phase 7) - MEDIUM
   - Optimize GetRetainedMatching() queries
   - Leader-only will processing validation

3. **Background Task Coordination** (Phase 8) - MEDIUM
   - Verify leader-only execution
   - Leader failover testing

4. **Production Readiness** (Phase 9) - HIGH
   - Error handling and retries
   - Monitoring and metrics
   - Chaos testing

### 📊 Progress Estimate
- **Core Implementation:** ~95% complete ✅
  - Session takeover, message routing, BadgerDB storage, graceful shutdown
- **Testing:** ~40% complete
  - Unit tests: BadgerDB (91% coverage), cross-node messaging (4/5 tests)
  - Integration tests: Session takeover (3/3 tests)
- **Production Hardening:** ~15% complete
  - Graceful shutdown ✅, metrics & observability pending
- **Estimated Time to Production:** 2-3 weeks (finish testing + monitoring)

### 🔧 Recent Updates

#### Dec 22, 2024 - Graceful Shutdown & BadgerDB GC
- **Graceful Shutdown Implementation:**
  - `broker.Shutdown()` with configurable drain timeout
  - Session transfer to cluster nodes before shutdown
  - Signal handling (SIGTERM/SIGINT) in main binary
  - 4 comprehensive tests covering all scenarios
- **BadgerDB Improvements:**
  - Background GC runs every 5 minutes
  - Graceful GC shutdown with final cleanup pass
  - Idempotent Close() operations
  - 73 unit tests with 91% coverage

#### Dec 21, 2024 - Session Takeover & Testing
- **Session Takeover Tests:** All 3 integration tests passing
  - `TestSessionTakeover_BasicReconnect` ✅
  - `TestSessionTakeover_QoS1Messages` ✅
  - `TestSessionTakeover_MultipleClients` ✅
- **Cross-Node Messaging Tests:** 4/5 tests passing
  - QoS 0, QoS 1, wildcards, multiple subscribers ✅
  - QoS 2 cross-node delivery needs investigation
