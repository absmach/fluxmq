# MQTT Broker Development Roadmap

**Last Updated:** 2026-01-09
**Current Phase:** Phase 2 - Queue Replication

---

## Overview

Production-ready MQTT broker with focus on high performance, durability, and scalability.

### Current Status

- ✅ **Phase 1: Performance Optimization** - COMPLETE (3.27x faster)
- 🔄 **Phase 2: Queue Replication** - IN PROGRESS (~80% complete)
  - ✅ Phase 2.1: Raft Infrastructure - COMPLETE
  - ✅ Phase 2.2: Queue Integration - COMPLETE
  - ✅ Phase 2.3: Testing & Optimization - MOSTLY COMPLETE (core tests done, benchmarks done)
  - 📋 Phase 2.4: Retention Policies - NEXT (Kafka-style time & size + compaction)
  - 📋 Phase 2.5: Observability & Migration - PLANNED (deferred)
- ⏳ **Phase 3: E2E Cluster Testing** - PLANNED (after Phase 2)
- 📋 **Phase 4: Custom Raft** - FUTURE (50M+ clients only)

---

## 🎯 Immediate Next Steps

**Current Sprint Focus: Phase 2.3 - Testing & Optimization**

**Completed Today (2026-01-09):**
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

**Next: Phase 2.4 Retention Policies**
- Time-based retention (background cleanup) - delete messages older than X
- Size-based retention (active on enqueue) - enforce max queue size
- Log compaction (background) - keep only latest per partition key
- ~2 weeks effort (Week 1: retention, Week 2: compaction)
- High business value (prevents unbounded growth)

**Deferred: Phase 2.5 Observability & Migration**
- Will be implemented after retention policies
- Metrics, health checks, and migration tooling
- Can be done in parallel with production usage

**See Phase 2.3 and Phase 2.4 sections below for complete details.**

---

## Phase 1: Performance Optimization ✅ COMPLETE

**Status:** All performance work completed (2026-01-04)

### Results Achieved

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **Latency** | 220 μs/op | 67 μs/op | **3.27x faster** ⚡ |
| **Memory** | 424 KB/op | 177 B/op | **2,456x less** 💾 |
| **Allocations** | 3,005/op | 4/op | **751x fewer** 🎯 |
| **GC Time** | 75% CPU | ~40% CPU | **35% reduction** 🔧 |
| **Throughput** | 4.6K msg/s | 14.9K msg/s | **3.24x more** 📈 |

### Optimizations Implemented

1. **Object Pooling** - 2.98x speedup
   - Message pooling (`storage/pool.go`)
   - v5 Publish packet pooling (`core/packets/v5/pool.go`)
   - v3 Publish packet pooling (`core/packets/v3/pool.go`)

2. **Router Optimization** - 1.10x additional speedup
   - Subscription slice pooling (`broker/router/pool.go`)
   - Pre-allocated capacity

3. **Profiling & Analysis**
   - CPU/memory/mutex profiling complete
   - Bottlenecks identified and eliminated
   - Decision: Session map sharding not needed (minimal contention)

### Documentation

- **Scaling & Performance:** [`docs/scaling.md`](scaling.md)
- **Benchmarks:** [`benchmarks/README.md`](../benchmarks/README.md)
- **Summary:** [`PERFORMANCE_COMPLETE.md`](../PERFORMANCE_COMPLETE.md)

---

## Phase 2: Queue Replication 🔄 IN PROGRESS

**Goal:** Durable, replicated queues with automatic failover

**Overall Progress:** ~65% complete (implementation done, testing remains)

### Architecture Overview

**Raft-based per-partition replication:**
- Each queue partition = independent Raft group (leader + replicas)
- Configurable replication factor (default: 3 replicas)
- Hybrid sync/async replication modes
- ISR (In-Sync Replicas) tracking for durability
- Backward compatible (replication opt-in via config)

**Example:**
```
Queue: "orders" (3 partitions, 3 replicas each)

Partition 0: Leader=Node1, Followers=[Node2, Node3]
Partition 1: Leader=Node2, Followers=[Node3, Node1]
Partition 2: Leader=Node3, Followers=[Node1, Node2]
```

---

### Phase 2.1: Raft Infrastructure ✅ COMPLETE

**Status:** ✅ Complete (January 4, 2026)
**Goal:** Core Raft components working independently

**Files Implemented:**
- ✅ `queue/raft/raft_group.go` (361 lines) - Raft group lifecycle management
- ✅ `queue/raft/fsm.go` (417 lines) - Finite State Machine for queue operations
- ✅ `queue/raft/storage.go` (220 lines) - BadgerDB-backed Raft log storage
- ✅ `queue/raft/storage_test.go` (227 lines) - Storage layer tests
- ✅ `queue/raft/fsm_test.go` (414 lines) - FSM operation tests
- ✅ `proto/broker.proto` (+65 lines) - Raft RPC definitions

**Tests:** ✅ All 13 unit tests passing (0.208s)
- BadgerLogStore: FirstIndex, LastIndex, StoreLog, StoreLogs, DeleteRange
- BadgerStableStore: Set/Get, SetUint64/GetUint64
- PartitionFSM: ApplyEnqueue, ApplyAck, ApplyNack, ApplyReject
- Snapshot/Restore, ISR tracking

**Key Components:**
```go
type RaftGroup struct {
    queueName   string
    partitionID int
    nodeID      string
    raft        *raft.Raft            // hashicorp/raft
    fsm         *PartitionFSM
    logStore    *BadgerLogStore        // Separate DB per partition
    syncMode    bool                   // sync vs async
    isLeader    atomic.Bool
}

type PartitionFSM struct {
    queueName    string
    partitionID  int
    messageStore storage.MessageStore
    isr          map[string]time.Time  // In-Sync Replicas
}
```

**Transport:** Uses hashicorp/raft's built-in TCP transport (gRPC integration optional)

---

### Phase 2.2: Queue Integration ✅ COMPLETE

**Status:** ✅ Complete (January 4, 2026)
**Goal:** Integrate Raft with queue operations

**Modified Files:**
- ✅ `queue/storage/storage.go` (+75 lines) - ReplicationConfig schema
- ✅ `queue/manager.go` (+150 lines) - Replicated enqueue routing
- ✅ `queue/partition_worker.go` (+25 lines) - Leader-only message delivery
- ✅ `queue/delivery_worker.go` (+5 lines) - RaftManager integration

**New Files:**
- ✅ `queue/raft_manager.go` (350 lines) - Raft group lifecycle management

**Key Features Implemented:**
1. **Replication Configuration**
   ```go
   type ReplicationConfig struct {
       Enabled           bool
       ReplicationFactor int               // Default: 3
       Mode              ReplicationMode   // "sync" or "async"
       Placement         PlacementStrategy // "round-robin" or "manual"
       ManualReplicas    map[int][]string
       MinInSyncReplicas int               // Default: 2
       AckTimeout        time.Duration     // Default: 5s
   }
   ```

2. **Hybrid Operation**
   ```go
   func (m *Manager) Enqueue(...) error {
       if config.Replication.Enabled {
           return m.enqueueReplicated(...)  // Via Raft
       }
       // Non-replicated path (original logic)
   }
   ```

3. **Leader-Only Delivery**
   ```go
   func (pw *PartitionWorker) ProcessMessages(ctx) {
       if pw.raftManager != nil {
           if !pw.raftManager.IsLeader(pw.partitionID) {
               return  // Only leaders deliver messages
           }
       }
   }
   ```

4. **Placement Strategies**
   - ✅ Round-robin (default): Distribute replicas evenly
   - ✅ Manual: Operator-specified via config

**Total Code:** ~2,300 lines of production code complete

---

### Phase 2.3: Testing & Optimization ✅ MOSTLY COMPLETE

**Status:** ✅ Core Testing Complete (January 9, 2026)
**Goal:** Comprehensive testing and performance validation

**Priority 1: Integration Tests** ✅ COMPLETE
- [x] Replicated queue enqueue/dequeue with 3 replicas
- [x] Leader failover during active processing (`TestFailover_LeaderElection`)
- [x] Message durability across node failures (`TestFailover_MessageDurability`)
- [x] ISR tracking correctness (`TestReplication_ISRTracking`)
- [x] Follower lag and catch-up (`TestFailover_FollowerCatchup`)
- [ ] Sync vs async mode behavior verification (can defer)

**Priority 2: Failure Scenarios** ✅ CORE COMPLETE
- [x] Leader failure and re-election (<5s achieved)
- [x] Follower lag and catch-up (100 msg delta tested)
- [x] ISR tracking and quorum maintenance
- [ ] Network partition handling (optional - Raft handles this)
- [ ] Split-brain prevention (optional - tested via ISR tests)

**Priority 3: Performance Benchmarks** ✅ COMPLETE
- [x] Sync mode: >5K enqueues/sec per partition ✅
- [x] Async mode: >50K enqueues/sec per partition ✅
- [x] Leader failover time: <5s (2.4s achieved) ✅
- [x] P99 latency: <50ms (sync), <10ms (async) ✅
- [ ] Overhead comparison: <10% vs non-replicated (optional)

**Priority 4: Known Issues** (Deferred to Phase 2.4)
- [ ] Fix integration test race condition (`queue/integration_test.go:331`)
- [ ] Add partition strategy configuration (`queue/manager.go:705`)

**Files Created:**
- ✅ `queue/replication_test.go` (606 lines) - Integration tests
- ✅ `queue/failover_test.go` (428 lines) - 4 complete failover tests
- ✅ `queue/replication_bench_test.go` (351 lines) - Performance benchmarks

**Results:** Core replication functionality validated and benchmarked. Ready for Phase 2.5.

---

### Phase 2.4: Retention Policies 📋 NEXT

**Goal:** Production readiness with full observability

**Metrics to Add:**
- Raft leader election count
- Log replication lag (ms)
- FSM apply latency (P50/P99)
- ISR replica count per partition
- Leader availability percentage

**Health Checks:**
- Raft group status endpoint
- Leader availability check
- Replica health monitoring

**Migration Tooling:**
- Online migration: snapshot → bootstrap → switch routing
- Config validation and warnings
- Rollback capability

**Documentation:**
- Replication configuration guide
- Migration guide for existing queues
- Troubleshooting runbook

**Estimated Timeline:** 2-3 weeks

---

### Phase 2.5: Retention Policies 📋 NEXT (RECOMMENDED)

**Goal:** Kafka-style retention policies for queue management

**Why This Phase Next:**
- Prevents unbounded queue growth (critical for production)
- High business value with moderate implementation effort
- Observability (Phase 2.4) can be done in parallel with real usage
- Natural extension of current replication work

**Retention Configuration:**
```go
type RetentionPolicy struct {
    // Time-based retention (Kafka-style)
    RetentionTime time.Duration  // e.g., 7 days - delete messages older than this

    // Size-based retention
    RetentionBytes int64          // e.g., 10GB - max total queue size
    RetentionMessages int64       // e.g., 1M - max message count

    // Log compaction (Kafka-style - included in this phase)
    Compaction bool               // Keep only latest message per partition key
    CompactionLag time.Duration   // Wait before compacting (e.g., 5m)
    CompactionKey string          // Message property to use as compaction key
}
```

**Implementation Strategy: Hybrid Approach**

1. **Size-Based Retention (Active - checked on enqueue)**
   - Check `RetentionBytes` and `RetentionMessages` during enqueue
   - If exceeded, trigger deletion of oldest messages from partition head
   - Batch deletions to minimize overhead
   - **Pro:** Prevents unbounded growth, immediate enforcement
   - **Con:** Small enqueue latency increase (~1-2ms)
   - **Optimization:** Only check every N messages (e.g., every 100th)

2. **Time-Based Retention (Passive - background cleanup)**
   - Background job per partition runs periodically (every 5 minutes default)
   - Scans for messages older than `RetentionTime` (based on CreatedAt)
   - Batch delete expired messages
   - **Pro:** Zero impact on enqueue path
   - **Con:** Messages may exist slightly beyond retention window
   - **Acceptable:** 5-minute drift is acceptable for time-based retention

3. **Log Compaction (Background - separate job)**
   - Background job per partition (every 10 minutes default, configurable lag)
   - Scan messages and group by `CompactionKey` (from message properties)
   - Keep only latest message per key, delete older duplicates
   - **Pro:** Reduces storage for event sourcing use cases
   - **Con:** Additional I/O overhead
   - **Optimization:** Run less frequently than time-based cleanup

4. **Replication Integration (Raft)**
   - New Raft operations:
     - `OpRetentionDelete` - replicate batch message deletions
     - `OpCompact` - replicate compaction decisions
   - Leader executes retention/compaction logic
   - Followers apply deletions via Raft FSM
   - Ensures all replicas converge to same state
   - **Critical:** Only leader runs retention logic to avoid duplication

**Implementation Plan (2 Weeks):**

**Week 1: Retention Policies (Time + Size)**

1. **Day 1-2: Schema & Core Logic**
   - `queue/storage/storage.go` - Add `RetentionPolicy` to `QueueConfig`
   - `queue/retention.go` (NEW) - RetentionManager with:
     - `CheckSizeRetention()` - called on enqueue
     - `RunTimeBasedCleanup()` - background job
     - `DeleteBatch()` - efficient batch deletion

2. **Day 3-4: Integration & Replication**
   - `queue/raft/fsm.go` - Add `OpRetentionDelete` operation
   - `queue/manager.go` - Integrate size checks on enqueue path
   - `queue/partition_worker.go` - Background time-based cleanup job
   - Ensure only leader runs retention logic

3. **Day 5: Testing**
   - `queue/retention_test.go` - Unit tests:
     - Time-based retention deletes old messages
     - Size-based retention enforces limits
     - Batch deletions work correctly
   - `queue/replication_retention_test.go` - Integration tests:
     - Retention replicates via Raft
     - All replicas converge to same state
     - Leader-only execution verified

**Week 2: Log Compaction**

1. **Day 1-2: Compaction Logic**
   - `queue/compaction.go` (NEW) - CompactionManager with:
     - `ExtractCompactionKey()` - from message properties
     - `ScanAndCompact()` - background job
     - `FindDuplicates()` - group messages by key
   - Configurable compaction lag (5m default)

2. **Day 3-4: Integration**
   - `queue/raft/fsm.go` - Add `OpCompact` operation
   - `queue/partition_worker.go` - Background compaction job
   - Integrate with retention pipeline

3. **Day 5: Testing**
   - `queue/compaction_test.go` - Unit tests:
     - Extract compaction key from properties
     - Keep only latest per key
     - Compaction lag respected
   - `queue/replication_compaction_test.go` - Integration tests:
     - Compaction replicates correctly
     - Event sourcing use case tested

**Files to Create:**
- `queue/retention.go` (~250 lines) - Retention manager
- `queue/compaction.go` (~200 lines) - Compaction manager
- `queue/retention_test.go` (~300 lines) - Retention tests
- `queue/compaction_test.go` (~250 lines) - Compaction tests
- `queue/replication_retention_test.go` (~200 lines) - Integration tests

**Files to Modify:**
- `queue/storage/storage.go` (+30 lines) - Add RetentionPolicy
- `queue/manager.go` (+50 lines) - Integrate retention checks
- `queue/raft/fsm.go` (+80 lines) - OpRetentionDelete, OpCompact
- `queue/partition_worker.go` (+100 lines) - Background jobs

**Success Criteria:**
- Time-based retention deletes messages older than threshold
- Size-based retention prevents queue from exceeding limits
- Retention operations replicate correctly via Raft
- No message loss during retention cleanup
- Minimal performance impact (<5% overhead)

**Success Criteria:**

**Retention Policies (Week 1):**
- ✅ Time-based retention deletes messages older than threshold
- ✅ Size-based retention prevents queue from exceeding limits
- ✅ Retention operations replicate correctly via Raft
- ✅ No message loss during retention cleanup
- ✅ Minimal performance impact (<5% enqueue latency)
- ✅ Only leader executes retention logic
- ✅ All replicas converge to same state

**Log Compaction (Week 2):**
- ✅ Compaction key extracted from message properties
- ✅ Only latest message per key retained
- ✅ Compaction lag prevents premature deletion
- ✅ Compaction replicates via Raft
- ✅ Event sourcing use case works correctly
- ✅ Configurable compaction interval

**Performance Targets:**
- Size check overhead: <2ms per enqueue
- Time-based cleanup: <100ms per partition (5min interval)
- Compaction: <500ms per partition (10min interval)
- Storage reduction: 50-90% for event sourcing workloads

**Estimated Timeline:** 2 weeks

---

### Phase 2.5: Observability & Migration 📋 PLANNED

**Status:** Deferred until after Phase 2.4
**Goal:** Production readiness with full observability

**Metrics to Add:**
- Raft leader election count
- Log replication lag (ms)
- FSM apply latency (P50/P99)
- ISR replica count per partition
- Leader availability percentage
- Retention cleanup metrics (messages deleted, bytes freed)
- Compaction metrics (dedupe ratio, runtime)

**Health Checks:**
- Raft group status endpoint
- Leader availability check
- Replica health monitoring
- Retention job status

**Migration Tooling:**
- Online migration: snapshot → bootstrap → switch routing
- Config validation and warnings
- Rollback capability

**Documentation:**
- Replication configuration guide
- Retention policy guide
- Migration guide for existing queues
- Troubleshooting runbook

**Estimated Timeline:** 2-3 weeks

---

### Configuration Example

```yaml
queue:
  replication:
    enabled: true
    replication_factor: 3          # Number of replicas per partition
    mode: sync                     # "sync" or "async"
    placement: round-robin         # "round-robin" or "manual"
    min_in_sync_replicas: 2        # Minimum ISR for write acknowledgment
    ack_timeout: 5s                # Raft operation timeout

    # Raft tuning (optional)
    election_timeout_ms: 1000      # Raft election timeout
    heartbeat_interval_ms: 100     # Raft heartbeat interval
    snapshot_interval: 8192        # Log entries before snapshot
    snapshot_threshold: 8192       # Trigger snapshot cleanup

  # Retention policies (Phase 2.4 - NEXT)
  retention:
    # Time-based retention (background cleanup)
    time: 168h                     # 7 days - delete older messages
    time_check_interval: 5m        # How often to check (default: 5m)

    # Size-based retention (active on enqueue)
    bytes: 10737418240             # 10GB max queue size
    messages: 1000000              # 1M max message count
    size_check_every: 100          # Check every N enqueues (optimization)

    # Log compaction (background)
    compaction_enabled: false      # Enable log compaction
    compaction_key: "entity_id"    # Message property to use as key
    compaction_lag: 5m             # Wait before compacting new messages
    compaction_interval: 10m       # How often to compact (default: 10m)
```

---

### Success Criteria

**Phase 2.1 & 2.2** (Complete):
- ✅ Raft groups elect leaders correctly
- ✅ Operations replicate to followers
- ✅ Snapshots and restore work
- ✅ ISR tracking implemented
- ✅ All 13 unit tests passing
- ✅ Replicated enqueue routes through Raft
- ✅ Leader-only delivery enforced
- ✅ Backward compatibility maintained

**Phase 2.3 & 2.4** (Remaining):
- 📝 Integration tests passing
- 📝 Leader failover < 5s
- 📝 Zero message loss during failures
- 📝 Sync mode: >5K enqueues/sec
- 📝 Async mode: >50K enqueues/sec
- 📝 P99 latency: <50ms (sync), <10ms (async)
- 📝 Migration without downtime
- 📝 Full observability metrics

---

### Overall Timeline

| Sub-Phase | Duration | Status | Completion |
|-----------|----------|--------|------------|
| 2.1: Raft Infrastructure | Week 1 | ✅ Complete | 100% |
| 2.2: Queue Integration | Week 2 | ✅ Complete | 100% |
| 2.3: Testing & Optimization | Week 3 | ✅ Mostly Complete | 90% |
| 2.4: Retention Policies | Weeks 4-5 | 📋 **NEXT** | 0% |
| 2.5: Observability & Migration | Weeks 6-7 | 📋 Planned (deferred) | 0% |

**Total:** 7 weeks (4 weeks remaining)
**Overall Progress:** ~80% complete

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
| **Phase 1: Performance Optimization** | 2 weeks | 100% | ✅ Complete |
| **Phase 2: Queue Replication** | 6 weeks | 65% | 🔄 In Progress |
| └─ 2.1: Raft Infrastructure | 1 week | 100% | ✅ Complete |
| └─ 2.2: Queue Integration | 1 week | 100% | ✅ Complete |
| └─ 2.3: Testing & Optimization | 1 week | 90% | ✅ Mostly Complete |
| └─ 2.4: Retention Policies | 2 weeks | 0% | 📋 **NEXT** |
| └─ 2.5: Observability & Migration | 2-3 weeks | 0% | 📋 Planned |
| **Phase 3: E2E Cluster Testing** | 2-3 weeks | 0% | ⏳ Planned |
| **Phase 4: Custom Raft** | 20 weeks | N/A | 📋 Future (50M+ only) |

**Current Sprint:** Phase 2.3 Complete → Phase 2.4 Retention Next (Week 4 of 7)

**Key Metrics:**
- ✅ 2,300+ lines of queue replication code complete
- ✅ 13/13 unit tests passing
- ✅ 4/4 core failover tests passing (leader election, durability, ISR, catch-up)
- ✅ Performance benchmarks complete (sync/async modes, latency, concurrency)
- 📋 **Next:** Phase 2.4 Retention Policies
  - Week 1: Time-based (background) + Size-based (active on enqueue)
  - Week 2: Log compaction (Kafka-style)

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

**Next Milestone:** Queue Replication Testing Complete (Phase 2.3) - Target: 1-2 weeks
**Final Goal:** Queue Replication GA (Phase 2 complete) - Target: 4 weeks
