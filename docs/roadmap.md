# MQTT Broker Development Roadmap

**Last Updated:** 2026-01-04
**Current Phase:** Phase 2 - Queue Replication

---

## Overview

Production-ready MQTT broker with focus on high performance, durability, and scalability.

### Current Status

- ✅ **Phase 1: Performance Optimization** - COMPLETE (3.27x faster)
- 🔄 **Phase 2: Queue Replication** - IN PROGRESS
- ⏳ **Phase 3: E2E Cluster Testing** - PLANNED
- 📋 **Phase 4: Custom Raft** - FUTURE (50M+ clients)

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

- **Results:** [`docs/PERFORMANCE_RESULTS.md`](PERFORMANCE_RESULTS.md)
- **Benchmarks:** [`benchmarks/README.md`](../benchmarks/README.md)
- **Summary:** [`PERFORMANCE_COMPLETE.md`](../PERFORMANCE_COMPLETE.md)

---

## Phase 2: Queue Replication 🔄 IN PROGRESS

**Goal:** Durable, replicated queues with automatic failover

### Architecture

**Raft-based partition replication:**
- Each queue partition has a leader and N replicas (configurable replication factor)
- Leader handles all writes, replicas synchronize via Raft
- Automatic leader election on partition leader failure
- ISR (In-Sync Replicas) tracking for durability guarantees

### Key Features

1. **Replication Configuration**
   - Configurable replication factor (default: 3)
   - Partition-level replication (not queue-level)
   - Synchronous or asynchronous replication modes

2. **Leader Election**
   - Automatic partition leader election via Raft
   - Sub-second failover (<500ms typical)
   - ISR-based writes for durability

3. **Replica Synchronization**
   - Raft log-based replication
   - Catch-up protocol for lagging replicas
   - Automatic replica recovery

4. **Storage Integration**
   - Uses existing BadgerDB storage backend
   - Raft log stored separately from queue data
   - Snapshot support for fast replica bootstrap

### Implementation Tasks

- [ ] Define replication configuration API
- [ ] Implement Raft-based partition replication
- [ ] Add replica synchronization protocol
- [ ] Implement partition leader election
- [ ] Add ISR tracking and management
- [ ] Create partition assignment strategy
- [ ] Test failure scenarios and recovery
- [ ] Performance benchmarking (target: minimal overhead vs single-node)

### Configuration Example

```yaml
queue:
  replication:
    enabled: true
    replication_factor: 3          # Number of replicas per partition
    min_in_sync_replicas: 2         # Minimum ISR for write acknowledgment
    election_timeout_ms: 1000       # Raft election timeout
    heartbeat_interval_ms: 100      # Raft heartbeat interval
```

### Success Criteria

- ✅ Partition leader failover < 500ms
- ✅ Zero message loss (with min_in_sync_replicas >= 2)
- ✅ < 10% latency overhead vs non-replicated queues
- ✅ Automatic replica catch-up after network partition
- ✅ All existing queue tests passing

### Estimated Timeline

**Effort:** 2-3 weeks
**Complexity:** Medium

---

## Phase 3: E2E Cluster Testing ⏳ PLANNED

**Goal:** Comprehensive cluster failure testing and validation

### Scope

Extensive end-to-end testing of cluster behavior under various failure scenarios.

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

4. **Queue Replication**
   - Partition leader failover
   - Replica lag and catch-up
   - ISR changes under load
   - Message durability guarantees

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

- [`docs/custom-raft-implementation-plan.md`](custom-raft-implementation-plan.md) - Detailed design
- [`docs/etcd-vs-custom-raft.md`](etcd-vs-custom-raft.md) - Trade-off analysis
- [`docs/alternatives-to-raft.md`](alternatives-to-raft.md) - Other coordination options

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

### Performance
- [`docs/PERFORMANCE_RESULTS.md`](PERFORMANCE_RESULTS.md) - Optimization results
- [`benchmarks/README.md`](../benchmarks/README.md) - Benchmark guide
- [`docs/topic-sharding-guide.md`](topic-sharding-guide.md) - 10x throughput guide

### Operations
- [`docs/configuration.md`](configuration.md) - Configuration reference
- [`docs/webhooks.md`](webhooks.md) - Event notification system
- [`docs/scaling-quick-reference.md`](scaling-quick-reference.md) - Capacity planning

---

## Contributing

When working on this roadmap:

1. **Phase 2 (Queue Replication)** - Current focus
   - Start with design doc and API definition
   - Incremental implementation with tests
   - Performance benchmarking at each step

2. **Phase 3 (E2E Testing)** - After replication complete
   - Build test infrastructure first
   - Automate all scenarios
   - Run in CI for regression prevention

3. **Phase 4 (Custom Raft)** - Only if needed at scale
   - Requires clear business justification (>10M clients)
   - Proof that etcd is bottleneck (profiling required)
   - Consider geographic sharding first (better ROI)

---

**Next Milestone:** Queue Replication (Phase 2) - Target: 3 weeks
