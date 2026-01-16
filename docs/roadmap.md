# MQTT Broker Development Roadmap

**Last Updated:** 2026-01-16
**Current Phase:** Phase 0 - Production Hardening (TOP PRIORITY)

---

## Overview

Production-ready MQTT broker with focus on high performance, durability, and scalability.

### Current Status

- 🚨 **Phase 0: Production Hardening** - TOP PRIORITY (Critical security & operational fixes)
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

**Current Sprint Focus: Phase 0 - Production Hardening (TOP PRIORITY)**

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

## Phase 0: Production Hardening 🚨 TOP PRIORITY

**Status:** NOT STARTED - Must complete before production deployment
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
  ws_allowed_origins:
    - "https://example.com"
    - "https://app.example.com"
    - "*.example.com"  # Wildcard subdomain support
```

**Implementation:**
- [x] Add `ws_allowed_origins` configuration option (`config/config.go`)
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
| **Phase 0: Production Hardening** | 3-4 weeks | 50% | 🚨 **TOP PRIORITY** |
| └─ 0.1: Critical Security Fixes | 1 week | 67% | 🔄 In Progress (2/3 complete) |
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

**Current Sprint:** Phase 0 - Production Hardening (TOP PRIORITY)

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

**Next Milestone:** Secure Default ACL (Phase 0.1.3) - Change to deny-all default
**Final Goal:** Production Hardening Complete (Phase 0) - Required before production deployment

---

## What's Next: Recommended Priority Order

The queue replication system is now feature-complete. Here are the recommended next steps:

### Option A: E2E Cluster Testing First (Recommended)
Validate the full system under stress before production hardening:
1. **Phase 3: E2E Cluster Testing** - 2-3 weeks
   - Multi-node failure scenarios
   - Network partition testing
   - Load testing with chaos engineering
   - Combined pub-sub + queue workloads
   - Session takeover under load

### Option B: Production Hardening
Security and operational improvements for production deployment:
1. **Secure Default ACL** (P0) - 1-2 days
   - Change default to deny-all when no authorizer configured
   - Add `development_mode: true` flag for permissive mode

2. ~~**Rate Limiting** (P1)~~✅ COMPLETE
   - ~~Per-IP connection rate limiting~~
   - ~~Per-client message rate limiting~~
   - ~~Per-client subscription rate limiting~~

3. **Observability** (P2) - 3-5 days
   - Distributed tracing instrumentation
   - Prometheus `/metrics` endpoint

### Option C: Observability First
If you want visibility before testing/hardening:
1. **Phase 2.5: Observability & Migration** - 2-3 weeks
   - Raft metrics (leader elections, lag, apply latency)
   - Retention metrics (deleted count, bytes freed)
   - Health check endpoints
   - Migration tooling
