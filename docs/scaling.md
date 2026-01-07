# Scaling & Performance

**Last Updated:** 2026-01-07

This document consolidates all scaling, performance, and optimization information for the MQTT broker.

---

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Capacity Analysis](#capacity-analysis)
3. [Performance Optimizations](#performance-optimizations)
4. [Benchmark Results](#benchmark-results)
5. [Topic Sharding](#topic-sharding)
6. [Benchmarking Guide](#benchmarking-guide)
7. [100M Clients Architecture](#100m-clients-architecture)
8. [etcd Deep Dive & Alternatives](#etcd-deep-dive--alternatives)
9. [Custom Raft Implementation Plan](#custom-raft-implementation-plan)

---

## Quick Reference

**TL;DR: "Can we handle 10M clients on 20 nodes?"**

### Answer: Yes, with modifications (6 weeks effort)

**Current Bottleneck:**
- 🔴 Subscription storage exceeds etcd capacity
- 10M clients × 10 subs = 100M subscriptions × 500B = 50GB
- etcd limit: 8GB → Won't fit

**Solution: Gossip Protocol for Subscriptions**
- Store subscriptions in local BadgerDB (per node)
- Propagate changes via gossip (hashicorp/memberlist)
- Reduces etcd to 2GB (session ownership only)
- Effort: 6 weeks

### Capacity Summary (After Fix)

| Resource        | Per Node | 20 Nodes        | Status |
| --------------- | -------- | --------------- | ------ |
| **Connections** | 500K     | 10M             | ✅ OK   |
| **RAM**         | 15GB     | 300GB           | ✅ OK   |
| **Disk**        | 5GB      | 100GB           | ✅ OK   |
| **Throughput**  | 8K msg/s | 160K-500K msg/s | ✅ OK   |

### Architecture Comparison

| Approach                | Effort   | Scalability  | Risk |
| ----------------------- | -------- | ------------ | ---- |
| **Keep etcd (current)** | 0 weeks  | 5M clients   | Low  |
| **etcd + Gossip** ⭐     | 6 weeks  | 10M clients  | Low  |
| **Custom Raft**         | 20 weeks | 50M+ clients | High |

⭐ = Recommended for 10M client requirement

---

## Capacity Analysis

### Deployment Tiers

#### Tier 1: Single Node (Development)
- **Hardware:** 16-32 cores, 64-128GB RAM, 1-2TB NVMe
- **Connections:** 100K-500K (tuned)
- **Throughput:** 300-500K msg/s
- **Cost:** $500-1,000/month

#### Tier 2: 3-Node Cluster ⭐ RECOMMENDED
- **Hardware per node:** 8-16 cores, 32-64GB RAM, 500GB-1TB SSD
- **Connections:** 300K-1.5M (with tuning)
- **Throughput:** 1M-2M msg/s (with topic sharding)
- **Cost:** $1,200-1,500/month

#### Tier 3: 5-Node Cluster (High Scale)
- **Connections:** 500K-2.5M (with tuning)
- **Throughput:** 2M-4M msg/s (with topic sharding)
- **Cost:** $2,000-2,500/month

### OS Tuning for High Connections

**Standard Tuning (100K-250K connections):**
```bash
# /etc/sysctl.conf
fs.file-max = 1000000
net.core.somaxconn = 32768
net.ipv4.ip_local_port_range = 1024 65535

# Per-process
ulimit -n 131072
```

**Aggressive Tuning (250K-500K connections):**
```bash
fs.file-max = 2097152
fs.nr_open = 2097152
net.core.somaxconn = 65535
net.core.netdev_max_backlog = 8192
net.ipv4.tcp_max_syn_backlog = 8192
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_fin_timeout = 30
net.ipv4.tcp_keepalive_time = 1200

# Per-process
ulimit -n 524288
```

**Memory Requirements:**

| Connections | Memory (Conservative) | Memory (Realistic) |
|-------------|----------------------|-------------------|
| 100K        | 800 MB               | 600 MB            |
| 250K        | 2 GB                 | 1.5 GB            |
| 500K        | 4 GB                 | 3 GB              |
| 1M          | 8 GB                 | 6 GB              |

**Per-connection breakdown:** ~6-8KB total (TCP buffers 4KB + session state 2KB + Go runtime 0.5KB)

---

## Performance Optimizations

### Optimization Journey

Comprehensive performance optimization achieved **3.3x performance improvement** through systematic profiling and targeted optimizations.

#### Phase 1: Baseline & Profiling

**Critical discovery:** 75% of CPU spent in Garbage Collection
- 49.5 GB allocated in 88 seconds (560 MB/sec allocation rate)
- 3,005 allocations per message to 1000 subscribers

#### Phase 2: Object Pooling (2.98x gain)

**Implemented pools for:**
1. `storage.Message` - Eliminated 26.28 GB allocations
2. `v5.Publish` packets - Eliminated 12.89 GB allocations
3. `v5.PublishProperties` - Eliminated 9.20 GB allocations
4. `v3.Publish` packets - Eliminated allocations for v3 clients

**Results:**
- Latency: 220 μs → 74 μs (2.98x faster)
- Memory: 424 KB → 8.4 KB (50.6x reduction)
- Allocations: 3,005 → 5 (601x reduction)
- GC time: 75% → 54%

**Implementation files:**
- `storage/pool.go` - Message pooling
- `core/packets/v5/pool.go` - v5 Publish pooling
- `core/packets/v3/pool.go` - v3 Publish pooling

#### Phase 3: Router Match Pooling (1.10x additional gain)

**Implemented pool for subscription slices:**
- Router.Match() uses pooled subscription slice
- Pre-allocated capacity of 64 subscribers
- Slice header reused across match operations

**Results:**
- Latency: 74 μs → 67 μs (1.10x faster)
- Memory: 8.4 KB → 177 B (47x reduction)
- Allocations: 5 → 4 (1 fewer)

**Implementation:** `broker/router/pool.go`

#### Phase 4: Mutex Profiling & Analysis

**Findings:**
- Total mutex contention: 231ms over entire benchmark
- 98.88% from test cleanup (disconnect/close), not publish path
- Actual publish path shows minimal lock contention
- **Conclusion: Session map sharding NOT needed**

### Final Pooling Architecture

```
Message Flow (QoS 0):
1. Publish → broker.distribute()
2. AcquireMessage() from pool
3. Router.Match() uses pooled subscription slice
4. For each subscriber:
   - Set message fields (Topic, QoS, etc.)
   - AcquirePublish() from v5/v3 pool
   - DeliverMessage() → WritePacket()
   - ReleasePublish() back to pool (defer)
5. ReleaseMessage() back to pool

Pooled Objects:
- storage.Message (per subscriber)
- v5.Publish + Properties (per delivery)
- v3.Publish (per delivery)
- Subscription slice header (per match)
```

### Zero-Copy Implementation

The zero-copy optimization using reference-counted buffers provides significant performance improvements:

**Key Features:**
- Reference-counted buffers with pooling
- Zero allocations for message payload sharing
- Constant memory usage independent of subscriber count
- Excellent scalability from 1 to 1000+ subscribers

**Memory Analysis:**

| Approach        | Memory for 1000 subs × 64KB msg |
|-----------------|--------------------------------|
| Traditional     | 64 MB per message              |
| Zero-Copy       | 64 KB + 400 KB ≈ **0.5 MB**    |

**Savings: ~128x reduction**

---

## Benchmark Results

### Summary Results

| Metric                  | Before     | After       | Improvement          |
| ----------------------- | ---------- | ----------- | -------------------- |
| **Latency (1000 subs)** | 220 μs/op  | 67 μs/op    | **3.27x faster**     |
| **Memory/op**           | 424 KB     | 177 B       | **2,456x reduction** |
| **Allocations/op**      | 3,005      | 4           | **751x reduction**   |
| **Throughput**          | 4.6K msg/s | 14.9K msg/s | **3.24x increase**   |
| **GC CPU Time**         | 75%        | ~40%        | **35% reduction**    |

📁 **Test file:** [broker/message_bench_test.go](file:///home/dusan/go/src/github.com/absmach/mqtt/broker/message_bench_test.go)

```bash
# Run this benchmark
go test -bench=BenchmarkMessagePublish_MultipleSubscribers/1000 -benchmem -benchtime=10s ./broker
```

### Zero-Copy vs Legacy Performance

| Message Size | Legacy (ns/op) | Zero-Copy (ns/op) | Speedup   | Memory Saved |
| ------------ | -------------- | ----------------- | --------- | ------------ |
| 100 bytes    | 135.3          | 48.1              | **2.8x**  | 336 B → 0 B  |
| 1 KB         | 839.1          | 54.9              | **15.3x** | 3 KB → 0 B   |
| 10 KB        | 6,763          | 138.9             | **48.7x** | 30 KB → 0 B  |
| 64 KB        | 44,830         | 1,229             | **36.5x** | 196 KB → 0 B |

**Key insight:** Larger messages see dramatically better improvements.

📁 **Test file:** [broker/message_bench_test.go](file:///home/dusan/go/src/github.com/absmach/mqtt/broker/message_bench_test.go)

```bash
# Run zero-copy comparison
make bench-zerocopy
# Or directly:
go test -bench=BenchmarkMessageCopy -benchmem ./broker
```

### Single Subscriber Performance

| Message Size | Time (ns/op) | Memory (B/op) | Allocs (op) |
|--------------|--------------|---------------|-------------|
| 100 bytes    | 539.1        | 600           | 8           |
| 1 KB         | 545.7        | 600           | 8           |
| 10 KB        | 654.0        | 600           | 8           |
| 64 KB        | 1,758        | 600           | 8           |

**Key insight:** Memory usage is constant (~600 bytes) regardless of message size.

```bash
go test -bench=BenchmarkMessagePublish_SingleSubscriber -benchmem ./broker
```

### Multiple Subscribers Scalability

| Subscribers | Time (ns/op) | Time/Sub (ns) | Memory (B/op) | Allocs/op |
|-------------|--------------|---------------|---------------|-----------|
| 1           | 568.6        | 568.6         | 600           | 8         |
| 10          | 2,393        | 239.3         | 4,416         | 35        |
| 100         | 21,997       | 220.0         | 42,672        | 305       |
| 1,000       | 240,064      | 240.1         | 424,369       | 3,005     |

**Key insights:**
- Near-linear scalability: ~230-240 ns per subscriber
- Memory per subscriber: ~424 bytes (mostly overhead, not message payload)
- Zero-copy prevents O(N*MessageSize) memory usage

```bash
go test -bench=BenchmarkMessagePublish_MultipleSubscribers -benchmem ./broker
```

### Fan-Out Pattern (1:N)

Publishing 256-byte messages (typical sensor data) to many subscribers:

| Subscribers | Time (ns/op) | Throughput (msg/s) | Memory (B/op) |
|-------------|--------------|-------------------|---------------|
| 10          | 2,346        | 426,000           | 4,416         |
| 100         | 21,538       | 46,400            | 42,672        |
| 500         | 115,021      | 8,700             | 212,272       |
| 1,000       | 253,930      | 3,940             | 424,370       |

```bash
go test -bench=BenchmarkMessagePublish_FanOut -benchmem ./broker
```

### Buffer Pool Performance

| Operation              | Time (ns/op) | Memory (B/op) | Allocs |
| ---------------------- | ------------ | ------------- | ------ |
| Retain/Release         | 3.5          | 0             | 0      |
| Get/Release            | 31.9         | 0             | 0      |
| Get/Release (Parallel) | 81.6         | 0             | 0      |
| GetWithData 1KB        | 33.8         | 0             | 0      |
| GetWithData 64KB       | 34.2         | 0             | 0      |

**Key insights:**
- Extremely fast atomic reference counting (3.5 ns)
- Pool provides consistent performance regardless of buffer size
- Zero allocations when pool is warm

📁 **Test file:** [core/refbuffer_test.go](file:///home/dusan/go/src/github.com/absmach/mqtt/core/refbuffer_test.go)

```bash
go test -bench=BenchmarkBufferPool -benchmem ./core
```

### Router Performance

| Operation            | Time       | Memory   | Allocs |
|---------------------|------------|----------|--------|
| Match (10K subs)    | 148 ns/op  | 143 B/op | 5      |
| Subscribe           | 521 ns/op  | 221 B/op | 5      |
| Unsubscribe         | 21.7 μs/op | 109 B/op | 4      |
| Wildcard matching   | 212 ns/op  | 199 B/op | 6      |

📁 **Test file:** [broker/router/router_bench_test.go](file:///home/dusan/go/src/github.com/absmach/mqtt/broker/router/router_bench_test.go)

```bash
go test -bench=. -benchmem ./broker/router
```

### Stress Test Results

| Test | Config | Result |
|------|--------|--------|
| **Buffer Pool Exhaustion** | 100 concurrent goroutines, 1M ops | 8.4M ops/sec, 99.95% hit rate |
| **High Throughput** | 10K msg/s for 10s to 100 subs | Stable, <1000 B/msg overhead |
| **Concurrent Publishers** | 10 publishers × 10K msgs | Zero errors, linear scaling |
| **Memory Pressure** | 64KB-1MB messages to 20 subs | <100MB total |
| **Extreme Fan-Out** | 1K msgs to 5K subs | 5M deliveries, <100 B/delivery |

📁 **Test file:** [broker/message_stress_test.go](file:///home/dusan/go/src/github.com/absmach/mqtt/broker/message_stress_test.go)

```bash
# Run all stress tests
make stress

# Run specific stress test
go test -v -run=TestStress_BufferPoolExhaustion ./broker
go test -v -run=TestStress_HighThroughput ./broker
go test -v -run=TestStress_ConcurrentPublishers ./broker
go test -v -run=TestStress_MemoryPressure ./broker
go test -v -run=TestStress_ExtremeFanOut ./broker

# With race detector
go test -race -run=TestStress_ConcurrentPublishers ./broker
```

### Queue Performance

| Operation              | Time       | Memory   | Allocs |
|-----------------------|------------|----------|--------|
| Enqueue (1 partition)  | 1,459 ns/op | 776 B/op | 5      |
| Enqueue (10 partitions)| 1,519 ns/op | 795 B/op | 5      |
| Dequeue (1 consumer)   | 337 μs/op  | 5,645 B/op | 9    |

📁 **Test files:**
- [queue/enqueue_bench_test.go](file:///home/dusan/go/src/github.com/absmach/mqtt/queue/enqueue_bench_test.go)
- [queue/dequeue_bench_test.go](file:///home/dusan/go/src/github.com/absmach/mqtt/queue/dequeue_bench_test.go)

```bash
go test -bench=. -benchmem ./queue
```

---

## Topic Sharding

**Key Insight:** Cross-node routing adds 1-5ms overhead. With topic sharding, achieve **95% local routing** and **10x throughput improvement**.

### Performance Comparison

| Scenario                        | Throughput     | Latency       |
|---------------------------------|----------------|---------------|
| Single node                     | 500K msgs/sec  | <100μs        |
| 20-node (no sharding)           | 600K-1M msgs/s | 1-5ms         |
| Single shard (3 nodes)          | 1-1.5M msgs/s  | <100μs local  |
| 20-node (7 shards, 95% local)   | **5-10M msgs/s** | <100μs local |

### ROI Calculation

```
Improvement Factor = (Shards × Local_Routing_Ratio) + Cross_Routing_Ratio

Example (7 shards, 95% local):
= (7 × 0.95) + 0.05
= 6.65 + 0.05
= 6.7x throughput increase

With zero-copy optimization (2-3x):
Total = 6.7 × 2.5 = 16.75x vs original baseline
```

### HAProxy Configuration

```haproxy
global
    log /dev/log local0
    maxconn 50000

defaults
    mode tcp
    timeout connect 5s
    timeout client 30s
    timeout server 30s

frontend mqtt_frontend
    bind *:1883
    
    # Extract shard from ClientID prefix
    tcp-request inspect-delay 5s
    tcp-request content accept if { req.len gt 0 }
    
    # Route based on ClientID prefix
    use_backend mqtt_shard1 if { req.payload(0,100),lower,word(1,-) -m beg tenant1 }
    use_backend mqtt_shard2 if { req.payload(0,100),lower,word(1,-) -m beg tenant2 }
    use_backend mqtt_shard3 if { req.payload(0,100),lower,word(1,-) -m beg tenant3 }
    
    default_backend mqtt_cluster

backend mqtt_shard1
    mode tcp
    balance roundrobin
    server node1 10.0.1.1:1883 check
    server node2 10.0.1.2:1883 check
    server node3 10.0.1.3:1883 check

backend mqtt_cluster
    mode tcp
    balance leastconn
    server node1 10.0.1.1:1883 check
    server node2 10.0.1.2:1883 check
    # ... all nodes
```

### ClientID Naming Conventions

**Multi-Tenant SaaS:**
```
Format: {tenant_id}-{app}-{device_id}
Example: acme-corp-sensor-001
```

**IoT Platform:**
```
Format: {device_type}-{region}-{device_id}
Example: sensor-us-west-001
```

**Geographic Sharding:**
```
Format: {region}-{service}-{id}
Example: us-east-sensor-001
```

### Metrics to Monitor

```promql
# Local routing ratio (target: >95%)
sum(rate(mqtt_messages_delivered_locally[5m])) 
/ sum(rate(mqtt_messages_total[5m]))

# Cross-node routing (target: <5%)
sum(rate(mqtt_messages_routed_remote[5m]))
/ sum(rate(mqtt_messages_total[5m]))

# Average publish-to-deliver latency
histogram_quantile(0.95,
  rate(mqtt_message_delivery_duration_seconds_bucket[5m])
)
```

---

## Benchmarking Guide

### Quick Start

```bash
# Show all available targets
make help

# Run all benchmarks
make bench

# Broker benchmarks only
make bench-broker

# Zero-copy comparison
make bench-zerocopy

# Generate report
make bench-report

# All stress tests (~30 minutes)
make stress
```

### Stress Test Targets

| Test       | Command                  | Duration | What It Tests               |
| ---------- | ------------------------ | -------- | --------------------------- |
| Pool       | `make stress-pool`       | ~1s      | Buffer pool thread safety   |
| Throughput | `make stress-throughput` | ~15s     | Sustained 10K msg/s         |
| Memory     | `make stress-memory`     | ~20s     | Large messages (64KB-1MB)   |
| Fan-out    | `make stress-fanout`     | ~60s     | 1K msgs to 5K subscribers   |
| Churn      | `make stress-churn`      | ~20s     | Rapid subscribe/unsubscribe |
| Sustained  | `make stress-sustained`  | ~30s     | Mixed load, multiple topics |
| Concurrent | `make stress-concurrent` | ~10s     | 10 publishers × 10K msgs    |

### Running Tests Directly

```bash
# All benchmarks
go test -bench=. -benchmem ./broker

# Specific benchmark
go test -bench=BenchmarkMessagePublish_FanOut -benchmem ./broker

# With specific subscriber count
go test -bench=BenchmarkMessagePublish_MultipleSubscribers/1000 -benchtime=10s ./broker

# Stress tests
go test -v -run=TestStress -timeout=30m ./broker

# Specific stress test
go test -v -run=TestStress_BufferPoolExhaustion ./broker

# With race detector
go test -race -run=TestStress_ConcurrentPublishers ./broker
```

### Interpreting Results

**Benchmark output format:**
```
BenchmarkName-16    iterations    ns/op    B/op    allocs/op
```

**Zero-copy indicators:**
1. Zero allocations in payload sharing: `0 B/op, 0 allocs/op`
2. Memory independent of subscriber count
3. Constant time for different message sizes

**CI regression checks:**
```bash
# Fail if latency > 100 μs/op (current: 67 μs)
# Fail if allocations > 10/op (current: 4)
# Fail if memory > 1 KB/op (current: 177 B)
```

### Comparing Results

```bash
# Run baseline
make bench-broker | tee baseline.txt

# Make changes...

# Run comparison
make bench-broker | tee optimized.txt

# Use benchstat to compare
benchstat baseline.txt optimized.txt
```

---

## 100M Clients Architecture

**Target:** 100M concurrent clients (100-200 nodes)

### Key Insight

100M clients cannot be handled as "one big cluster":
- Network: 200 nodes × 199 peers = 39,800 connections
- Gossip: 200 nodes = 100x network amplification
- etcd: Maximum 7-9 members (not 200)

**Required Architecture: Hierarchical Partitioned Design**

```
                Global Control Plane (5 nodes)
                        |
        ┌───────────────┼───────────────┐
        |               |               |
    Region 1        Region 2        Region 3
    (40M clients)   (40M clients)   (20M clients)
        |               |               |
    ┌───┴───┐       ┌───┴───┐       ┌───┴───┐
   Zone A  Zone B  Zone C  Zone D  Zone E  Zone F
   [50 nodes]     [50 nodes]      [25 nodes]
```

### Per-Node Capacity (100M Scale)

- Connections: 500K-1M clients
- RAM: 20-40GB
- Disk: 100-500GB SSD
- Network: 1-10 Gbps

### Session Ownership Solutions

| Option                 | Capacity   | Effort      | Use Case               |
| ---------------------- | ---------- | ----------- | ---------------------- |
| **etcd per region**    | 40M/region | 0 weeks     | Initial deployment     |
| **Custom Raft batch**  | 100M+      | 16-20 weeks | High churn             |
| **Consistent hashing** | Unlimited  | 8-12 weeks  | No coordination needed |

### Cost Estimate (3 regions, 100M clients)

| Component                  | Count | Cost/Month          |
| -------------------------- | ----- | ------------------- |
| Broker nodes (c6i.8xlarge) | 100   | $108,800            |
| etcd clusters              | 9     | $2,808              |
| Redis clusters             | 25    | $12,600             |
| Storage + network          | -     | $5,000              |
| **Total**                  |       | **~$167,000/month** |

**Cost per client:** $0.00167/month ($0.02/year)

---

## etcd Deep Dive & Alternatives

### Why etcd is Limited to ~5K Writes/Sec

1. **Single-Leader Bottleneck:** All writes go through the leader node
2. **Consensus Overhead:** 2 network round-trips per write (propose → vote → commit)
3. **Disk Fsync:** 1ms+ per fsync for durability (SSD), 10ms+ (HDD)
4. **BoltDB Backend:** B+ tree with ~10x write amplification

**Measured Performance (etcd v3.5, SSD):**
- Small writes (100 bytes): 8K writes/sec
- Medium writes (1KB): 5K writes/sec
- Large writes (10KB): 1K writes/sec

### Why Can't We Make etcd Faster?

| Option            | Effect                | Result                |
| ----------------- | --------------------- | --------------------- |
| More nodes        | ❌ Doesn't help writes | Still single leader   |
| Better hardware   | +50%                  | Still <10K writes/sec |
| Aggressive tuning | 2-3x                  | ~15K writes/sec max   |

**Theoretical Maximum (aggressive tuning):**
- Batching: 100 writes/batch
- No fsync: Skip disk writes (dangerous)
- Result: ~50K writes/sec
- **Still not enough for 100M client high-churn scenarios**

### Alternative Architectures

**1. Consistent Hashing + Gossip (Cassandra-Style)**
- Linear scaling, no SPOF
- Eventual consistency (acceptable for subscriptions)
- Effort: 3-4 months

**2. Kafka-Based Event Sourcing**
- Full audit trail, scalable
- External dependency (Kafka cluster)
- Effort: 4-6 months

---

## Custom Raft Implementation Plan

**Goal:** Replace etcd with MQTT-optimized Raft for 10-50x throughput

### Expected Performance

| Metric           | etcd | Custom Raft | Improvement |
| ---------------- | ---- | ----------- | ----------- |
| Write throughput | 5K/s | 50K-250K/s  | **10-50x**  |
| Session connects | 5K/s | 50K/s       | **10x**     |
| Subscriptions    | 5K/s | 1M/s        | **200x**    |
| Storage capacity | 8GB  | Unlimited   | ∞           |

### Why Custom Raft is Faster

1. **MQTT-Specific Batching:** 1000 subscriptions → 1 Raft entry (100x reduction)
2. **Eventual Consistency:** Skip consensus for ephemeral data (subscriptions)
3. **In-Memory State Machine:** No BoltDB overhead
4. **Relaxed Durability:** Async fsync for sessions (100ms window acceptable)

### Technology Stack

- **Consensus:** hashicorp/raft (battle-tested, used in Consul/Nomad)
- **Storage:** BadgerDB (already in codebase, LSM-tree optimized)
- **Transport:** gRPC (existing infrastructure)

### Implementation Phases

| Phase              | Duration     | Deliverable               |
| ------------------ | ------------ | ------------------------- |
| 1. Raft Core Setup | 4 weeks      | Basic consensus working   |
| 2. State Machine   | 3 weeks      | MQTT-aware FSM            |
| 3. Integration     | 4 weeks      | Replace etcd calls        |
| 4. Testing         | 3 weeks      | Chaos testing, benchmarks |
| 5. Migration       | 3 weeks      | Zero-downtime migration   |
| 6. Polish          | 3 weeks      | Monitoring, documentation |
| **Total**          | **20 weeks** | Production ready          |

### When to Build Custom Raft

**Don't build if:**
- Target <20M clients (gossip sufficient)
- Timeline <6 months
- Team <3 senior engineers

**Consider if:**
- Target >50M clients
- 12+ month timeline
- Experienced distributed systems team

**For 10M clients:** Gossip protocol is the clear winner (6 weeks vs 20 weeks)

---

## Summary

### Key Takeaways

1. ✅ **10M clients achievable** with gossip protocol (6 weeks)
2. ✅ **Zero-copy delivers 3-46x improvement** for messages
3. ✅ **Object pooling delivers 3.3x improvement** (2,456x memory reduction)
4. ✅ **Topic sharding gives 10x throughput** (load balancer config only)
5. ✅ **3-5 nodes optimal** for most production deployments
6. ⚠️ **Custom Raft overkill** for <50M clients

### Scaling Roadmap

| Phase      | Target     | Approach                        |
| ---------- | ---------- | ------------------------------- |
| **Start**  | 500K msg/s | Single powerful node            |
| **HA**     | 2M msg/s   | 3-node cluster + sharding       |
| **Scale**  | 4M msg/s   | 5 nodes (only if >700K sustain) |
| **Global** | 10M+ msg/s | Multi-region 3-5 node clusters  |
