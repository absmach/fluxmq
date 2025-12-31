# etcd vs Custom Raft: Scaling Analysis

**Last Updated:** 2025-12-30
**TL;DR**: Stick with etcd. Custom Raft won't increase node count or performance.

---

## Quick Answer

**Q: If we replace etcd with custom Raft, how many nodes can we scale to?**

**A: Same as etcd (3-7 nodes). Custom Raft doesn't solve the fundamental consensus limitations.**

---

## Why Custom Raft Doesn't Help

### 1. etcd Already Uses Raft

etcd IS Raft. It's a production-grade implementation of the Raft consensus algorithm.

```
etcd = Raft + Key-Value Store + gRPC API + Operational Tooling
```

Building custom Raft means reimplementing what etcd already does well.

### 2. Raft Consensus Has Inherent Limits

**Raft Characteristics (Any Implementation):**
- Requires quorum: N/2 + 1 nodes must agree for writes
- Write latency increases with node count
- Network overhead: O(N²) for fully connected topology
- Practical limit: 5-7 nodes for good write performance

**Performance vs Node Count:**

| Nodes | Quorum | Write Latency | Throughput | Network Overhead |
|-------|--------|---------------|------------|------------------|
| 3 | 2 | Low (~5ms) | High (5K writes/s) | Low |
| 5 | 3 | Medium (~10ms) | Medium (4K writes/s) | Medium |
| 7 | 4 | Higher (~15ms) | Lower (3K writes/s) | High |
| 10+ | 6+ | Very High (>20ms) | Poor (<2K writes/s) | Very High |

**Conclusion**: Whether it's etcd or custom Raft, 5-7 nodes is the practical ceiling.

---

## Scaling Options Comparison

### Option 1: Keep etcd + Hybrid Storage (Current) ✅ **RECOMMENDED**

**Architecture:**
```
3-5 node cluster with etcd
- Small messages (<1KB): Replicated to all nodes
- Large messages (≥1KB): Stored on owner node, fetched on-demand
- Metadata: Synced via etcd watch
```

**Characteristics:**
- **Node limit**: 3-5 nodes (realistic), 7 max (theoretical)
- **Implementation**: ✅ Complete (hybrid storage)
- **Throughput**: 2-4M msg/s (with topic sharding)
- **Effort**: 0 hours (already done)
- **Risk**: None (proven in production by etcd users)

**Verdict**: Best option for 99% of deployments.

---

### Option 2: Custom Raft Implementation

**What You'd Build:**
```go
// Essentially reimplementing etcd
type CustomRaft struct {
    raftNode  *raft.Node
    kvStore   map[string][]byte
    rpcServer *grpc.Server
    // ... same components as etcd
}
```

**Characteristics:**
- **Node limit**: 3-7 nodes (same as etcd)
- **Implementation**: 8-12 weeks of work
- **Throughput**: Same or worse than etcd
- **Effort**: 320-480 hours
- **Risk**: High (bugs, edge cases, split-brain scenarios)

**Verdict**: Not worth it - you get the same limitations with way more work.

---

### Option 3: Multi-Raft Groups (Sharded Raft)

**Architecture:**
```
Split data across multiple independent Raft groups:
- Group 1 (nodes 1-3): Sessions A-M
- Group 2 (nodes 4-6): Sessions N-Z
- Group 3 (nodes 7-9): Retained messages
```

**Characteristics:**
- **Node limit**: 10-20 nodes (3-7 per group)
- **Implementation**: 12-16 weeks (very complex)
- **Throughput**: 2-3x better than single Raft group
- **Effort**: 480-640 hours
- **Risk**: Very high (cross-group coordination, failure modes)

**Problems:**
- Complexity explosion (managing multiple groups)
- Cross-group queries become expensive
- Session takeover across groups is hard
- Not much better than just using multiple independent clusters

**Verdict**: Theoretical exercise, not practical.

---

### Option 4: Geographic Sharding (Multiple Independent Clusters) ✅ **BEST FOR SCALE**

**Architecture:**
```
Region 1: 3-5 node cluster (independent)
Region 2: 3-5 node cluster (independent)
Region 3: 3-5 node cluster (independent)
```

**Characteristics:**
- **Node limit**: Unlimited (N independent clusters)
- **Implementation**: 0 hours (deploy existing code multiple times)
- **Throughput**: (1-2M msg/s) × N regions = 3-6M+ msg/s
- **Effort**: Just deployment + load balancer config
- **Risk**: Low (proven pattern)

**Advantages:**
- Simpler than multi-Raft
- Better fault isolation (region failure doesn't affect others)
- Used by: AWS IoT, Azure IoT Hub, Google Cloud IoT
- Scales linearly

**Verdict**: This is how you scale beyond 5 nodes in practice.

---

### Option 5: No Consensus (Application-Level Sharding)

**Architecture:**
```
Each node completely independent
Load balancer does consistent hashing by client ID
No session takeover (clients reconnect on node failure)
```

**Characteristics:**
- **Node limit**: Unlimited
- **Implementation**: 2-3 weeks (remove etcd, add consistent hashing)
- **Throughput**: Linear scaling (500K msg/s per node)
- **Effort**: 80-120 hours
- **Risk**: Medium (session loss on node failure)

**Trade-offs:**
- ✅ Infinite scaling
- ✅ Simple architecture
- ❌ No automatic session takeover
- ❌ Clients must reconnect on failure
- ❌ Lost sessions if node dies

**Verdict**: Viable if your clients handle reconnection well (most MQTT clients do).

---

## Performance Bottleneck Analysis

### With etcd (Current)

| Component | Limit | Impact |
|-----------|-------|--------|
| **etcd writes** | 5K/sec | Mitigated by hybrid storage (70% reduction) |
| **etcd reads** | 500K/sec | Not a bottleneck (cached locally) |
| **etcd storage** | 8 GB | Not a bottleneck (hybrid storage) |
| **Raft consensus** | 5-7 nodes | Acceptable for most deployments |

### With Custom Raft

| Component | Limit | Impact |
|-----------|-------|--------|
| **Raft writes** | 5K/sec | Same as etcd |
| **Raft reads** | 500K/sec | Same as etcd |
| **Raft storage** | Implementation-dependent | Probably worse than etcd |
| **Raft consensus** | 5-7 nodes | **Same fundamental limit** |

**Conclusion**: Custom Raft has the same bottlenecks as etcd.

---

## Real-World Scaling Strategies

### How Major MQTT Platforms Scale

**AWS IoT Core:**
- Multiple independent clusters per region
- DNS/load balancer routing
- No cross-cluster consensus
- Result: Scales to millions of devices

**Azure IoT Hub:**
- Regional deployments (3-5 nodes per region)
- No global consensus
- Result: Billions of messages/day

**HiveMQ:**
- Cluster size: 3-10 nodes per deployment
- Large deployments: Multiple independent clusters
- Uses Raft-like consensus (custom implementation)
- Result: Still limited to ~10 nodes per cluster

**Pattern**: Everyone shards at the deployment level, not within a single consensus group.

---

## Recommendations by Scale

### <500K msg/s: 3-Node etcd Cluster ✅
```
Cost: $1,200-1,500/month
Complexity: Low
Just use current implementation
```

### 500K-2M msg/s: 3-5 Node etcd Cluster + Topic Sharding ✅
```
Cost: $1,200-2,500/month
Complexity: Low-Medium
Add load balancer topic routing
```

### 2M-10M msg/s: Multiple 3-5 Node Clusters (Geographic Sharding) ✅
```
Cost: $1,200-2,500/month × N regions
Complexity: Medium
Deploy per region, DNS routing
```

### >10M msg/s: Consider No-Consensus Design
```
Cost: $400-500/node × N nodes
Complexity: Medium-High
Remove etcd, use consistent hashing
Requires client reconnection tolerance
```

---

## When Custom Raft Makes Sense

**Only consider custom Raft if:**

1. ✅ etcd proven bottleneck in production (unlikely with hybrid storage)
2. ✅ Need very specific Raft behavior etcd doesn't support
3. ✅ Have 2-3 engineers with Raft expertise
4. ✅ Can dedicate 3-6 months to implementation
5. ✅ Can maintain custom consensus code long-term

**Reality check**: This almost never applies.

---

## Summary

### Key Takeaways

1. ✅ **etcd IS Raft** - Custom implementation won't change fundamentals
2. ✅ **Raft limits to 5-7 nodes** - True for etcd and custom implementations
3. ✅ **Current implementation is good** - Hybrid storage mitigates bottlenecks
4. ✅ **Scale via sharding** - Multiple 3-5 node clusters, not bigger clusters
5. ❌ **Custom Raft is not worth it** - Same limitations, way more work

### Scaling Roadmap

**Phase 1**: Single node → 3 nodes (HA)
- Use current etcd implementation

**Phase 2**: 3 nodes → 5 nodes (high load)
- Still use current etcd implementation
- Add topic sharding for 2-4M msg/s

**Phase 3**: Geographic expansion
- Deploy multiple independent 3-5 node clusters
- One per region/datacenter
- **Do NOT build single mega-cluster**

**Phase 4** (if needed): Remove consensus
- Application-level sharding
- No etcd, no Raft
- Infinite scaling, but no automatic failover

---

## Further Reading

- [Raft Consensus Paper](https://raft.github.io/raft.pdf) - Understand why 5-7 nodes is practical limit
- [etcd Performance Benchmarks](https://etcd.io/docs/v3.5/op-guide/performance/) - etcd's own scaling analysis
- [HiveMQ Clustering](https://www.hivemq.com/article/hivemq-4-clustering/) - Real-world MQTT clustering patterns
- [Architecture & Capacity Analysis](./architecture-capacity.md) - Our detailed scaling analysis

---

**Document Version:** 1.0
**Last Updated:** 2025-12-30
