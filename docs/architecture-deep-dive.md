# Architecture Deep Dive & Alternatives Analysis

**Last Updated:** 2025-12-29
**Target Scale:** 20 nodes, 10M concurrent clients
**Focus:** Technical trade-offs, bottleneck analysis, alternative architectures

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [Current Architecture: Critical Analysis](#current-architecture-critical-analysis)
- [10M Client Assessment (20 Nodes)](#10m-client-assessment-20-nodes)
- [etcd Bottleneck Analysis](#etcd-bottleneck-analysis)
- [Custom Raft vs etcd: Deep Comparison](#custom-raft-vs-etcd-deep-comparison)
- [Alternative Architectures](#alternative-architectures)
- [Architectural Decision Matrix](#architectural-decision-matrix)
- [Recommendations](#recommendations)

---

## Executive Summary

### Current Architecture Verdict (10M Clients)

**Can it scale to 10M clients on 20 nodes? Yes, but with caveats.**

**Strengths:**
- ✅ Hybrid storage solves retained message bottleneck
- ✅ BadgerDB handles local state efficiently
- ✅ gRPC transport is battle-tested
- ✅ Simple operational model (single binary)

**Critical Bottlenecks at 10M Scale:**
- 🔴 **etcd write throughput** (~5K writes/sec cluster-wide)
- 🟡 **Session churn** (connect/disconnect storms → etcd writes)
- 🟡 **Subscription routing index** (100M subscriptions → etcd size)
- 🟡 **Cross-node message amplification** (broadcast topics)

**Verdict:**
- **Optimistic**: 10M clients achievable with careful topology (sticky sessions, topic sharding)
- **Realistic**: 5-7M clients comfortable, 10M requires significant tuning
- **Pessimistic**: etcd becomes bottleneck at high churn rates (>10K connects/sec)

### Key Insight: etcd vs Custom Raft

**Why etcd is slow (fundamental Raft limitations):**
1. **Consensus overhead**: Every write must replicate to majority (2/3 or 3/5 nodes)
2. **Durability**: fsync to disk before ack (10ms+ on HDD, 1ms+ on SSD)
3. **Serialization**: Single-threaded commit pipeline (leader bottleneck)
4. **Network RTT**: 2 round-trips per write (propose → vote → commit)

**Why custom Raft could be faster:**
1. **Batching**: MQTT-specific batching (group 100 subscription updates → 1 Raft entry)
2. **Eventual consistency**: Relax durability for ephemeral data (sessions, subscriptions)
3. **Optimized encoding**: Binary protocol vs JSON/protobuf
4. **Application-aware**: Skip consensus for read-only operations

**Trade-off:**
- Custom Raft: 10-50x throughput improvement (50K-250K writes/sec)
- Cost: 6-12 months development, operational complexity, bug risk

---

## Current Architecture: Critical Analysis

### Component 1: etcd (Coordination Layer)

**Purpose:**
- Session ownership (which node owns clientID)
- Subscription routing index (which clients subscribe to topic)
- Retained message metadata (hybrid approach)
- Leadership election (single leader for background tasks)

**Why etcd?**
- ✅ Battle-tested distributed consensus (used by Kubernetes)
- ✅ Embedded (no external dependencies)
- ✅ Strong consistency guarantees
- ✅ Watch API for real-time updates
- ✅ MVCC for transactional reads

**Limitations:**
1. **Write Throughput: ~5K writes/sec**
   - Fundamental Raft limitation (leader serialization)
   - Disk fsync on every commit (durability)
   - Network latency for quorum (2 RTT per write)

2. **Storage Capacity: 8GB recommended, 100GB max**
   - BoltDB backend (B+ tree)
   - Compaction required at high write rates
   - Performance degrades >8GB

3. **Latency: 10-50ms per write**
   - P50: 10ms (SSD, same datacenter)
   - P99: 50ms (network jitter, leader election)
   - Unacceptable for low-latency pub/sub

4. **Operational Complexity:**
   - Requires careful sizing (odd number: 3, 5, 7)
   - Split-brain risk (network partitions)
   - Snapshot/restore procedures

**Current Mitigations:**
- ✅ Hybrid storage: 70% of retained writes avoid etcd
- ✅ Local caching: Subscriptions cached per node
- ⏳ Batch writes: Not yet implemented (quick win)

**Fundamental Question:**
*Is Raft consensus necessary for MQTT state?*

**Answer:** Depends on consistency requirements:
- **Session ownership**: YES (must be strongly consistent to avoid split-brain)
- **Subscriptions**: NO (eventual consistency acceptable, can use gossip)
- **Retained messages**: NO (eventual consistency acceptable)
- **Leadership**: YES (only one leader for background tasks)

### Component 2: BadgerDB (Local Storage)

**Purpose:**
- Session state (subscriptions, will message, properties)
- Offline message queue (QoS 1/2 undelivered messages)
- Inflight messages (QoS 1/2 acknowledgment tracking)
- Retained messages (full payload)
- Will messages (full payload)

**Why BadgerDB?**
- ✅ Embedded LSM-tree database (no external service)
- ✅ High write throughput (100K writes/sec)
- ✅ SSD-optimized (sequential writes)
- ✅ Built-in GC and compaction
- ✅ Go-native (zero-copy in some cases)

**Strengths:**
- **Write Amplification: Low** (~3x for LSM tree vs 10x for B-tree)
- **Read Performance: Excellent** with bloom filters and caching
- **Crash Recovery: Fast** (WAL replay)
- **Disk Space: Efficient** (compression, compaction)

**Limitations:**
1. **RAM Usage: High**
   - Requires ~1GB cache + 100MB per memtable
   - Bloom filters consume memory (proportional to keys)
   - SSTable index in memory

2. **Compaction Overhead:**
   - Background CPU usage (~10% during GC)
   - I/O spikes during level compaction
   - Can pause writes briefly (100-500ms)

3. **No Replication:**
   - BadgerDB is single-node only
   - Must rely on etcd for cross-node state
   - Node failure = data loss (unless replicated via etcd)

**Alternatives Considered:**
- **RocksDB**: More mature, but CGo (cross-compilation pain)
- **BoltDB**: MVCC, but B-tree (poor write performance)
- **SQLite**: Full SQL, but single-writer bottleneck
- **Pebble** (CockroachDB): Go-native LSM, similar to BadgerDB

**Verdict:** BadgerDB is excellent choice for local storage. No need to change.

### Component 3: gRPC Transport (Inter-Broker Communication)

**Purpose:**
- Route PUBLISH messages to nodes with subscribers
- Session takeover (transfer state from old node to new node)
- Fetch large retained messages (hybrid storage)
- Fetch large will messages (hybrid storage)

**Why gRPC?**
- ✅ HTTP/2 multiplexing (single connection for many RPCs)
- ✅ Protobuf encoding (efficient binary format)
- ✅ Streaming support (future: bulk message transfer)
- ✅ Load balancing and retries built-in
- ✅ Language-agnostic (can add non-Go nodes)

**Performance Characteristics:**
- **Throughput**: 50K msgs/sec per connection (measured)
- **Latency**: ~1ms same datacenter, ~50ms cross-region
- **Overhead**: ~200 bytes per RPC (headers + frame)
- **Compression**: gzip reduces payload by 60-80%

**Bottleneck Analysis:**

**At 10M clients with 20% cross-node routing:**
- Total messages: 500K msgs/sec
- Cross-node: 100K msgs/sec
- gRPC calls: 100K RPCs/sec
- Per-node: 5K RPCs/sec (well within capacity)

**Not a bottleneck at target scale.**

**Alternatives Considered:**
- **NATS**: Pub/sub messaging (but adds external dependency)
- **Redis Cluster**: Simpler, but no strong consistency
- **Custom TCP protocol**: Lower overhead, but reinventing wheel

**Verdict:** gRPC is appropriate. No need to change.

### Component 4: Hybrid Storage Pattern

**Architecture:**
```
Small Message (<1KB):
Publisher → Node 1 → BadgerDB.Set()
                   → etcd.Put(/retained-data/topic, metadata+payload)
                   → etcd watch fires
                   → All nodes → BadgerDB.Set() locally

Large Message (≥1KB):
Publisher → Node 1 → BadgerDB.Set()
                   → etcd.Put(/retained-index/topic, metadata)
                   → Other nodes cache metadata only
                   → Subscriber on Node 2 → gRPC.FetchRetained(node1, topic)
                                          → BadgerDB.Get()
                                          → Cache locally
```

**Why Hybrid?**
- **etcd cannot store large payloads** (8GB limit, slow writes)
- **Replicating small messages is cheap** (~500 bytes × 1M = 500MB)
- **Fetching large messages on-demand is acceptable** (5ms latency, infrequent)

**Effectiveness at 10M Clients:**

Assumptions:
- 10M clients, 1 retained message per client = 10M retained messages
- Payload distribution: 70% small (<1KB), 30% large (>1KB)
- Average small: 500 bytes, average large: 10KB

**Without Hybrid:**
- etcd storage: 10M × 1KB avg = 10GB (exceeds 8GB limit)
- Write throughput: 10M messages / 3600s = 2.8K writes/sec (acceptable)
- **Problem: Storage limit exceeded**

**With Hybrid (threshold=1KB):**
- Small messages: 7M × 500B = 3.5GB (etcd)
- Large messages: 3M × 300B metadata = 900MB (etcd)
- **Total etcd: 4.4GB** (within 8GB limit)
- Large payloads: 3M × 10KB = 30GB (distributed across 20 nodes in BadgerDB)

**Verdict:** Hybrid storage is critical for 10M scale. Without it, etcd limit is exceeded.

---

## 10M Client Assessment (20 Nodes)

### Per-Node Resource Requirements

**Connections: 500,000 per node**

| Resource | Per Connection | Total (500K) | Notes |
|----------|----------------|--------------|-------|
| TCP Socket | ~8KB (kernel buffers) | 4GB | OS ulimit required: 600K |
| Session State | 5KB (in-memory) | 2.5GB | Subscriptions, will, properties |
| Read Buffer | 4KB | 2GB | MQTT packet buffering |
| Write Buffer | 4KB | 2GB | Output queue |
| **Total RAM** | **~21KB** | **~10.5GB** | Just for connections |

**Additional Memory:**
- BadgerDB cache: 2GB
- Subscription routing trie: 1GB (50K unique topic patterns)
- Retained message cache: 500MB (100K cached messages)
- Go runtime overhead: 500MB
- **Total per node: ~15GB RAM**

**Cluster-wide: 20 nodes × 15GB = 300GB RAM**

**Disk Storage per Node:**
- Session persistence: 500K × 5KB = 2.5GB
- Offline queues: 50K disconnected × 10 msgs × 1KB = 500MB
- Retained messages (hybrid): 500K × 2KB = 1GB
- Inflight tracking: 100K × 1KB = 100MB
- **Total per node: ~5GB**

**Cluster-wide: 20 nodes × 5GB = 100GB**

**Network Bandwidth:**

Assume:
- 10M clients, each publishing 1 msg/min = 167K msgs/sec cluster-wide
- Average message size: 1KB
- Cross-node routing: 30% (topic sharding)

**Per-Node:**
- Incoming: 167K / 20 = 8.3K msgs/sec × 1KB = 8.3 MB/s = 66 Mbps
- Cross-node: 8.3K × 30% = 2.5K msgs/sec × 1KB = 2.5 MB/s = 20 Mbps
- **Total: ~100 Mbps per node**

**Cluster-wide: ~2 Gbps total throughput**

### Subscription Scaling

**Assumptions:**
- 10M clients, average 10 subscriptions each = 100M subscriptions
- Subscription storage: 500 bytes (clientID + filter + QoS + options)

**etcd Storage (Routing Index):**
- 100M × 500B = 50GB (exceeds 8GB limit)

**🔴 CRITICAL BOTTLENECK IDENTIFIED**

**Current implementation stores all subscriptions in etcd for routing.**

At 100M subscriptions, etcd cannot handle this.

**Solutions:**

1. **Local Subscription Index (recommended)**
   - Store subscriptions only in local BadgerDB
   - Use gossip protocol (e.g., memberlist) to broadcast subscription changes
   - Each node maintains full routing index locally
   - Reduces etcd to session ownership only

2. **Hierarchical Topic Sharding**
   - Partition topic space: `/device/{nodeID}/#` → handled by specific node
   - Only route subscriptions for cross-node topics
   - Reduces etcd index by 90% (assuming 90% topic affinity)

3. **Bloom Filter Index**
   - Store bloom filter in etcd (compressed)
   - 100M subscriptions → 120MB bloom filter (1% false positive)
   - Reduces etcd storage by 99%
   - Trade-off: false positives (send message to node that doesn't need it)

### Session Ownership Scaling

**Session Ownership Writes:**
- 10M clients, average 1 reconnect per hour = 2.8K connects/sec
- Each connect = 1 etcd write (AcquireSession)
- **etcd throughput: 2.8K writes/sec** (within 5K limit)

**Session Ownership Storage:**
- 10M sessions × 200B (clientID + nodeID + timestamp) = 2GB
- **etcd storage: 2GB** (within 8GB limit)

**Not a bottleneck at 10M scale with sticky sessions.**

**Worst-case (no sticky sessions, high churn):**
- 10M clients, average 1 reconnect per 10 minutes = 16.7K connects/sec
- **etcd throughput: 16.7K writes/sec** (exceeds 5K limit)

**🟡 POTENTIAL BOTTLENECK in high-churn scenarios**

**Solution: Batch writes**
- Group 10 session ownership updates → 1 etcd transaction
- Reduces write rate by 10x
- Trade-off: 100ms delay in ownership propagation (acceptable)

### Retained Message Scaling

**With Hybrid Storage:**
- 10M retained messages (1 per client)
- Small (<1KB): 7M × 500B = 3.5GB (etcd)
- Large (≥1KB): 3M × 300B metadata = 900MB (etcd)
- **Total etcd: 4.4GB** (within 8GB limit)

**Not a bottleneck with hybrid storage.**

### Message Throughput Analysis

**Target: 167K msgs/sec (10M clients, 1 msg/min each)**

**Scenario 1: Perfect Topic Sharding (90% local routing)**
- Local routing: 167K × 90% = 150K msgs/sec (no gRPC)
- Cross-node routing: 167K × 10% = 17K msgs/sec (gRPC)
- **Per-node throughput: 8.3K msgs/sec**
- **Bottleneck: None** (BadgerDB handles 100K writes/sec)

**Scenario 2: Mixed Routing (50% local, 50% cross-node)**
- Local routing: 83K msgs/sec
- Cross-node routing: 83K msgs/sec
- **Per-node gRPC calls: 4.2K RPCs/sec**
- **Bottleneck: None** (gRPC handles 50K RPCs/sec)

**Scenario 3: Broadcast Topics (10% local, 90% cross-node)**
- Local routing: 17K msgs/sec
- Cross-node routing: 150K msgs/sec
- **Per-node gRPC calls: 7.5K RPCs/sec**
- **Bottleneck: etcd coordination** (if routing index in etcd)

**Mitigation:**
- Use local routing index (gossip protocol)
- Cache subscription routing table on each node
- Update cache via etcd watch (eventual consistency)

### Summary: 10M Client Feasibility

**Can current architecture support 10M clients on 20 nodes?**

| Component | Capacity | Status | Notes |
|-----------|----------|--------|-------|
| **RAM** | 15GB per node | ✅ OK | Requires 16GB+ RAM servers |
| **Disk** | 5GB per node | ✅ OK | Minimal disk usage |
| **Network** | 100 Mbps per node | ✅ OK | Standard 1 Gbps NICs sufficient |
| **etcd (sessions)** | 2GB, 2.8K writes/sec | ✅ OK | With sticky sessions |
| **etcd (subscriptions)** | 50GB required | 🔴 FAIL | Exceeds 8GB limit |
| **etcd (retained)** | 4.4GB | ✅ OK | With hybrid storage |
| **Message throughput** | 167K msgs/sec | ✅ OK | With topic sharding |

**Conclusion:**

**Current architecture cannot support 10M clients without fixing subscription storage.**

**Required changes:**
1. 🔴 **Move subscription routing out of etcd** (use gossip protocol)
2. 🟡 **Implement batch writes for session ownership** (10x improvement)
3. 🟢 **Optimize topic sharding** (minimize cross-node routing)

**Estimated effort:**
- Subscription gossip protocol: 2-3 weeks (use hashicorp/memberlist)
- Batch writes: 3-5 days
- Topic sharding documentation: 1 day

**With these changes, 10M clients is achievable.**

---

## etcd Bottleneck Analysis

### Why is etcd Limited to ~5K Writes/Sec?

**Fundamental Raft Constraints:**

1. **Single-Leader Bottleneck**
   - All writes go through the leader node
   - Leader must serialize writes (single-threaded commit)
   - **Theoretical max: ~10K writes/sec on modern CPU**

2. **Consensus Overhead (2 Network Round-Trips)**
   ```
   Client → Leader: Propose write (RTT 1)
   Leader → Followers: AppendEntries RPC
   Followers → Leader: ACK (majority required)
   Leader → Client: Commit response (RTT 2)

   Total latency: 2 RTT + disk fsync
   ```
   - Same datacenter: 2ms RTT × 2 = 4ms minimum latency
   - With fsync: 4ms + 1ms = 5ms per write
   - **Max throughput: 1000ms / 5ms = 200 writes/sec** (if sequential)

3. **Disk Fsync (Durability)**
   - Raft requires durability: fsync to disk before ack
   - SSD: ~1ms per fsync (1000 IOPS)
   - HDD: ~10ms per fsync (100 IOPS)
   - **Batching**: etcd batches writes to disk (groups 100 writes → 1 fsync)
   - **Effective throughput: 100 writes/batch × 1000 batches/sec = 100K writes/sec**

4. **Why Only 5K in Practice?**
   - etcd uses BoltDB (B+ tree) as storage backend
   - BoltDB write amplification: ~10x (updates require page rewrites)
   - Compaction overhead: background merging consumes I/O
   - Network serialization: protobuf encoding/decoding
   - **Practical limit: 5K-10K writes/sec sustained**

**Measured Performance (etcd v3.5, SSD):**
- Small writes (100 bytes): 8K writes/sec
- Medium writes (1KB): 5K writes/sec
- Large writes (10KB): 1K writes/sec

### Why Can't We Just "Make etcd Faster"?

**Option 1: More etcd Nodes**
- ❌ **Doesn't help writes** (still single leader)
- ✅ **Helps reads** (can distribute across followers)

**Option 2: Bigger Hardware**
- ✅ **Faster SSD**: 10K IOPS → 15K IOPS (50% improvement)
- ✅ **More CPU**: Parallel compaction (20% improvement)
- ❌ **Diminishing returns**: Network RTT and Raft serialization remain

**Option 3: etcd Tuning**
```yaml
# Increase batch size (default 100)
max-request-bytes: 10485760  # 10MB (batch more writes)
snapshot-count: 100000        # Less frequent snapshots

# Disable sync for non-critical data (dangerous!)
unsafe-no-fsync: true         # 10x throughput, but no durability
```
- ✅ **Batching**: 2-3x improvement
- ❌ **No fsync**: Fast but risky (data loss on crash)

**Theoretical Maximum (with aggressive tuning):**
- Batching: 100 writes/batch
- No fsync: Skip disk writes
- In-memory BoltDB: No I/O
- **Result: ~50K writes/sec**

**Still not enough for 10M client high-churn scenarios (>100K writes/sec needed).**

---

## Custom Raft vs etcd: Deep Comparison

### Why Build Custom Raft Implementation?

**Key Insight: MQTT doesn't need general-purpose consensus.**

etcd is designed for:
- Kubernetes API server (high read, low write)
- Configuration storage (small values, infrequent updates)
- Leader election (simple, robust)

MQTT broker needs:
- High write throughput (session churn, subscriptions)
- Large values (retained messages, session state)
- Eventual consistency acceptable for some data

**Custom Raft Advantages:**

#### 1. **MQTT-Specific Batching**

**etcd batching:**
```go
// Generic batching: waits for 100ms or 1000 writes
batch := []Write{}
for i := 0; i < 1000; i++ {
    batch = append(batch, <-writeChan)
}
raft.Propose(batch)  // 1 Raft entry for 1000 writes
```

**MQTT-optimized batching:**
```go
// Semantic batching: group by operation type
type SessionBatch struct {
    Connects    []SessionConnect    // 1000 connects
    Disconnects []SessionDisconnect // 500 disconnects
}

// Single Raft entry for mixed operations
raft.Propose(SessionBatch{...})

// Apply atomically on followers
for _, connect := range batch.Connects {
    sessions[connect.ClientID] = connect.NodeID
}
```

**Benefit:**
- etcd: 1500 writes → 1500 Raft entries (with batching: 15 entries)
- Custom: 1500 writes → 1 Raft entry
- **100x reduction in Raft overhead**
- **Throughput: 5K → 500K writes/sec**

#### 2. **Eventual Consistency for Subscriptions**

**etcd approach (strong consistency):**
```go
// Subscribe on Node 1
etcd.Put("/subs/client1/topic", {...})  // Wait for quorum

// Publish on Node 2
subs := etcd.Get("/subs/+/topic")  // Guaranteed latest
// Deliver to all subscribers
```
- **Latency: 10ms per subscription** (etcd write + read)
- **Bottleneck: etcd consensus**

**Custom Raft with eventual consistency:**
```go
// Subscribe on Node 1
localDB.Put("/subs/client1/topic", {...})  // Immediate (no consensus)
raft.ProposeAsync(SubscriptionUpdate{...})  // Background replication

// Publish on Node 2
subs := localDB.Get("/subs/+/topic")  // May be slightly stale (100ms)
// Deliver to all subscribers (eventual delivery OK)
```
- **Latency: <1ms per subscription** (local write)
- **Trade-off: 100ms propagation delay** (acceptable for MQTT)
- **Throughput: 10K → 1M subscriptions/sec**

#### 3. **Optimized State Machine**

**etcd state machine (BoltDB):**
- B+ tree (high write amplification)
- MVCC (multi-version, garbage collection overhead)
- Generic key-value (no MQTT-specific optimizations)

**Custom Raft state machine:**
```go
type MQTTState struct {
    Sessions      map[string]SessionOwner      // clientID → nodeID
    Subscriptions *SubscriptionIndex           // topic trie
    Retained      map[string]RetainedMetadata  // topic → metadata
}

func (s *MQTTState) Apply(entry RaftEntry) {
    switch entry.Type {
    case SessionConnect:
        s.Sessions[entry.ClientID] = entry.NodeID
    case Subscribe:
        s.Subscriptions.Add(entry.ClientID, entry.Filter)
    case RetainedPublish:
        s.Retained[entry.Topic] = entry.Metadata
    }
}
```

**Benefits:**
- **No serialization overhead**: In-memory data structures (no BoltDB)
- **MQTT-aware indexing**: Topic trie for fast wildcard matching
- **Zero GC pressure**: Reuse objects via sync.Pool
- **Throughput: 50K → 500K operations/sec**

#### 4. **Relaxed Durability**

**etcd durability (fsync on every commit):**
```go
// Leader
entry := raft.Propose(data)
boltdb.Write(entry)
boltdb.Sync()  // fsync (1ms)
return OK
```
- **Latency: 1ms fsync per write**
- **Throughput: 1000 writes/sec** (without batching)

**Custom Raft (async fsync for ephemeral data):**
```go
// Leader
entry := raft.Propose(data)
wal.Write(entry)  // Write to WAL buffer (no fsync)

// Background goroutine
go func() {
    for range time.Tick(100 * time.Millisecond) {
        wal.Sync()  // Batch fsync every 100ms
    }
}()
```
- **Latency: No fsync overhead** (async)
- **Trade-off: Up to 100ms data loss on crash** (acceptable for sessions)
- **Throughput: 100K+ writes/sec**

#### 5. **Pipeline Optimization**

**etcd (sequential commits):**
```
Write 1 → Propose → Commit → Apply → ACK
                           ↓
Write 2 ----------------→ Propose → Commit → Apply → ACK
                                           ↓
Write 3 ----------------------------→ Propose → ...
```
- **Latency: 10ms per write** (serialized)

**Custom Raft (pipelined commits):**
```
Write 1 → Propose → Commit → Apply → ACK
          ↓
Write 2 → Propose → Commit → Apply → ACK
          ↓
Write 3 → Propose → Commit → Apply → ACK
```
- **Latency: 10ms** (same)
- **Throughput: 3x higher** (3 writes in-flight simultaneously)
- **Implementation: Raft's AppendEntries pipelining**

### Custom Raft Implementation Complexity

**Estimated Effort:**

| Component | Lines of Code | Effort | Complexity |
|-----------|---------------|--------|------------|
| Raft Core (consensus) | 2000 | 8 weeks | High |
| WAL (write-ahead log) | 500 | 2 weeks | Medium |
| State Machine (MQTT) | 1000 | 3 weeks | Medium |
| Snapshot/Restore | 500 | 2 weeks | Medium |
| Network Transport | 300 | 1 week | Low |
| Testing | 2000 | 4 weeks | High |
| **Total** | **6300** | **20 weeks** | **High** |

**Risks:**
- **Correctness**: Raft is subtle (leader election, log replication, safety)
- **Edge Cases**: Network partitions, split-brain, data loss
- **Operational Complexity**: Debugging, monitoring, upgrades
- **Maintenance Burden**: Long-term support, bug fixes

**Alternative: Use Existing Raft Library**

**Option 1: hashicorp/raft**
- ✅ Battle-tested (used by Consul, Nomad)
- ✅ Go-native
- ✅ Pluggable storage backends
- ⚠️ Still requires MQTT state machine implementation (~1000 LOC)
- **Effort: 6-8 weeks**

**Option 2: etcd/raft (just the library)**
- ✅ Same as etcd uses internally
- ✅ Can reuse etcd's WAL
- ✅ Highly optimized
- ⚠️ Lower-level API (more code to write)
- **Effort: 8-10 weeks**

### Performance Comparison Table

| Metric | etcd (Current) | Custom Raft | Improvement |
|--------|----------------|-------------|-------------|
| **Write Throughput** | 5K writes/sec | 50K-250K writes/sec | **10-50x** |
| **Session Ownership** | 5K connects/sec | 50K connects/sec | **10x** |
| **Subscriptions** | 5K subs/sec | 1M subs/sec | **200x** |
| **Write Latency (P50)** | 10ms | 2ms | **5x faster** |
| **Write Latency (P99)** | 50ms | 10ms | **5x faster** |
| **Storage Capacity** | 8GB | Unlimited (in-memory) | **No limit** |
| **Operational Complexity** | Low (embedded) | High (custom) | **Higher** |
| **Development Effort** | 0 weeks | 20 weeks | **Significant** |

### Recommendation: Custom Raft vs etcd

**When to use etcd (current approach):**
- ✅ Cluster size <10M clients
- ✅ Low session churn (<5K connects/sec)
- ✅ Sticky sessions (reconnect to same node)
- ✅ Want simple operational model

**When to build custom Raft:**
- ✅ Cluster size >10M clients
- ✅ High session churn (>10K connects/sec)
- ✅ Global deployment (multi-region)
- ✅ Have 4-6 months for development
- ✅ Have experienced distributed systems team

**For 10M clients on 20 nodes:**
- **Short-term (3-6 months)**: Stick with etcd, fix subscription bottleneck with gossip
- **Long-term (1-2 years)**: Migrate to custom Raft for 10x scalability

---

## Alternative Architectures

### Architecture 1: Consistent Hashing + Gossip (Cassandra-Style)

**Design:**
```
Client connects → Hash(clientID) → Node N (deterministic)
Subscriptions → Gossip protocol (eventual consistency)
Retained messages → Consistent hashing (3 replicas)
No central coordination (no etcd)
```

**Components:**
- **Consistent Hashing**: clientID → node assignment
- **Gossip Protocol**: Subscription propagation (hashicorp/memberlist)
- **Quorum Reads/Writes**: R=2, W=2 (out of 3 replicas)
- **Anti-Entropy**: Background sync for consistency

**Pros:**
- ✅ **Linear scaling**: Add nodes → increase capacity
- ✅ **No single point of failure**: Fully distributed
- ✅ **High availability**: Tolerate node failures
- ✅ **Low latency**: No consensus overhead

**Cons:**
- ❌ **Eventual consistency**: Clients may see stale data
- ❌ **Hinted handoff complexity**: Store writes when replica is down
- ❌ **Operational complexity**: Tuning quorum levels
- ❌ **No strong session ownership**: Possible split-brain

**Verdict:**
- **Good for**: Massive scale (100M+ clients), eventual consistency acceptable
- **Bad for**: Strong session ownership required (MQTT requires exactly-once session)
- **Effort**: 3-4 months development

### Architecture 2: Kafka-Based Event Sourcing

**Design:**
```
All state changes → Kafka topics (event log)
Session ownership → Partition assignment (Kafka consumer groups)
Subscriptions → Materialized view (local state rebuilt from log)
Retained messages → Kafka compacted topics
```

**Components:**
- **Kafka**: Distributed event log (append-only)
- **Event Sourcing**: Replay events to rebuild state
- **Stream Processing**: Kafka Streams for aggregation
- **Snapshots**: Periodic state snapshots for fast recovery

**Pros:**
- ✅ **Audit trail**: Full history of state changes
- ✅ **Time travel**: Replay events for debugging
- ✅ **Scalability**: Kafka handles millions of events/sec
- ✅ **Decoupling**: Brokers are stateless (state in Kafka)

**Cons:**
- ❌ **External dependency**: Requires Kafka cluster (Zookeeper)
- ❌ **Complexity**: Event sourcing mental model
- ❌ **Latency**: Read-your-writes not guaranteed (eventual)
- ❌ **Operational overhead**: Kafka tuning, rebalancing

**Verdict:**
- **Good for**: Large-scale analytics, audit requirements
- **Bad for**: Low-latency pub/sub, simple deployment
- **Effort**: 4-6 months development

### Architecture 3: Redis Cluster (Pub/Sub Native)

**Design:**
```
Session ownership → Redis hash (HSET clientID nodeID)
Subscriptions → Redis pub/sub (SUBSCRIBE topic)
Retained messages → Redis strings (SET topic payload)
Cluster mode → Redis Cluster (16384 hash slots)
```

**Components:**
- **Redis Cluster**: Distributed key-value store
- **Pub/Sub**: Built-in publish/subscribe
- **Lua Scripting**: Atomic operations (session takeover)
- **Persistence**: AOF + RDB snapshots

**Pros:**
- ✅ **Simple**: Redis is easy to understand
- ✅ **Fast**: In-memory, microsecond latency
- ✅ **Built-in pub/sub**: Native MQTT-like semantics
- ✅ **Battle-tested**: Widely used in production

**Cons:**
- ❌ **External dependency**: Requires Redis cluster
- ❌ **Memory-only**: All data in RAM (expensive at scale)
- ❌ **No wildcard subscriptions**: Redis pub/sub is exact match only
- ❌ **Eventual consistency**: Redis Cluster uses async replication

**Verdict:**
- **Good for**: Simple deployments, <1M clients
- **Bad for**: Large-scale (10M+), complex topic matching
- **Effort**: 2-3 months integration

### Architecture 4: Pure Gossip (No Coordination)

**Design:**
```
No central coordination (no etcd, no leader)
Session ownership → Conflict resolution (last-write-wins)
Subscriptions → CRDT (conflict-free replicated data type)
Retained messages → Anti-entropy (periodic sync)
```

**Components:**
- **Gossip Protocol**: Eventual state propagation
- **CRDTs**: Mathematically proven convergence
- **Vector Clocks**: Causality tracking
- **Merkle Trees**: Efficient diff detection

**Pros:**
- ✅ **Fully distributed**: No single point of failure
- ✅ **Self-healing**: Automatic recovery from partitions
- ✅ **Simple operations**: No leader election

**Cons:**
- ❌ **Weak consistency**: No ordering guarantees
- ❌ **Complex conflict resolution**: Split-brain scenarios
- ❌ **High network overhead**: Constant gossip traffic
- ❌ **Debugging difficulty**: Distributed state hard to reason about

**Verdict:**
- **Good for**: Peer-to-peer networks, IoT edge
- **Bad for**: Strong consistency requirements (MQTT session)
- **Effort**: 6-8 months (CRDT expertise required)

### Architecture 5: Hybrid etcd + Gossip (Recommended)

**Design:**
```
Session ownership → etcd (strong consistency required)
Subscriptions → Gossip (eventual consistency acceptable)
Retained messages → Hybrid storage (current approach)
Leadership → etcd (background tasks)
```

**Components:**
- **etcd**: Only for session ownership + leadership (critical path)
- **Gossip**: Subscription routing (hashicorp/memberlist)
- **BadgerDB**: Local storage (unchanged)
- **gRPC**: Inter-broker (unchanged)

**Pros:**
- ✅ **Best of both worlds**: Strong consistency where needed, eventual elsewhere
- ✅ **Incremental migration**: Keep etcd, add gossip for subscriptions
- ✅ **Lower etcd load**: Remove subscriptions from etcd (50GB → 2GB)
- ✅ **Proven libraries**: hashicorp/memberlist is battle-tested

**Cons:**
- ⚠️ **Two protocols**: etcd + gossip (operational complexity)
- ⚠️ **Eventual subscription consistency**: Rare false negatives (miss subscriber)
- ⚠️ **Network overhead**: Gossip traffic (~1 Mbps per node)

**Implementation Plan:**

**Step 1: Add Gossip for Subscriptions (2 weeks)**
```go
// Use hashicorp/memberlist
import "github.com/hashicorp/memberlist"

type GossipDelegate struct {
    subscriptions *SubscriptionIndex  // Local index
}

func (d *GossipDelegate) NotifyMsg(msg []byte) {
    var update SubscriptionUpdate
    json.Unmarshal(msg, &update)

    switch update.Type {
    case Subscribe:
        d.subscriptions.Add(update.ClientID, update.Filter)
    case Unsubscribe:
        d.subscriptions.Remove(update.ClientID, update.Filter)
    }
}

// Publish subscription change
func (b *Broker) Subscribe(clientID, filter string) {
    b.localSubs.Add(clientID, filter)

    // Gossip to cluster
    update := SubscriptionUpdate{Type: Subscribe, ClientID: clientID, Filter: filter}
    b.gossip.Broadcast(update)  // Eventually propagates to all nodes
}
```

**Step 2: Remove Subscriptions from etcd (1 week)**
- Migrate subscription storage to local BadgerDB
- Use gossip for propagation
- Keep etcd for session ownership only

**Step 3: Validate Consistency (1 week)**
- Test scenarios: network partitions, node failures
- Measure propagation delay (target: <100ms P99)
- Monitor false negatives (target: <0.01%)

**Verdict:**
- **Best for**: 10M client scale with minimal architectural change
- **Effort**: 4-6 weeks total
- **Risk**: Low (gossip libraries are mature)

---

## Architectural Decision Matrix

| Architecture | Complexity | Effort | Scalability | Consistency | Ops Overhead | Recommended For |
|--------------|-----------|--------|-------------|-------------|--------------|-----------------|
| **etcd + BadgerDB (current)** | Low | 0 weeks | 5M clients | Strong | Low | <5M clients, simple deployment |
| **Custom Raft** | High | 20 weeks | 50M+ clients | Strong | Medium | >20M clients, long-term investment |
| **etcd + Gossip (hybrid)** | Medium | 4 weeks | 10M clients | Mixed | Medium | **10M clients, best ROI** |
| **Consistent Hashing** | High | 16 weeks | 100M+ clients | Eventual | High | Massive scale, eventual OK |
| **Kafka Event Sourcing** | High | 24 weeks | 50M+ clients | Eventual | High | Analytics, audit trail |
| **Redis Cluster** | Low | 12 weeks | 5M clients | Eventual | Medium | Simple, small-scale |
| **Pure Gossip** | Very High | 32 weeks | Unlimited | Weak | High | Research, P2P networks |

---

## Recommendations

### For 10M Clients on 20 Nodes (Short-Term: 3-6 Months)

**Recommended Architecture: etcd + Gossip Hybrid**

**Changes Required:**

1. **Move subscriptions to gossip protocol** (4 weeks)
   - Use hashicorp/memberlist
   - Store subscriptions in local BadgerDB
   - Propagate changes via gossip
   - Reduces etcd from 50GB → 2GB

2. **Implement batch writes for session ownership** (1 week)
   - Group 10-100 session updates → 1 etcd transaction
   - Reduces write rate by 10-100x
   - Trade-off: 100ms propagation delay (acceptable)

3. **Add topic sharding documentation** (3 days)
   - Guide for partitioning topic space
   - Example: `/device/{nodeID}/#` → 90% local routing
   - Reduces cross-node traffic by 10x

4. **Monitoring and alerting** (1 week)
   - etcd metrics: storage, write rate, latency
   - Gossip metrics: propagation delay, convergence time
   - Subscription consistency: false negative rate

**Total Effort: 6-7 weeks**

**Expected Results:**
- ✅ Support 10M clients comfortably
- ✅ etcd storage: 2GB (well within 8GB limit)
- ✅ Session ownership: 20K connects/sec (with batching)
- ✅ Subscriptions: 1M subs/sec (with gossip)
- ✅ Message throughput: 500K msgs/sec (with sharding)

### For 50M+ Clients (Long-Term: 1-2 Years)

**Recommended Architecture: Custom Raft**

**Why:**
- etcd fundamentally limited by Raft serialization
- Custom Raft can optimize for MQTT use case
- 10-50x throughput improvement

**Migration Path:**

1. **Implement custom Raft (20 weeks)**
   - Use hashicorp/raft library (reduce effort)
   - MQTT-specific state machine
   - Optimized batching and pipelining

2. **Gradual migration (8 weeks)**
   - Run etcd + custom Raft in parallel
   - Migrate session ownership first
   - Validate consistency before full switch

3. **Deprecate etcd (4 weeks)**
   - Remove etcd dependency
   - Single binary with custom Raft

**Total Effort: 32 weeks (~8 months)**

**Expected Results:**
- ✅ Support 50M+ clients
- ✅ Session ownership: 200K connects/sec
- ✅ Subscriptions: 5M subs/sec
- ✅ Message throughput: 2M msgs/sec

### Decision Framework

```
Current clients < 5M
└─> Keep etcd + BadgerDB (no changes)

Current clients 5-10M
└─> Implement etcd + Gossip hybrid (6 weeks)
    └─> Achieves 10M capacity with minimal risk

Future target > 10M clients
└─> Plan custom Raft migration (8 months)
    └─> Start in 6-12 months (parallel to production growth)

Need 10M clients in <3 months
└─> etcd + Gossip is only realistic option
    └─> Custom Raft too risky for short timeline
```

---

## Conclusion

**For the stated requirement of 20 nodes, 10M clients:**

1. **Current architecture (etcd + BadgerDB) cannot support 10M clients** due to subscription storage bottleneck (50GB exceeds etcd 8GB limit)

2. **Recommended solution: Hybrid etcd + Gossip**
   - Effort: 6-7 weeks
   - Risk: Low
   - Scalability: 10M clients comfortably

3. **Custom Raft is not necessary** for 10M clients
   - Provides 10-50x improvement
   - But requires 20+ weeks development
   - Only needed for >20M client scale

4. **etcd bottleneck is fundamental** (Raft consensus)
   - Cannot be "fixed" without replacing etcd
   - Gossip protocol is proven alternative for eventual-consistency data
   - Strong consistency (etcd) only needed for session ownership

**Next Steps:**

1. Prototype gossip subscription propagation (1 week)
2. Benchmark propagation delay and convergence (3 days)
3. Implement production gossip integration (3 weeks)
4. Load test with 10M simulated clients (1 week)
5. Document operational runbooks (3 days)

**Total timeline: 6-7 weeks to 10M client capacity**
