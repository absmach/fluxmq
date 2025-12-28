# Scaling to 100M Clients: Architecture for Hyperscale

**Target:** 100M concurrent clients
**Required Nodes:** 100-200 nodes (500K-1M clients per node)
**Architectural Shift:** From "cluster" to "distributed system"

---

## Executive Summary

### Can Current Architecture Scale to 100M Clients?

**Answer: No. Fundamental architectural changes required.**

**Why 100M is Different from 10M:**

| Aspect | 10M Clients (20 nodes) | 100M Clients (200 nodes) | Change |
|--------|------------------------|--------------------------|--------|
| **Coordination** | Single etcd cluster (5 members) | Multiple coordination zones | Partitioned |
| **Routing** | Gossip (all-to-all) | Hierarchical routing | Structured |
| **Storage** | BadgerDB per node | Distributed storage tier | Tiered |
| **Network** | Flat cluster | Multi-region mesh | Geographic |
| **Consistency** | Strong (Raft) | Eventual (AP system) | Relaxed |
| **Failure Domain** | Single cluster | Regional isolation | Partitioned |

**Key Insight: 100M clients cannot be handled as "one big cluster"**
- Network topology: 200 nodes × 199 peers = 39,800 connections (unmanageable)
- Gossip overhead: 200 nodes gossiping = 100x network amplification
- etcd cluster: Maximum practical size is 7-9 members (not 200)
- Blast radius: Single bug affects all 100M clients (unacceptable)

**Required Architecture: Hierarchical Partitioned Design**

```
                    Global Control Plane (5 nodes)
                            |
        ┌───────────────────┼───────────────────┐
        |                   |                   |
    Region 1           Region 2           Region 3
    (40M clients)      (40M clients)      (20M clients)
        |                   |                   |
    ┌───┴───┐           ┌───┴───┐         ┌───┴───┐
   Zone A  Zone B      Zone C  Zone D     Zone E  Zone F
   (20M)   (20M)       (20M)   (20M)      (10M)   (10M)
    |       |            |       |          |       |
  [Broker Cluster]  [Broker Cluster]   [Broker Cluster]
   50 nodes         50 nodes           25 nodes
```

**Estimated Effort: 6-12 months for full rewrite**

---

## Architecture for 100M Clients

### Design Principle: Partition Everything

**Partitioning Strategy:**

1. **Geographic Partitioning** (Region Level)
   - Clients distributed by geography: US-East, EU-West, Asia-Pacific
   - Each region is independent (99% local traffic)
   - Cross-region traffic only for special cases (global topics)

2. **Topic Partitioning** (Zone Level)
   - Topic space partitioned by hash or prefix
   - Example: `/device/{shard-id}/+` where shard-id determines zone
   - Each zone handles 1/N of topic space

3. **Client Partitioning** (Node Level)
   - Consistent hashing assigns clients to nodes
   - Rebalancing during node add/remove
   - Each node handles 500K-1M clients

**Result: No single component sees all 100M clients**

### Tier 1: Global Control Plane (5 Nodes)

**Responsibilities:**
- Global cluster membership (which regions exist)
- Cross-region topic routing (rare global topics)
- Configuration distribution
- Cluster-wide metrics aggregation

**Technology:**
- etcd cluster (5 members, <1GB data)
- gRPC for cross-region communication
- Metrics: Prometheus federation

**Does NOT handle:**
- Individual client sessions (too many)
- Regional subscriptions (handled locally)
- Message routing (delegated to regions)

**Capacity:**
- Membership: 100 regions × 1KB = 100KB
- Global topics: <1000 topics (rare)
- Write rate: <100 writes/sec (low churn)

### Tier 2: Regional Clusters (10-20 Regions)

**Region Size:** 5-10M clients per region
**Nodes per Region:** 10-20 nodes
**Responsibility:** Independent MQTT broker cluster

**Architecture per Region:**
```
Region (10M clients, 20 nodes)
├── Regional Control Plane (3 node etcd)
│   ├── Session ownership (10M × 200B = 2GB)
│   └── Regional topic index
├── Broker Nodes (20 nodes, 500K clients each)
│   ├── BadgerDB (local state)
│   └── gRPC (inter-broker)
└── Regional Gossip (subscription propagation)
```

**Key Design:**
- Each region is a complete MQTT cluster (10M client capacity)
- Regions are isolated (failure in US-East doesn't affect EU-West)
- Cross-region traffic is rare (1-5% of messages)

**Technology:**
- etcd per region (NOT shared across regions)
- Gossip within region (hashicorp/memberlist)
- BadgerDB per broker node

### Tier 3: Broker Nodes (100-200 Total)

**Per-Node Capacity:**
- Connections: 500K-1M clients
- RAM: 20-40GB
- Disk: 100-500GB SSD
- Network: 1-10 Gbps

**Distribution:**
- Region 1 (US-East): 40 nodes → 40M clients
- Region 2 (EU-West): 40 nodes → 40M clients
- Region 3 (Asia-Pacific): 20 nodes → 20M clients
- Total: 100 nodes → 100M clients

**Why 500K-1M per node?**
- OS limits: `ulimit -n 1048576` (1M file descriptors)
- RAM: 40KB per connection × 1M = 40GB
- Network: 1M clients × 1KB/min = 16MB/s = 128 Mbps
- CPU: <50% with efficient event loop

---

## Component-by-Component Analysis

### Session Ownership at 100M Scale

**Problem with etcd:**
- 100M sessions × 200B = 20GB (exceeds 8GB limit by 2.5x)
- Connect churn: 100M clients × 1/hour = 28K connects/sec (exceeds 5K limit)

**Solution: Partition by Region + Custom Storage**

**Option 1: etcd per Region (Recommended for Initial Deployment)**
```
Region 1: 40M sessions → 8GB etcd (at capacity limit)
Region 2: 40M sessions → 8GB etcd (at capacity limit)
Region 3: 20M sessions → 4GB etcd (comfortable)

Total: 3 independent etcd clusters
```

**Per-region limits:**
- Storage: 8GB (at limit, need to tune)
- Writes: 28K/sec total / 3 regions = 9K/sec per region (exceeds 5K limit)

**🔴 Still bottlenecked by write throughput at 40M clients per region**

**Option 2: Custom Raft with Batching (Required for 40M+ per Region)**
```go
// Batch 1000 session connects → 1 Raft entry
type SessionBatch struct {
    Connects []SessionConnect  // 1000 connects
}

// Single Raft commit for 1000 operations
raft.Propose(SessionBatch{...})

// Effective throughput: 5K entries/sec × 1000 ops/entry = 5M ops/sec
```

**Benefits:**
- 5K writes/sec → 5M writes/sec (1000x improvement)
- Can handle 100M client churn
- Single region can handle 100M clients (no need to partition)

**Effort: 16-20 weeks**

**Option 3: Consistent Hashing (No Central Coordination)**
```
clientID → hash(clientID) % 200 nodes → Node N

No etcd needed for session ownership
Each node knows which clients it owns (deterministic)
Rebalancing on node add/remove
```

**Benefits:**
- Infinite scalability (add nodes linearly)
- No coordination overhead
- Simple operational model

**Cons:**
- Session takeover requires consistent hash ring gossip
- Rebalancing complexity (move clients when nodes change)
- Split-brain possible (network partition)

**Effort: 8-12 weeks (use hashicorp/consistent library)**

**Recommendation for 100M:**

```
Use Consistent Hashing + Gossip for session ownership
├── No etcd bottleneck (distributed coordination)
├── Linear scaling (add nodes → increase capacity)
├── Proven libraries (hashicorp/consistent, memberlist)
└── Eventual consistency acceptable (MQTT sessions are ephemeral)

Fall back to region-local etcd only for global state:
├── Region membership (which nodes in region)
├── Leadership election (background tasks)
└── Configuration (low-churn data)
```

### Subscription Routing at 100M Scale

**Problem:**
- 100M clients × 10 subs = 1B subscriptions
- 1B × 500B = 500GB subscription data
- Cannot fit in memory on any single node
- Gossip all-to-all doesn't scale to 200 nodes

**Solution: Hierarchical Topic Sharding**

**Design:**
```
Topic Space Partitioning:
/device/{region}/{zone}/{device-id}/+

Examples:
/device/us-east/zone-a/12345/temperature → Zone A (US-East Region)
/device/eu-west/zone-b/67890/humidity    → Zone B (EU-West Region)

Routing:
1. Extract {region} from topic → Route to region
2. Extract {zone} from topic → Route to zone within region
3. Extract {device-id} from topic → Route to specific node (consistent hash)

Result: 99% of traffic stays local to zone
```

**Subscription Storage:**

| Level | Subscriptions | Storage | Technology |
|-------|--------------|---------|------------|
| **Global** | 0 (no global subs) | 0 | N/A |
| **Regional** | 1M (cross-zone topics) | 500MB | etcd per region |
| **Zonal** | 10M (cross-node topics) | 5GB | Gossip within zone |
| **Local** | 1B (node-local) | 500GB total | BadgerDB (5GB per node) |

**Routing Algorithm:**
```go
func RouteMessage(topic string, payload []byte) {
    // Parse topic: /device/{region}/{zone}/{device-id}/sensor
    parts := strings.Split(topic, "/")
    region := parts[2]
    zone := parts[3]
    deviceID := parts[4]

    // Level 1: Regional routing
    if region != myRegion {
        grpc.SendToRegion(region, topic, payload)
        return
    }

    // Level 2: Zonal routing
    if zone != myZone {
        grpc.SendToZone(zone, topic, payload)
        return
    }

    // Level 3: Node routing (consistent hash)
    nodeID := consistentHash.Get(deviceID)
    if nodeID != myNodeID {
        grpc.SendToNode(nodeID, topic, payload)
        return
    }

    // Level 4: Local delivery
    localSubscribers := badgerDB.GetSubscribers(topic)
    for _, sub := range localSubscribers {
        deliverToClient(sub.ClientID, payload)
    }
}
```

**Benefits:**
- O(log N) routing hops (3 levels: region → zone → node)
- 99% local delivery (topic sharding)
- No full-mesh gossip (only within 10-20 node zones)
- Linear scaling (add zones → increase capacity)

**Subscription Latency:**
- Local (same node): <1ms
- Zonal (same region): ~5ms (1 gRPC hop)
- Regional (same region): ~10ms (2 gRPC hops)
- Cross-region: ~50-200ms (WAN latency)

**Wildcard Subscriptions:**

**Problem:** Wildcard subscriptions can match across shards
```
Subscribe: /device/+/+/+/temperature
Matches: All temperature sensors globally (100M potential matches)
```

**Solution: Bloom Filters + Index Servers**

```
Each zone maintains Bloom filter of subscribed topics:
- Size: 10M topics × 10 bits = 12.5MB per zone
- False positive rate: 1%
- Check: Is "/device/us-east/zone-a/12345/temp" subscribed?
  → Query Bloom filter → 99% accurate

For wildcards:
1. Publish to local zone → Check Bloom filter → Deliver
2. Publish to remote zones only if Bloom filter matches
3. Accept 1% false positives (message sent to zone with no subscribers)
```

**Benefits:**
- 99% reduction in cross-zone traffic
- 12.5MB per zone (fits in memory)
- <1µs Bloom filter lookup

### Retained Messages at 100M Scale

**Capacity:**
- 100M clients × 1 retained message = 100M retained messages
- 100M × 2KB average = 200GB total storage

**Current Hybrid Approach:**
- Small (<1KB): 70M × 500B = 35GB (etcd → exceeds limit)
- Large (≥1KB): 30M × 300B metadata = 9GB (etcd → OK)
- Large payloads: 30M × 10KB = 300GB (BadgerDB distributed)

**Problem: 35GB small messages exceeds etcd 8GB limit**

**Solution: Multi-Tier Storage**

**Tier 1: Hot Retained (Active Topics, ~1M messages)**
- Storage: Redis Cluster (in-memory, 10-node cluster)
- Size: 1M × 1KB = 1GB
- Latency: <1ms
- Replication: 3x (3GB total RAM)
- Use case: High-traffic topics (sensors publishing every second)

**Tier 2: Warm Retained (Recent Topics, ~10M messages)**
- Storage: BadgerDB per node (local SSD)
- Size: 10M × 2KB = 20GB distributed (200MB per node)
- Latency: ~5ms
- Use case: Medium-traffic topics (hourly updates)

**Tier 3: Cold Retained (Archived Topics, ~89M messages)**
- Storage: S3/Object Storage (eventual consistency)
- Size: 89M × 2KB = 178GB
- Latency: ~100ms (async fetch)
- Use case: Low-traffic topics (daily/weekly updates)

**Tiered Caching:**
```go
func GetRetained(topic string) (*Message, error) {
    // L1: Check Redis (hot tier)
    if msg, ok := redis.Get(topic); ok {
        return msg, nil
    }

    // L2: Check local BadgerDB (warm tier)
    if msg, ok := badgerDB.Get(topic); ok {
        redis.Set(topic, msg)  // Promote to hot tier
        return msg, nil
    }

    // L3: Fetch from S3 (cold tier)
    msg := s3.GetObject(topic)
    badgerDB.Set(topic, msg)  // Promote to warm tier
    return msg, nil
}
```

**TTL Policy:**
```
Hot tier (Redis): TTL 1 hour → Evict to warm
Warm tier (BadgerDB): TTL 24 hours → Evict to cold
Cold tier (S3): TTL 30 days → Delete
```

**Benefits:**
- Total cost: Redis (3GB × $0.50/GB = $1.50/hr) + S3 (178GB × $0.023/GB = $4/mo)
- Hot latency: <1ms for active topics
- Scalable: S3 handles petabytes
- Cost-effective: 89% of data in cheap cold storage

### Message Throughput at 100M Scale

**Target Throughput:**
- 100M clients × 1 message/min = 1.67M messages/sec
- Peak (5x average): 8.3M messages/sec

**Per-Node Throughput:**
- 100 nodes → 16.7K messages/sec per node (average)
- 100 nodes → 83K messages/sec per node (peak)

**Bottleneck Analysis:**

| Component | Capacity | Required | Status |
|-----------|----------|----------|--------|
| **BadgerDB writes** | 100K/sec | 83K/sec | ✅ OK |
| **Network** | 1 Gbps = 125 MB/s | 83 MB/s | ✅ OK |
| **CPU** | 16 cores | ~8 cores | ✅ OK |
| **Cross-region** | 10 Gbps backbone | 1.67 Gbps | ✅ OK |

**With Topic Sharding (99% local):**
- Cross-node traffic: 1.67M × 1% = 16.7K messages/sec
- Per-node cross-node: 167 messages/sec
- **Not a bottleneck**

**Conclusion: Message throughput is NOT the bottleneck at 100M scale**

### Network Topology at 100M Scale

**Problem: Full Mesh Doesn't Scale**
- 200 nodes full mesh = 200 × 199 / 2 = 19,900 connections
- Each connection: 1MB buffers = 19.9GB RAM just for connections
- Gossip overhead: 200 nodes × 200 messages = 40K messages/sec

**Solution: Hierarchical Topology**

```
                  Global Backbone (5 nodes)
                          |
        ┌─────────────────┼─────────────────┐
        |                 |                 |
    Region 1          Region 2          Region 3
    (Gateway)         (Gateway)         (Gateway)
        |                 |                 |
    ┌───┴───┐         ┌───┴───┐         ┌───┴───┐
   Zone A  Zone B    Zone C  Zone D    Zone E  Zone F
  (10 nodes)(10)    (10)    (10)       (10)    (10)
   Full Mesh        Full Mesh          Full Mesh
```

**Connection Count:**
- Global: 5 nodes × 3 regions = 15 connections
- Regional: 3 regions × 2 zones = 6 gateways
- Zonal: 6 zones × 10 nodes mesh = 6 × 45 = 270 connections
- **Total: 291 connections** (vs 19,900 full mesh)

**Routing Hops:**
- Same zone: 1 hop (direct)
- Same region: 2 hops (node → zone gateway → node)
- Cross-region: 4 hops (node → zone → region → region → zone → node)

**Benefits:**
- 98% reduction in connection count
- Bounded gossip (only within 10-node zones)
- Failure isolation (zone failure doesn't affect region)

---

## Data Consistency Model

**At 100M scale, strong consistency everywhere is impractical.**

### CAP Theorem Trade-offs

**Partition Tolerance (P):** Required (network partitions are inevitable at scale)

**Choose between:**
- **CP (Consistency + Partition Tolerance):** Strong consistency, but unavailable during partitions
- **AP (Availability + Partition Tolerance):** Always available, but eventually consistent

**Recommendation for MQTT at 100M scale: Hybrid CP/AP**

| Data Type | Consistency Model | Technology | Why |
|-----------|-------------------|------------|-----|
| **Session Ownership** | Strong (CP) | etcd or Custom Raft | Prevent split-brain |
| **Subscriptions** | Eventual (AP) | Gossip or CRDT | Accept 100ms propagation delay |
| **Retained Messages** | Eventual (AP) | Tiered storage | Accept 1s stale reads |
| **Message Routing** | Eventual (AP) | Bloom filters | Accept 1% false positives |
| **Leadership** | Strong (CP) | etcd | Only one leader for background tasks |

**Example: Subscription Consistency**

```
Client subscribes on Node A at T=0
Node A gossips to cluster: "Client subscribed to /sensor/temp"
Gossip propagates: T=0ms (Node A) → T=50ms (same zone) → T=100ms (same region) → T=500ms (cross-region)

Message published at T=25ms on Node B (same zone)
- Node B hasn't received gossip yet (50ms propagation)
- Message NOT delivered to subscriber
- **False negative** (missed message)

Acceptable for MQTT:
- Rare: Subscription followed by immediate publish (1% of traffic)
- Not silent: Client can re-subscribe if no messages received
- Self-healing: After 100ms, propagation complete
```

**Tuning Consistency:**
- Lower gossip interval: 100ms → 50ms (faster propagation, more bandwidth)
- Add quorum: Wait for 3 nodes to ack (slower, stronger consistency)
- Read-your-writes: Subscribe on Node A → Wait for local confirmation before publishing

### Conflict Resolution

**Scenario: Split-Brain (Network Partition)**

```
Partition between US-East and EU-West regions
Client reconnects:
- US-East thinks: Session on Node 1
- EU-West thinks: Session on Node 50

Both regions independently accept connection
Result: Client has 2 simultaneous sessions (violation of MQTT spec)
```

**Solution: Vector Clocks + Last-Write-Wins**

```go
type SessionOwnership struct {
    ClientID string
    NodeID   string
    Timestamp time.Time
    VectorClock map[string]int  // region → version
}

// On partition heal:
func ResolveConflict(session1, session2 SessionOwnership) SessionOwnership {
    // Compare vector clocks
    if session1.VectorClock.HappensBefore(session2.VectorClock) {
        return session2  // session2 is newer
    }
    if session2.VectorClock.HappensBefore(session1.VectorClock) {
        return session1  // session1 is newer
    }

    // Concurrent updates (both regions accepted independently)
    // Last-write-wins based on timestamp
    if session1.Timestamp.After(session2.Timestamp) {
        return session1
    }
    return session2
}

// Disconnect losing session
DisconnectClient(loser.NodeID, loser.ClientID, reason="split-brain resolved")
```

**Trade-off:**
- Partition: Client gets 2 sessions (brief violation)
- Heal: One session disconnected (brief disruption)
- Alternative: Reject all connections during partition (unavailable)
- **MQTT recommends: Availability over consistency (AP system)**

---

## Technology Stack for 100M Clients

### Coordination Layer

**Option 1: etcd (Per-Region, <40M clients per region)**
- Use: Session ownership, regional metadata
- Limit: 40M sessions × 200B = 8GB (at capacity)
- Pros: Battle-tested, strong consistency
- Cons: Write throughput bottleneck (5K/sec)

**Option 2: Custom Raft (Per-Region, 40M+ clients per region)**
- Use: Session ownership with MQTT-optimized batching
- Capacity: 100M+ sessions (in-memory state machine)
- Pros: 10-50x write throughput (50K-250K/sec)
- Cons: 16-20 weeks development, operational complexity

**Option 3: Consistent Hashing + Gossip (Global, Unlimited)**
- Use: Session ownership without central coordination
- Capacity: Unlimited (deterministic hash assignment)
- Pros: Linear scaling, no single bottleneck
- Cons: Eventual consistency, split-brain possible

**Recommendation:**
- Start: etcd per region (proven, <40M per region)
- Scale: Custom Raft when hitting 40M per region
- Ultimate: Consistent hashing for 1B+ clients

### Storage Layer

**Local State (per broker node):**
- Technology: BadgerDB
- Size: 100-500GB SSD per node
- Use: Session state, offline queues, inflight tracking
- Unchanged from current architecture

**Distributed State (cluster-wide):**

| Data Type | Technology | Capacity | Latency |
|-----------|-----------|----------|---------|
| **Hot Retained** | Redis Cluster | 10M msgs | <1ms |
| **Warm Retained** | BadgerDB | 100M msgs | ~5ms |
| **Cold Retained** | S3/Object Store | 1B+ msgs | ~100ms |
| **Session Index** | etcd or Custom Raft | 100M sessions | ~10ms |
| **Subscription Index** | Gossip + Bloom Filter | 1B subs | <1ms |

### Messaging Layer

**Intra-Zone (Same Zone, 10-20 Nodes):**
- Technology: gRPC (current)
- Topology: Full mesh
- Latency: <5ms
- Unchanged from current architecture

**Inter-Zone (Same Region, Cross-Zone):**
- Technology: gRPC with connection pooling
- Topology: Star (via zone gateway)
- Latency: ~10ms

**Inter-Region (Cross-Region):**
- Technology: gRPC over WAN, or message queue (Kafka, NATS)
- Topology: Region gateways
- Latency: 50-200ms (WAN)
- Consider: Message batching, compression

### Subscription Propagation

**Intra-Zone (Same Zone):**
- Technology: hashicorp/memberlist (gossip)
- Propagation: 50-100ms
- Unchanged from 10M architecture

**Inter-Zone (Same Region):**
- Technology: Bloom filter exchange (1s interval)
- Size: 12.5MB per zone
- Propagation: 1-2 seconds (acceptable)

**Inter-Region:**
- Technology: Global topic index (etcd, <1000 topics)
- Propagation: 5-10 seconds
- Use: Only for globally-routed topics (rare)

---

## Operational Considerations

### Deployment Strategy

**Geographic Distribution:**
```
US-East (Virginia):
├── 40 broker nodes → 40M clients
├── 3 etcd nodes (regional control)
├── 10 Redis nodes (hot retained)
└── S3 bucket (cold retained)

EU-West (Ireland):
├── 40 broker nodes → 40M clients
├── 3 etcd nodes (regional control)
├── 10 Redis nodes (hot retained)
└── S3 bucket (cold retained)

Asia-Pacific (Singapore):
├── 20 broker nodes → 20M clients
├── 3 etcd nodes (regional control)
├── 5 Redis nodes (hot retained)
└── S3 bucket (cold retained)

Global Control:
├── 5 etcd nodes (global membership)
└── Prometheus federation (metrics)
```

### Cost Estimate (AWS)

**Per-Region (40M clients, 40 nodes):**

| Component | Instance Type | Count | Cost/Month | Total |
|-----------|---------------|-------|------------|-------|
| **Broker Nodes** | c6i.8xlarge (32 vCPU, 64GB RAM) | 40 | $1,088 | $43,520 |
| **etcd Cluster** | m6i.2xlarge (8 vCPU, 32GB RAM) | 3 | $312 | $936 |
| **Redis Cluster** | r6i.2xlarge (8 vCPU, 64GB RAM) | 10 | $504 | $5,040 |
| **Load Balancers** | Network LB | 3 | $22 | $66 |
| **EBS Storage** | gp3 (500GB per broker) | 40 | $40 | $1,600 |
| **S3 Storage** | Standard (200GB) | 1 | $5 | $5 |
| **Data Transfer** | 10 Gbps cross-region | - | $1,000 | $1,000 |
| **CloudWatch** | Metrics + Logs | - | $500 | $500 |
| **Total per Region** | | | | **$52,667/month** |

**Total (3 regions):**
- Infrastructure: $157,000/month
- Support (Enterprise): $10,000/month
- **Total: ~$167,000/month for 100M clients**

**Cost per Client:**
- $167,000 / 100M = **$0.00167 per client per month**
- Or: **$0.02 per client per year**

**Surprisingly cost-effective at scale**

### Team Requirements

**For 100M client deployment:**

| Role | Count | Responsibility |
|------|-------|----------------|
| **Site Reliability Engineers** | 4 | 24/7 on-call, infrastructure automation |
| **Backend Engineers** | 3 | Custom Raft, performance optimization |
| **Platform Engineers** | 2 | Deployment pipelines, IaC (Terraform) |
| **Monitoring Engineer** | 1 | Observability, alerting, dashboards |
| **Security Engineer** | 1 | TLS, auth, compliance (SOC2, GDPR) |
| **Engineering Manager** | 1 | Coordination, roadmap |
| **Total** | **12 engineers** | |

**Ongoing Effort:**
- Development: 50% (new features, optimizations)
- Operations: 30% (incidents, scaling, deployments)
- On-call: 20% (24/7 rotation, 4 engineers)

### Monitoring at 100M Scale

**Metrics Volume:**
- 100 broker nodes × 100 metrics/node = 10K time series
- 1s granularity × 10K series = 10K samples/sec
- Retention: 30 days
- Storage: 10K samples/sec × 86400 sec/day × 30 days × 8 bytes = 207GB

**Recommended:**
- Prometheus federation (global → regional → local)
- Thanos or Cortex for long-term storage
- Grafana for dashboards

**Critical Alerts:**

| Alert | Threshold | Severity | Response Time |
|-------|-----------|----------|---------------|
| **Node Down** | 1 minute offline | Critical | Immediate (page) |
| **Region Unreachable** | 3 regions unable to communicate | Critical | Immediate |
| **etcd Storage** | >90% capacity | Warning | 1 hour |
| **Connection Rate** | >10K/sec per node | Warning | 1 hour |
| **Message Drop Rate** | >0.1% | Critical | Immediate |
| **Cross-Region Latency** | >500ms P99 | Warning | 4 hours |

---

## Migration Path: 10M → 100M

**Phase 1: Regional Partitioning (Months 1-3)**

**Goal:** Split 10M single-cluster into 3 regional clusters (prepare for 30M)

**Tasks:**
1. Implement regional routing (topic prefix: `/region/{region-id}/...`)
2. Deploy 3 regional clusters (each 10M capacity)
3. Migrate clients region-by-region (blue/green deployment)
4. Validate cross-region routing

**Effort:** 12 weeks, 3 engineers

**Phase 2: Zonal Sharding (Months 4-6)**

**Goal:** Partition each region into zones (prepare for 40M per region)

**Tasks:**
1. Implement zonal routing (topic prefix: `/region/{region}/zone/{zone}/...`)
2. Deploy 2-4 zones per region
3. Implement Bloom filter subscription index
4. Test cross-zone message routing

**Effort:** 12 weeks, 3 engineers

**Phase 3: Custom Raft (Months 7-9)**

**Goal:** Replace etcd with custom Raft for 40M+ per region

**Tasks:**
1. Implement MQTT-optimized Raft state machine
2. Migrate session ownership from etcd to custom Raft
3. Benchmark: Validate 10x write throughput improvement
4. Blue/green deployment per region

**Effort:** 12 weeks, 4 engineers (parallel with Phase 2)

**Phase 4: Tiered Storage (Months 10-12)**

**Goal:** Implement hot/warm/cold retained message storage

**Tasks:**
1. Deploy Redis cluster (hot tier)
2. Implement TTL-based eviction to S3 (cold tier)
3. Migrate retained messages from BadgerDB
4. Validate <1ms latency for hot topics

**Effort:** 12 weeks, 2 engineers

**Total: 12 months, peak 6 engineers, sustained 3 engineers**

---

## Alternatives for 100M Scale

### Alternative 1: Kafka-Based Architecture

**Design:**
```
All messages → Kafka topics (partitioned by region/zone)
Brokers are stateless (state in Kafka)
Session state → Kafka compacted topics
Subscriptions → Kafka consumer groups
```

**Pros:**
- Kafka handles 10M+ messages/sec easily
- Horizontal scaling (add partitions)
- Built-in replication (3x)
- Mature ecosystem (Kafka Streams, Connect)

**Cons:**
- External dependency (Kafka cluster = 30+ nodes)
- Operational complexity (Zookeeper, rebalancing)
- Latency: ~10ms (Kafka ack)
- Cost: Kafka cluster = $50K/month additional

**Verdict:**
- Good for: Analytics-heavy workloads (message replay, audit)
- Bad for: Low-latency MQTT (<10ms)
- **Not recommended for 100M MQTT** (complexity outweighs benefits)

### Alternative 2: Serverless (AWS IoT Core)

**Design:**
```
Use AWS IoT Core (managed MQTT service)
Pay per message: $1 per 1M messages
100M clients × 1 msg/min = 1.67M msgs/sec
Cost: 1.67M × 60 × 24 × 30 × $1/1M = $72K/month
```

**Pros:**
- Zero operational burden
- Auto-scaling
- Integrated with AWS (Lambda, DynamoDB)

**Cons:**
- Vendor lock-in (AWS only)
- Cost: 2.5x more expensive than self-hosted ($72K vs $167K for full stack)
- Limited customization (no custom protocols)
- Black box (no control over internals)

**Verdict:**
- Good for: Startups, proof-of-concept
- Bad for: Cost-sensitive at scale, customization required
- **Possible for 100M MQTT** but expensive

### Alternative 3: VerneMQ (Erlang-Based MQTT Broker)

**Design:**
```
VerneMQ cluster (Erlang/OTP)
Built-in clustering (Distributed Erlang)
Hot code upgrades (zero downtime)
Battle-tested at 10M+ scale (used by AdTech companies)
```

**Pros:**
- Purpose-built for MQTT
- Proven at scale (documented 10M clients)
- Erlang fault tolerance (let it crash)
- Plugin system (custom auth, webhooks)

**Cons:**
- Erlang expertise required (small talent pool)
- Distributed Erlang full mesh (doesn't scale to 100+ nodes)
- No multi-region story (same cluster)
- Eventual consistency (gossip-based)

**Verdict:**
- Good for: Single-region deployment (10-20M clients)
- Bad for: Multi-region (100M global)
- **Partial solution for 100M** (need regional partitioning)

### Alternative 4: EMQX (Erlang + Raft)

**Design:**
```
EMQX cluster (Erlang + Raft)
Core/Replica node architecture
Raft for coordination (strong consistency)
Clustering up to 100 nodes
```

**Pros:**
- Enterprise-grade MQTT 5.0 implementation
- Strong consistency option
- Multi-protocol (MQTT, CoAP, LwM2M)
- Proven at scale (claims 100M clients)

**Cons:**
- Closed-source (enterprise version)
- Licensing cost (~$100K/year for 100M clients)
- Erlang expertise required
- Black box (limited customization)

**Verdict:**
- Good for: Buy vs build decision (faster time to market)
- Bad for: Open-source requirement, cost-sensitive
- **Viable alternative for 100M** (commercial option)

---

## Recommendations

### For 100M Clients: Build vs Buy

**Build (Custom Go Implementation):**
- Effort: 12 months, 6 engineers
- Cost: $1M development + $167K/month infrastructure
- Pros: Full control, open-source, cost-effective at scale
- Cons: Operational burden, need experienced team

**Buy (EMQX Enterprise):**
- Effort: 3 months, 2 engineers (integration)
- Cost: $100K/year license + $167K/month infrastructure
- Pros: Faster time to market, vendor support, proven at scale
- Cons: Vendor lock-in, licensing cost, limited customization

**Hybrid (Build on Open-Source Kafka/Redis):**
- Effort: 8 months, 4 engineers
- Cost: $600K development + $250K/month infrastructure
- Pros: Proven components, ecosystem support
- Cons: Complexity, external dependencies, higher cost

**Decision Matrix:**

| Criterion | Build | Buy (EMQX) | Hybrid (Kafka) |
|-----------|-------|------------|----------------|
| **Time to Market** | 12 months | 3 months ⭐ | 8 months |
| **Development Cost** | $1M | $200K ⭐ | $600K |
| **Ongoing Cost** | $167K/mo ⭐ | $181K/mo | $250K/mo |
| **Customization** | Full ⭐ | Limited | Moderate |
| **Operational Burden** | High | Low ⭐ | Moderate |
| **Talent Pool** | Go (large) ⭐ | Erlang (small) | Java/Kafka (moderate) |

**Recommendation:**
- **Start with EMQX** (fast time to market, proven at scale)
- **Migrate to custom** after 12-24 months (when hitting scale limits or cost concerns)
- **Avoid Kafka** (complexity not justified for MQTT use case)

### Technical Architecture (If Building Custom)

**Coordination:**
- Use: Consistent Hashing + Gossip (not etcd, not custom Raft)
- Why: Linear scaling, no single bottleneck, simpler than custom Raft
- Effort: 8-12 weeks

**Storage:**
- Local: BadgerDB (unchanged)
- Hot Retained: Redis Cluster (10 nodes per region)
- Cold Retained: S3/Object Storage
- Effort: 8-10 weeks

**Routing:**
- Regional: gRPC with hierarchical topology
- Subscription Index: Bloom filters (12.5MB per zone)
- Topic Sharding: `/region/{region}/zone/{zone}/...`
- Effort: 12-16 weeks

**Total Development: 28-38 weeks (~9 months)**

---

## Summary

### Can You Scale to 100M Clients?

**Yes, but it requires fundamental architectural changes:**

1. **Regional Partitioning** (not single cluster)
2. **Consistent Hashing** (not etcd for session ownership)
3. **Tiered Storage** (not just BadgerDB)
4. **Hierarchical Routing** (not flat gossip)

### Effort Summary

| Approach | Effort | Time to 100M | Risk |
|----------|--------|--------------|------|
| **Current Architecture** | 0 | Never | - |
| **EMQX Enterprise** | 3 months | 3 months | Low ⭐ |
| **Custom (Recommended)** | 9 months | 12 months | Medium |
| **Kafka-Based** | 12 months | 15 months | High |

### Cost Summary

| Approach | Development | Infrastructure | Total (Year 1) |
|----------|-------------|----------------|----------------|
| **Current Architecture** | $0 | - | Cannot scale |
| **EMQX Enterprise** | $200K | $2M | $2.2M ⭐ |
| **Custom Build** | $1M | $2M | $3M |
| **Kafka-Based** | $600K | $3M | $3.6M |

### Final Recommendation

**For 100M clients:**

1. **Short-term (0-12 months):** Deploy EMQX Enterprise
   - Fastest path to 100M
   - Proven technology
   - Vendor support
   - Cost: $2.2M year 1

2. **Long-term (12-24 months):** Migrate to custom implementation
   - Lower ongoing cost ($167K vs $181K/month)
   - Full control and customization
   - Amortize development cost over 2-3 years

**Do NOT attempt:**
- Scaling current architecture to 100M (will fail)
- Building custom Raft from scratch (9 months additional effort)
- Using Kafka as MQTT backend (complexity not justified)

**Timeline:**
- Month 0-3: Deploy EMQX, reach 30M clients
- Month 3-6: Scale EMQX to 60M clients
- Month 6-9: Begin custom implementation (parallel)
- Month 9-12: Reach 100M on EMQX
- Month 12-24: Gradual migration to custom (region by region)
- Month 24+: 100M on custom architecture, decommission EMQX

**Total cost (2 years):** $2.2M (EMQX) + $3M (custom) = $5.2M
**Steady-state cost:** $2M/year infrastructure (100M clients)
