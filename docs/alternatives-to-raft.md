# Alternatives to Custom Raft for MQTT Clustering

## Overview

This document explores alternatives to custom Raft for achieving scalable MQTT broker clustering. Each approach has different trade-offs in terms of consistency, availability, performance, and complexity.

## Quick Comparison Table

| Approach | Consistency | Availability | Complexity | Write Throughput | Best For |
|----------|-------------|--------------|------------|------------------|----------|
| **Custom Raft** | Strong (CP) | Medium | High | 50K-250K/sec | 50M+ clients |
| **Gossip Protocol** ⭐ | Eventual (AP) | High | Low | 1M+/sec | 10M-20M clients |
| **Consistent Hashing** | None (AP) | Very High | Low | Unlimited | Stateless routing |
| **CRDTs** | Eventual (AP) | High | Medium | 500K+/sec | Collaborative state |
| **Redis Cluster** | Eventual (AP) | High | Low | 100K-500K/sec | Quick solution |
| **Hybrid (Gossip + Raft)** ⭐ | Mixed | High | Medium | 200K-500K/sec | **10M clients (recommended)** |
| **Event Sourcing (Kafka)** | Strong (CP) | Medium | Very High | 100K+/sec | Audit requirements |
| **No Coordination (Local Only)** | None | Very High | Very Low | Unlimited | Small deployments |

⭐ = Recommended for 10M client scale

## 1. Gossip Protocol (Recommended for 10M Clients)

### What is Gossip?

Gossip (also called epidemic protocol) is a peer-to-peer communication protocol where nodes randomly exchange state updates with neighbors. Updates propagate through the network like gossip or epidemics.

**Popular library:** `hashicorp/memberlist` (used in Consul, Nomad)

### How It Works for MQTT

```
Node 1: Client subscribes to "sensors/temp"
  ↓
Node 1: Update local state + broadcast gossip message
  ↓
Node 2, 3, 4: Receive gossip → update local state → forward to neighbors
  ↓
Within 100ms: All nodes know about subscription (eventually consistent)
```

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  MQTT Broker Cluster                    │
│                                                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐│
│  │  Node 1  │  │  Node 2  │  │  Node 3  │  │  Node 4  ││
│  │          │  │          │  │          │  │          ││
│  │ Gossip ←─┼──┼─→ Gossip │  │ Gossip ←─┼──┼─→ Gossip ││
│  │  State   │  │  State   │  │  State   │  │  State   ││
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘│
│                                                          │
│  Each node maintains:                                   │
│  • Local BadgerDB (subscriptions, retained messages)    │
│  • In-memory peer list                                  │
│  • Gossip protocol for state propagation                │
└─────────────────────────────────────────────────────────┘
```

### Implementation Example

```go
// cluster/gossip_cluster.go

import "github.com/hashicorp/memberlist"

type GossipCluster struct {
    nodeID    string
    localDB   *badger.DB
    memberlist *memberlist.Memberlist

    // Local state
    subscriptions map[string][]Subscription  // clientID → subscriptions
    sessions      map[string]string          // clientID → nodeID

    mu sync.RWMutex
}

type GossipMessage struct {
    Type      string    // "subscribe", "unsubscribe", "session_connect"
    ClientID  string
    Topic     string
    NodeID    string
    Timestamp time.Time
}

// NotifyMsg is called when a gossip message is received
func (g *GossipCluster) NotifyMsg(msg []byte) {
    var gossipMsg GossipMessage
    if err := json.Unmarshal(msg, &gossipMsg); err != nil {
        return
    }

    g.mu.Lock()
    defer g.mu.Unlock()

    switch gossipMsg.Type {
    case "subscribe":
        // Add subscription to local state
        subs := g.subscriptions[gossipMsg.ClientID]
        subs = append(subs, Subscription{
            Filter: gossipMsg.Topic,
        })
        g.subscriptions[gossipMsg.ClientID] = subs

        // Persist to local BadgerDB
        g.localDB.Update(func(txn *badger.Txn) error {
            key := fmt.Sprintf("sub:%s:%s", gossipMsg.ClientID, gossipMsg.Topic)
            return txn.Set([]byte(key), []byte{})
        })

    case "session_connect":
        g.sessions[gossipMsg.ClientID] = gossipMsg.NodeID
    }
}

// Subscribe propagates subscription via gossip
func (g *GossipCluster) Subscribe(clientID, topic string) error {
    msg := GossipMessage{
        Type:      "subscribe",
        ClientID:  clientID,
        Topic:     topic,
        NodeID:    g.nodeID,
        Timestamp: time.Now(),
    }

    data, _ := json.Marshal(msg)

    // Broadcast to all nodes via gossip
    return g.memberlist.SendReliable(nil, data)
}

// GetSubscribers reads from local state (no network call)
func (g *GossipCluster) GetSubscribers(topic string) []string {
    g.mu.RLock()
    defer g.mu.RUnlock()

    var subscribers []string
    for clientID, subs := range g.subscriptions {
        for _, sub := range subs {
            if topics.Match(sub.Filter, topic) {
                subscribers = append(subscribers, clientID)
                break
            }
        }
    }
    return subscribers
}
```

### Gossip Performance Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Convergence Time** | 50-200ms P95 | Depends on cluster size |
| **Write Throughput** | 1M+ ops/sec | No coordination bottleneck |
| **Network Bandwidth** | O(log N) messages | Efficient propagation |
| **Memory per Node** | 10-50 MB | Peer list + local state |
| **Scalability** | 100-1000 nodes | Proven in Consul clusters |

### Advantages
- ✅ **Very high throughput** (no consensus bottleneck)
- ✅ **High availability** (no single leader)
- ✅ **Self-healing** (automatic failure detection)
- ✅ **Simple implementation** (use existing library)
- ✅ **Low latency reads** (local state, no network call)

### Disadvantages
- ❌ **Eventual consistency** (not immediate)
- ❌ **No ordering guarantees** (messages may arrive out of order)
- ❌ **Conflict resolution needed** (for concurrent updates)
- ❌ **Network overhead** (constant gossip traffic)

### When to Use Gossip
- ✅ Eventual consistency is acceptable (subscriptions, retained metadata)
- ✅ High write throughput required (>50K ops/sec)
- ✅ High availability critical
- ✅ 10M-20M client scale

### Gossip Best Practices

**1. Use vector clocks for conflict resolution:**
```go
type GossipMessage struct {
    ClientID  string
    Topic     string
    VectorClock map[string]uint64  // nodeID → counter
}

func (g *GossipCluster) shouldApply(msg *GossipMessage) bool {
    // Only apply if message is newer than local state
    localClock := g.getVectorClock(msg.ClientID, msg.Topic)
    return isNewer(msg.VectorClock, localClock)
}
```

**2. Batch gossip messages:**
```go
type GossipBatch struct {
    Messages []GossipMessage
    Checksum uint64
}

// Send 100 updates in one gossip message
func (g *GossipCluster) flushBatch() {
    batch := GossipBatch{Messages: g.pendingUpdates}
    g.memberlist.SendReliable(nil, marshal(batch))
}
```

**3. Use anti-entropy for consistency:**
```go
// Periodic sync to detect missed gossip messages
func (g *GossipCluster) runAntiEntropy() {
    ticker := time.NewTicker(1 * time.Minute)
    for range ticker.C {
        peer := g.selectRandomPeer()
        localHash := g.computeStateHash()
        peerHash := g.fetchPeerHash(peer)

        if localHash != peerHash {
            g.syncWithPeer(peer)  // Exchange missing updates
        }
    }
}
```

---

## 2. Consistent Hashing

### What is Consistent Hashing?

Deterministic algorithm that maps clients to nodes without central coordination. Each node knows the same mapping function, so all nodes agree on which node owns a client.

### How It Works

```
Hash Ring:
  0° ─────────────────────────────── Node 1 (90°)
  │                                     │
  │                                     │
Node 4 (270°)                      Node 2 (180°)
  │                                     │
  │                                     │
  └─────────────────────────────── Node 3 (225°)

Client "abc123":
  hash("abc123") = 195°
  → Routes to Node 3 (next node clockwise)

When Node 3 fails:
  hash("abc123") = 195°
  → Routes to Node 4 (next available node)
```

### Implementation Example

```go
// cluster/consistent_hash.go

import "github.com/stathat/consistent"

type ConsistentHashCluster struct {
    nodeID    string
    hashRing  *consistent.Consistent
    transport *Transport  // gRPC for cross-node messaging
}

func NewConsistentHashCluster(nodeID string, peers []string) *ConsistentHashCluster {
    ring := consistent.New()

    // Add all nodes to hash ring
    for _, peer := range peers {
        ring.Add(peer)
    }

    return &ConsistentHashCluster{
        nodeID:   nodeID,
        hashRing: ring,
    }
}

// GetSessionOwner deterministically determines which node owns a session
func (c *ConsistentHashCluster) GetSessionOwner(clientID string) (string, error) {
    return c.hashRing.Get(clientID)
}

// Connect handles client connection
func (c *ConsistentHashCluster) Connect(clientID string) error {
    owner, err := c.GetSessionOwner(clientID)
    if err != nil {
        return err
    }

    if owner != c.nodeID {
        // Client connected to wrong node, redirect or proxy
        return c.transport.ProxyConnect(owner, clientID)
    }

    // This node owns the client, handle locally
    return nil
}

// Subscribe routes subscription to owner node
func (c *ConsistentHashCluster) Subscribe(clientID, topic string) error {
    owner, _ := c.GetSessionOwner(clientID)

    if owner == c.nodeID {
        // Local subscription
        return c.localStore.AddSubscription(clientID, topic)
    } else {
        // Forward to owner node
        return c.transport.SendSubscribe(owner, clientID, topic)
    }
}

// Publish routes message to subscribers
func (c *ConsistentHashCluster) Publish(topic string, payload []byte) error {
    // Find all nodes that might have subscribers
    // (In practice, you'd maintain a topic → nodes index)

    for _, node := range c.hashRing.Members() {
        if node == c.nodeID {
            c.localPublish(topic, payload)
        } else {
            c.transport.SendPublish(node, topic, payload)
        }
    }
    return nil
}
```

### Advantages
- ✅ **No coordination needed** (deterministic)
- ✅ **Simple implementation**
- ✅ **Linear scalability** (add nodes without rebalancing all data)
- ✅ **Very high availability** (no leader, no consensus)
- ✅ **Predictable performance**

### Disadvantages
- ❌ **No state replication** (single point of failure per client)
- ❌ **Requires client redirection** (clients may connect to wrong node)
- ❌ **Uneven load distribution** (without virtual nodes)
- ❌ **No session persistence** (client lost if node fails)

### When to Use Consistent Hashing
- ✅ Stateless or ephemeral sessions acceptable
- ✅ Client redirection is feasible
- ✅ Very high availability critical
- ✅ 50M+ client scale

### Combining with Replication

```go
type ReplicatedConsistentHash struct {
    hashRing      *consistent.Consistent
    replicationFactor int  // 3 = store on 3 nodes
}

func (r *ReplicatedConsistentHash) GetSessionOwners(clientID string) []string {
    var owners []string

    // Get primary owner
    primary, _ := r.hashRing.Get(clientID)
    owners = append(owners, primary)

    // Get N-1 replicas (next nodes on ring)
    for i := 1; i < r.replicationFactor; i++ {
        replica, _ := r.hashRing.GetN(clientID, i)
        owners = append(owners, replica)
    }

    return owners
}
```

---

## 3. CRDTs (Conflict-free Replicated Data Types)

### What are CRDTs?

Data structures designed for eventual consistency without coordination. Guarantee that concurrent updates always converge to the same state.

### CRDT Types for MQTT

**1. OR-Set (Observed-Remove Set) for Subscriptions:**
```go
type ORSet struct {
    adds    map[string]map[string]bool  // element → {unique-id → true}
    removes map[string]map[string]bool  // element → {unique-id → true}
}

func (s *ORSet) Add(element string, uniqueID string) {
    if s.adds[element] == nil {
        s.adds[element] = make(map[string]bool)
    }
    s.adds[element][uniqueID] = true
}

func (s *ORSet) Remove(element string, uniqueID string) {
    if s.removes[element] == nil {
        s.removes[element] = make(map[string]bool)
    }
    s.removes[element][uniqueID] = true
}

func (s *ORSet) Contains(element string) bool {
    adds := s.adds[element]
    removes := s.removes[element]

    // Element is present if there are adds not in removes
    for addID := range adds {
        if !removes[addID] {
            return true
        }
    }
    return false
}

// Merge two OR-Sets (commutative, associative, idempotent)
func (s *ORSet) Merge(other *ORSet) {
    for element, addIDs := range other.adds {
        for id := range addIDs {
            s.Add(element, id)
        }
    }
    for element, removeIDs := range other.removes {
        for id := range removeIDs {
            s.Remove(element, id)
        }
    }
}
```

**2. LWW-Register (Last-Write-Wins) for Session Ownership:**
```go
type LWWRegister struct {
    value     string
    timestamp time.Time
    nodeID    string  // Tiebreaker
}

func (r *LWWRegister) Set(value string, ts time.Time, nodeID string) {
    if ts.After(r.timestamp) || (ts.Equal(r.timestamp) && nodeID > r.nodeID) {
        r.value = value
        r.timestamp = ts
        r.nodeID = nodeID
    }
}

func (r *LWWRegister) Merge(other *LWWRegister) {
    r.Set(other.value, other.timestamp, other.nodeID)
}
```

### CRDT-Based MQTT Cluster

```go
type CRDTCluster struct {
    nodeID string

    // CRDTs for different state types
    subscriptions map[string]*ORSet           // clientID → subscription set
    sessions      map[string]*LWWRegister     // clientID → session owner
    retained      map[string]*LWWRegister     // topic → retained message

    // Gossip for CRDT propagation
    gossip *memberlist.Memberlist
}

func (c *CRDTCluster) Subscribe(clientID, topic string) error {
    uniqueID := fmt.Sprintf("%s-%d", c.nodeID, time.Now().UnixNano())

    if c.subscriptions[clientID] == nil {
        c.subscriptions[clientID] = NewORSet()
    }

    c.subscriptions[clientID].Add(topic, uniqueID)

    // Propagate CRDT update via gossip
    c.gossip.SendReliable(nil, marshalCRDTUpdate("sub_add", clientID, topic, uniqueID))

    return nil
}

func (c *CRDTCluster) Unsubscribe(clientID, topic string) error {
    set := c.subscriptions[clientID]
    if set == nil {
        return nil
    }

    // Get all add IDs for this topic
    for uniqueID := range set.adds[topic] {
        set.Remove(topic, uniqueID)

        // Propagate remove
        c.gossip.SendReliable(nil, marshalCRDTUpdate("sub_remove", clientID, topic, uniqueID))
    }

    return nil
}
```

### Advantages
- ✅ **Strong eventual consistency** (guaranteed convergence)
- ✅ **No coordination** (each node can update independently)
- ✅ **Automatic conflict resolution** (built into CRDT)
- ✅ **High availability**

### Disadvantages
- ❌ **Memory overhead** (must store metadata)
- ❌ **Complexity** (requires understanding CRDT semantics)
- ❌ **Garbage collection needed** (removed items accumulate)
- ❌ **Limited operations** (not all operations have CRDT equivalents)

### When to Use CRDTs
- ✅ Collaborative/concurrent updates common
- ✅ Eventual consistency acceptable
- ✅ Automatic conflict resolution desired
- ✅ Medium scale (1M-10M clients)

---

## 4. Hybrid Approach: Gossip + etcd (Recommended for 10M)

Combine gossip protocol for subscriptions with etcd for critical state.

### Architecture

```
┌─────────────────────────────────────────────────────┐
│              Hybrid Cluster Architecture            │
│                                                      │
│  ┌──────────────────────────────────────────────┐  │
│  │   etcd (Strong Consistency, CP)              │  │
│  │   • Session ownership (critical)             │  │
│  │   • Will messages (need durability)          │  │
│  │   • Cluster membership                       │  │
│  └──────────────────────────────────────────────┘  │
│                                                      │
│  ┌──────────────────────────────────────────────┐  │
│  │   Gossip (Eventual Consistency, AP)          │  │
│  │   • Subscriptions (ephemeral)                │  │
│  │   • Retained message metadata                │  │
│  │   • Topic routing index                      │  │
│  └──────────────────────────────────────────────┘  │
│                                                      │
│  ┌──────────────────────────────────────────────┐  │
│  │   Local BadgerDB (Per-node storage)          │  │
│  │   • Message payloads                         │  │
│  │   • Session state cache                      │  │
│  │   • Subscription cache                       │  │
│  └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Decision Matrix

| State Type | Storage | Why |
|------------|---------|-----|
| **Session ownership** | etcd | Need strong consistency to prevent split-brain |
| **Subscriptions** | Gossip | High write rate, eventual consistency OK |
| **Retained metadata** | Gossip | Can tolerate brief inconsistency |
| **Retained payloads** | BadgerDB | Too large for etcd/gossip |
| **Will messages** | etcd | Need durability, infrequent writes |

### Implementation

```go
type HybridCluster struct {
    nodeID string

    // Strong consistency for critical state
    etcd *EtcdCluster

    // Eventual consistency for high-throughput state
    gossip *GossipCluster

    // Local storage
    localDB *badger.DB
}

func (h *HybridCluster) Connect(clientID string) error {
    // Session ownership: use etcd (strong consistency)
    return h.etcd.SetSessionOwner(clientID, h.nodeID)
}

func (h *HybridCluster) Subscribe(clientID, topic string) error {
    // Subscriptions: use gossip (eventual consistency, high throughput)
    return h.gossip.Subscribe(clientID, topic)
}

func (h *HybridCluster) SetWillMessage(clientID string, will *WillMessage) error {
    // Will messages: use etcd (need durability)
    return h.etcd.SetWillMessage(clientID, will)
}

func (h *HybridCluster) SetRetained(topic string, message *Message) error {
    // Metadata: gossip (for routing)
    h.gossip.BroadcastRetainedMetadata(topic, message.QoS, len(message.Payload))

    // Payload: local BadgerDB
    return h.localDB.Update(func(txn *badger.Txn) error {
        key := []byte("retained:" + topic)
        return txn.Set(key, message.Payload)
    })
}
```

### Advantages
- ✅ **Best of both worlds** (strong consistency where needed, high throughput elsewhere)
- ✅ **Scales to 10M+ clients**
- ✅ **Lower etcd load** (70% reduction)
- ✅ **Lower latency** (gossip propagation <100ms)

### Disadvantages
- ❌ **Increased complexity** (two coordination systems)
- ❌ **Operational overhead** (manage both etcd and gossip)

---

## 5. Redis Cluster

Use Redis as distributed cache and coordination layer.

### Architecture

```
┌─────────────────────────────────────────────────┐
│              Redis Cluster                      │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐      │
│  │ Master 1 │  │ Master 2 │  │ Master 3 │      │
│  │ (slots   │  │ (slots   │  │ (slots   │      │
│  │  0-5461) │  │ 5462-    │  │ 10923-   │      │
│  └────┬─────┘  │ 10922)   │  │ 16383)   │      │
│       │        └────┬─────┘  └────┬─────┘      │
│  ┌────▼─────┐  ┌───▼──────┐  ┌───▼──────┐      │
│  │ Replica  │  │ Replica  │  │ Replica  │      │
│  └──────────┘  └──────────┘  └──────────┘      │
└─────────────────────────────────────────────────┘
```

### Implementation

```go
import "github.com/go-redis/redis/v8"

type RedisCluster struct {
    client *redis.ClusterClient
}

func NewRedisCluster(addrs []string) *RedisCluster {
    return &RedisCluster{
        client: redis.NewClusterClient(&redis.ClusterOptions{
            Addrs: addrs,
        }),
    }
}

func (r *RedisCluster) SetSessionOwner(clientID, nodeID string) error {
    key := "session:" + clientID
    return r.client.Set(ctx, key, nodeID, 0).Err()
}

func (r *RedisCluster) Subscribe(clientID, topic string) error {
    key := "subs:" + clientID
    return r.client.SAdd(ctx, key, topic).Err()
}

func (r *RedisCluster) GetSubscriptions(clientID string) ([]string, error) {
    key := "subs:" + clientID
    return r.client.SMembers(ctx, key).Result()
}

// Use Redis Pub/Sub for gossip-like propagation
func (r *RedisCluster) PublishSubscriptionChange(clientID, topic string) error {
    channel := "sub_changes"
    payload := fmt.Sprintf("%s:%s", clientID, topic)
    return r.client.Publish(ctx, channel, payload).Err()
}
```

### Advantages
- ✅ **Quick to implement** (proven solution)
- ✅ **High performance** (in-memory)
- ✅ **Rich data structures** (sets, sorted sets, etc.)
- ✅ **Built-in replication**

### Disadvantages
- ❌ **External dependency** (not embedded)
- ❌ **Eventual consistency** (not CP)
- ❌ **Memory-only** (optional persistence)
- ❌ **Cluster resharding complexity**

---

## 6. Event Sourcing with Kafka

Store all events (subscribe, unsubscribe, connect, etc.) in Kafka, rebuild state from log.

### Architecture

```
┌──────────────────────────────────────────────────┐
│                 Kafka Cluster                    │
│                                                   │
│  Topic: mqtt_events (partitioned by clientID)    │
│  ┌─────────────────────────────────────────┐    │
│  │ Partition 0  Partition 1  Partition 2   │    │
│  │ [Events...]  [Events...]  [Events...]   │    │
│  └─────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │   MQTT Broker Nodes    │
         │                        │
         │  Each node:            │
         │  • Consumes events     │
         │  • Rebuilds state      │
         │  • Publishes new events│
         └────────────────────────┘
```

### Advantages
- ✅ **Full audit trail** (every event logged)
- ✅ **Replay capability** (rebuild from any point)
- ✅ **Scalable** (Kafka handles millions of events/sec)
- ✅ **Decoupled** (brokers are stateless consumers)

### Disadvantages
- ❌ **Very complex** (event sourcing is hard)
- ❌ **High latency** (Kafka round-trip)
- ❌ **External dependency**
- ❌ **Operational overhead**

---

## 7. No Coordination (Local-Only)

Each broker operates independently, no cluster coordination.

### When to Use
- Small deployments (<10K clients)
- Sticky load balancing (clients always connect to same broker)
- No cross-broker messaging needed

### Implementation
```go
type LocalCluster struct {
    localSessions map[string]*Session
    localSubs     map[string][]Subscription
}

// Everything is local, no network calls
func (l *LocalCluster) Subscribe(clientID, topic string) error {
    l.localSubs[clientID] = append(l.localSubs[clientID], Subscription{Filter: topic})
    return nil
}
```

### Advantages
- ✅ **Simplest possible**
- ✅ **Highest performance**
- ✅ **No network overhead**

### Disadvantages
- ❌ **No high availability**
- ❌ **No load balancing**
- ❌ **No cross-broker messaging**

---

## Recommendation Matrix

| Scale | Requirement | Recommended Approach | Effort | Reason |
|-------|-------------|---------------------|--------|--------|
| **<1M clients** | Simple | No Coordination | 0 weeks | Already implemented |
| **1M-5M clients** | Basic HA | etcd (current) | 0 weeks | Current architecture sufficient |
| **5M-10M clients** | High throughput | **Hybrid (Gossip + etcd)** ⭐ | **6 weeks** | Best balance |
| **10M-20M clients** | Very high throughput | Gossip only | 4 weeks | Accept eventual consistency |
| **20M-50M clients** | Strong consistency | Custom Raft | 20 weeks | Worth the investment |
| **50M+ clients** | Massive scale | Consistent Hashing + Gossip | 12 weeks | Regional partitioning |

⭐ = **Recommended for 10M client target**

---

## Implementation Recommendation

For **10M clients on 20 nodes**, use **Hybrid (Gossip + etcd)**:

### What to Move to Gossip
1. ✅ Subscriptions (100M records → 50GB saved from etcd)
2. ✅ Retained message metadata (routing index)
3. ✅ Topic routing cache

### What to Keep in etcd
1. ✅ Session ownership (need strong consistency)
2. ✅ Will messages (need durability)
3. ✅ Cluster membership

### Timeline
- **Week 1-2**: Integrate hashicorp/memberlist
- **Week 3-4**: Move subscriptions to gossip
- **Week 5**: Move retained metadata to gossip
- **Week 6**: Testing, validation, production rollout

### Expected Results
- 70% reduction in etcd storage (50GB → 2GB)
- 200x improvement in subscription throughput (5K → 1M ops/sec)
- <100ms subscription propagation (P95)
- 10M+ client capacity

---

## Conclusion

**For 10M clients:**
- **Don't use:** Custom Raft (overkill, 20 weeks)
- **Don't use:** Event Sourcing (too complex)
- **Maybe use:** Consistent Hashing (if session loss acceptable)
- **Recommended:** **Hybrid (Gossip + etcd)** ⭐

**For 50M+ clients:**
- **Use:** Custom Raft or Consistent Hashing + Regional Partitioning
- **Consider:** Building on EMQX Enterprise first, migrate to custom later

The key insight: **Choose the weakest consistency model that satisfies your requirements.** Strong consistency (Raft, Paxos) is expensive. Eventual consistency (Gossip, CRDTs) scales much better.
