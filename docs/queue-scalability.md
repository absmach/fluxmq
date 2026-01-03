# Distributed Queue Scalability Implementation Plan

## Goal
Make MQTT queues scalable to >100K msgs/sec per queue through:
1. **Single-node optimizations** - Lock-free performance improvements
2. **Multi-node distribution** - Partition distribution across cluster nodes
3. **Fault tolerance** - Automatic rebalancing and replication

## Architecture Overview - Configurable Design

The implementation will support **multiple strategies** for key architectural decisions, configurable per queue or globally. This allows users to optimize for their specific requirements (throughput vs consistency, simplicity vs fault tolerance).

### 1. Partition Assignment Strategy (Configurable)

**Default: Static Hash-Based**
- `partitionID % nodeCount` determines owner
- Zero coordination overhead, deterministic routing
- Best for: High throughput, predictable distribution

**Alternative: Dynamic etcd-Based**
- Partitions claimed via etcd leases with rebalancing
- Perfect load balancing, flexible reassignment
- Best for: Uneven workloads, frequent topology changes

**Configuration:**
```go
type QueueConfig struct {
    PartitionStrategy string `json:"partitionStrategy"` // "hash" (default) | "dynamic"
}
```

### 2. Storage and Replication Strategy (Configurable)

**Default: Single-Owner with Async Replication**
- One owner per partition, BadgerDB WAL for durability
- Async replication to N replicas via gRPC streaming
- Fast writes, eventual consistency on failover
- Best for: Maximum throughput (100K+ msgs/sec)

**Alternative A: Synchronous Replication**
- Write to primary + wait for 1 replica ACK
- Strong consistency, ~2x write latency
- Best for: Critical data, zero message loss tolerance

**Alternative B: Raft-Based (Future)**
- Full Raft consensus per partition
- Strongest consistency, highest overhead
- Best for: Financial transactions, strict ordering

**Configuration:**
```go
type QueueConfig struct {
    ReplicationMode   string `json:"replicationMode"`   // "async" (default) | "sync" | "raft"
    ReplicationFactor int    `json:"replicationFactor"` // Default: 2 (1 primary + 1 replica)
    ReplicationAckPolicy string `json:"replicationAckPolicy"` // "none" | "one" | "quorum"
}
```

### 3. Consumer Routing Strategy (Configurable)

**Default: Proxy via Consumer's Node**
- Messages routed to consumer's connected node (ProxyNodeID)
- No consumer reconnection on partition migration
- ~1ms extra hop for cross-node delivery
- Best for: MQTT persistent connections, minimal disruption

**Alternative: Direct to Partition Owner**
- Consumer connects to partition owner node
- Zero proxy overhead, lowest latency
- Consumer reconnects on partition migration
- Best for: Ultra-low latency, short-lived connections

**Configuration:**
```go
type QueueConfig struct {
    RoutingMode string `json:"routingMode"` // "proxy" (default) | "direct"
}
```

### 4. Lock-Free Optimizations (Always Enabled)
- Ring buffer hot path (configurable size: 4K-64K messages)
- Batch processing (configurable: 100-1000 messages)
- Zero-copy message passing
- These are performance optimizations with no trade-offs

### Key Design Patterns
- Leverage existing cluster infrastructure (etcd leases, gRPC mesh, session takeover pattern)
- Maintain storage interface compatibility
- Support both single-node and multi-node deployments
- **Strategy pattern**: All major decisions pluggable via configuration

---

## Implementation Phases

### Phase 1: Single-Node Optimizations (1 week)
**Goal**: Achieve 100K msgs/sec on single node

#### Tasks

1. **Increase ring buffer capacity**
   - File: `queue/storage/memory/lockfree/ringbuffer.go`
   - Change: `DefaultCapacity = 4096` → `16384`
   - Rationale: Reduce BadgerDB reads, trivial memory cost (160KB vs 40KB per 10 partitions)

2. **Tune batch sizes**
   - File: `queue/partition_worker.go`
   - Change: `batchSize = 100` → `500` in `NewPartitionWorker`
   - Rationale: Reduce atomic operations from 100/sec to 20/sec per partition

3. **Implement cross-partition batch writes**
   - File: `queue/manager.go`
   - Add: Buffer enqueues across partitions, batch write to BadgerDB every 10ms
   - Rationale: Reduce syscalls, improve write throughput

4. **Profile and optimize hot path**
   - Remove allocations in `partition_worker.go:deliverMessage`
   - Verify zero-copy message passing via `core.GetBufferWithData`
   - Pool consumer state objects

**Validation**: Benchmark 100K msgs/sec sustained, p99 < 10ms

---

### Phase 2: Partition Distribution Infrastructure (2 weeks)
**Goal**: Distribute partitions across cluster nodes with static assignment

#### etcd Key Structure
```
# Partition ownership (leased, 30s TTL)
/mqtt/queue/{queueName}/partitions/{partitionID}/owner → nodeID
/mqtt/queue/{queueName}/partitions/{partitionID}/replicas → ["node2", "node3"]

# Consumer metadata (persistent)
/mqtt/queue/{queueName}/consumers/{groupID}/{consumerID} → {
    "proxyNodeID": "node2",
    "assignedPartitions": [0, 3, 6],
    "lastHeartbeat": "2024-01-03T12:00:00Z"
}
```

#### Tasks

1. **Add partition ownership APIs to cluster**
   - File: `cluster/etcd.go`
   - Add methods:
     - `AcquirePartition(ctx, queueName, partitionID, nodeID) error`
     - `ReleasePartition(ctx, queueName, partitionID) error`
     - `GetPartitionOwner(ctx, queueName, partitionID) (nodeID string, exists bool, error)`
     - `WatchPartitionOwnership(ctx, queueName) <-chan PartitionOwnershipChange`
   - Implementation: Similar to `AcquireSession` using leases

2. **Implement partition assignment strategies**
   - New file: `queue/partition_assigner.go`
   - Implement both hash and dynamic strategies:
     ```go
     // Hash-based (default)
     type HashPartitionAssigner struct{}

     func (h *HashPartitionAssigner) GetOwner(queueName string, partitionID int, nodes []string) string {
         return nodes[partitionID % len(nodes)]
     }

     // Dynamic etcd-based
     type DynamicPartitionAssigner struct {
         cluster cluster.Cluster
     }

     func (d *DynamicPartitionAssigner) GetOwner(queueName string, partitionID int, nodes []string) string {
         // Query etcd for current owner
         owner, exists := d.cluster.GetPartitionOwner(ctx, queueName, partitionID)
         if exists {
             return owner
         }
         // If unclaimed, claim it
         return d.claimPartition(queueName, partitionID)
     }
     ```

3. **Add partition ownership calculation in queue manager**
   - File: `queue/manager.go`
   - Add method that uses configured strategy:
     ```go
     func (m *Manager) getPartitionOwner(queueName string, partitionID int) string {
         if m.cluster == nil {
             return m.localNodeID  // Single-node mode
         }

         queue, _ := m.GetQueue(queueName)
         config := queue.Config()

         nodes := m.cluster.Nodes()
         assigner := m.getPartitionAssigner(config.PartitionStrategy)
         return assigner.GetOwner(queueName, partitionID, nodes)
     }

     func (m *Manager) getPartitionAssigner(strategy string) PartitionAssigner {
         switch strategy {
         case "dynamic":
             return &DynamicPartitionAssigner{cluster: m.cluster}
         default: // "hash"
             return &HashPartitionAssigner{}
         }
     }
     ```
   - On manager start: Acquire ownership for partitions assigned to this node

3. **Route enqueue to partition owner**
   - File: `queue/manager.go` in `Enqueue` method
   - Add: Check if partition owner is local or remote
   - If remote: `m.cluster.EnqueueRemote(ctx, ownerNode, queueName, partitionID, msg)`
   - Reuse gRPC transport pattern (like `RoutePublish`)

4. **Partition workers check ownership**
   - File: `queue/partition_worker.go` in `ProcessMessages`
   - Add ownership check at start:
     ```go
     owner := pw.queue.manager.getPartitionOwner(pw.queueName, pw.partitionID)
     if owner != pw.localNodeID {
         return  // Skip processing, not our partition
     }
     ```

5. **Add gRPC RPCs for queue distribution**
   - File: `cluster/grpc/broker.proto` (or create `cluster/grpc/queue.proto`)
   - Add:
     ```protobuf
     rpc EnqueueRemote(EnqueueRemoteRequest) returns (EnqueueRemoteResponse);
     rpc TransferPartition(TransferPartitionRequest) returns (TransferPartitionResponse);
     ```
   - File: `cluster/transport.go`
   - Implement handlers calling into queue manager

6. **Implement consumer routing strategies**
   - New file: `queue/routing_strategy.go`
   - Implement both proxy and direct routing:
     ```go
     // Proxy routing (default) - route via consumer's connected node
     type ProxyRoutingStrategy struct {
         cluster   cluster.Cluster
         localNode string
         deliverFn DeliverFn
     }

     func (p *ProxyRoutingStrategy) Route(ctx context.Context, msg *Message, consumer *Consumer) error {
         if consumer.ProxyNodeID != "" && consumer.ProxyNodeID != p.localNode {
             // Cross-node delivery via gRPC
             return p.cluster.RouteQueueMessage(ctx, consumer.ProxyNodeID, consumer.ClientID, msg)
         }
         // Local delivery
         return p.deliverFn(ctx, consumer.ClientID, msg)
     }

     // Direct routing - consumer connects to partition owner
     type DirectRoutingStrategy struct {
         cluster   cluster.Cluster
         localNode string
         deliverFn DeliverFn
     }

     func (d *DirectRoutingStrategy) Route(ctx context.Context, msg *Message, consumer *Consumer) error {
         // Always deliver locally (consumer must be on partition owner node)
         return d.deliverFn(ctx, consumer.ClientID, msg)
     }
     ```

7. **Update partition worker to use routing strategy**
   - File: `queue/partition_worker.go` in `deliverMessage`
   - Use configured routing strategy:
     ```go
     func (pw *PartitionWorker) deliverMessage(ctx context.Context, msg *Message, consumer *Consumer) error {
         // ... (ordering checks, inflight marking, etc.)

         // Use configured routing strategy
         routingStrategy := pw.getRoutingStrategy()
         if err := routingStrategy.Route(ctx, msg, consumer); err != nil {
             return fmt.Errorf("failed to route message: %w", err)
         }

         // Mark delivered in ordering enforcer
         if pw.queue.OrderingEnforcer() != nil {
             pw.queue.OrderingEnforcer().MarkDelivered(msg)
         }
         return nil
     }

     func (pw *PartitionWorker) getRoutingStrategy() RoutingStrategy {
         config := pw.queue.Config()
         switch config.RoutingMode {
         case "direct":
             return &DirectRoutingStrategy{
                 cluster:   pw.cluster,
                 localNode: pw.localNodeID,
                 deliverFn: pw.deliverFn,
             }
         default: // "proxy"
             return &ProxyRoutingStrategy{
                 cluster:   pw.cluster,
                 localNode: pw.localNodeID,
                 deliverFn: pw.deliverFn,
             }
         }
     }
     ```

**Validation**:
- 3-node cluster with 30 partitions (10 per node)
- Enqueue routes to correct owner
- Consumer delivery works cross-node
- Throughput: 150K msgs/sec across 3 nodes

---

### Phase 3: Rebalancing and Fault Tolerance (2 weeks)
**Goal**: Automatic partition migration on topology changes, message replication

#### Tasks

1. **Create rebalancer coordinator**
   - New file: `queue/rebalancer.go`
   - Responsibilities:
     - Watch etcd node membership changes
     - Calculate new partition→node mapping on topology change
     - Orchestrate partition migrations
   - Algorithm:
     ```go
     func (r *Rebalancer) onTopologyChange(nodes []string) {
         for queueName, queue := range r.queues {
             for partitionID := 0; partitionID < queue.Config().Partitions; partitionID++ {
                 newOwner := nodes[partitionID % len(nodes)]
                 currentOwner := r.getPartitionOwner(queueName, partitionID)

                 if newOwner != currentOwner {
                     r.migratePartition(queueName, partitionID, currentOwner, newOwner)
                 }
             }
         }
     }
     ```

2. **Implement partition snapshot/restore**
   - File: `queue/storage/hybrid/store.go` (or `queue/storage/badger/badger.go`)
   - Add methods:
     - `ExportPartition(queueName, partitionID, startSeq, limit) ([]*Message, error)`
     - `ImportPartition(queueName, partitionID, messages) error`
   - Export: Read from BadgerDB using partition prefix key range
   - Import: Batch write messages preserving sequence numbers

3. **Complete TransferPartition RPC**
   - File: `cluster/transport.go`
   - Server handler:
     ```go
     func (t *Transport) TransferPartition(ctx, req *pb.TransferPartitionRequest) (*pb.TransferPartitionResponse, error) {
         // 1. Pause partition worker
         // 2. Flush ring buffer to BadgerDB
         // 3. Export partition snapshot (last 10K messages)
         // 4. Return snapshot + inflight IDs
     }
     ```
   - Client caller:
     ```go
     func (r *Rebalancer) migratePartition(queueName, partitionID, fromNode, toNode) {
         // 1. Call TransferPartition RPC to fromNode
         snapshot := transport.SendTransferPartition(ctx, fromNode, queueName, partitionID)

         // 2. Import snapshot to toNode's storage
         storage.ImportPartition(queueName, partitionID, snapshot.Messages)

         // 3. Update etcd ownership
         cluster.AcquirePartition(ctx, queueName, partitionID, toNode)

         // 4. Start partition worker on toNode
         // 5. Stop partition worker on fromNode
     }
     ```

4. **Implement replication strategies (async, sync, raft)**
   - New file: `queue/replication_strategy.go`
   - Implement all three replication modes:
     ```go
     // Async replication (default) - fire-and-forget
     type AsyncReplicationStrategy struct {
         transport     cluster.Transport
         batchInterval time.Duration
         pendingBatches map[int][]*Message  // partitionID -> messages
         mu            sync.Mutex
     }

     func (a *AsyncReplicationStrategy) Replicate(ctx context.Context, msg *Message, replicas []string) error {
         // Add to batch (non-blocking)
         a.mu.Lock()
         a.pendingBatches[msg.PartitionID] = append(a.pendingBatches[msg.PartitionID], msg)
         a.mu.Unlock()
         return nil  // Don't wait
     }

     func (a *AsyncReplicationStrategy) Start() {
         ticker := time.NewTicker(a.batchInterval)  // 10ms default
         for range ticker.C {
             a.flushBatches()
         }
     }

     func (a *AsyncReplicationStrategy) flushBatches() {
         a.mu.Lock()
         batches := a.pendingBatches
         a.pendingBatches = make(map[int][]*Message)
         a.mu.Unlock()

         for partitionID, batch := range batches {
             replicas := a.getReplicaNodes(partitionID)
             for _, replica := range replicas {
                 a.transport.SendReplicateMessages(ctx, replica, batch)
             }
         }
     }

     // Sync replication - wait for ACKs
     type SyncReplicationStrategy struct {
         transport cluster.Transport
         ackPolicy string  // "one" or "quorum"
         ackWaiters map[string]chan error  // msgID -> ack channel
         mu        sync.RWMutex
     }

     func (s *SyncReplicationStrategy) Replicate(ctx context.Context, msg *Message, replicas []string) error {
         // Create ack waiter
         ackCh := make(chan error, len(replicas))
         s.mu.Lock()
         s.ackWaiters[msg.ID] = ackCh
         s.mu.Unlock()

         // Send to replicas
         for _, replica := range replicas {
             go func(node string) {
                 err := s.transport.SendReplicateMessages(ctx, node, []*Message{msg})
                 ackCh <- err
             }(replica)
         }

         // Wait for ACKs based on policy
         requiredAcks := 1  // "one"
         if s.ackPolicy == "quorum" {
             requiredAcks = (len(replicas) / 2) + 1
         }

         successCount := 0
         for i := 0; i < len(replicas); i++ {
             if err := <-ackCh; err == nil {
                 successCount++
                 if successCount >= requiredAcks {
                     return nil  // Success
                 }
             }
         }
         return fmt.Errorf("failed to achieve %s replication", s.ackPolicy)
     }

     // Raft replication (future) - full consensus
     type RaftReplicationStrategy struct {
         raftGroups map[int]*raft.Raft  // partitionID -> Raft instance
     }

     func (r *RaftReplicationStrategy) Replicate(ctx context.Context, msg *Message, replicas []string) error {
         raftGroup := r.raftGroups[msg.PartitionID]
         // Append to Raft log (blocks until committed)
         return raftGroup.Apply(msg, 5*time.Second).Error()
     }
     ```

5. **Integrate replication into enqueue path**
   - New file: `queue/replication.go` (manager component)
   - Component: `ReplicationManager`
   - Integration:
     ```go
     // In queue/manager.go Enqueue() method, after storage write:
     func (m *Manager) Enqueue(ctx context.Context, queueTopic string, payload []byte, props map[string]string) error {
         // ... (existing enqueue logic)

         // Store locally
         err = m.messageStore.Enqueue(ctx, queueTopic, msg)
         if err != nil {
             return err
         }

         // Replicate based on configured strategy
         if m.cluster != nil {
             queue, _ := m.GetQueue(queueTopic)
             config := queue.Config()

             replicationStrategy := m.getReplicationStrategy(config)
             replicas := m.getReplicaNodes(queueTopic, partitionID, config.ReplicationFactor)

             if err := replicationStrategy.Replicate(ctx, msg, replicas); err != nil {
                 // Log but don't fail the enqueue (depends on mode)
                 if config.ReplicationMode == "sync" {
                     return fmt.Errorf("replication failed: %w", err)
                 }
             }
         }

         // ... (notify partition worker)
         return nil
     }

     func (m *Manager) getReplicationStrategy(config QueueConfig) ReplicationStrategy {
         switch config.ReplicationMode {
         case "sync":
             return &SyncReplicationStrategy{
                 transport: m.cluster.Transport(),
                 ackPolicy: config.ReplicationAckPolicy,
             }
         case "raft":
             return &RaftReplicationStrategy{
                 raftGroups: m.raftGroups,
             }
         default: // "async"
             return m.asyncReplication  // Singleton, started at manager init
         }
     }

     func (m *Manager) getReplicaNodes(queueName string, partitionID int, replicationFactor int) []string {
         nodes := m.cluster.Nodes()
         owner := m.getPartitionOwner(queueName, partitionID)

         // Simple strategy: next N nodes in ring
         replicas := []string{}
         ownerIdx := -1
         for i, node := range nodes {
             if node.ID == owner {
                 ownerIdx = i
                 break
             }
         }

         for i := 1; i < replicationFactor; i++ {
             replicaIdx := (ownerIdx + i) % len(nodes)
             replicas = append(replicas, nodes[replicaIdx].ID)
         }
         return replicas
     }
     ```
   - Storage: Replica nodes write to replica keyspace `queue:replica:msg:{queueName}:{partitionID}:{sequence}`

5. **Node failure detection and recovery**
   - File: `queue/manager.go`
   - Add watch on partition ownership lease expiry
   - On expiry: Trigger rebalancer to reassign orphaned partitions
   - Replica promotion: Read from replica storage, replay to ring buffer

**Validation**:
- Kill node during 50K msgs/sec load
- Partitions migrate within 5 seconds
- Message loss < 50 messages (async replication window)
- Consumer delivery continues (via ProxyNodeID routing)

---

## Critical Files to Modify

### Existing Files

1. **`queue/storage/memory/lockfree/ringbuffer.go`**
   - Change: Increase `DefaultCapacity` to 16384
   - Impact: Memory increase, fewer BadgerDB reads

2. **`queue/partition_worker.go`**
   - Change: Add ownership check, increase batch size, route via ProxyNodeID
   - Impact: Core delivery logic

3. **`queue/manager.go`**
   - Change: Add cluster awareness, partition ownership calculation, remote routing
   - Impact: Central coordination point

4. **`cluster/etcd.go`**
   - Change: Add partition ownership APIs (acquire, release, watch)
   - Impact: New coordination primitives

5. **`cluster/transport.go`**
   - Change: Add queue distribution RPC handlers
   - Impact: Inter-node communication

6. **`queue/storage/badger/badger.go`** or **`queue/storage/hybrid/store.go`**
   - Change: Add partition export/import for migration
   - Impact: Snapshot functionality

### New Files

1. **`queue/partition_assigner.go`**
   - Purpose: Partition assignment strategies (hash, dynamic)
   - Complexity: ~200 lines
   - Exports: `PartitionAssigner` interface, `HashPartitionAssigner`, `DynamicPartitionAssigner`

2. **`queue/replication_strategy.go`**
   - Purpose: Message replication strategies (async, sync, raft)
   - Complexity: ~300 lines
   - Exports: `ReplicationStrategy` interface, `AsyncReplicationStrategy`, `SyncReplicationStrategy`, `RaftReplicationStrategy`

3. **`queue/routing_strategy.go`**
   - Purpose: Consumer routing strategies (proxy, direct)
   - Complexity: ~150 lines
   - Exports: `RoutingStrategy` interface, `ProxyRoutingStrategy`, `DirectRoutingStrategy`

4. **`queue/rebalancer.go`**
   - Purpose: Partition rebalancing coordinator
   - Complexity: ~300 lines
   - Exports: `Rebalancer`, watches topology changes and orchestrates migrations

5. **`queue/replication.go`**
   - Purpose: Replication manager component
   - Complexity: ~200 lines
   - Exports: `ReplicationManager`, coordinates strategy selection and replica node selection

6. **`cluster/grpc/queue.proto`** (optional, can extend `broker.proto`)
   - Purpose: Queue distribution RPC definitions
   - Complexity: ~80 lines
   - Messages: `EnqueueRemoteRequest`, `TransferPartitionRequest`, `ReplicateMessagesRequest`

---

## Testing Strategy

### Benchmarks (Phase 1)
- `queue/enqueue_bench_test.go`: Measure enqueue throughput with new ring buffer size
- Target: 100K ops/sec

### Integration Tests (Phase 2)
- `queue/cluster_integration_test.go`: 3-node cluster, cross-node enqueue and delivery
- Validate partition ownership, consumer routing

### Load Tests (Phase 3)
- `queue/failover_test.go`: Kill node during sustained load, measure recovery time
- Target: <5s recovery, <50 messages lost

### Existing Test Compatibility
- Ensure all existing queue tests pass (single-node mode)
- Add cluster config flag to toggle distributed mode

---

## Configuration

### Complete Queue Config with All Options
```go
type QueueConfig struct {
    // Existing fields...
    Name       string
    Partitions int
    Ordering   OrderingMode
    // ... (other existing fields)

    // NEW: Distribution and replication strategy
    PartitionStrategy    string        `json:"partitionStrategy"`    // "hash" (default) | "dynamic"
    ReplicationMode      string        `json:"replicationMode"`      // "async" (default) | "sync" | "raft"
    ReplicationFactor    int           `json:"replicationFactor"`    // Default: 2
    ReplicationAckPolicy string        `json:"replicationAckPolicy"` // "none" | "one" | "quorum"
    ReplicationLag       time.Duration `json:"replicationLag"`       // Default: 10ms (for async)

    // NEW: Consumer routing
    RoutingMode string `json:"routingMode"` // "proxy" (default) | "direct"

    // NEW: Performance tuning
    RingBufferSize int `json:"ringBufferSize"` // Default: 16384
    BatchSize      int `json:"batchSize"`      // Default: 500
}
```

### Manager Config
```go
type Config struct {
    QueueStore    storage.QueueStore
    MessageStore  storage.MessageStore
    ConsumerStore storage.ConsumerStore
    DeliverFn     DeliverFn

    // NEW: Cluster support
    Cluster       cluster.Cluster  // nil for single-node mode
    LocalNodeID   string
}
```

### Strategy Interfaces (Strategy Pattern)

```go
// Partition assignment strategy
type PartitionAssigner interface {
    GetOwner(queueName string, partitionID int, nodes []string) string
    OnTopologyChange(queueName string, oldNodes, newNodes []string) []PartitionMigration
}

// Implementations:
type HashPartitionAssigner struct{}      // partitionID % len(nodes)
type DynamicPartitionAssigner struct{}   // etcd-based with rebalancing

// Replication strategy
type ReplicationStrategy interface {
    Replicate(ctx context.Context, msg *Message, replicas []string) error
    WaitForAcks(ctx context.Context, msgID string) error
}

// Implementations:
type AsyncReplication struct{}   // Fire-and-forget streaming
type SyncReplication struct{}    // Wait for N acks
type RaftReplication struct{}    // Raft consensus (future)

// Routing strategy
type RoutingStrategy interface {
    Route(ctx context.Context, partitionID int, consumer *Consumer, msg *Message) error
}

// Implementations:
type ProxyRouting struct{}   // Via consumer's ProxyNodeID
type DirectRouting struct{}  // Direct to partition owner
```

### Configuration Helpers

```go
// Default configurations for common use cases
func DefaultHighThroughputConfig(name string) QueueConfig {
    return QueueConfig{
        Name:                 name,
        Partitions:           30,
        PartitionStrategy:    "hash",
        ReplicationMode:      "async",
        ReplicationFactor:    2,
        ReplicationAckPolicy: "none",
        RoutingMode:          "proxy",
        RingBufferSize:       16384,
        BatchSize:            500,
    }
}

func DefaultHighConsistencyConfig(name string) QueueConfig {
    return QueueConfig{
        Name:                 name,
        Partitions:           10,
        PartitionStrategy:    "dynamic",
        ReplicationMode:      "sync",
        ReplicationFactor:    3,
        ReplicationAckPolicy: "quorum",
        RoutingMode:          "direct",
        RingBufferSize:       8192,
        BatchSize:            100,
    }
}

func DefaultBalancedConfig(name string) QueueConfig {
    return QueueConfig{
        Name:                 name,
        Partitions:           20,
        PartitionStrategy:    "hash",
        ReplicationMode:      "sync",
        ReplicationFactor:    2,
        ReplicationAckPolicy: "one",
        RoutingMode:          "proxy",
        RingBufferSize:       16384,
        BatchSize:            300,
    }
}
```

---

## Rollout Plan

### Week 1: Phase 1 Implementation
- Implement single-node optimizations
- Run benchmarks, validate 100K msgs/sec
- Deploy to staging for performance testing

### Week 2-3: Phase 2 Implementation
- Add partition ownership to etcd
- Implement cross-node routing
- Test 3-node cluster in staging

### Week 4-5: Phase 3 Implementation
- Build rebalancer and replication
- Failover testing in staging
- Load testing at scale

### Week 6: Production Rollout
- Deploy Phase 1 optimizations to production
- Monitor single-node performance gains

### Week 7-8: Gradual Cluster Rollout
- Enable distributed mode for non-critical queues
- Monitor cross-node routing and latency
- Gradually migrate critical queues

---

## Performance Targets

### Phase 1: Single Node
- Throughput: 100-120K msgs/sec
- Latency: p50=1ms, p99=10ms
- Memory: 200MB for 10 partitions

### Phase 2: 3-Node Cluster
- Throughput: 300K+ msgs/sec total (100K per node)
- Latency: p50=2ms, p99=15ms (includes cross-node routing)

### Phase 3: With Replication
- Throughput: 250-300K msgs/sec (replication overhead)
- Latency: p50=3ms, p99=20ms
- Recovery time: <5s on node failure
- Message loss: <50 messages (async replication window)

---

## Strategy Trade-offs Summary

### Partition Assignment Strategy

| Strategy | Pros | Cons | Best For |
|----------|------|------|----------|
| **Hash** (default) | Zero coordination overhead, deterministic, simple | Uneven if partitions % nodes != 0 | High throughput, stable topology |
| **Dynamic** | Perfect load balancing, flexible | etcd coordination overhead, complexity | Uneven workloads, frequent changes |

### Replication Strategy

| Strategy | Throughput | Latency | Consistency | Message Loss on Failure |
|----------|-----------|---------|-------------|-------------------------|
| **Async** (default) | Highest | Lowest | Eventual | 10-50ms window (~10-500 msgs @ 100K/sec) |
| **Sync (one)** | Medium | 2x | Strong | Zero (after ACK) |
| **Sync (quorum)** | Lower | 3-4x | Strongest | Zero (after quorum) |
| **Raft** (future) | Lowest | Highest | Linearizable | Zero (always) |

### Routing Strategy

| Strategy | Latency | Consumer Impact | Partition Migration Impact |
|----------|---------|-----------------|---------------------------|
| **Proxy** (default) | +1ms (cross-node) | No reconnection | Transparent to consumer |
| **Direct** | Lowest | No extra hop | Consumer must reconnect |

---

## Configuration Decision Guide

**Choose Hash + Async + Proxy if:**
- Maximum throughput is priority (100K+ msgs/sec target)
- Can tolerate ~10-50ms message loss on node failure
- Want stable consumer connections
- Predictable, static cluster topology

**Choose Dynamic + Sync + Direct if:**
- Zero message loss is critical
- Workload varies significantly across partitions
- Can accept 2-3x latency increase
- Cluster topology changes frequently

**Choose Balanced (Hash + Sync-one + Proxy) if:**
- Need strong guarantees but maximize throughput
- Moderate latency acceptable
- Balance between consistency and performance

---

## Open Questions / Implementation Notes

1. **Partition count recommendation**:
   - Implement validation: Warn if `partitions % nodes != 0` when using hash strategy
   - Suggest: `partitions = nodes * 10` for good distribution

2. **Replication consistency trade-offs**:
   - Now configurable! Users can choose async, sync-one, sync-quorum, or raft
   - Default: Async (best throughput, matches user's 100K+ requirement)
   - Users with strict consistency needs can opt into sync

3. **Consumer reconnection**:
   - Now configurable! Proxy (default) vs Direct routing
   - Proxy: No reconnection, transparent migration
   - Direct: Must reconnect, but lower latency

4. **etcd load at scale**:
   - Hash strategy: No per-partition watches, only node membership
   - Dynamic strategy: Watch prefix `/mqtt/queue/*/partitions/` (single watch, all partitions)
   - Acceptable: Single prefix watch handles 1000s of partitions

5. **Strategy instance caching**:
   - Create strategy instances once at manager init
   - Reuse across enqueue/delivery operations
   - Avoid allocation overhead in hot path
