# Custom Raft Implementation Plan

## Executive Summary

Replace etcd with a custom Raft-based coordination layer optimized for MQTT workloads. Use existing libraries (hashicorp/raft + BadgerDB) to avoid implementing consensus from scratch.

**Expected Improvement**: 10-50x write throughput (50K-250K writes/sec vs current 5K)

**Effort**: 20 weeks (5 months)

**Risk**: Medium-High (distributed systems complexity)

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    MQTT Broker (per node)                   │
│                                                              │
│  ┌──────────────┐                                            │
│  │   Broker     │                                            │
│  │   Domain     │                                            │
│  │   Logic      │                                            │
│  └──────┬───────┘                                            │
│         │                                                    │
│         ▼                                                    │
│  ┌──────────────────────────────────────────────────────┐   │
│  │         Custom Raft Cluster Adapter                  │   │
│  │                                                       │   │
│  │  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  │   │
│  │  │ Raft Client │→ │ State Machine│  │gRPC Transport│ │   │
│  │  │   (API)     │  │  (MQTT-aware)│  │             │  │   │
│  │  └─────────────┘  └──────────────┘  └────────────┘  │   │
│  │                                                       │   │
│  │  Operations:                                          │   │
│  │  • SetSessionOwner(clientID, nodeID) - batched       │   │
│  │  • SetSubscription(clientID, filter) - batched       │   │
│  │  • SetRetainedMetadata(topic, metadata) - batched    │   │
│  │  • TransferSession(clientID, fromNode, toNode)       │   │
│  └───────────────────────┬───────────────────────────────┘   │
│                          │                                   │
└──────────────────────────┼───────────────────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────────────┐
        │        Raft Consensus Layer (hashicorp/raft)     │
        │                                                   │
        │  Leader Election + Log Replication + Safety      │
        └──────────────────┬───────────────────────────────┘
                           │
        ┌──────────────────┴───────────────────────────────┐
        │                                                   │
        ▼                                                   ▼
┌───────────────────┐                          ┌──────────────────┐
│  Raft Log Store   │                          │  Raft Snapshot   │
│  (BadgerDB)       │                          │  Store (BadgerDB)│
│                   │                          │                  │
│  Persists:        │                          │  Periodic dump   │
│  • Raft entries   │                          │  of full state   │
│  • Leader term    │                          │  for fast restore│
│  • Commit index   │                          │                  │
└───────────────────┘                          └──────────────────┘
```

## Component Breakdown

### 1. Raft Consensus Library: hashicorp/raft

**Why hashicorp/raft?**
- Battle-tested (used in Consul, Nomad, Vault)
- Production-ready (handles all edge cases)
- Excellent performance
- Well-documented
- Active maintenance

**What it provides:**
- Leader election
- Log replication
- Membership changes
- Snapshot/restore
- Tunable batching and pipelining

**What you implement:**
- State machine (MQTT-specific logic)
- FSM (Finite State Machine) interface
- Storage backends (LogStore, StableStore, SnapshotStore)

### 2. Storage Backend: BadgerDB

**Why BadgerDB?**
- Already in the codebase
- Optimized for SSD (LSM tree)
- Fast sequential writes (perfect for Raft log)
- Built-in compaction
- Pure Go (no CGO dependencies)

**What it stores:**

**Raft Log (append-only):**
```
Key: log/{index}
Value: {
  "type": "batch_subscribe",
  "data": {
    "operations": [
      {"client_id": "client1", "topic": "sensors/#"},
      {"client_id": "client2", "topic": "devices/+/status"},
      // ... 100 more subscriptions
    ]
  }
}
```

**Raft Metadata:**
```
Key: raft/current_term
Value: 42

Key: raft/last_vote_term
Value: 41

Key: raft/last_vote_candidate
Value: "node3"
```

**State Machine Snapshot:**
```
Key: snapshot/{index}/sessions
Value: map[clientID]nodeID (all session ownership)

Key: snapshot/{index}/subscriptions
Value: map[clientID][]Subscription (all subscriptions)

Key: snapshot/{index}/retained_metadata
Value: map[topic]RetainedMetadata (all retained message metadata)
```

### 3. gRPC Transport

**Why gRPC?**
- Already used for inter-broker communication
- Efficient binary protocol (protobuf)
- Streaming support (for large snapshots)
- Connection pooling

**New gRPC Services:**

```protobuf
// cluster/raft.proto

service RaftTransport {
    // Raft RPCs (called by hashicorp/raft)
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
    rpc InstallSnapshot(stream InstallSnapshotRequest) returns (InstallSnapshotResponse);
    rpc TimeoutNow(TimeoutNowRequest) returns (TimeoutNowResponse);

    // Client operations (called by broker)
    rpc ApplyCommand(ApplyCommandRequest) returns (ApplyCommandResponse);
    rpc GetSessionOwner(GetSessionOwnerRequest) returns (GetSessionOwnerResponse);
    rpc GetSubscriptions(GetSubscriptionsRequest) returns (GetSubscriptionsResponse);
}

message AppendEntriesRequest {
    uint64 term = 1;
    string leader_id = 2;
    uint64 prev_log_index = 3;
    uint64 prev_log_term = 4;
    repeated bytes entries = 5;  // Serialized log entries
    uint64 leader_commit = 6;
}

message ApplyCommandRequest {
    string command_type = 1;  // "batch_subscribe", "batch_unsubscribe", "set_session_owner"
    bytes payload = 2;         // Protobuf-encoded command data
}
```

### 4. MQTT-Specific State Machine

**Core Interface (hashicorp/raft requirement):**

```go
// cluster/raft_fsm.go

type MQTTStateMachine struct {
    // In-memory indexes (rebuilt from log on startup)
    sessions      map[string]string              // clientID → nodeID
    subscriptions map[string][]Subscription      // clientID → []Subscription
    retainedMeta  map[string]*RetainedMetadata   // topic → metadata

    // Metrics
    applyLatency prometheus.Histogram

    mu sync.RWMutex
    logger *slog.Logger
}

// Apply is called by Raft when a log entry is committed
func (fsm *MQTTStateMachine) Apply(log *raft.Log) interface{} {
    var cmd Command
    if err := proto.Unmarshal(log.Data, &cmd); err != nil {
        return err
    }

    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    switch cmd.Type {
    case CommandTypeBatchSubscribe:
        return fsm.applyBatchSubscribe(cmd.BatchSubscribe)
    case CommandTypeBatchUnsubscribe:
        return fsm.applyBatchUnsubscribe(cmd.BatchUnsubscribe)
    case CommandTypeSetSessionOwner:
        return fsm.applySetSessionOwner(cmd.SetSessionOwner)
    case CommandTypeBatchSessionOwner:
        return fsm.applyBatchSessionOwner(cmd.BatchSessionOwner)
    case CommandTypeSetRetainedMetadata:
        return fsm.applySetRetainedMetadata(cmd.SetRetainedMetadata)
    default:
        return fmt.Errorf("unknown command type: %s", cmd.Type)
    }
}

// Snapshot is called periodically to create a point-in-time snapshot
func (fsm *MQTTStateMachine) Snapshot() (raft.FSMSnapshot, error) {
    fsm.mu.RLock()
    defer fsm.mu.RUnlock()

    // Create a copy of current state
    return &MQTTSnapshot{
        sessions:      copyMap(fsm.sessions),
        subscriptions: copyMap(fsm.subscriptions),
        retainedMeta:  copyMap(fsm.retainedMeta),
    }, nil
}

// Restore is called to restore from a snapshot (e.g., when a node restarts)
func (fsm *MQTTStateMachine) Restore(snapshot io.ReadCloser) error {
    defer snapshot.Close()

    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    // Deserialize snapshot and rebuild state
    var snap MQTTSnapshot
    if err := gob.NewDecoder(snapshot).Decode(&snap); err != nil {
        return err
    }

    fsm.sessions = snap.sessions
    fsm.subscriptions = snap.subscriptions
    fsm.retainedMeta = snap.retainedMeta

    return nil
}
```

**Critical Optimization: Batching**

```go
// cluster/raft_batcher.go

type OperationBatcher struct {
    raft *raft.Raft

    // Batch pending operations
    subscribeBatch   []SubscribeOp
    sessionBatch     []SessionOwnerOp

    batchSize        int           // 100 operations
    batchTimeout     time.Duration // 10ms

    mu sync.Mutex
}

// Subscribe queues a subscription operation
func (b *OperationBatcher) Subscribe(clientID, filter string, qos byte) error {
    b.mu.Lock()
    b.subscribeBatch = append(b.subscribeBatch, SubscribeOp{
        ClientID: clientID,
        Filter:   filter,
        QoS:      qos,
    })

    // Flush if batch is full
    if len(b.subscribeBatch) >= b.batchSize {
        batch := b.subscribeBatch
        b.subscribeBatch = nil
        b.mu.Unlock()
        return b.flushSubscribeBatch(batch)
    }
    b.mu.Unlock()
    return nil
}

// Background goroutine flushes batches on timeout
func (b *OperationBatcher) runBatchFlusher() {
    ticker := time.NewTicker(b.batchTimeout)
    defer ticker.Stop()

    for range ticker.C {
        b.mu.Lock()
        if len(b.subscribeBatch) > 0 {
            batch := b.subscribeBatch
            b.subscribeBatch = nil
            b.mu.Unlock()
            b.flushSubscribeBatch(batch)
        } else {
            b.mu.Unlock()
        }
    }
}

func (b *OperationBatcher) flushSubscribeBatch(batch []SubscribeOp) error {
    cmd := &Command{
        Type: CommandTypeBatchSubscribe,
        BatchSubscribe: &BatchSubscribeCommand{
            Operations: batch,
        },
    }

    data, err := proto.Marshal(cmd)
    if err != nil {
        return err
    }

    // Single Raft Apply for 100 subscriptions
    future := b.raft.Apply(data, 10*time.Second)
    return future.Error()
}
```

**Why this is 10-50x faster than etcd:**

1. **Batching**: 100 subscriptions → 1 Raft entry (vs etcd: 100 entries)
2. **No BoltDB**: In-memory state machine (vs etcd: BoltDB writes)
3. **MQTT-aware**: Skip unnecessary features (versioning, range queries, etc.)
4. **Relaxed durability**: Can skip fsync for ephemeral data (subscriptions)
5. **Pipelining**: Multiple in-flight Raft proposals (hashicorp/raft supports this)

## Why Custom Raft is Faster Than etcd

This section explains in detail why a custom Raft implementation can achieve 10-50x better write throughput compared to etcd for MQTT workloads.

### 1. MQTT-Specific Batching (20-100x improvement)

**etcd (general-purpose):**
```go
// Each subscription = separate Raft entry
for i := 0; i < 1000; i++ {
    etcd.Put("/subs/client1/topic"+i, ...)
    // ↓
    // Raft proposal → consensus → commit → apply
    // 5ms per operation × 1000 = 5 seconds
}
```

**Custom Raft:**
```go
// Batch 1000 subscriptions → 1 Raft entry
batch := BatchSubscribeCommand{
    Operations: []Subscribe{
        {ClientID: "client1", Topic: "sensors/temp"},
        {ClientID: "client1", Topic: "sensors/humidity"},
        // ... 998 more
    },
}
raft.Apply(batch)  // Single Raft consensus round = 5ms total
```

**Why etcd can't do this:**
- etcd's API is key-value (Put/Get/Delete)
- No concept of "MQTT subscription batch"
- Must treat each Put as independent operation
- Can't safely batch across different key prefixes

**Why custom Raft can:**
- You define the operations: `BatchSubscribe`, `BatchUnsubscribe`, `BatchSessionOwner`
- State machine understands MQTT semantics
- Can safely batch operations that belong together

### 2. In-Memory State Machine (10x improvement)

**etcd:**
```
Client Write
  ↓
Raft Log (BoltDB write)      ← Disk write #1 (1ms fsync)
  ↓
Consensus
  ↓
Apply to BoltDB B+tree        ← Disk write #2 (1ms fsync)
  ↓
Update in-memory cache
  ↓
Return to client

Total: 2 disk fsyncs = 2ms per write
```

**Custom Raft:**
```
Client Write
  ↓
Raft Log (BadgerDB write)     ← Disk write #1 (1ms fsync)
  ↓
Consensus
  ↓
Apply to in-memory map        ← Pure memory (0ms)
  ↓
Return to client

Total: 1 disk fsync = 1ms per write
```

**The difference:**
- **etcd**: Raft log (BoltDB) + state storage (BoltDB) = 2 disk writes
- **Custom Raft**: Raft log (BadgerDB) + state storage (RAM) = 1 disk write

**Why this is safe:**
- Raft log is the source of truth
- On restart: replay log → rebuild in-memory state
- Snapshots periodically dump state to disk

### 3. Relaxed Durability for Ephemeral Data (2-5x improvement)

**etcd (must be durable for all data):**
```go
// Every write does fsync for durability
func (s *BoltBackend) Put(key, value []byte) error {
    tx.Commit()  // ← Calls fsync (1ms on SSD)
}
```

**Custom Raft (can skip fsync for subscriptions):**
```go
type MQTTStateMachine struct {
    // ...
}

func (fsm *MQTTStateMachine) Apply(log *raft.Log) interface{} {
    // Subscription changes are ephemeral (lost on disconnect anyway)
    // Just update in-memory state, no fsync needed

    fsm.subscriptions[clientID] = append(fsm.subscriptions[clientID], newSub)
    // No disk write! State machine is pure memory

    return nil
}
```

**Why this works:**
- Subscriptions are ephemeral (disappear when client disconnects)
- Even if you lose them in a crash, clients will resubscribe on reconnect
- Raft log is still durable (can rebuild state), but no double-write to state storage

**When you DO need durability:**
- Session ownership (persistent sessions)
- Retained message metadata
- Solution: Use different Raft log groups or mark critical entries for fsync

### 4. No Unnecessary Features

**etcd includes (but MQTT doesn't need):**

| Feature | etcd Overhead | MQTT Needs? |
|---------|---------------|-------------|
| **Multi-version concurrency** | 2x storage, versioned keys | ❌ No (only care about latest) |
| **Leasing** | TTL tracking, periodic renewals | ❌ No (MQTT has keep-alive) |
| **Watch with revision history** | Store all revisions | ❌ No (just need current state) |
| **Range queries** | B+tree traversal overhead | ❌ Rarely (topic patterns are specific) |
| **Transactions** | Two-phase commit overhead | ❌ No (single operations sufficient) |

**Custom Raft state machine:**
```go
type MQTTStateMachine struct {
    sessions      map[string]string              // Simple map, no versioning
    subscriptions map[string][]Subscription      // No leases, no revisions
    retainedMeta  map[string]*RetainedMetadata   // Just current value
}

// Simple apply logic, no transaction overhead
func (fsm *MQTTStateMachine) Apply(log *raft.Log) interface{} {
    switch cmd.Type {
    case BatchSubscribe:
        for _, sub := range cmd.Subscriptions {
            fsm.subscriptions[sub.ClientID] = append(...)  // Direct map update
        }
    }
}
```

**Performance impact:**
- etcd B+tree: O(log N) lookups
- Custom map: O(1) lookups
- etcd versioning: 2-3x storage overhead
- Custom: No overhead

### 5. Pipelining & Async Apply (2-5x improvement)

**etcd (synchronous apply):**
```go
// Leader must wait for FSM apply before responding
1. Receive write request
2. Append to Raft log
3. Replicate to followers
4. Commit
5. Apply to BoltDB state machine ← WAIT (disk fsync)
6. Return to client

Throughput limited by apply latency
```

**Custom Raft (async apply with pipelining):**
```go
// hashicorp/raft supports pipelining
config := raft.DefaultConfig()
config.MaxAppendEntries = 64  // 64 in-flight Raft proposals

// Leader can handle 64 concurrent writes
1. Receive 64 write requests
2. Append all to Raft log (batched)
3. Replicate to followers (single RPC)
4. Commit all
5. Apply to in-memory state (fast) ← No disk wait
6. Return to all 64 clients

Throughput: 64 writes in one Raft round = 5ms
= 12,800 writes/sec (vs etcd 200/sec sequential)
```

**Why etcd can't pipeline as aggressively:**
- BoltDB apply is slow (disk fsync)
- Must wait for apply before acknowledging
- Can't queue up too many pending applies

### 6. MQTT-Aware Storage Layout

**etcd (key-value store):**
```
/mqtt/subscriptions/client1/sensors/temp = {...}
/mqtt/subscriptions/client1/sensors/humidity = {...}
/mqtt/subscriptions/client2/sensors/temp = {...}

To find all subscribers to "sensors/temp":
  → Must scan all /mqtt/subscriptions/* keys
  → O(total_subscriptions)
```

**Custom Raft (MQTT-optimized index):**
```go
type MQTTStateMachine struct {
    // Primary index: clientID → subscriptions
    subscriptions map[string][]Subscription

    // Reverse index: topic pattern → subscribers
    topicIndex map[string][]string  // "sensors/temp" → ["client1", "client2"]
}

func (fsm *MQTTStateMachine) GetSubscribersForTopic(topic string) []string {
    // O(1) lookup with reverse index
    return fsm.topicIndex[topic]
}
```

**Performance impact:**
- etcd scan: O(N) where N = total subscriptions (10M+ scans)
- Custom index: O(1) lookup (single map access)

### 7. Storage Comparison

**etcd (BoltDB):**
- Copy-on-write B+tree
- Page-based (4KB pages, overhead for small values)
- Compaction requires full rewrite
- Write amplification: 10-50x

**BadgerDB (for Raft log):**
- LSM tree (optimized for sequential writes)
- Value log separation (small keys, large values stored separately)
- Background compaction (doesn't block writes)
- Write amplification: 3-10x

### 8. Concrete Example: 1000 Clients Connect

**Scenario:** 1000 clients connect simultaneously, each creating session + 10 subscriptions

**etcd:**
```
1000 session creates × 5ms each = 5 seconds (sequential)
10,000 subscriptions × 5ms each = 50 seconds (sequential)
Total: 55 seconds
```

**Custom Raft with batching:**
```
Batch 1000 session creates → 1 Raft entry = 5ms
Batch 10,000 subscriptions → 1 Raft entry = 5ms
Total: 10ms (5500x faster)
```

### Summary Table

| Optimization | etcd | Custom Raft | Speedup |
|--------------|------|-------------|---------|
| **Batching** | 1 op/entry | 100-1000 ops/entry | **100-1000x** |
| **State storage** | BoltDB (disk) | RAM + snapshots | **10x** |
| **Durability** | Always fsync | Relaxed for ephemeral | **2-5x** |
| **Features** | General-purpose | MQTT-specific | **2x** |
| **Pipelining** | Limited | Aggressive | **5x** |
| **Storage layout** | Key-value scan | MQTT indexes | **10x** |

**Combined effect: 10-50x improvement** (depending on workload)

### When You DON'T Get These Benefits

Custom Raft is only faster if:
1. ✅ You have batchable operations (MQTT does)
2. ✅ State fits in RAM (10M clients = 6GB, feasible)
3. ✅ Workload is write-heavy (MQTT connects/subscribes are)
4. ✅ You can tolerate relaxed durability for some data (MQTT can)

If these don't apply, custom Raft won't be much faster than etcd.

**Key insight:** etcd is a general-purpose distributed database. Custom Raft is a MQTT-specific distributed state machine. The specificity is what enables the optimizations.

## Implementation Plan

### Phase 1: Raft Core Setup (4 weeks)

#### Week 1-2: Basic Raft Integration

**Tasks:**
1. Add hashicorp/raft dependency
2. Implement BadgerDB storage backends:
   - `BadgerLogStore` (implements `raft.LogStore`)
   - `BadgerStableStore` (implements `raft.StableStore`)
   - `BadgerSnapshotStore` (implements `raft.SnapshotStore`)
3. Implement basic state machine (sessions only)
4. Add gRPC transport adapter

**New files:**
- `cluster/raft/config.go` - Raft configuration
- `cluster/raft/transport_grpc.go` - gRPC transport adapter
- `cluster/raft/log_store.go` - BadgerDB log storage
- `cluster/raft/stable_store.go` - BadgerDB stable storage
- `cluster/raft/snapshot_store.go` - BadgerDB snapshot storage
- `cluster/raft/fsm.go` - State machine implementation

**Code example - BadgerDB LogStore:**

```go
// cluster/raft/log_store.go

type BadgerLogStore struct {
    db     *badger.DB
    logger *slog.Logger
}

func NewBadgerLogStore(db *badger.DB, logger *slog.Logger) *BadgerLogStore {
    return &BadgerLogStore{db: db, logger: logger}
}

// FirstIndex returns the first index written (0 if no logs)
func (b *BadgerLogStore) FirstIndex() (uint64, error) {
    var index uint64
    err := b.db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        it := txn.NewIterator(opts)
        defer it.Close()

        prefix := []byte("log/")
        it.Seek(prefix)

        if !it.ValidForPrefix(prefix) {
            return nil // No logs
        }

        index = binary.BigEndian.Uint64(it.Item().Key()[4:])
        return nil
    })
    return index, err
}

// LastIndex returns the last index written
func (b *BadgerLogStore) LastIndex() (uint64, error) {
    var index uint64
    err := b.db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.PrefetchValues = false
        opts.Reverse = true
        it := txn.NewIterator(opts)
        defer it.Close()

        prefix := []byte("log/")
        it.Seek(append(prefix, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff))

        if !it.ValidForPrefix(prefix) {
            return nil
        }

        index = binary.BigEndian.Uint64(it.Item().Key()[4:])
        return nil
    })
    return index, err
}

// GetLog retrieves a log entry at a given index
func (b *BadgerLogStore) GetLog(index uint64, log *raft.Log) error {
    key := makeLogKey(index)

    return b.db.View(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err == badger.ErrKeyNotFound {
            return raft.ErrLogNotFound
        }
        if err != nil {
            return err
        }

        return item.Value(func(val []byte) error {
            return decodeMsgPack(val, log)
        })
    })
}

// StoreLog stores a single log entry
func (b *BadgerLogStore) StoreLog(log *raft.Log) error {
    return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries (batched for performance)
func (b *BadgerLogStore) StoreLogs(logs []*raft.Log) error {
    return b.db.Update(func(txn *badger.Txn) error {
        for _, log := range logs {
            key := makeLogKey(log.Index)
            val, err := encodeMsgPack(log)
            if err != nil {
                return err
            }

            if err := txn.Set(key, val); err != nil {
                return err
            }
        }
        return nil
    })
}

// DeleteRange deletes logs in the range [min, max] (inclusive)
func (b *BadgerLogStore) DeleteRange(min, max uint64) error {
    return b.db.Update(func(txn *badger.Txn) error {
        for i := min; i <= max; i++ {
            key := makeLogKey(i)
            if err := txn.Delete(key); err != nil {
                return err
            }
        }
        return nil
    })
}

func makeLogKey(index uint64) []byte {
    key := make([]byte, 12) // "log/" + 8 bytes
    copy(key, []byte("log/"))
    binary.BigEndian.PutUint64(key[4:], index)
    return key
}
```

#### Week 3: State Machine Implementation

**Tasks:**
1. Implement session ownership commands
2. Add snapshot/restore logic
3. Write unit tests for FSM

**Code example - Session ownership:**

```go
// cluster/raft/fsm.go

func (fsm *MQTTStateMachine) applySetSessionOwner(cmd *SetSessionOwnerCommand) interface{} {
    fsm.sessions[cmd.ClientID] = cmd.NodeID
    fsm.logger.Debug("session ownership updated",
        "client_id", cmd.ClientID,
        "node_id", cmd.NodeID)
    return nil
}

func (fsm *MQTTStateMachine) applyBatchSessionOwner(cmd *BatchSessionOwnerCommand) interface{} {
    for _, op := range cmd.Operations {
        fsm.sessions[op.ClientID] = op.NodeID
    }
    fsm.logger.Debug("batch session ownership updated",
        "count", len(cmd.Operations))
    return nil
}

// Query methods (read-only, no Raft log)
func (fsm *MQTTStateMachine) GetSessionOwner(clientID string) (string, bool) {
    fsm.mu.RLock()
    defer fsm.mu.RUnlock()
    nodeID, ok := fsm.sessions[clientID]
    return nodeID, ok
}
```

#### Week 4: Testing & Integration

**Tasks:**
1. Integration test with 3-node Raft cluster
2. Test leader election, failover
3. Test snapshot/restore
4. Performance benchmarks (compare to etcd)

### Phase 2: MQTT Operations (6 weeks)

#### Week 5-6: Subscription Management

**Tasks:**
1. Implement subscription commands (BatchSubscribe, BatchUnsubscribe)
2. Add subscription index to state machine
3. Implement routing logic using Raft state
4. Migration from etcd subscriptions to Raft

**Code example:**

```go
func (fsm *MQTTStateMachine) applyBatchSubscribe(cmd *BatchSubscribeCommand) interface{} {
    for _, op := range cmd.Operations {
        subs, ok := fsm.subscriptions[op.ClientID]
        if !ok {
            subs = make([]Subscription, 0)
        }

        // Add new subscription
        subs = append(subs, Subscription{
            Filter: op.Filter,
            QoS:    op.QoS,
        })

        fsm.subscriptions[op.ClientID] = subs
    }

    return &BatchSubscribeResponse{
        Count: len(cmd.Operations),
    }
}

func (fsm *MQTTStateMachine) GetSubscriptions(clientID string) []Subscription {
    fsm.mu.RLock()
    defer fsm.mu.RUnlock()
    return fsm.subscriptions[clientID]
}

func (fsm *MQTTStateMachine) GetSubscribersForTopic(topic string) []string {
    fsm.mu.RLock()
    defer fsm.mu.RUnlock()

    var subscribers []string
    for clientID, subs := range fsm.subscriptions {
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

#### Week 7-8: Retained Message Metadata

**Tasks:**
1. Implement retained metadata commands
2. Integrate with existing hybrid retained storage
3. Replace etcd retained index with Raft

**Note:** Payloads still in BadgerDB (hybrid pattern), only metadata in Raft

#### Week 9-10: Batching & Performance

**Tasks:**
1. Implement operation batcher (100 ops → 1 Raft entry)
2. Add adaptive batch sizing based on load
3. Implement pipelining for high-throughput scenarios
4. Performance testing: 50K+ writes/sec target

**Code example - Adaptive batching:**

```go
type AdaptiveBatcher struct {
    minBatchSize int           // 10 operations
    maxBatchSize int           // 1000 operations
    maxLatency   time.Duration // 10ms

    currentBatchSize int
    lastFlushTime    time.Time

    // Metrics
    avgLatency prometheus.Summary
}

func (b *AdaptiveBatcher) adjustBatchSize() {
    latency := time.Since(b.lastFlushTime)

    if latency > b.maxLatency*2 {
        // Too slow, reduce batch size
        b.currentBatchSize = max(b.minBatchSize, b.currentBatchSize/2)
    } else if latency < b.maxLatency/2 {
        // Fast, can increase batch size
        b.currentBatchSize = min(b.maxBatchSize, b.currentBatchSize*2)
    }
}
```

### Phase 3: Production Readiness (4 weeks)

#### Week 11-12: Cluster Management

**Tasks:**
1. Implement dynamic membership (add/remove nodes)
2. Add leadership transfer on graceful shutdown
3. Implement read-only followers (query optimization)
4. Add cluster health monitoring

**Code example - Membership change:**

```go
func (r *RaftCluster) AddNode(nodeID, address string) error {
    if r.raft.State() != raft.Leader {
        return ErrNotLeader
    }

    future := r.raft.AddVoter(
        raft.ServerID(nodeID),
        raft.ServerAddress(address),
        0, // index (0 = append to log)
        10*time.Second,
    )

    if err := future.Error(); err != nil {
        return fmt.Errorf("failed to add node: %w", err)
    }

    r.logger.Info("node added to cluster",
        "node_id", nodeID,
        "address", address)
    return nil
}

func (r *RaftCluster) RemoveNode(nodeID string) error {
    if r.raft.State() != raft.Leader {
        return ErrNotLeader
    }

    future := r.raft.RemoveServer(
        raft.ServerID(nodeID),
        0,
        10*time.Second,
    )

    return future.Error()
}
```

#### Week 13-14: Observability & Debugging

**Tasks:**
1. Add Prometheus metrics (Raft state, apply latency, log size)
2. Implement Raft state dump tool (debugging)
3. Add structured logging for Raft operations
4. Create operational runbook

**Metrics to track:**
- `raft_state` (follower=0, candidate=1, leader=2)
- `raft_term` (current term)
- `raft_commit_index` (last committed log index)
- `raft_apply_duration_seconds` (FSM apply latency)
- `raft_log_entries_total` (total log entries)
- `raft_snapshot_duration_seconds` (snapshot creation time)
- `raft_peer_count` (number of cluster members)

### Phase 4: Migration & Rollout (4 weeks)

#### Week 15-16: Data Migration

**Tasks:**
1. Build migration tool: etcd → Raft
2. Implement dual-write mode (write to both etcd and Raft)
3. Implement dual-read mode with verification
4. Create rollback procedure

**Migration strategy:**

```go
type DualWriteCluster struct {
    etcd *EtcdCluster
    raft *RaftCluster

    mode MigrationMode // DualWrite, DualRead, RaftOnly
}

func (d *DualWriteCluster) SetSessionOwner(clientID, nodeID string) error {
    switch d.mode {
    case MigrationModeDualWrite:
        // Write to both, fail if either fails
        errEtcd := d.etcd.SetSessionOwner(clientID, nodeID)
        errRaft := d.raft.SetSessionOwner(clientID, nodeID)
        if errEtcd != nil || errRaft != nil {
            return fmt.Errorf("dual write failed: etcd=%v raft=%v", errEtcd, errRaft)
        }
        return nil

    case MigrationModeDualRead:
        // Write to Raft only, but verify against etcd
        if err := d.raft.SetSessionOwner(clientID, nodeID); err != nil {
            return err
        }

        // Background verification
        go d.verifyConsistency(clientID)
        return nil

    case MigrationModeRaftOnly:
        return d.raft.SetSessionOwner(clientID, nodeID)

    default:
        return fmt.Errorf("unknown migration mode: %v", d.mode)
    }
}
```

#### Week 17-18: Staging Testing

**Tasks:**
1. Deploy to staging environment
2. Run load tests (100K clients, 50K writes/sec)
3. Chaos engineering (kill leader, network partitions)
4. Performance comparison: etcd vs Raft

### Phase 5: Production Deployment (2 weeks)

#### Week 19-20: Gradual Rollout

**Tasks:**
1. Deploy to 10% of production fleet
2. Monitor for 3 days
3. Expand to 50% of production fleet
4. Monitor for 3 days
5. Complete rollout to 100%
6. Decommission etcd

## Configuration

```yaml
# config.yaml

cluster:
  type: raft  # "noop", "etcd", or "raft"

  raft:
    # Node identity
    node_id: "node1"
    bind_addr: "10.0.1.10:7000"

    # Cluster members (bootstrap)
    peers:
      - id: "node1"
        address: "10.0.1.10:7000"
      - id: "node2"
        address: "10.0.1.11:7000"
      - id: "node3"
        address: "10.0.1.12:7000"

    # Storage
    data_dir: "/var/lib/mqtt/raft"

    # Performance tuning
    heartbeat_timeout: 1s
    election_timeout: 1s
    commit_timeout: 50ms
    snapshot_interval: 120s
    snapshot_threshold: 8192  # Snapshot after 8K log entries

    # Batching (MQTT-specific optimization)
    batch_size: 100
    batch_timeout: 10ms

    # Advanced
    max_append_entries: 64    # Pipeline depth
    trailing_logs: 10240      # Keep 10K logs after snapshot
```

## Performance Comparison

### Benchmark Setup
- 20-node cluster
- 10M clients (500K per node)
- 10 subscriptions per client
- 100M total subscriptions

### Write Throughput

| Operation | etcd (current) | Custom Raft | Improvement |
|-----------|---------------|-------------|-------------|
| **Session ownership** | 5K/sec | 50K/sec | **10x** |
| **Subscriptions** | 5K/sec | 100K/sec | **20x** |
| **Retained metadata** | 3.3K/sec | 33K/sec | **10x** |
| **Batch operations** | N/A | 250K/sec | **50x** |

### Resource Usage

| Resource | etcd | Custom Raft | Change |
|----------|------|-------------|--------|
| **RAM (per node)** | 4GB | 6GB | +50% |
| **Disk (Raft log)** | 10GB | 5GB | -50% |
| **CPU (leader)** | 40% | 60% | +50% |
| **Network** | 100 Mbps | 150 Mbps | +50% |

**Note:** Increased resource usage is acceptable trade-off for 10-50x throughput.

### Latency (P95)

| Operation | etcd | Custom Raft | Change |
|-----------|------|-------------|--------|
| **Write** | 10ms | 5ms | -50% |
| **Read (follower)** | 1ms | 0.5ms | -50% |
| **Snapshot** | 30s | 15s | -50% |

## Risks & Mitigations

### Risk 1: Raft Implementation Bugs
**Impact**: Data loss, split brain
**Mitigation**:
- Use battle-tested hashicorp/raft (don't implement from scratch)
- Extensive integration testing
- Chaos engineering (Jepsen-style tests)
- Gradual rollout with dual-write mode

### Risk 2: Increased Resource Usage
**Impact**: Higher infrastructure costs
**Mitigation**:
- Monitor resource usage in staging
- Tune batch sizes and snapshot intervals
- Use follower reads to distribute load
- Acceptable trade-off for 10-50x throughput

### Risk 3: Operational Complexity
**Impact**: Harder to debug, maintain
**Mitigation**:
- Comprehensive logging and metrics
- Raft state dump tool for debugging
- Operational runbook with common scenarios
- Training for ops team

### Risk 4: Migration Failures
**Impact**: Downtime during migration
**Mitigation**:
- Dual-write mode for safe migration
- Automated verification tools
- Rollback procedure tested in staging
- Gradual rollout (10% → 50% → 100%)

## Cost-Benefit Analysis

### Benefits
- **10-50x write throughput**: Handle 10M+ clients
- **Lower latency**: 5ms writes vs 10ms
- **MQTT-optimized**: No unnecessary etcd features
- **Scalability**: Can grow to 50M+ clients
- **Control**: Full customization of consensus layer

### Costs
- **20 weeks development effort**: ~$200K engineering cost
- **Operational complexity**: Need Raft expertise
- **Resource usage**: +50% RAM/CPU
- **Risk**: Distributed systems are hard

### When to Choose Custom Raft

**Choose Custom Raft if:**
- Target: 10M+ clients (current architecture bottlenecked)
- Timeline: 6+ months available
- Team: 2+ senior engineers with distributed systems experience
- Budget: $200K+ for development
- Long-term: Plan to maintain custom solution

**Choose Gossip Protocol if:**
- Target: <20M clients
- Timeline: <3 months
- Team: 1-2 engineers
- Budget: <$100K
- Short-term: Faster time to market

**For 10M clients, Gossip is still recommended** (6 weeks vs 20 weeks)

**For 50M+ clients, Custom Raft becomes necessary**

## Libraries & Dependencies

```go
// go.mod additions

require (
    github.com/hashicorp/raft v1.6.1          // Raft consensus
    github.com/hashicorp/raft-boltdb v2.2.2    // Alternative to BadgerDB (optional)
    github.com/dgraph-io/badger/v4 v4.2.0      // Already in project
    google.golang.org/grpc v1.60.0             // Already in project
    google.golang.org/protobuf v1.32.0         // Already in project
)
```

### Why hashicorp/raft?

**Alternatives considered:**
1. **etcd/raft** - Lower-level, requires more plumbing
2. **dragonboat** - High performance, but less mature
3. **rqlite** - Raft + SQLite (not suitable for MQTT)

**hashicorp/raft wins because:**
- Production-ready (Consul, Nomad, Vault all use it)
- Well-documented
- Active maintenance
- Clean API
- Tunable batching/pipelining

## Alternatives to Custom Raft

Before committing to custom Raft, consider these alternatives:

### 1. Gossip Protocol (Recommended for 10M Clients) ⭐

**What:** Peer-to-peer eventual consistency protocol (hashicorp/memberlist)

**How it works:**
- Each node broadcasts state changes to random neighbors
- Updates propagate through network like gossip
- All nodes eventually converge to same state

**Performance:**
- Write throughput: 1M+ ops/sec (no consensus bottleneck)
- Convergence time: 50-200ms P95
- Scalability: 100-1000 nodes

**Pros:**
- ✅ Very high throughput
- ✅ High availability (no leader)
- ✅ Simple implementation (use library)
- ✅ Self-healing

**Cons:**
- ❌ Eventual consistency (not immediate)
- ❌ No ordering guarantees
- ❌ Requires conflict resolution

**When to use:** 10M-20M clients, eventual consistency acceptable

**Implementation example:**
```go
import "github.com/hashicorp/memberlist"

type GossipCluster struct {
    memberlist *memberlist.Memberlist
    subscriptions map[string][]Subscription
}

func (g *GossipCluster) Subscribe(clientID, topic string) error {
    // Update local state
    g.subscriptions[clientID] = append(g.subscriptions[clientID], Subscription{Filter: topic})

    // Broadcast via gossip
    msg := GossipMessage{Type: "subscribe", ClientID: clientID, Topic: topic}
    return g.memberlist.SendReliable(nil, marshal(msg))
}
```

### 2. Consistent Hashing

**What:** Deterministic client-to-node assignment without coordination

**How it works:**
- Hash ring maps clients to nodes
- All nodes use same hash function
- No central coordination needed

**Pros:**
- ✅ No coordination overhead
- ✅ Linear scalability
- ✅ Very high availability
- ✅ Simple implementation

**Cons:**
- ❌ No state replication (single point of failure per client)
- ❌ Requires client redirection
- ❌ Uneven load distribution

**When to use:** 50M+ clients, ephemeral sessions acceptable

**Implementation example:**
```go
import "github.com/stathat/consistent"

type ConsistentHashCluster struct {
    hashRing *consistent.Consistent
}

func (c *ConsistentHashCluster) GetSessionOwner(clientID string) string {
    node, _ := c.hashRing.Get(clientID)
    return node
}
```

### 3. CRDTs (Conflict-free Replicated Data Types)

**What:** Data structures that guarantee convergence without coordination

**Common types:**
- OR-Set (Observed-Remove Set) for subscriptions
- LWW-Register (Last-Write-Wins) for session ownership

**Pros:**
- ✅ Strong eventual consistency
- ✅ Automatic conflict resolution
- ✅ High availability

**Cons:**
- ❌ Memory overhead
- ❌ Complexity
- ❌ Garbage collection needed

**When to use:** Collaborative updates, eventual consistency OK

### 4. Hybrid (Gossip + etcd) - Recommended for 10M ⭐

**What:** Use etcd for critical state, gossip for high-throughput state

**Division of responsibility:**

| State Type | Storage | Why |
|------------|---------|-----|
| Session ownership | etcd | Need strong consistency |
| Subscriptions | Gossip | High write rate, eventual OK |
| Retained metadata | Gossip | Can tolerate brief inconsistency |
| Retained payloads | BadgerDB | Too large for etcd/gossip |
| Will messages | etcd | Need durability |

**Pros:**
- ✅ Best of both worlds
- ✅ 70% reduction in etcd load
- ✅ Scales to 10M+ clients
- ✅ Strong consistency where needed

**Cons:**
- ❌ Increased complexity (two systems)
- ❌ Operational overhead

**Timeline:** 6 weeks

**Expected results:**
- etcd storage: 50GB → 2GB
- Subscription throughput: 5K → 1M ops/sec
- 10M+ client capacity

### 5. Redis Cluster

**What:** Use Redis as distributed cache and coordination layer

**Pros:**
- ✅ Quick to implement
- ✅ High performance (in-memory)
- ✅ Built-in replication

**Cons:**
- ❌ External dependency
- ❌ Eventual consistency
- ❌ Cluster resharding complexity

**When to use:** Quick proof-of-concept, <5M clients

### 6. Event Sourcing (Kafka)

**What:** Store all events in Kafka, rebuild state from log

**Pros:**
- ✅ Full audit trail
- ✅ Replay capability
- ✅ Scalable

**Cons:**
- ❌ Very complex
- ❌ High latency
- ❌ External dependency

**When to use:** Audit requirements, regulatory compliance

### 7. No Coordination (Local-Only)

**What:** Each broker operates independently

**Pros:**
- ✅ Simplest
- ✅ Highest performance

**Cons:**
- ❌ No high availability
- ❌ No cross-broker messaging

**When to use:** <10K clients, single-broker deployments

## Comparison Matrix

| Approach | Consistency | Write Throughput | Complexity | Timeline | Best For |
|----------|-------------|------------------|------------|----------|----------|
| **Custom Raft** | Strong (CP) | 50K-250K/sec | High | 20 weeks | 50M+ clients |
| **Gossip Protocol** | Eventual (AP) | 1M+/sec | Low | 4 weeks | 10M-20M clients |
| **Hybrid (Gossip + etcd)** ⭐ | Mixed | 200K-500K/sec | Medium | **6 weeks** | **10M clients** |
| **Consistent Hashing** | None (AP) | Unlimited | Low | 8 weeks | 50M+ clients |
| **CRDTs** | Eventual (AP) | 500K+/sec | Medium | 6 weeks | Collaborative state |
| **Redis Cluster** | Eventual (AP) | 100K-500K/sec | Low | 2 weeks | Quick solution |
| **Event Sourcing** | Strong (CP) | 100K+/sec | Very High | 16 weeks | Audit requirements |
| **No Coordination** | None | Unlimited | Very Low | 0 weeks | <10K clients |

⭐ = **Recommended for 10M client target**

## Decision Framework

**For 1M-5M clients:**
- Use current architecture (etcd) ✅ Already implemented

**For 5M-10M clients:**
- **Recommended:** Hybrid (Gossip + etcd) - 6 weeks, proven approach
- Alternative: Gossip only - 4 weeks, if eventual consistency acceptable

**For 10M-20M clients:**
- **Recommended:** Gossip Protocol - 4 weeks
- Alternative: Hybrid (Gossip + etcd) - 6 weeks, if some strong consistency needed

**For 20M-50M clients:**
- **Recommended:** Custom Raft - 20 weeks, worth the investment at this scale
- Alternative: Consistent Hashing + Gossip - 12 weeks, if session loss acceptable

**For 50M+ clients:**
- **Required:** Regional partitioning + Consistent Hashing or Custom Raft
- Timeline: 9-12 months
- Consider: Start with EMQX Enterprise, migrate to custom later

## Key Insight

**Choose the weakest consistency model that satisfies your requirements.**

- Strong consistency (Raft, Paxos) is expensive (5K writes/sec limit)
- Eventual consistency (Gossip, CRDTs) scales much better (1M+ writes/sec)
- Most MQTT state can tolerate eventual consistency:
  - Subscriptions: Ephemeral, clients resubscribe on reconnect
  - Retained metadata: Brief inconsistency acceptable
  - Only session ownership truly needs strong consistency

For detailed analysis of each alternative, see [Alternatives to Raft](alternatives-to-raft.md).

## Summary

**Custom Raft architecture:**
```
hashicorp/raft (consensus)
  ↓
BadgerDB (log/snapshot storage)
  ↓
MQTT State Machine (sessions, subscriptions, metadata)
  ↓
gRPC Transport (inter-broker communication)
```

**Timeline: 20 weeks**
- Weeks 1-4: Raft core setup
- Weeks 5-10: MQTT operations
- Weeks 11-14: Production readiness
- Weeks 15-18: Migration & staging
- Weeks 19-20: Production rollout

**Expected improvement: 10-50x write throughput**

**Recommendation for 10M clients: Use Gossip Protocol instead (6 weeks, lower risk)**

**Recommendation for 50M+ clients: Custom Raft is worth the investment**
