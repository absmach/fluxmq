# Durable Queues (MQTT + AMQP)

> **Status**: Log Storage Engine Complete
> **Last Updated**: 2026-01-25
> **Compilation Status**: ✅ All code compiles successfully
> **Test Status**: ✅ All tests passing
> **Storage Engine**: ✅ Kafka/Redis Streams-style append-only log (AOL)
> **Documentation**: ✅ Consolidated

This document provides comprehensive documentation for the durable queue functionality shared by the MQTT, AMQP 1.0, and AMQP 0.9.1 brokers. It covers architecture, implementation details, configuration guidelines, performance & scalability, current progress, and planned features.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Log Storage Engine](#log-storage-engine)
4. [Implementation Details](#implementation-details)
5. [Configuration Guidelines](#configuration-guidelines)
6. [Performance & Scalability](#performance--scalability)
7. [Current Progress](#current-progress)
8. [Missing Features & Next Steps](#missing-features--next-steps)
9. [Quick Start](#quick-start)
10. [Best Practices](#best-practices)
11. [Troubleshooting](#troubleshooting)

---

## Overview

FluxMQ supports both traditional pub/sub messaging and durable queues across protocols. Queues provide:

- **Persistent storage**: Messages survive broker restarts
- **Consumer groups**: Load balancing across multiple consumers
- **Acknowledgments**: Application-level message processing confirmation
- **Retry handling**: Automatic retry with exponential backoff
- **Dead-letter queues**: Failed message handling
- **Ordering guarantees**: Configurable FIFO ordering
- **Full MQTT compatibility**: Standard MQTT topics remain unchanged

### Design Principles

1. **Protocol Independence**: MQTT and AMQP brokers remain independent
2. **Binding Glue**: A shared queue manager provides cross-protocol durability
3. **Protocol-Native**: Queue features accessible via MQTT v5 topics/properties and AMQP queue capabilities
4. **Pluggable**: Queue storage backend uses the same interface pattern as existing stores
5. **Observable**: Built-in metrics and monitoring from day one

---

## Architecture

### High-Level Architecture

```
┌──────────────┐  ┌──────────────┐  ┌───────────────┐
│ MQTT Broker  │  │ AMQP Broker  │  │ AMQP091 Broker│
│ (TCP/WS/     │  │ (AMQP 1.0)   │  │ (AMQP 0.9.1)  │
│ HTTP/CoAP)   │  │              │  │               │
└──────┬───────┘  └──────┬───────┘  └──────┬────────┘
       │                │                │
       └────────────────┼────────────────┘
                        ▼
              ┌─────────────────────────┐
              │ Shared Queue Manager    │
              │ - Topic bindings        │
              │ - Consumer groups       │
              │ - DLQ handling          │
              └──────────┬──────────────┘
                         ▼
              ┌─────────────────────────┐
              │ Log Storage (AOL)       │
              │ + Topic Index           │
              └─────────────────────────┘
```

### Topic & Queue Addressing

The queue manager routes **published topics** into queues using MQTT-style topic filters.
Queue names are logical identifiers; bindings decide which topics feed a queue.
One publish can fan out into multiple queues if multiple bindings match.

#### Standard MQTT Topics (Existing Behavior - No Changes)
```
sensors/temperature              → Normal pub/sub
events/user/created             → Normal pub/sub + retained
$share/web/api/responses        → Shared subscription (ephemeral)
```

#### Topic-to-Queue Bindings (New)

Queues can be bound to topic patterns in configuration:

```yaml
queues:
  - name: "orders"
    topics: ["orders/#", "payments/approved"]
```

```
orders/123                       → Enqueued into "orders"
payments/approved                → Enqueued into "orders"
```

By default, auto-created queues bind to `$queue/<name>/#`. You can add additional
topic patterns to the same queue to fan-in related topics.

#### Explicit Queue Topics (MQTT/AMQP Interop)
```
$queue/tasks/image-processing           → Durable queue
$queue/tasks/email-sending              → Durable queue
$queue/events/user-lifecycle            → Durable queue
```

AMQP 1.0 clients can use the `queue` capability or address `$queue/<name>` directly.
AMQP 0.9.1 clients declare/bind queues and the broker maps those bindings to
`$queue/<name>` internally for durable delivery.

#### Acknowledgment Topics (New)
```
$queue/tasks/image-processing/$ack      → Success acknowledgment
$queue/tasks/image-processing/$nack     → Failure (retry)
$queue/tasks/image-processing/$reject   → Reject (move to DLQ)
```

#### Dead-Letter Queue Topics
```
$dlq/{queue-name}                       → DLQ for failed messages
```

#### Admin Topics (Phase 4)
```
$admin/queue/create                     → Create queue with config
$admin/queue/tasks/image/config        → Update queue settings
$admin/queue/tasks/image/stats         → Queue metrics stream
```

### Message Flow

#### Queue Message Flow
```
┌──────────────┐
│  Publisher   │
└──────┬───────┘
       │ PUBLISH to orders/123
       │ (topic bound to queue "orders")
       │ User Property: partition-key=user-123
       ▼
┌──────────────────┐
│  Queue Manager   │
│  - Hash partition key → partition ID
│  - Store in log storage (AOL)
│  - Assign to consumer
└──────┬───────────┘
       │
       ▼
┌──────────────────┐
│  Consumer Group  │
│  Group: image-workers-v1
│  Consumers: worker-1, worker-2, worker-3
└──────┬───────────┘
       │ Load balanced delivery
       ▼
┌──────────────────┐
│  Consumer        │
│  (worker-1)      │
│  - Receives PUBLISH
│  - Processes message
│  - Sends $ack
└──────────────────┘
```

#### Routing Logic
```
PUBLISH to $queue/orders/123 (MQTT/AMQP queue publish)
  ↓
queueManager.Publish()
  ↓
FindMatchingQueues(topic) using queue bindings
  ↓
Append to each matching queue
```

```
SUBSCRIBE to $queue/orders/#
  ↓
broker.Subscribe()
  ↓
isQueueTopic() = true
  ↓
queueManager.Subscribe()
  ↓
Consumer added to group → Partitions rebalanced
```

```
PUBLISH to $queue/orders/123/$ack
  ↓
broker.Publish()
  ↓
isQueueAckTopic() = true
  ↓
handleQueueAck()
  ↓
queueManager.Ack() → Remove from inflight → Delete message
```

### Message Lifecycle State Machine

```
QUEUED → DELIVERED → ACKED → DELETED
         ↓
         TIMEOUT → RETRY → DELIVERED (retry count++)
                   ↓
                   MAX_RETRIES → DLQ
```

**States**:
1. **QUEUED**: Message persisted in storage, waiting for delivery
2. **DELIVERED**: Message sent to consumer, waiting for acknowledgment
3. **ACKED**: Consumer confirmed successful processing
4. **DELETED**: Message removed from storage
5. **RETRY**: Delivery timeout, message scheduled for retry
6. **DLQ**: Max retries exceeded or explicitly rejected

### Cluster Architecture - Configurable Design

The implementation supports **multiple strategies** for key architectural decisions, configurable per queue or globally.

#### 1. Partition Assignment Strategy

**Default: Hash-Based** - `partitionID % nodeCount`
**Alternative: Dynamic** - etcd-based with rebalancing

#### 2. Replication Strategy

**Default: Async** - Maximum throughput
**Alternative: Sync** - Strong consistency
**Future: Raft** - Linearizable consistency

#### 3. Consumer Routing Strategy

**Default: Proxy** - Route via consumer's node
**Alternative: Direct** - Connect to partition owner

---

## Log Storage Engine

The queue system uses a custom **append-only log (AOL)** storage engine inspired by Apache Kafka and Redis Streams. This design provides high-throughput, durable message storage with efficient offset-based access.

### Key Concepts

**Append-Only Log**: Messages are appended sequentially to immutable segment files. Each message receives a monotonically increasing offset. This design enables:
- High write throughput (sequential I/O)
- Efficient range queries by offset
- Simple replication (just stream the log)
- Time-travel reads (replay from any offset)

**Segments**: The log is divided into segments of configurable size (default 64MB). Only the active segment is writable; older segments are read-only and candidates for retention-based deletion.

**Sparse Indexing**: Offset and time indexes enable fast random access without scanning entire segments. Indexes are built at configurable intervals (default every 4KB).

### Storage Hierarchy

```
Store
├── Queue (name)
│   ├── Partition (ID) → SegmentManager
│   │   ├── Segments (ordered by base offset)
│   │   │   ├── 00000000000000000000.seg
│   │   │   ├── 00000000000000000000.idx  (offset index)
│   │   │   ├── 00000000000000000000.tix  (time index)
│   │   │   └── ...
│   │   └── Write buffer (batched writes)
│   └── ConsumerManager
│       └── ConsumerGroup (groupID) → ConsumerState
│           ├── Partition cursors
│           ├── PEL Shards (8 default)
│           └── Operation log
```

### Segment Files

Segments are the fundamental storage unit. Each segment contains:

**Segment File (`.seg`)**:
- Magic number: `0x464C5558` ("FLUX")
- Batches of encoded records with compression
- CRC32-C checksums for integrity validation

**Offset Index (`.idx`)**:
- Sparse mapping: relative offset → file position
- Binary search for O(log n) lookups
- Entry every 4KB of segment data

**Time Index (`.tix`)**:
- Mapping: timestamp → offset
- Enables time-based queries (e.g., "read from 1 hour ago")
- Minimum 1-second interval between entries

### Batch Encoding

Messages are grouped into batches for efficient storage:

```
Batch Header (44 bytes):
┌─────────┬─────────┬────────────┬──────────┬───────┬───────┐
│ Magic   │ CRC     │ BaseOffset │ BatchLen │ Count │ Flags │
│ (4B)    │ (4B)    │ (8B)       │ (4B)     │ (2B)  │ (2B)  │
├─────────┴─────────┴────────────┴──────────┴───────┴───────┤
│ Compression(1) │ Version(1) │ Reserved(2) │               │
├────────────────┴────────────┴─────────────┤               │
│ BaseTimestamp (8B)          │ MaxTimestamp (8B)           │
└─────────────────────────────┴─────────────────────────────┘

Records (variable):
┌─────────────────┬────────────────────┬─────┬───────┬─────────┐
│ OffsetDelta(v)  │ TimestampDelta(v)  │ Key │ Value │ Headers │
└─────────────────┴────────────────────┴─────┴───────┴─────────┘
(v) = varint encoded
```

**Compression Options**:
| Type | ID | Description |
|------|-----|-------------|
| None | 0 | No compression |
| S2 | 1 | Snappy-compatible, fastest (default) |
| Zstd | 2 | Best compression ratio |

### Consumer State & PEL

The Pending Entry List (PEL) tracks messages that have been delivered but not yet acknowledged:

**Consumer State (per group)**:
```go
type ConsumerState struct {
    partitions map[uint32]*PartitionState  // Per-partition cursors
    pelShards  []*PELShard                 // Sharded PEL (8 default)
    opLog      *os.File                    // Operation log for durability
}
```

**Partition Cursor**:
```go
type PartitionCursor struct {
    Cursor    uint64  // Next offset to read
    Committed uint64  // Last committed offset (safe for truncation)
}
```

**PEL Entry**:
```go
type PELEntry struct {
    Offset        uint64  // Message offset
    PartitionID   uint32  // Which partition
    ConsumerID    string  // Assigned consumer
    ClaimedAt     int64   // Delivery timestamp
    DeliveryCount uint16  // Redelivery count
}
```

**PEL Sharding**: Entries are distributed across 8 shards (default) based on `offset % NumPELShards`. This enables parallel redelivery processing and reduces lock contention.

**PEL Operations**:
| Operation | ID | Description |
|-----------|-----|-------------|
| Add | 1 | Message delivered to consumer |
| Ack | 2 | Consumer acknowledged message |
| Claim | 3 | Work stealing (reassign to different consumer) |
| Expire | 4 | Entry timeout for redelivery |

### Message Flow

**Write Path**:
```
Append(queue, partition, message)
  → SegmentManager.AppendMessage()
    → Batch.Append(record)         # Add to current batch
    → Batch.Encode()               # Serialize + compress
    → Segment.Write()              # Write to file + buffer
    → UpdateIndexes()              # Sparse index + time index
    → Return assigned offset
```

**Read Path**:
```
Read(queue, partition, offset)
  → SegmentManager.findSegment(offset)
    → Index.Lookup(offset)         # Binary search → file position
    → Segment.ReadAt(position)     # Read batch from disk
    → Batch.Decode()               # Decompress + parse
    → Return message at offset
```

**Delivery Path**:
```
Deliver(queue, groupID, partition, offset, consumerID)
  → ConsumerManager.GetOrCreate(groupID)
    → PELShard.Add(entry)          # Track pending delivery
    → OpLog.Append(AddOp)          # Durability
    → Return message to consumer
```

**Acknowledgment Path**:
```
Ack(queue, groupID, partition, offset)
  → PELShard.Remove(offset)        # Remove from pending
  → UpdateCommitted(offset)        # Advance committed cursor
  → OpLog.Append(AckOp)            # Durability
```

### Directory Structure

```
basedir/
└── queues/
    └── {queue_name}/
        ├── queue.json                     # Queue metadata
        ├── partitions/
        │   ├── 0/
        │   │   ├── 00000000000000000000.seg
        │   │   ├── 00000000000000000000.idx
        │   │   ├── 00000000000000000000.tix
        │   │   ├── 00000000067108864.seg  # Next segment at offset 64M
        │   │   └── ...
        │   └── 1/
        │       └── ...
        └── consumers/
            └── {group_id}/
                ├── {group_id}.cur         # Cursor snapshots (JSON)
                ├── {group_id}.pel         # PEL snapshots
                └── {group_id}.pel.log     # Operation log
```

### Configuration Defaults

```go
DefaultMaxSegmentSize      = 64 * 1024 * 1024  // 64MB per segment
DefaultMaxSegmentAge       = time.Hour          // Roll after 1 hour
DefaultIndexIntervalBytes  = 4096               // Index every 4KB
DefaultMaxBatchSize        = 1024 * 1024        // 1MB max batch
DefaultWriteBufferSize     = 64 * 1024          // 64KB write buffer
DefaultSyncInterval        = time.Second         // Sync every 1s
DefaultPELCompactThreshold = 10000              // Compact after 10k ops
```

### Retention & Recovery

**Retention**: Segments are deleted when:
- Size-based: Total partition size exceeds limit
- Time-based: Segment age exceeds retention duration
- Offset-based: `Truncate(minOffset)` removes segments with `endOffset < minOffset`

**Recovery on Startup**:
1. Scan segment directory, load segments in base-offset order
2. Validate segment integrity (magic numbers, CRC)
3. Rebuild missing indexes from segment data
4. Recover PEL state from snapshot + operation log replay
5. Optionally truncate corrupted trailing batches

---

## Implementation Details

### Package Structure

**Core Packages**:
```
queue/                          - Core queue package (manager, workers)
logstorage/                  - Storage interfaces (LogStore, ConsumerGroupStore)
logstorage/log/              - Append-only log implementation
    ├── store.go                - Main store orchestrator (~1055 LOC)
    ├── segment.go              - Segment file management
    ├── manager.go              - Segment manager (lifecycle, rolling)
    ├── batch.go                - Batch encoding/decoding
    ├── encoding.go             - Record serialization
    ├── index.go                - Sparse offset index
    ├── timeindex.go            - Time-based index
    ├── consumer_state.go       - Consumer state persistence
    ├── consumer_manager.go     - Consumer group management
    ├── pel.go                  - Pending Entry List implementation
    ├── cursor.go               - Cursor store
    ├── adapter.go              - LogStore interface adapter
    └── types.go                - Types, constants, defaults
queue/types/                    - Shared type definitions
queue/cluster/                  - Cluster coordination
queue/cluster/proto/            - gRPC definitions
```

**Modified Existing Code** (~15% of code):
```
broker/broker.go               - Add QueueManager, routing logic (~350 LOC)
broker/queue_utils.go          - Helper functions (~110 LOC)
broker/v5_handler.go           - Extract User Properties (~50 LOC)
cmd/main.go                    - QueueHandler wiring
```

### Storage Layer

**Core Interfaces** (in `logstorage/log.go`):

```go
// LogStore provides append-only log storage with offset-based access.
type LogStore interface {
    // Queue lifecycle
    CreateQueue(ctx context.Context, config QueueConfig) error
    GetQueue(ctx context.Context, queueName string) (*QueueConfig, error)
    DeleteQueue(ctx context.Context, queueName string) error
    ListQueues(ctx context.Context) ([]QueueConfig, error)

    // Append adds a message, returns assigned offset
    Append(ctx context.Context, queueName string, partitionID int, msg *Message) (uint64, error)
    AppendBatch(ctx context.Context, queueName string, partitionID int, msgs []*Message) (uint64, error)

    // Read retrieves message at specific offset
    Read(ctx context.Context, queueName string, partitionID int, offset uint64) (*Message, error)
    ReadBatch(ctx context.Context, queueName string, partitionID int, startOffset uint64, limit int) ([]*Message, error)

    // Offset queries
    Head(ctx context.Context, queueName string, partitionID int) (uint64, error)  // First valid offset
    Tail(ctx context.Context, queueName string, partitionID int) (uint64, error)  // Next offset to assign
    Count(ctx context.Context, queueName string, partitionID int) (uint64, error) // tail - head

    // Retention
    Truncate(ctx context.Context, queueName string, partitionID int, minOffset uint64) error
}

// ConsumerGroupStore manages cursor-based consumer groups with PEL tracking.
type ConsumerGroupStore interface {
    // Consumer group lifecycle
    CreateConsumerGroup(ctx context.Context, group *ConsumerGroupState) error
    GetConsumerGroup(ctx context.Context, queueName, groupID string) (*ConsumerGroupState, error)
    DeleteConsumerGroup(ctx context.Context, queueName, groupID string) error

    // PEL operations
    AddPendingEntry(ctx context.Context, queueName, groupID string, entry *PendingEntry) error
    RemovePendingEntry(ctx context.Context, queueName, groupID, consumerID string, partitionID int, offset uint64) error
    GetPendingEntries(ctx context.Context, queueName, groupID, consumerID string) ([]*PendingEntry, error)
    TransferPendingEntry(ctx context.Context, queueName, groupID string, partitionID int, offset uint64, fromConsumer, toConsumer string) error

    // Cursor management
    UpdateCursor(ctx context.Context, queueName, groupID string, partitionID int, cursor uint64) error
    UpdateCommitted(ctx context.Context, queueName, groupID string, partitionID int, committed uint64) error

    // Consumer registration
    RegisterConsumer(ctx context.Context, queueName, groupID string, consumer *ConsumerInfo) error
    UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
}
```

**File Format Magic Numbers**:
```go
SegmentMagic   = 0x464C5558  // "FLUX" - segment files
IndexMagic     = 0x464C5849  // "FLXI" - offset index
TimeIndexMagic = 0x464C5854  // "FLXT" - time index
```

### Partition Strategy

Messages are assigned to partitions based on `partition-key` property:

```
partition_id = hash(partition_key) % num_partitions
```

Partitions are assigned to consumers using round-robin strategy:

```
Consumer 1: [0, 1, 2]
Consumer 2: [3, 4, 5]
Consumer 3: [6, 7, 8, 9]
```

All messages with the same partition key are:
1. Assigned to the same partition
2. Delivered in FIFO order
3. Processed by the same consumer (until rebalancing)

Messages without a partition key are randomly assigned to partitions.

### Consumer Group Semantics

**Consumer Group**: A named group of consumers that share message processing load.

- Each message delivered to **exactly one consumer** in the group
- Multiple consumer groups can subscribe to the same queue (each group gets all messages)
- Consumers identify their group via `consumer-group` User Property on SUBSCRIBE
- If no group specified, client ID prefix is used as group name

**Example**:
```
Queue: $queue/tasks/image-processing

Consumer Group "workers-v1": [worker-1, worker-2, worker-3]
Consumer Group "analytics": [analyzer-1]

Message A → Delivered to one of: worker-1, worker-2, or worker-3
Message A → Also delivered to: analyzer-1
```

### Zero-Copy Integration

The queue system integrates with the broker's existing zero-copy buffer system:
```go
// In delivery.go
buf := core.GetBufferWithData(msg.Payload)
storageMsg := &brokerStorage.Message{...}
storageMsg.SetPayloadFromBuffer(buf)
```

Messages delivered via queues use the same `RefCountedBuffer` mechanism as standard MQTT messages, ensuring minimal memory overhead.

---

## Configuration Guidelines

### Queue Configuration Properties

```json
{
  "name": "tasks",
  "topics": ["$queue/tasks/#", "orders/#"],
  "partitions": 10,
  "ordering": "partition",

  "retry_policy": {
    "max_retries": 10,
    "initial_backoff": "5s",
    "max_backoff": "5m",
    "backoff_multiplier": 2.0,
    "total_timeout": "3h"
  },

  "dlq_config": {
    "enabled": true,
    "topic": "$dlq/tasks/image-processing",
    "alert_webhook": "https://supermq.com/alerts/queue-failure"
  },

  "limits": {
    "max_message_size": 1048576,
    "max_queue_depth": 100000,
    "message_ttl": "168h"
  },

  "performance": {
    "delivery_timeout": "30s",
    "batch_size": 1
  },

  "cluster": {
    "consumer_routing_mode": "proxy",
    "partition_strategy": "hash"
  }
}
```

### Default Configuration

```go
QueueConfig{
    Partitions:      10,
    Ordering:        OrderingPartition,
    MaxMessageSize:  1MB,
    MaxQueueDepth:   100,000,
    MessageTTL:      7 days,
    DeliveryTimeout: 30 seconds,

    RetryPolicy: {
        MaxRetries:        10,
        InitialBackoff:    5s,
        MaxBackoff:        5m,
        BackoffMultiplier: 2.0,
        TotalTimeout:      3h,
    },

    DLQConfig: {
        Enabled: true,
        Topic:   "$dlq/{queue-name}",
    },
}
```

### Ordering Modes

| Mode        | Description            | Partition Count            | Use Case                                       |
| ----------- | ---------------------- | -------------------------- | ---------------------------------------------- |
| `none`      | No ordering guarantees | N (parallel processing)    | High throughput, order doesn't matter          |
| `partition` | FIFO per partition key | Configurable (default: 10) | User-specific events, session-based processing |
| `strict`    | Global FIFO            | 1                          | Critical ordering requirements                 |

### Consumer Routing Modes (Cluster)

| Mode     | Description                                     | Network Hops | Use Case                           |
| -------- | ----------------------------------------------- | ------------ | ---------------------------------- |
| `proxy`  | Route messages through consumer's proxy node    | 2            | Default, simple consumer setup     |
| `direct` | Consumers connect directly to partition owners  | 1            | Lower latency, complex client code |

**Proxy Mode** (Default):
- Consumer connects to any broker node
- Partition worker routes messages to consumer's node via gRPC
- Consumer receives message from local broker
- Flow: Partition Owner → Consumer's Node → Consumer

**Direct Mode**:
- Consumer must connect to partition owner node
- No cross-node message routing needed
- Lower latency but requires client-side partition awareness
- Flow: Partition Owner → Consumer (same node)

### Configuration Quick Reference

```go
// High Throughput (Default)
config.PartitionStrategy = "hash"
config.ReplicationMode = "async"
config.RoutingMode = "proxy"
config.BatchSize = 500
config.RingBufferSize = 16384

// High Consistency
config.PartitionStrategy = "dynamic"
config.ReplicationMode = "sync"
config.ReplicationAckPolicy = "quorum"
config.RoutingMode = "direct"
```

### Retry Policy

**Exponential Backoff**:
```
Retry 1: 5s
Retry 2: 10s (5s × 2.0)
Retry 3: 20s (10s × 2.0)
Retry 4: 40s
Retry 5: 80s
Retry 6: 160s
Retry 7: 300s (capped at max_backoff)
Retry 8: 300s
...
```

**Termination Conditions**:
- Max retries exceeded (default: 10)
- OR total time elapsed > total_timeout (default: 3h)

**Then**: Message moved to dead-letter queue

### Acknowledgment Types

| Type     | Topic Suffix | Meaning                              | Next State                |
| -------- | ------------ | ------------------------------------ | ------------------------- |
| Success  | `/$ack`      | Message processed successfully       | DELETED                   |
| Negative | `/$nack`     | Processing failed, retry immediately | RETRY (increment counter) |
| Reject   | `/$reject`   | Permanent failure, don't retry       | DLQ                       |
| Timeout  | N/A          | No ack within timeout (30s default)  | RETRY                     |

---

## Performance & Scalability

### Achieved Performance (Phase 1 ✅)

**Target**: 100K msgs/sec on single node
**Achieved**: **812K msgs/sec** (8x target exceeded!)

#### Enqueue Performance
- Single partition: **812K msgs/sec** (1,232 ns/op, 5 allocs/op)
- Multiple partitions (10): **815K msgs/sec** (1,227 ns/op, 5 allocs/op)
- Parallel concurrent: **678K msgs/sec** (1,474 ns/op, 7 allocs/op)
- Small payload: **887K msgs/sec** (1,128 ns/op, 7 allocs/op)
- Large payload (64KB): **725K msgs/sec** (1,380 ns/op, 5 allocs/op)

#### Delivery Performance
- Single consumer: 3,320 msgs/sec per worker
- 10 consumers: 7,670 msgs/sec per partition
- Sustained throughput: 53,665 msgs/sec measured

#### Memory Efficiency
- Enqueue: 5-7 allocs/op, ~800 B/op (41% reduction after optimization)
- Dequeue: 8-16 allocs/op, 1.5-5.5 KB/op (49% reduction after optimization)
- Per message: ~735 B/op (after pooling optimizations)
- Per consumer: ~200 bytes
- Per partition: ~100 bytes

### Performance Optimizations Implemented

#### 1. Ring Buffer Capacity (16K messages)
**File**: `queue/storage/memory/lockfree/ringbuffer.go`
- Increased from 4,096 to 16,384 messages per partition
- Reduces BadgerDB reads during hot path
- Memory cost: 160KB vs 40KB per 10 partitions

#### 2. Batch Processing (500 messages/batch)
**Files**: `queue/partition_worker.go`, `queue/delivery_worker.go`, `queue/storage/storage.go`
- Increased from 100 to 500 messages per batch
- Reduces atomic operations from 100/sec to 20/sec per partition
- Default in `DefaultQueueConfig`: 500

#### 3. Cross-Partition Batch Writes
**File**: `queue/storage/hybrid/store.go`
- Async batching of messages across partitions before BadgerDB flush
- Flush interval: 10ms or when batch reaches 100 messages
- Uses double-buffering pattern to minimize lock time
- Methods: `flushLoop()`, `flushBatch()`, `Close()`

#### 4. Count() Optimization (O(1) Atomic Counters) ⭐
**Files**: `queue/storage/memory/memory.go`, `queue/storage/badger/badger.go`

**Critical optimization** for TTL/MaxQueueDepth enforcement:
- **Before**: O(n) scan of all messages on every enqueue
- **After**: O(1) atomic counter lookup
- **Performance impact**: 1000x+ speedup for MaxQueueDepth checks

**Implementation**:
- Memory store: `counts map[string]int64`
- Badger store: 8-byte counter with atomic increment/decrement
- Increment on `Enqueue`, decrement on `DeleteMessage`

### Performance Characteristics

**Algorithmic Complexity**:
- Partition assignment: O(consumers)
- Message lookup: O(1) (BadgerDB key lookup)
- Inflight tracking: O(1) (hash map)
- Queue depth check: O(1) (atomic counter)

### Cluster Performance (Phase 2 ✅)

| Configuration                   | Throughput (Target) | Status |
| ------------------------------- | ------------------- | ------ |
| Single node (achieved)          | **812K msgs/sec**   | ✅ Complete |
| 3-node cluster (target)         | 300K+ msgs/sec      | 📋 Testing Phase |
| With replication (target)       | 250K msgs/sec       | ⏳ Planned |

### Resource Usage

| Messages in Queue | Memory Usage | Disk Usage |
| ----------------- | ------------ | ---------- |
| 10,000            | ~50 MB       | ~10 MB     |
| 100,000           | ~200 MB      | ~100 MB    |
| 1,000,000         | ~1.5 GB      | ~1 GB      |

**Note**: Assumes average message size of 1 KB

### Performance Targets

Based on requirements (thousands of msgs/sec, 20-40 services):

**Single Queue**:
- Enqueue: 10,000 msgs/sec
- Dequeue: 10,000 msgs/sec
- Latency: <10ms p99

**Cluster**:
- Cross-node routing overhead: <5ms
- Partition rebalancing: <1 second
- Failover recovery: <5 seconds

**Storage**:
- BadgerDB write throughput: >50,000 writes/sec
- Disk space: ~1KB per message average
- 100,000 messages = ~100MB disk

### Performance Tuning Tips

1. **Right-Size Partitions**: Balance between parallelism and overhead
   - Too few partitions: Limited consumer parallelism
   - Too many partitions: Rebalancing overhead, uneven distribution
   - **Recommendation**: `partitions = 2 × max_consumers`

2. **Set Realistic Timeouts**: Configure based on actual processing time
   ```json
   {
     "performance": {
       "delivery_timeout": "60s"  // Allow 60s for processing before retry
     },
     "retry_policy": {
       "total_timeout": "6h"  // Keep trying for 6 hours total
     }
   }
   ```

---

## Current Progress

### Progress Summary

| Phase | Status | LOC | Notes |
|-------|--------|-----|-------|
| Phase 1: Core Infrastructure | ✅ Complete | 3,600 | Single-node queue functionality |
| Phase 2: Retry & DLQ | ✅ Complete | ~2,500 | Retry state machine, DLQ, ordering |
| Phase 3: Performance Optimization | ✅ Complete | ~1,200 | Memory/latency improvements |
| Phase 4: Cluster RPC Integration | ✅ Complete | ~800 | Cross-node routing |
| **Phase 5: Log Storage Engine** | ✅ **Complete** | **~3,500** | **Kafka-style AOL, segments, indexes, PEL** |
| Phase 6: Cluster Testing | 📋 Next | ~500 | 3-node deployment validation |
| Phase 7: Fault Tolerance | 📋 Planned | ~2,500 | Rebalancing, failover |
| Phase 8: Admin API | 📋 Planned | ~2,300 | REST/gRPC APIs |

### Log Storage Engine (Phase 5) ✅

**Completed 2026-01-25** - Major storage layer rewrite from BadgerDB to custom append-only log.

**Architecture**:
- ✅ Kafka/Redis Streams-style append-only log (AOL)
- ✅ Segment-based storage with automatic rolling (64MB default)
- ✅ Sparse offset index for O(log n) lookups
- ✅ Time index for timestamp-based queries
- ✅ Batch encoding with S2/Zstd compression
- ✅ CRC32-C integrity validation

**Consumer State**:
- ✅ Cursor-based consumer groups (replaces message-ID tracking)
- ✅ Sharded PEL (Pending Entry List) for scalable acknowledgment tracking
- ✅ Operation log for PEL durability with compaction
- ✅ Work stealing via `TransferPendingEntry`

**Files** (in `logstorage/log/`):
```
store.go           ~1,055 LOC  Main store orchestrator
segment.go         ~350 LOC    Segment file management
manager.go         ~280 LOC    Segment lifecycle
batch.go           ~320 LOC    Batch encoding/decoding
encoding.go        ~250 LOC    Record serialization
index.go           ~200 LOC    Sparse offset index
timeindex.go       ~180 LOC    Time-based index
consumer_state.go  ~300 LOC    Consumer state persistence
consumer_manager.go ~250 LOC   Consumer group management
pel.go             ~280 LOC    Pending Entry List
cursor.go          ~150 LOC    Cursor store
adapter.go         ~120 LOC    Interface adapter
types.go           ~185 LOC    Types and constants
```

### Previous Phase Completions

**Phase 1-4 Features**:
- ✅ Durable queue storage with offset-based access
- ✅ Queue manager with basic operations
- ✅ Consumer group assignment (round-robin rebalancing)
- ✅ Message delivery loop (background workers)
- ✅ Ack/nack/reject handling
- ✅ Retry state machine with exponential backoff
- ✅ DLQ movement and alerts
- ✅ Partition-based ordering (3 modes: none, partition, strict)
- ✅ Broker integration (highly decoupled)
- ✅ Zero-copy buffer integration
- ✅ Cluster RPC integration (ProxyMode/DirectMode)
- ✅ Cross-node message routing

### Test Coverage

- **Log Storage Tests**: Comprehensive test suite for segment, index, batch, PEL operations
- **Queue Package Tests**: 111+ tests (manager, consumer groups, partitions, workers)
- **All Tests Passing**: ✅ Race detection enabled
- **Coverage**: High coverage across all packages

### Key Design Decisions

1. **Offset-based vs Message-ID**: Switched from random message IDs to sequential offsets for:
   - Efficient range queries
   - Simpler cursor management
   - Natural ordering guarantees

2. **Segments vs Single File**: Segment-based design enables:
   - Retention without rewriting (delete old segments)
   - Parallel reads across segments
   - Bounded recovery time

3. **PEL Sharding**: 8-shard default for:
   - Reduced lock contention
   - Parallel redelivery processing
   - Better scalability with many consumers

---

## Missing Features & Next Steps

### What's Implemented

**Log Storage Engine** (✅ Complete - 2026-01-25)
- ✅ Append-only log with segment management
- ✅ Sparse offset and time indexes
- ✅ Batch encoding with compression (S2/Zstd)
- ✅ Consumer groups with cursor-based tracking
- ✅ Sharded PEL with operation log durability
- ✅ Retention and recovery mechanisms

**Cluster RPC Integration** (✅ Complete)
- ✅ Partition ownership via etcd with hash-based assignment
- ✅ Cross-node message routing via gRPC
- ✅ Consumer routing modes (Proxy/Direct)
- ✅ Remote enqueue operations

### What's NOT Implemented Yet

**Phase 6: Cluster Testing & Validation** (📋 Next)
- ❌ 3-node cluster deployment testing
- ❌ Cross-node throughput benchmarks
- ❌ Partition rebalancing validation
- ❌ Network partition handling

**Phase 7: Fault Tolerance** (📋 Planned)
- ❌ Automatic partition rebalancing
- ❌ Partition failover on node crash
- ❌ Partition migration/transfer
- ❌ Message replication (optional)

**Phase 8: Admin API & Helpers** (📋 Planned)
- ❌ REST admin API
- ❌ gRPC admin API
- ❌ Request/response helpers

**Other Pending Items**
- ⏱️ Prometheus metrics integration
- ⏱️ Enhanced consumer heartbeat monitoring

### Admin API Plan (Future)

**REST Endpoints**:
```
POST   /api/v1/queues                 - Create queue
GET    /api/v1/queues                 - List all queues
GET    /api/v1/queues/{name}          - Get queue details
DELETE /api/v1/queues/{name}          - Delete queue
GET    /api/v1/queues/{name}/stats    - Get queue metrics
GET    /api/v1/queues/{name}/consumers - List consumers
POST   /api/v1/queues/{name}/purge    - Purge all messages
```

### Future Enhancements (Post-MVP)

- [ ] Message priority queues
- [ ] Scheduled message delivery
- [ ] Batch acknowledgments
- [ ] Consumer prefetching
- [ ] Schema validation
- [ ] Queue templates
- [ ] Advanced routing (topic exchange, header routing)

---

## Quick Start

### Publishing to a Queue

```go
// Standard MQTT publish with queue topic prefix
client.Publish("$queue/tasks/image-processing", qos, retain, payload)

// With MQTT v5 properties for ordering
props := &packets.PublishProperties{
    UserProperty: []packets.UserProperty{
        {Key: "partition-key", Value: "user-123"},
    },
}
client.PublishWithProperties("$queue/tasks/image-processing", qos, retain, payload, props)
```

### Consuming from a Queue

```go
// Subscribe with consumer group
subProps := &packets.SubscribeProperties{
    UserProperty: []packets.UserProperty{
        {Key: "consumer-group", Value: "image-workers-v1"},
    },
}
client.SubscribeWithProperties("$queue/tasks/image-processing", qos, subProps)

// Receive message
msg := <-client.Messages()

// Process message
err := processImage(msg.Payload)

// Acknowledge success (using offset from message properties)
if err == nil {
    ackProps := &packets.PublishProperties{
        UserProperty: []packets.UserProperty{
            {Key: "partition", Value: extractPartition(msg)},
            {Key: "offset", Value: extractOffset(msg)},
        },
    }
    client.PublishWithProperties("$queue/tasks/image-processing/$ack", 1, false, nil, ackProps)
} else {
    // Negative acknowledgment (triggers retry)
    client.PublishWithProperties("$queue/tasks/image-processing/$nack", 1, false, nil, ackProps)
}
```

### Broker Initialization

```go
// Initialize broker with queue manager
broker := broker.NewBroker(store, nil, logger, nil, nil, nil, nil)

// Create log-based queue storage
logStore, _ := log.NewStore(log.Config{
    BaseDir:        "/var/lib/fluxmq/queues",
    MaxSegmentSize: 64 * 1024 * 1024,  // 64MB segments
    SyncInterval:   time.Second,
    Compression:    log.CompressionS2,  // Snappy-compatible
})

// Create queue manager with log store
queueMgr, _ := queue.NewManager(queue.Config{
    LogStore:           logStore,
    ConsumerGroupStore: logStore,
    Broker:             broker,
})

// Set queue manager on broker
broker.SetQueueManager(queueMgr)

// Now clients can use $queue/* topics via standard MQTT
```

---

## Best Practices

### 1. Choose Appropriate Ordering Mode

- **Use `none`** for high throughput, independent tasks
- **Use `partition`** for user-specific or session-based operations
- **Use `strict`** only when global ordering is critical (low throughput)

### 2. Set Partition Keys Wisely

**Good partition keys**:
- User ID (distribute by user)
- Tenant ID (multi-tenant isolation)
- Session ID (maintain session context)

**Bad partition keys**:
- Timestamp (hotspot on latest partition)
- Random UUID (defeats ordering purpose)

### 3. Handle Idempotency

Queue provides at-least-once delivery. Make consumers idempotent:

```go
func processMessage(msg *Message) error {
    // Check if already processed
    if isProcessed(msg.ID) {
        return nil  // Already done, ack and skip
    }

    // Process
    result := doWork(msg.Payload)

    // Mark as processed
    recordProcessed(msg.ID)

    return result
}
```

### 4. Monitor DLQ

Set up alerts for DLQ messages:

```json
{
  "dlq_config": {
    "alert_webhook": "https://monitoring.example.com/alerts/queue-failure"
  }
}
```

Regularly review DLQ for patterns:
- Same message failing repeatedly → code bug
- Spike in DLQ messages → external dependency issue

### 5. Right-Size Partitions

**Recommendation**: `partitions = 2 × max_consumers`

### 6. Set Realistic Timeouts

Configure based on actual processing time:
```json
{
  "performance": {
    "delivery_timeout": "60s"
  },
  "retry_policy": {
    "total_timeout": "6h"
  }
}
```

---

## Troubleshooting

### Messages Not Being Delivered

**Check**:
1. Consumer subscribed with correct consumer group?
2. Queue has available partitions?
3. Consumer still connected? (check heartbeat)
4. Messages in DLQ? (check DLQ topic)

### Messages Delivered Multiple Times

**Causes**:
- Consumer not sending ack
- Consumer sending ack to wrong topic
- Ack timeout too short

**Fix**:
```go
// Ensure ack sent with correct partition and offset
ackProps := &packets.PublishProperties{
    UserProperty: []packets.UserProperty{
        {Key: "partition", Value: msg.Properties["partition"]},
        {Key: "offset", Value: msg.Properties["offset"]},
    },
}
client.Publish("$queue/tasks/image/$ack", 1, false, nil, ackProps)
```

### Queue Depth Growing

**Causes**:
- Consumers slower than producers
- Consumers crashed or stuck
- Retry loop (messages failing and retrying)

**Check**:
```bash
curl http://broker:8080/api/v1/queues/\$queue\$tasks\$image/stats
```

**Fix**:
- Scale up consumers
- Optimize consumer processing
- Check DLQ for poison messages

### Cluster Failover Slow

**Causes**:
- etcd lease timeout too long
- Large partition state to transfer
- Network latency between nodes

**Tune**:
```yaml
cluster:
  etcd_lease_ttl: 5s  # Faster failure detection
  partition_sync_timeout: 10s
```

---

## Monitoring and Metrics

### Prometheus Metrics

```
# Queue depth
mqtt_queue_depth{queue="$queue/tasks/image-processing"} 150

# Throughput
mqtt_queue_enqueue_total{queue="..."} 10500
mqtt_queue_dequeue_total{queue="..."} 10450
mqtt_queue_ack_total{queue="..."} 10400
mqtt_queue_nack_total{queue="..."} 50

# Latency
mqtt_queue_delivery_latency_seconds{queue="...",quantile="0.5"} 0.005
mqtt_queue_delivery_latency_seconds{queue="...",quantile="0.99"} 0.015

# Errors
mqtt_queue_dlq_total{queue="...",reason="max_retries"} 5
mqtt_queue_dlq_total{queue="...",reason="timeout"} 2

# Consumer health
mqtt_queue_active_consumers{queue="...",group="workers-v1"} 3
```

---

## Use Cases

### 1. Task Queue

**Scenario**: Distribute image processing across worker pool

```
Producer: Web API
Queue: $queue/tasks/image-processing
Consumers: 5 worker instances (group: "workers-v1")
Ordering: None (partition-key not needed)
```

### 2. User Event Stream

**Scenario**: Process user lifecycle events in order

```
Producer: User service
Queue: $queue/events/user-lifecycle
Consumers: 3 analytics workers
Ordering: Partition (partition-key: user-id)
```

### 3. Request/Response

**Scenario**: Synchronous auth token validation

```
Requester: API Gateway
Queue: $queue/requests/auth-validate
Responder: Auth service
Pattern: Request/response with correlation-id
```

### 4. Dead-Letter Monitoring

**Scenario**: Track and retry failed email deliveries

```
Original: $queue/tasks/email-sending
DLQ: $dlq/tasks/email-sending
Alert: Webhook to PagerDuty on DLQ message
Manual: Retry via admin API after fixing email template
```

---

## Migration Guide

### From Standard MQTT to Queues

**Before** (ephemeral pub/sub):
```go
client.Subscribe("tasks/image-processing", qos)
```

**After** (durable queue):
```go
props := &packets.SubscribeProperties{
    UserProperty: []packets.UserProperty{
        {Key: "consumer-group", Value: "workers"},
    },
}
client.SubscribeWithProperties("$queue/tasks/image-processing", qos, props)
```

### From Shared Subscriptions to Queues

**Before** (MQTT v5 shared subscriptions):
```go
client.Subscribe("$share/workers/tasks/image-processing", qos)
```

**After** (durable queue with consumer group):
```go
props := &packets.SubscribeProperties{
    UserProperty: []packets.UserProperty{
        {Key: "consumer-group", Value: "workers"},
    },
}
client.SubscribeWithProperties("$queue/tasks/image-processing", qos, props)
```

**Benefits**:
- Messages persist if all workers offline
- Automatic retry on failure
- Dead-letter queue for poison messages
- Ordering guarantees

---

## Support

For questions or issues with durable queues:
- GitHub Issues: https://github.com/absmach/fluxmq/issues
- Documentation: https://github.com/absmach/fluxmq/docs
