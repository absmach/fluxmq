# Durable Queue Implementation Plan

## Overview

This document outlines the implementation plan for adding durable queue functionality to the MQTT broker while maintaining 100% MQTT protocol compatibility. The queue system will be a first-class feature alongside traditional pub/sub messaging.

## Design Principles

1. **Full MQTT Compatibility**: All existing MQTT functionality remains unchanged
2. **Maximum Decoupling**: Queue implementation in separate packages with minimal broker modifications
3. **Protocol-Native**: Queue features accessible via MQTT v5 topics and properties (no external APIs required for basic usage)
4. **Pluggable**: Queue storage backend uses same interface pattern as existing stores
5. **Observable**: Built-in metrics and monitoring from day one

## Architecture Summary

```
┌─────────────────────────────────────────────────────────┐
│ MQTT Protocol Layer (Unchanged)                         │
│ - V3Handler, V5Handler                                   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│ Broker Core (Minimal Changes)                           │
│ - Add QueueManager injection                            │
│ - Route $queue/* topics to QueueManager                 │
│ - Route $ack/$nack topics to QueueManager               │
└────────────────────┬────────────────────────────────────┘
                     │
      ┌──────────────┴──────────────┐
      │                             │
┌─────▼──────────────┐    ┌─────────▼────────────────────┐
│ Topic Router       │    │ Queue Manager (NEW)          │
│ (Unchanged)        │    │ Package: queue/              │
└────────────────────┘    └──────────────────────────────┘
                                      │
                          ┌───────────┴───────────┐
                          │                       │
                    ┌─────▼─────┐         ┌──────▼──────┐
                    │ Queue     │         │ Consumer    │
                    │ Store     │         │ Group Mgr   │
                    │ (NEW)     │         │ (NEW)       │
                    └───────────┘         └─────────────┘
```

## Topic Namespace Design

### Standard MQTT Topics (Existing Behavior - No Changes)
```
sensors/temperature              → Normal pub/sub
events/user/created             → Normal pub/sub + retained
$share/web/api/responses        → Shared subscription (ephemeral)
```

### Queue Topics (New Durable Queue Behavior)
```
$queue/tasks/image-processing           → Durable queue
$queue/tasks/email-sending              → Durable queue
$queue/events/user-lifecycle            → Durable queue
```

### Acknowledgment Topics (New)
```
$queue/tasks/image-processing/$ack      → Success acknowledgment
$queue/tasks/image-processing/$nack     → Failure (retry)
$queue/tasks/image-processing/$reject   → Reject (move to DLQ)
```

### Admin Topics (New - Optional for Phase 4)
```
$admin/queue/create                     → Create queue with config
$admin/queue/tasks/image/config        → Update queue settings
$admin/queue/tasks/image/stats         → Queue metrics stream
```

## Message Flow Examples

### Example 1: Basic Queue Publish & Consume

**Publisher**:
```
PUBLISH to: $queue/tasks/image-processing
Payload: {"image_url": "...", "user_id": 123}
QoS: 1
User Properties:
  - message-id: "msg-uuid-12345"
  - partition-key: "user-123"  (optional, for ordering)
```

**Broker**:
1. Detects `$queue/` prefix
2. Routes to QueueManager instead of standard router
3. QueueManager.Enqueue() stores message persistently
4. Returns PUBACK to publisher

**Consumer** (subscribed to `$queue/tasks/image-processing`):
1. Subscribes with User Property: `consumer-group=image-workers-v1`
2. QueueManager assigns consumer to partition
3. Consumer receives message via PUBLISH packet
4. Processes image
5. Publishes to `$queue/tasks/image-processing/$ack` with `message-id` property
6. QueueManager marks message as acked and deletes from storage

### Example 2: Failure & Retry

**Consumer crashes after receiving message, before acking**:
1. QueueManager has message in "delivered" state with timeout (30s)
2. Timeout expires, message state → "retry-1"
3. After backoff delay (5s), message re-delivered to another consumer in group
4. Consumer successfully processes and acks
5. Message deleted

### Example 3: Dead Letter Queue

**Message fails 10 times over 3 hours**:
1. Each retry doubles backoff (5s, 10s, 20s, 40s, 80s, 160s, 300s, ...)
2. After 10 retries or 3h total elapsed time: move to DLQ
3. Message published to `$queue/dlq/tasks/image-processing`
4. Webhook alert triggered (if configured)
5. Manual intervention required

## Implementation Phases

---

## Phase 1: Core Queue Infrastructure (Single Node)

**Goal**: Basic durable queue working on single broker instance

**Duration**: 10-12 days

### 1.1 Queue Storage Layer

**New Package**: `queue/storage/`

**Files to Create**:
- `queue/storage/storage.go` - Interfaces and domain types
- `queue/storage/memory/memory.go` - In-memory implementation (for testing)
- `queue/storage/badger/badger.go` - BadgerDB implementation
- `queue/storage/message.go` - Queue message wrapper

**Interfaces**:
```go
// QueueStore manages queue metadata and configuration
type QueueStore interface {
    Create(ctx context.Context, config QueueConfig) error
    Get(ctx context.Context, queueName string) (*QueueConfig, error)
    Delete(ctx context.Context, queueName string) error
    List(ctx context.Context) ([]QueueConfig, error)
}

// MessageStore manages queue messages
type MessageStore interface {
    Enqueue(ctx context.Context, queueName string, msg *QueueMessage) error
    Dequeue(ctx context.Context, queueName string, partitionID int) (*QueueMessage, error)
    Ack(ctx context.Context, queueName, messageID string) error
    Nack(ctx context.Context, queueName, messageID string) error
    Reject(ctx context.Context, queueName, messageID string) error
    GetInflight(ctx context.Context, queueName string) ([]*QueueMessage, error)
}

// ConsumerStore manages consumer group state
type ConsumerStore interface {
    RegisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
    UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
    GetConsumers(ctx context.Context, queueName, groupID string) ([]Consumer, error)
    UpdateOffset(ctx context.Context, queueName string, partitionID int, offset uint64) error
    GetOffset(ctx context.Context, queueName string, partitionID int) (uint64, error)
}
```

**BadgerDB Key Schema**:
```
queue:meta:{queueName} → QueueConfig (JSON)
queue:msg:{queueName}:{partitionID}:{seq} → QueueMessage (protobuf)
queue:inflight:{queueName}:{messageID} → DeliveryState (JSON)
queue:dlq:{queueName}:{messageID} → QueueMessage (protobuf)
queue:consumer:{queueName}:{groupID}:{consumerID} → ConsumerState (JSON)
queue:offset:{queueName}:{partitionID} → uint64
```

**Estimated LOC**: 800-1000
**Duration**: 3-4 days
**Testing**: Unit tests for all storage operations

---

### 1.2 Queue Manager

**New Package**: `queue/`

**Files to Create**:
- `queue/manager.go` - Main queue manager
- `queue/queue.go` - Single queue instance
- `queue/partition.go` - Partition management
- `queue/config.go` - Configuration types

**Core Types**:
```go
type Manager struct {
    queues       map[string]*Queue
    queueStore   storage.QueueStore
    messageStore storage.MessageStore
    consumerStore storage.ConsumerStore
    mu           sync.RWMutex
}

type Queue struct {
    name       string
    config     QueueConfig
    partitions []*Partition
    consumers  *ConsumerGroupManager
}

type Partition struct {
    id            int
    queue         *Queue
    nextSeq       uint64
    assignedTo    string  // consumerID
}
```

**Key Methods**:
```go
func (m *Manager) Enqueue(ctx context.Context, topic string, msg *storage.QueueMessage) error
func (m *Manager) Subscribe(ctx context.Context, topic, clientID, groupID string) error
func (m *Manager) Unsubscribe(ctx context.Context, topic, clientID string) error
func (m *Manager) Ack(ctx context.Context, topic, messageID string) error
func (m *Manager) Nack(ctx context.Context, topic, messageID string) error
```

**Estimated LOC**: 600-800
**Duration**: 3-4 days
**Testing**: Integration tests with in-memory storage

---

### 1.3 Consumer Group Manager

**File**: `queue/consumer_group.go`

**Responsibilities**:
- Track active consumers per group
- Assign partitions to consumers
- Rebalance on consumer join/leave
- Deliver messages to assigned consumers

**Core Logic**:
```go
type ConsumerGroupManager struct {
    groups map[string]*ConsumerGroup
}

type ConsumerGroup struct {
    id         string
    consumers  map[string]*Consumer  // consumerID -> Consumer
    partitions map[int]string        // partitionID -> consumerID
}

type Consumer struct {
    id              string
    clientID        string
    assignedParts   []int
    deliveryChan    chan *storage.QueueMessage
    lastHeartbeat   time.Time
}
```

**Partition Assignment Strategy** (Phase 1 - Simple):
- Round-robin assignment
- Rebalance only on explicit join/leave (no automatic failure detection yet)

**Estimated LOC**: 400-500
**Duration**: 2-3 days
**Testing**: Consumer assignment and rebalancing tests

---

### 1.4 Broker Integration

**Files to Modify**:
- `broker/broker.go` - Add QueueManager field and routing logic
- `broker/v5handler.go` - Extract User Properties for queue operations
- `broker/v3handler.go` - Add queue support (limited, no User Properties)

**Changes in broker/broker.go**:

```go
type Broker struct {
    // ... existing fields ...
    queueManager *queue.Manager  // NEW
}

func New(opts ...Option) (*Broker, error) {
    // ... existing code ...

    // Initialize queue manager if enabled
    if cfg.QueueEnabled {
        qm, err := queue.NewManager(queue.Config{
            QueueStore:    queueStore,
            MessageStore:  messageStore,
            ConsumerStore: consumerStore,
        })
        if err != nil {
            return nil, err
        }
        b.queueManager = qm
    }

    return b, nil
}

// Modify Publish to route queue topics
func (b *Broker) Publish(ctx context.Context, msg *storage.Message) error {
    // NEW: Check if topic is queue topic
    if b.queueManager != nil && isQueueTopic(msg.Topic) {
        queueMsg := toQueueMessage(msg)
        return b.queueManager.Enqueue(ctx, msg.Topic, queueMsg)
    }

    // Existing pub/sub logic unchanged
    if msg.Retain {
        // ... existing retained message handling ...
    }

    return b.distribute(ctx, msg)
}

// Modify Subscribe to handle queue subscriptions
func (b *Broker) Subscribe(ctx context.Context, clientID, topic string, qos byte, props *packets.SubscribeProperties) error {
    // NEW: Check if topic is queue topic
    if b.queueManager != nil && isQueueTopic(topic) {
        groupID := extractConsumerGroup(props)  // From User Properties
        return b.queueManager.Subscribe(ctx, topic, clientID, groupID)
    }

    // Existing subscription logic unchanged
    return b.router.Subscribe(clientID, topic, qos)
}

// NEW: Helper to route ack/nack messages
func (b *Broker) handleQueueAck(ctx context.Context, topic string, props *packets.PublishProperties) error {
    messageID := extractMessageID(props)
    queueTopic := extractQueueTopicFromAck(topic)  // $queue/foo/$ack -> $queue/foo

    if strings.HasSuffix(topic, "/$ack") {
        return b.queueManager.Ack(ctx, queueTopic, messageID)
    } else if strings.HasSuffix(topic, "/$nack") {
        return b.queueManager.Nack(ctx, queueTopic, messageID)
    } else if strings.HasSuffix(topic, "/$reject") {
        return b.queueManager.Reject(ctx, queueTopic, messageID)
    }

    return fmt.Errorf("invalid ack topic: %s", topic)
}
```

**Utility Functions** (new file: `broker/queue_utils.go`):
```go
func isQueueTopic(topic string) bool {
    return strings.HasPrefix(topic, "$queue/")
}

func isQueueAckTopic(topic string) bool {
    return strings.HasSuffix(topic, "/$ack") ||
           strings.HasSuffix(topic, "/$nack") ||
           strings.HasSuffix(topic, "/$reject")
}

func extractConsumerGroup(props *packets.SubscribeProperties) string {
    if props == nil || props.UserProperty == nil {
        return ""  // Use clientID prefix as fallback
    }
    for _, prop := range props.UserProperty {
        if prop.Key == "consumer-group" {
            return prop.Value
        }
    }
    return ""
}

func extractMessageID(props *packets.PublishProperties) string {
    if props == nil || props.UserProperty == nil {
        return ""
    }
    for _, prop := range props.UserProperty {
        if prop.Key == "message-id" {
            return prop.Value
        }
    }
    return ""
}

func extractPartitionKey(props *packets.PublishProperties) string {
    if props == nil || props.UserProperty == nil {
        return ""
    }
    for _, prop := range props.UserProperty {
        if prop.Key == "partition-key" {
            return prop.Value
        }
    }
    return ""
}
```

**Estimated LOC**: 300-400 (mostly helpers and routing logic)
**Duration**: 2 days
**Testing**: Integration tests with broker

---

### 1.5 Message Delivery Loop

**File**: `queue/delivery.go`

**Responsibility**: Background goroutine per partition that delivers queued messages to assigned consumer.

**Core Logic**:
```go
type Delivery struct {
    queue     *Queue
    partition *Partition
    broker    BrokerInterface  // For publishing MQTT messages
    stopCh    chan struct{}
}

func (d *Delivery) Run(ctx context.Context) {
    ticker := time.NewTicker(100 * time.Millisecond)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-d.stopCh:
            return
        case <-ticker.C:
            d.deliverPendingMessages(ctx)
        }
    }
}

func (d *Delivery) deliverPendingMessages(ctx context.Context) {
    // 1. Check if partition has assigned consumer
    consumer := d.partition.assignedConsumer()
    if consumer == nil {
        return  // No consumer, skip
    }

    // 2. Dequeue next message for this partition
    msg, err := d.queue.messageStore.Dequeue(ctx, d.queue.name, d.partition.id)
    if err != nil || msg == nil {
        return  // No messages or error
    }

    // 3. Mark as inflight
    deliveryState := &DeliveryState{
        MessageID:   msg.ID,
        QueueName:   d.queue.name,
        PartitionID: d.partition.id,
        ConsumerID:  consumer.id,
        DeliveredAt: time.Now(),
        Timeout:     time.Now().Add(30 * time.Second),
        RetryCount:  msg.RetryCount,
    }
    d.queue.messageStore.MarkInflight(ctx, deliveryState)

    // 4. Publish MQTT message to consumer's client
    mqttMsg := toMQTTMessage(msg)
    mqttMsg.Topic = d.queue.name
    d.broker.DeliverToSession(ctx, consumer.clientID, mqttMsg)
}
```

**Estimated LOC**: 300-400
**Duration**: 2 days
**Testing**: Message delivery tests with mock broker

---

### 1.6 Basic Ack/Nack Handling

**File**: `queue/ack_handler.go`

**Responsibilities**:
- Process ack messages
- Process nack messages
- Remove from inflight tracking
- Delete or requeue messages

**Core Logic**:
```go
func (m *Manager) Ack(ctx context.Context, queueTopic, messageID string) error {
    queue := m.getQueue(queueTopic)
    if queue == nil {
        return ErrQueueNotFound
    }

    // 1. Get inflight message
    msg, err := queue.messageStore.GetInflight(ctx, queue.name, messageID)
    if err != nil {
        return err
    }

    // 2. Remove from inflight tracking
    if err := queue.messageStore.RemoveInflight(ctx, queue.name, messageID); err != nil {
        return err
    }

    // 3. Delete message from storage
    return queue.messageStore.Delete(ctx, queue.name, messageID)
}

func (m *Manager) Nack(ctx context.Context, queueTopic, messageID string) error {
    queue := m.getQueue(queueTopic)
    if queue == nil {
        return ErrQueueNotFound
    }

    // 1. Get inflight message
    msg, err := queue.messageStore.GetInflight(ctx, queue.name, messageID)
    if err != nil {
        return err
    }

    // 2. Increment retry count
    msg.RetryCount++
    msg.State = StateRetry
    msg.NextRetryAt = time.Now().Add(calculateBackoff(msg.RetryCount))

    // 3. Update in storage
    return queue.messageStore.Update(ctx, msg)
}
```

**Estimated LOC**: 200-300
**Duration**: 1 day
**Testing**: Ack/nack flow tests

---

### Phase 1 Summary

**Total Duration**: 10-12 days
**Total LOC**: ~2,600-3,400

**Deliverables**:
- ✅ Durable queue storage (BadgerDB)
- ✅ Queue manager with basic operations
- ✅ Consumer group assignment (simple round-robin)
- ✅ Message delivery loop
- ✅ Ack/nack handling
- ✅ Broker integration with minimal code changes
- ✅ Unit and integration tests

**Testing Checklist**:
- [ ] Publish to queue topic persists message
- [ ] Consumer subscription assigns to consumer group
- [ ] Message delivered to consumer via MQTT PUBLISH
- [ ] Ack removes message from queue
- [ ] Nack requeues message
- [ ] Multiple consumers in same group share load
- [ ] Broker restart preserves queued messages

**Known Limitations (to be addressed in later phases)**:
- No retry timeout checking (messages stay inflight forever if not acked)
- No dead-letter queue
- No partition-based ordering (single partition only)
- No cluster support
- No consumer failure detection
- No metrics/monitoring

---

## Phase 2: Retry, DLQ, and Ordering

**Goal**: Production-ready failure handling and message ordering

**Duration**: 8-10 days

### 2.1 Retry State Machine

**File**: `queue/retry.go`

**Responsibilities**:
- Monitor inflight messages for timeout
- Move timed-out messages to retry state
- Calculate exponential backoff
- Enforce max retries and total timeout

**Core Logic**:
```go
type RetryManager struct {
    queues        map[string]*Queue
    checkInterval time.Duration
    stopCh        chan struct{}
}

func (rm *RetryManager) Run(ctx context.Context) {
    ticker := time.NewTicker(rm.checkInterval)  // Default: 1s
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-rm.stopCh:
            return
        case <-ticker.C:
            rm.checkInflightTimeouts(ctx)
        }
    }
}

func (rm *RetryManager) checkInflightTimeouts(ctx context.Context) {
    for _, queue := range rm.queues {
        inflight, err := queue.messageStore.GetInflight(ctx, queue.name)
        if err != nil {
            continue
        }

        for _, msg := range inflight {
            if time.Now().After(msg.Timeout) {
                rm.handleTimeout(ctx, queue, msg)
            }
        }
    }
}

func (rm *RetryManager) handleTimeout(ctx context.Context, queue *Queue, msg *storage.QueueMessage) {
    // Check if max retries exceeded
    if msg.RetryCount >= queue.config.RetryPolicy.MaxRetries {
        rm.moveToDLQ(ctx, queue, msg, "max retries exceeded")
        return
    }

    // Check if total time exceeded
    totalElapsed := time.Since(msg.CreatedAt)
    if totalElapsed > queue.config.RetryPolicy.TotalTimeout {
        rm.moveToDLQ(ctx, queue, msg, "total timeout exceeded")
        return
    }

    // Schedule retry
    msg.RetryCount++
    msg.State = StateRetry
    msg.NextRetryAt = time.Now().Add(rm.calculateBackoff(msg.RetryCount, queue.config.RetryPolicy))

    queue.messageStore.Update(ctx, msg)
}

func (rm *RetryManager) calculateBackoff(retryCount int, policy RetryPolicy) time.Duration {
    backoff := float64(policy.InitialBackoff) * math.Pow(policy.BackoffMultiplier, float64(retryCount-1))

    if backoff > float64(policy.MaxBackoff) {
        backoff = float64(policy.MaxBackoff)
    }

    return time.Duration(backoff)
}
```

**Estimated LOC**: 300-400
**Duration**: 2 days
**Testing**: Timeout and retry tests with mock time

---

### 2.2 Dead Letter Queue

**File**: `queue/dlq.go`

**Responsibilities**:
- Move failed messages to DLQ topic
- Trigger alerts (webhook, log)
- Provide DLQ inspection API

**Core Logic**:
```go
type DLQManager struct {
    messageStore storage.MessageStore
    alertHandler AlertHandler
}

func (d *DLQManager) MoveToDLQ(ctx context.Context, queue *Queue, msg *storage.QueueMessage, reason string) error {
    // 1. Create DLQ message with metadata
    dlqMsg := &storage.QueueMessage{
        ID:             msg.ID,
        Payload:        msg.Payload,
        Topic:          queue.name,
        OriginalTopic:  queue.name,
        FailureReason:  reason,
        RetryCount:     msg.RetryCount,
        FirstAttempt:   msg.CreatedAt,
        LastAttempt:    time.Now(),
        MovedToDLQAt:   time.Now(),
    }

    // 2. Store in DLQ storage
    dlqTopic := "$queue/dlq/" + strings.TrimPrefix(queue.name, "$queue/")
    if err := d.messageStore.EnqueueDLQ(ctx, dlqTopic, dlqMsg); err != nil {
        return err
    }

    // 3. Remove from original queue
    if err := queue.messageStore.Delete(ctx, queue.name, msg.ID); err != nil {
        return err
    }

    // 4. Trigger alert
    if queue.config.DLQConfig.AlertWebhook != "" {
        d.alertHandler.Send(queue.config.DLQConfig.AlertWebhook, dlqMsg)
    }

    return nil
}
```

**Alert Handler** (new file: `queue/alerts.go`):
```go
type AlertHandler interface {
    Send(webhookURL string, msg *storage.QueueMessage) error
}

type HTTPAlertHandler struct {
    client *http.Client
}

func (h *HTTPAlertHandler) Send(webhookURL string, msg *storage.QueueMessage) error {
    payload := map[string]interface{}{
        "queue":         msg.Topic,
        "message_id":    msg.ID,
        "failure_reason": msg.FailureReason,
        "retry_count":   msg.RetryCount,
        "moved_at":      msg.MovedToDLQAt,
    }

    body, _ := json.Marshal(payload)
    resp, err := h.client.Post(webhookURL, "application/json", bytes.NewReader(body))
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode >= 400 {
        return fmt.Errorf("alert webhook failed: %d", resp.StatusCode)
    }

    return nil
}
```

**Estimated LOC**: 300-400
**Duration**: 2 days
**Testing**: DLQ movement and alert tests

---

### 2.3 Partition-Based Ordering

**File**: `queue/partitioning.go`

**Responsibilities**:
- Hash partition key to partition ID
- Maintain strict ordering within partition
- Assign consumers to partitions

**Core Logic**:
```go
type PartitionStrategy interface {
    GetPartition(partitionKey string, numPartitions int) int
}

type HashPartitionStrategy struct{}

func (h *HashPartitionStrategy) GetPartition(partitionKey string, numPartitions int) int {
    if partitionKey == "" {
        // No partition key = random assignment
        return rand.Intn(numPartitions)
    }

    hash := fnv.New32a()
    hash.Write([]byte(partitionKey))
    return int(hash.Sum32()) % numPartitions
}

// Update Queue to manage multiple partitions
type Queue struct {
    name       string
    config     QueueConfig
    partitions []*Partition
    strategy   PartitionStrategy
}

func (q *Queue) GetPartitionForMessage(msg *storage.QueueMessage) int {
    return q.strategy.GetPartition(msg.PartitionKey, len(q.partitions))
}

// Enqueue assigns to partition based on key
func (m *Manager) Enqueue(ctx context.Context, topic string, msg *storage.QueueMessage) error {
    queue := m.getOrCreateQueue(topic)

    partitionID := queue.GetPartitionForMessage(msg)
    msg.PartitionID = partitionID

    return queue.messageStore.Enqueue(ctx, queue.name, msg)
}
```

**Partition State Tracking**:
```go
type Partition struct {
    id            int
    queue         *Queue
    nextSeq       uint64
    assignedTo    string  // consumerID currently assigned
    lastDelivery  time.Time
}

func (p *Partition) AssignTo(consumerID string) {
    p.assignedTo = consumerID
}

func (p *Partition) IsAssigned() bool {
    return p.assignedTo != ""
}
```

**Consumer Rebalancing with Partitions**:
```go
func (cg *ConsumerGroup) Rebalance() {
    consumers := cg.getActiveConsumers()
    partitions := cg.queue.partitions

    // Simple strategy: divide partitions evenly
    partitionsPerConsumer := len(partitions) / len(consumers)
    remainder := len(partitions) % len(consumers)

    idx := 0
    for i, consumer := range consumers {
        count := partitionsPerConsumer
        if i < remainder {
            count++
        }

        consumer.assignedParts = make([]int, count)
        for j := 0; j < count; j++ {
            consumer.assignedParts[j] = partitions[idx].id
            partitions[idx].AssignTo(consumer.id)
            idx++
        }
    }
}
```

**Estimated LOC**: 400-500
**Duration**: 3 days
**Testing**: Partition assignment and ordering tests

---

### 2.4 Queue Configuration

**File**: `queue/config.go`

**Configuration Structure**:
```go
type QueueConfig struct {
    Name        string
    Partitions  int
    Ordering    OrderingMode  // "none", "partition", "strict"

    RetryPolicy RetryPolicy
    DLQConfig   DLQConfig

    // Limits
    MaxMessageSize   int
    MaxQueueDepth    int
    MessageTTL       time.Duration

    // Performance
    DeliveryTimeout  time.Duration  // How long to wait for ack
    BatchSize        int            // Messages per delivery batch
}

type OrderingMode string

const (
    OrderingNone      OrderingMode = "none"      // No ordering guarantees
    OrderingPartition OrderingMode = "partition" // FIFO per partition key
    OrderingStrict    OrderingMode = "strict"    // Global FIFO (single partition)
)

type RetryPolicy struct {
    MaxRetries         int
    InitialBackoff     time.Duration
    MaxBackoff         time.Duration
    BackoffMultiplier  float64
    TotalTimeout       time.Duration
}

type DLQConfig struct {
    Enabled      bool
    Topic        string  // DLQ topic name (auto-generated if empty)
    AlertWebhook string
}
```

**Default Configurations**:
```go
var DefaultQueueConfig = QueueConfig{
    Partitions:       10,
    Ordering:         OrderingPartition,
    MaxMessageSize:   1024 * 1024,  // 1MB
    MaxQueueDepth:    100000,
    MessageTTL:       7 * 24 * time.Hour,  // 7 days
    DeliveryTimeout:  30 * time.Second,
    BatchSize:        1,

    RetryPolicy: RetryPolicy{
        MaxRetries:        10,
        InitialBackoff:    5 * time.Second,
        MaxBackoff:        5 * time.Minute,
        BackoffMultiplier: 2.0,
        TotalTimeout:      3 * time.Hour,
    },

    DLQConfig: DLQConfig{
        Enabled: true,
    },
}
```

**Configuration via MQTT v5 Admin Topics** (optional):
```go
// Client publishes to $admin/queue/create
// Payload: JSON-encoded QueueConfig
func (m *Manager) handleAdminCreate(ctx context.Context, payload []byte) error {
    var config QueueConfig
    if err := json.Unmarshal(payload, &config); err != nil {
        return err
    }

    return m.CreateQueue(ctx, config)
}
```

**Estimated LOC**: 200-300
**Duration**: 1 day
**Testing**: Configuration parsing and validation tests

---

### 2.5 Metrics and Monitoring

**File**: `queue/metrics.go`

**Metrics to Track**:
```go
type QueueMetrics struct {
    // Queue depth
    TotalMessages      int64
    InflightMessages   int64
    DLQMessages        int64

    // Throughput
    EnqueueRate        float64  // msgs/sec
    DequeueRate        float64
    AckRate            float64
    NackRate           float64

    // Latency
    AvgDeliveryLatency time.Duration  // Time from enqueue to delivery
    AvgProcessLatency  time.Duration  // Time from delivery to ack

    // Errors
    RetryCount         int64
    DLQCount           int64

    // Consumer health
    ActiveConsumers    int
    PartitionsAssigned map[string]int  // consumerID -> partition count
}
```

**Prometheus Integration** (new file: `queue/prometheus.go`):
```go
var (
    queueDepth = promauto.NewGaugeVec(
        prometheus.GaugeOpts{
            Name: "mqtt_queue_depth",
            Help: "Number of messages in queue",
        },
        []string{"queue"},
    )

    queueEnqueueTotal = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mqtt_queue_enqueue_total",
            Help: "Total messages enqueued",
        },
        []string{"queue"},
    )

    queueDeliveryLatency = promauto.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "mqtt_queue_delivery_latency_seconds",
            Help:    "Time from enqueue to delivery",
            Buckets: prometheus.DefBuckets,
        },
        []string{"queue"},
    )

    queueDLQTotal = promauto.NewCounterVec(
        prometheus.CounterOpts{
            Name: "mqtt_queue_dlq_total",
            Help: "Total messages moved to DLQ",
        },
        []string{"queue", "reason"},
    )
)
```

**Estimated LOC**: 300-400
**Duration**: 2 days
**Testing**: Metrics collection tests

---

### Phase 2 Summary

**Total Duration**: 8-10 days
**Total LOC**: ~1,500-2,000

**Deliverables**:
- ✅ Retry state machine with exponential backoff
- ✅ Dead-letter queue with alerts
- ✅ Partition-based ordering
- ✅ Queue configuration system
- ✅ Metrics and Prometheus integration

**Testing Checklist**:
- [ ] Message timeout triggers retry
- [ ] Retry backoff increases exponentially
- [ ] Max retries exceeded moves to DLQ
- [ ] Total timeout exceeded moves to DLQ
- [ ] DLQ alert webhook triggered
- [ ] Messages with same partition key delivered in order
- [ ] Messages with different keys processed in parallel
- [ ] Queue depth metrics accurate
- [ ] Consumer rebalancing assigns partitions evenly

---

## Phase 3: Cluster Support

**Goal**: Distributed queues across multiple broker nodes

**Duration**: 10-12 days

### 3.1 Queue Ownership via etcd

**File**: `queue/cluster/ownership.go`

**Concept**: Each queue is "owned" by one broker node in the cluster. Ownership determined by consistent hashing.

**Core Logic**:
```go
type OwnershipManager struct {
    etcdClient *clientv3.Client
    nodeID     string
    ownedQueues map[string]*Queue
    mu          sync.RWMutex
}

func (om *OwnershipManager) GetOwner(queueName string) (string, error) {
    // Hash queue name to determine owner
    hash := consistentHash(queueName, om.getClusterNodes())
    return hash, nil
}

func (om *OwnershipManager) IsOwner(queueName string) bool {
    owner, _ := om.GetOwner(queueName)
    return owner == om.nodeID
}

func (om *OwnershipManager) WatchOwnership(ctx context.Context) {
    watchChan := om.etcdClient.Watch(ctx, "cluster/nodes", clientv3.WithPrefix())

    for watchResp := range watchChan {
        for _, event := range watchResp.Events {
            // Node joined or left cluster
            om.rebalanceQueues(ctx)
        }
    }
}

func (om *OwnershipManager) rebalanceQueues(ctx context.Context) {
    // Recalculate ownership for all queues
    allQueues := om.getAllQueueNames()

    for _, queueName := range allQueues {
        newOwner, _ := om.GetOwner(queueName)

        if newOwner == om.nodeID && !om.ownsQueue(queueName) {
            // We are new owner, take over queue
            om.takeoverQueue(ctx, queueName)
        } else if newOwner != om.nodeID && om.ownsQueue(queueName) {
            // We lost ownership, release queue
            om.releaseQueue(ctx, queueName)
        }
    }
}
```

**Estimated LOC**: 400-500
**Duration**: 3 days
**Testing**: Ownership assignment and failover tests

---

### 3.2 Cross-Node Message Routing

**File**: `queue/cluster/routing.go`

**Scenario**: Publisher connects to Node A, consumer subscribed to queue on Node B (owner).

**Flow**:
1. Publisher publishes to `$queue/tasks/image` on Node A
2. Node A detects it doesn't own this queue
3. Node A forwards message to Node B via gRPC
4. Node B enqueues message
5. Node B delivers to local consumer

**Core Logic**:
```go
type ClusterRouter struct {
    ownership *OwnershipManager
    grpcClients map[string]QueueServiceClient
}

func (cr *ClusterRouter) RouteEnqueue(ctx context.Context, queueName string, msg *storage.QueueMessage) error {
    owner, err := cr.ownership.GetOwner(queueName)
    if err != nil {
        return err
    }

    if owner == cr.ownership.nodeID {
        // We own this queue, enqueue locally
        return cr.localManager.Enqueue(ctx, queueName, msg)
    }

    // Forward to owner node via gRPC
    client := cr.grpcClients[owner]
    _, err = client.Enqueue(ctx, &pb.EnqueueRequest{
        QueueName: queueName,
        Message:   toProto(msg),
    })

    return err
}
```

**gRPC Service Definition** (new file: `queue/cluster/proto/queue.proto`):
```protobuf
syntax = "proto3";

package queue;

service QueueService {
    rpc Enqueue(EnqueueRequest) returns (EnqueueResponse);
    rpc Subscribe(SubscribeRequest) returns (SubscribeResponse);
    rpc Ack(AckRequest) returns (AckResponse);
    rpc Nack(NackRequest) returns (NackResponse);
}

message EnqueueRequest {
    string queue_name = 1;
    QueueMessage message = 2;
}

message QueueMessage {
    string id = 1;
    bytes payload = 2;
    string partition_key = 3;
    map<string, string> properties = 4;
}
```

**Estimated LOC**: 500-600 (including protobuf generated code)
**Duration**: 3 days
**Testing**: Cross-node message routing tests

---

### 3.3 Consumer Registration Across Cluster

**File**: `queue/cluster/consumer_registry.go`

**Scenario**: Consumer connects to Node A, but queue owned by Node B.

**Flow**:
1. Consumer subscribes to `$queue/tasks/image` on Node A
2. Node A detects Node B owns this queue
3. Node A registers consumer with Node B via gRPC
4. Node B adds consumer to consumer group and triggers rebalancing
5. Node B delivers messages to consumer via Node A (reverse proxy)

**Core Logic**:
```go
func (cr *ClusterRouter) RouteSubscribe(ctx context.Context, queueName, clientID, groupID string) error {
    owner, err := cr.ownership.GetOwner(queueName)
    if err != nil {
        return err
    }

    if owner == cr.ownership.nodeID {
        // We own this queue, subscribe locally
        return cr.localManager.Subscribe(ctx, queueName, clientID, groupID)
    }

    // Register consumer with owner node
    client := cr.grpcClients[owner]
    _, err = client.Subscribe(ctx, &pb.SubscribeRequest{
        QueueName:   queueName,
        ClientID:    clientID,
        GroupID:     groupID,
        ProxyNodeID: cr.ownership.nodeID,  // Messages should be sent back to us
    })

    return err
}

// On owner node: deliver message via proxy
func (d *Delivery) deliverViaProxy(ctx context.Context, msg *storage.QueueMessage, consumer *Consumer) error {
    if consumer.ProxyNodeID == "" {
        // Local consumer, deliver directly
        return d.broker.DeliverToSession(ctx, consumer.ClientID, toMQTTMessage(msg))
    }

    // Remote consumer, send via proxy node
    client := d.clusterRouter.grpcClients[consumer.ProxyNodeID]
    _, err := client.DeliverMessage(ctx, &pb.DeliverRequest{
        ClientID: consumer.ClientID,
        Message:  toProto(msg),
    })

    return err
}
```

**Estimated LOC**: 400-500
**Duration**: 3 days
**Testing**: Cross-node consumer registration and delivery tests

---

### 3.4 Partition Failover

**File**: `queue/cluster/failover.go`

**Scenario**: Owner node crashes, partitions need to migrate to new owner.

**Flow**:
1. Node B (owner of queue) crashes
2. etcd detects node failure (lease expiration)
3. Remaining nodes detect ownership change
4. Node C becomes new owner
5. Node C reads partition state from shared storage (BadgerDB or etcd)
6. Node C resumes delivery from last committed offset
7. Consumers automatically rebalanced

**Core Logic**:
```go
func (om *OwnershipManager) takeoverQueue(ctx context.Context, queueName string) error {
    // 1. Load queue config from storage
    config, err := om.queueStore.Get(ctx, queueName)
    if err != nil {
        return err
    }

    // 2. Create queue instance
    queue := om.queueManager.CreateQueue(config)

    // 3. Load partition offsets from storage
    for _, partition := range queue.partitions {
        offset, err := om.consumerStore.GetOffset(ctx, queueName, partition.id)
        if err != nil {
            continue
        }
        partition.nextSeq = offset
    }

    // 4. Resume delivery
    for _, partition := range queue.partitions {
        go partition.delivery.Run(ctx)
    }

    // 5. Register ownership in etcd
    om.registerOwnership(ctx, queueName)

    om.mu.Lock()
    om.ownedQueues[queueName] = queue
    om.mu.Unlock()

    return nil
}
```

**Estimated LOC**: 300-400
**Duration**: 2 days
**Testing**: Node failure and takeover tests

---

### 3.5 Ack Propagation

**File**: `queue/cluster/ack_sync.go`

**Scenario**: Consumer on Node A acks message from queue owned by Node B.

**Flow**:
1. Consumer publishes to `$queue/tasks/image/$ack` on Node A
2. Node A detects ack topic, extracts queue name
3. Node A forwards ack to Node B (owner) via gRPC
4. Node B processes ack and removes message

**Core Logic**:
```go
func (cr *ClusterRouter) RouteAck(ctx context.Context, queueName, messageID string) error {
    owner, err := cr.ownership.GetOwner(queueName)
    if err != nil {
        return err
    }

    if owner == cr.ownership.nodeID {
        return cr.localManager.Ack(ctx, queueName, messageID)
    }

    // Forward to owner
    client := cr.grpcClients[owner]
    _, err = client.Ack(ctx, &pb.AckRequest{
        QueueName: queueName,
        MessageID: messageID,
    })

    return err
}
```

**Estimated LOC**: 200-300
**Duration**: 1 day
**Testing**: Cross-node ack propagation tests

---

### Phase 3 Summary

**Total Duration**: 10-12 days
**Total LOC**: ~1,800-2,300

**Deliverables**:
- ✅ Queue ownership via etcd with consistent hashing
- ✅ Cross-node message routing via gRPC
- ✅ Remote consumer registration
- ✅ Partition failover on node crash
- ✅ Ack/nack propagation across cluster

**Testing Checklist**:
- [ ] Queue assigned to owner node
- [ ] Message published on Node A, delivered via Node B (owner)
- [ ] Consumer on Node A, queue owned by Node B
- [ ] Owner node crashes, new owner takes over
- [ ] Partitions resume from last offset after takeover
- [ ] Ack on Node A propagated to Node B
- [ ] Consumer rebalancing across cluster nodes

---

## Phase 4: Request/Response and Admin API

**Goal**: Developer-friendly features for inter-service communication

**Duration**: 6-8 days

### 4.1 Request/Response Helper

**File**: `queue/request_response.go`

**Pattern**: Service publishes request with `response-topic` and `correlation-id`, consumer replies to response topic.

**Helper Functions**:
```go
type RequestOptions struct {
    Timeout       time.Duration
    CorrelationID string
}

func (m *Manager) Request(ctx context.Context, queueTopic string, payload []byte, opts RequestOptions) (*storage.QueueMessage, error) {
    // 1. Generate correlation ID if not provided
    correlationID := opts.CorrelationID
    if correlationID == "" {
        correlationID = uuid.New().String()
    }

    // 2. Create temporary response topic
    responseTopic := "$queue/responses/" + correlationID

    // 3. Subscribe to response topic
    responseChan := make(chan *storage.QueueMessage, 1)
    m.subscribeToResponse(responseTopic, correlationID, responseChan)
    defer m.unsubscribeFromResponse(responseTopic, correlationID)

    // 4. Publish request with response-topic property
    msg := &storage.QueueMessage{
        ID:      uuid.New().String(),
        Payload: payload,
        Properties: map[string]string{
            "response-topic":  responseTopic,
            "correlation-id":  correlationID,
        },
    }

    if err := m.Enqueue(ctx, queueTopic, msg); err != nil {
        return nil, err
    }

    // 5. Wait for response
    timeout := opts.Timeout
    if timeout == 0 {
        timeout = 30 * time.Second
    }

    select {
    case response := <-responseChan:
        return response, nil
    case <-time.After(timeout):
        return nil, ErrRequestTimeout
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}
```

**Consumer-Side Helper**:
```go
func ExtractResponseInfo(msg *storage.QueueMessage) (responseTopic, correlationID string, ok bool) {
    responseTopic, ok1 := msg.Properties["response-topic"]
    correlationID, ok2 := msg.Properties["correlation-id"]
    return responseTopic, correlationID, ok1 && ok2
}

func Reply(ctx context.Context, m *Manager, msg *storage.QueueMessage, responsePayload []byte) error {
    responseTopic, correlationID, ok := ExtractResponseInfo(msg)
    if !ok {
        return ErrNoResponseInfo
    }

    responseMsg := &storage.QueueMessage{
        ID:      uuid.New().String(),
        Payload: responsePayload,
        Properties: map[string]string{
            "correlation-id": correlationID,
        },
    }

    return m.Enqueue(ctx, responseTopic, responseMsg)
}
```

**Estimated LOC**: 300-400
**Duration**: 2 days
**Testing**: Request/response flow tests

---

### 4.2 Admin REST API

**File**: `api/queue_http.go`

**Endpoints**:
```
POST   /api/v1/queues                 - Create queue
GET    /api/v1/queues                 - List all queues
GET    /api/v1/queues/{name}          - Get queue details
PUT    /api/v1/queues/{name}/config   - Update queue config
DELETE /api/v1/queues/{name}          - Delete queue
GET    /api/v1/queues/{name}/stats    - Get queue metrics
GET    /api/v1/queues/{name}/consumers - List consumers
POST   /api/v1/queues/{name}/purge    - Purge all messages
GET    /api/v1/queues/{name}/dlq      - List DLQ messages
POST   /api/v1/queues/{name}/dlq/{id}/retry - Retry DLQ message
```

**Example Handler**:
```go
func (h *QueueHandler) CreateQueue(w http.ResponseWriter, r *http.Request) {
    var config queue.QueueConfig
    if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest)
        return
    }

    if err := h.queueManager.CreateQueue(r.Context(), config); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).encode(config)
}

func (h *QueueHandler) GetStats(w http.ResponseWriter, r *http.Request) {
    queueName := chi.URLParam(r, "name")

    stats, err := h.queueManager.GetStats(r.Context(), queueName)
    if err != nil {
        http.Error(w, err.Error(), http.StatusNotFound)
        return
    }

    json.NewEncoder(w).Encode(stats)
}
```

**Estimated LOC**: 500-600
**Duration**: 3 days
**Testing**: API integration tests

---

### 4.3 Admin gRPC API

**File**: `api/queue_grpc.go`

**Proto Definition** (extends existing):
```protobuf
service QueueAdmin {
    rpc CreateQueue(CreateQueueRequest) returns (CreateQueueResponse);
    rpc ListQueues(ListQueuesRequest) returns (ListQueuesResponse);
    rpc GetQueueStats(GetQueueStatsRequest) returns (QueueStats);
    rpc PurgeQueue(PurgeQueueRequest) returns (PurgeQueueResponse);
}

message QueueStats {
    string name = 1;
    int64 total_messages = 2;
    int64 inflight_messages = 3;
    int64 dlq_messages = 4;
    double enqueue_rate = 5;
    double ack_rate = 6;
    int32 active_consumers = 7;
    map<string, int32> partitions_assigned = 8;
}
```

**Estimated LOC**: 400-500
**Duration**: 2 days
**Testing**: gRPC API tests

---

### 4.4 Documentation and Examples

**Files to Create**:
- `docs/queues.md` - Full queue documentation (expand placeholder)
- `examples/queue_publisher/main.go` - Example publisher
- `examples/queue_consumer/main.go` - Example consumer
- `examples/request_response/main.go` - Request/response example

**Estimated LOC**: 600-800 (including examples)
**Duration**: 2 days

---

### Phase 4 Summary

**Total Duration**: 6-8 days
**Total LOC**: ~1,800-2,300

**Deliverables**:
- ✅ Request/response helpers
- ✅ REST admin API
- ✅ gRPC admin API
- ✅ Documentation and examples

---

## Overall Implementation Summary

### Total Effort

| Phase | Duration | LOC | Complexity |
|-------|----------|-----|------------|
| Phase 1: Core Queue Infrastructure | 10-12 days | 2,600-3,400 | Medium |
| Phase 2: Retry, DLQ, Ordering | 8-10 days | 1,500-2,000 | Medium |
| Phase 3: Cluster Support | 10-12 days | 1,800-2,300 | High |
| Phase 4: Request/Response & Admin | 6-8 days | 1,800-2,300 | Low |
| **Total** | **34-42 days** | **7,700-10,000** | **Medium-High** |

**Realistic Timeline**: 6-8 weeks (accounting for testing, debugging, code review)

### Code Distribution

**New Packages** (~85% of code):
```
queue/                          - Core queue package
queue/storage/                  - Storage interfaces
queue/storage/badger/           - BadgerDB implementation
queue/storage/memory/           - Memory implementation (testing)
queue/cluster/                  - Cluster coordination
queue/cluster/proto/            - gRPC definitions
api/                           - HTTP/gRPC admin API
examples/                      - Usage examples
```

**Modified Existing Code** (~15% of code):
```
broker/broker.go               - Add QueueManager, routing logic (~300 LOC)
broker/queue_utils.go          - Helper functions (NEW file, ~200 LOC)
broker/v5handler.go            - Extract User Properties (~50 LOC modifications)
storage/storage.go             - Add queue message types (~100 LOC)
```

**Total Modifications to Existing Files**: ~650 LOC
**Total New Code**: ~7,050-9,350 LOC

### Decoupling Score: 92%

The implementation is highly decoupled from the existing broker code:
- Queue functionality is entirely in separate `queue/` package
- Broker modifications are minimal (routing + injection)
- No changes to MQTT protocol handlers
- No changes to existing pub/sub logic
- Storage interface pattern maintained
- Can be feature-flagged and disabled entirely

### Risk Assessment

**Low Risk**:
- Phase 1 & 2 (single-node queue functionality)
- Well-defined interfaces
- Independent testing possible

**Medium Risk**:
- Phase 3 (cluster support)
- Requires coordination with existing cluster logic
- Partition failover complexity

**High Risk**:
- None identified (phased approach mitigates risk)

### Testing Strategy

**Unit Tests**: ~3,000-4,000 LOC
- Storage layer tests
- Queue manager tests
- Consumer group tests
- Retry logic tests

**Integration Tests**: ~2,000-3,000 LOC
- End-to-end publish/consume tests
- Cluster routing tests
- Failover tests
- Performance tests

**Total Testing Code**: ~5,000-7,000 LOC (similar to implementation size)

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

### Rollout Strategy

1. **Phase 1-2**: Deploy to staging, test with synthetic load
2. **Phase 3**: Cluster testing with 3-node setup
3. **Phase 4**: Developer preview, gather feedback
4. **Production**: Gradual rollout, feature flag controlled

### Future Enhancements (Post-MVP)

- [ ] Message priority queues
- [ ] Scheduled message delivery
- [ ] Batch acknowledgments
- [ ] Consumer prefetching
- [ ] Message compression
- [ ] Schema validation
- [ ] Queue templates
- [ ] Advanced routing (topic exchange, header routing)

---

## Next Steps

1. **Review and approve this plan**
2. **Create placeholder documentation** (`docs/queues.md`)
3. **Set up development branch** (`feature/durable-queues`)
4. **Begin Phase 1 implementation**

Ready to proceed?
