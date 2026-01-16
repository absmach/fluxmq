# Examples

This directory contains example configurations and client applications for the MQTT broker.

## Configuration Examples

| File | Description |
|------|-------------|
| `config.yaml` | Full broker configuration with all options |
| `no-cluster.yaml` | Single-node mode (no clustering) |
| `node1.yaml`, `node2.yaml`, `node3.yaml` | 3-node cluster setup |
| `single-node-cluster.yaml` | Single-node with cluster features enabled |
| `tls-server.yaml` | TLS/SSL configuration |

## Client Examples

### Queue Client (`queue-client/`)

Demonstrates the broker's durable queue functionality with QoS 2 publishing, consumer groups, and message acknowledgments.

#### Scenario: Order Processing Pipeline

A queue named `tasks/orders` receives orders from multiple publishers. Two independent consumer groups process every message:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Publisher 1 │     │ Publisher 2 │     │ Publisher 3 │
│ (Alice)     │     │ (Bob)       │     │ (Charlie)   │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │    QoS 2 PUBLISH (exactly-once)       │
       └───────────────────┼───────────────────┘
                           ▼
                 ┌───────────────────┐
                 │  $queue/tasks/    │
                 │     orders        │
                 │  (Durable Queue)  │
                 └─────────┬─────────┘
                           │
           ┌───────────────┴───────────────┐
           │ Fan-out to consumer groups    │
           ▼                               ▼
┌─────────────────────┐         ┌─────────────────────┐
│ order-validators    │         │ order-fulfillment   │
│ (Consumer Group 1)  │         │ (Consumer Group 2)  │
├─────────────────────┤         ├─────────────────────┤
│ ┌─────┐   ┌─────┐   │         │ ┌─────┐             │
│ │ C1  │   │ C2  │   │         │ │ C1  │             │
│ └─────┘   └─────┘   │         │ └─────┘             │
│  Load balanced      │         │  Single consumer    │
└─────────────────────┘         └─────────────────────┘
```

#### Key Concepts Demonstrated

| Concept | Description |
|---------|-------------|
| **QoS 2 Publishing** | Exactly-once delivery from publisher to broker |
| **Consumer Groups** | Both groups receive ALL messages (fan-out pattern) |
| **Load Balancing** | Messages distributed across consumers within a group |
| **Partition Keys** | Orders from the same customer processed in FIFO order |
| **Acknowledgments** | `Ack()`, `Nack()`, `Reject()` for processing control |

#### Partition Key Ordering

Each publisher uses a customer ID as the partition key:

```go
c.PublishToQueueWithOptions(&client.QueuePublishOptions{
    QueueName:    "tasks/orders",
    Payload:      orderJSON,
    PartitionKey: "customer-alice",  // Same key → same partition → FIFO
    QoS:          2,
})
```

This guarantees that all orders from `customer-alice` are processed in the order they were published, even when load-balanced across multiple consumers.

#### Message Acknowledgments

Consumers control message lifecycle:

```go
c.SubscribeToQueue("tasks/orders", "order-validators", func(msg *client.QueueMessage) {
    if processedOK {
        msg.Ack()     // Remove from queue
    } else if retryable {
        msg.Nack()    // Retry with exponential backoff
    } else {
        msg.Reject()  // Send to dead-letter queue
    }
})
```

#### Running the Example

1. Start the broker:
   ```bash
   go run ./cmd/broker/ -config examples/no-cluster.yaml
   ```

2. Run the queue client:
   ```bash
   go run ./examples/queue-client/
   ```

3. Command-line options:
   ```
   -broker string    MQTT broker address (default "localhost:1883")
   -messages int     Number of messages per publisher (default 10)
   -rate duration    Delay between publishes (default 500ms)
   ```

#### Expected Output

```
Starting consumers...
[validator-1] Subscribed to queue 'tasks/orders' in group 'order-validators'
[validator-2] Subscribed to queue 'tasks/orders' in group 'order-validators'
[fulfillment-1] Subscribed to queue 'tasks/orders' in group 'order-fulfillment'
Starting publishers...
[publisher-1] Connected, publishing orders for customer-alice
[publisher-2] Connected, publishing orders for customer-bob
[publisher-3] Connected, publishing orders for customer-charlie
[publisher-1] Published order customer-alice-order-1 (QoS 2)
[validator-1] Validated order (partition=3, seq=1): {"order_id":"customer-alice-order-1"...}
[fulfillment-1] Fulfilled order (partition=3, seq=1): {"order_id":"customer-alice-order-1"...}
...

=== Statistics ===
Messages published:           30
Validator group processed:    30
Fulfillment group processed:  30
```

#### Architecture Notes

- **MQTT v5 Required**: Consumer groups and partition keys use MQTT v5 user properties
- **Durable Storage**: Messages persist to BadgerDB and survive broker restarts
- **Topic Prefix**: Queue topics use `$queue/` prefix (added automatically by client)
- **Ack Topics**: Acknowledgments sent to `$queue/{name}/$ack`, `$nack`, or `$reject`
