# Examples

This directory contains example configurations and client applications for the MQTT broker.

## Configuration Examples

| File                                     | Description                                |
| ---------------------------------------- | ------------------------------------------ |
| `config.yaml`                            | Full broker configuration with all options |
| `no-cluster.yaml`                        | Single-node mode (no clustering)           |
| `node1.yaml`, `node2.yaml`, `node3.yaml` | 3-node cluster setup                       |
| `single-node-cluster.yaml`               | Single-node with cluster features enabled  |
| `tls-server.yaml`                        | TLS/SSL configuration                      |

## Client Examples

### Queue Client (`queue-client/`)

Demonstrates cross-protocol queue interop between MQTT, AMQP 1.0, and AMQP 0.9.1 using the broker's durable queue functionality.

#### Scenario: Order Processing Pipeline

A queue named `tasks/orders` receives orders from multiple MQTT publishers. Three independent consumer groups process every message — two using MQTT, one using AMQP 1.0, and one using AMQP 0.9.1:

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ Publisher 1 │     │ Publisher 2 │     │ Publisher 3 │
│ (Alice)     │     │ (Bob)       │     │ (Charlie)   │
│   [MQTT]    │     │   [MQTT]    │     │   [MQTT]    │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │    QoS 1/2 PUBLISH                    │
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
│      [MQTT]         │         │    [AMQP 1.0]       │
├─────────────────────┤         ├─────────────────────┤
│ ┌─────┐   ┌─────┐   │         │ ┌─────┐             │
│ │ C1  │   │ C2  │   │         │ │ C1  │             │
│ └─────┘   └─────┘   │         │ └─────┘             │
│  Load balanced      │         │  Single consumer    │
└─────────────────────┘         └─────────────────────┘
```

#### Key Concepts Demonstrated

| Concept                    | Description                                                            |
| -------------------------- | ---------------------------------------------------------------------- |
| **Cross-Protocol Interop** | MQTT publishers, AMQP 1.0 and AMQP 0.9.1 consumers on the same queue   |
| **QoS 1/2 Publishing**     | Reliable publish with QoS guarantees                                   |
| **Consumer Groups**        | Both groups receive ALL messages (fan-out pattern)                     |
| **Load Balancing**         | Messages distributed across consumers within a group                   |
| **AMQP Dispositions**      | `AcceptMessage`, `ReleaseMessage`, `RejectMessage` for ack/nack/reject |
| **AMQP 0.9.1 Acks**        | `Ack`, `Nack`, `Reject` for processing control                         |
| **MQTT Acknowledgments**   | `Ack()`, `Nack()`, `Reject()` for processing control                   |

#### AMQP 1.0 Consumer

The fulfillment consumer connects via AMQP 1.0 using [Azure/go-amqp](https://github.com/Azure/go-amqp):

```go
conn, _ := amqp.Dial(ctx, "amqp://localhost:5672", &amqp.ConnOptions{
    ContainerID: "amqp-fulfillment-1",
    SASLType:    amqp.SASLTypeAnonymous(),
})
session, _ := conn.NewSession(ctx, nil)

receiver, _ := session.NewReceiver(ctx, "$queue/tasks/orders", &amqp.ReceiverOptions{
    Credit: 10,
    Properties: map[string]any{
        "consumer-group": "order-fulfillment",
    },
})

msg, _ := receiver.Receive(ctx, nil)
receiver.AcceptMessage(ctx, msg)  // Ack
receiver.ReleaseMessage(ctx, msg) // Nack (retry)
receiver.RejectMessage(ctx, msg, nil) // Reject (DLQ)
```

The consumer group is passed via AMQP link properties on attach, matching the MQTT v5 user property convention.

#### AMQP 0.9.1 Consumer

The shipper consumer connects via the FluxMQ AMQP 0.9.1 client:

```go
opts := amqp091.NewOptions().
    SetAddress("localhost:5682").
    SetCredentials("guest", "guest")

c, _ := amqp091.New(opts)
_ = c.Connect()

_ = c.SubscribeToQueue("tasks/orders", "order-shipper", func(msg *amqp091.QueueMessage) {
    _ = msg.Ack()
})
```

The consumer group is passed via the `x-consumer-group` argument on `basic.consume`, aligning with the MQTT v5 `consumer-group` user property.

#### Running the Example

1. Start the broker:
   ```bash
   go run ./cmd/ --config examples/no-cluster.yaml
   ```

2. Run the queue client:
   ```bash
   go run ./examples/queue-client/
   ```

3. Command-line options:
   ```
   -mqtt string      MQTT broker address (default "localhost:1883")
   -amqp string      AMQP 1.0 broker address (default "localhost:5672")
   -amqp091 string   AMQP 0.9.1 broker address (default "localhost:5682")
   -messages int      Number of messages per publisher (default 10)
   -rate duration     Delay between publishes (default 200ms)
   ```

#### Expected Output

```
Starting consumers...
[validator-1] Connected to localhost:1883 (MQTT)
[validator-2] Connected to localhost:1883 (MQTT)
[amqp-fulfillment-1] Connected to localhost:5672 (AMQP 1.0), receiving from queue 'tasks/orders'
[amqp091-shipper-1] Connected to localhost:5682 (AMQP 0.9.1), receiving from queue 'tasks/orders'
Starting publishers...
[publisher-1] Connected to localhost:1883 (MQTT), publishing orders for customer-alice
[publisher-2] Connected to localhost:1883 (MQTT), publishing orders for customer-bob
[publisher-3] Connected to localhost:1883 (MQTT), publishing orders for customer-charlie
[validator-1] Validated order (seq=1): {"order_id":"customer-alice-order-1"...}
[amqp-fulfillment-1] Fulfilled order (AMQP): {"order_id":"customer-alice-order-1"...}
[amqp091-shipper-1] Shipped order (AMQP 0.9.1): {"order_id":"customer-alice-order-1"...}
...

=== Statistics ===
Messages published:              30
Validator group processed (MQTT): 30
Fulfillment group processed (AMQP): 30
Shipper group processed (AMQP 0.9.1): 30
```

#### Architecture Notes

- **MQTT v5 Required**: Consumer groups and partition keys use MQTT v5 user properties
- **AMQP 1.0 SASL ANONYMOUS**: The example uses anonymous authentication
- **AMQP 0.9.1 PLAIN**: The example uses username/password authentication
- **Durable Storage**: Messages persist to BadgerDB and survive broker restarts
- **Topic Prefix**: Queue topics use `$queue/` prefix (added automatically by MQTT client, explicit in AMQP)
- **Consumer Group**: MQTT passes it as a user property, AMQP 1.0 passes it as a link property, AMQP 0.9.1 passes it as `x-consumer-group`
