# Clustering Infrastructure

This document provides an in-depth exploration of the three core infrastructure components that enable clustering: embedded etcd, gRPC transport, and BadgerDB storage.

## Table of Contents

- [Embedded etcd](#embedded-etcd)
- [gRPC Transport](#grpc-transport)
- [BadgerDB Storage](#badgerdb-storage)
- [Integration Points](#integration-points)

## Embedded etcd

### Overview

The broker uses **embedded etcd** - not an etcd client connecting to an external cluster, but a full etcd server running inside the broker process.

```go
import "go.etcd.io/etcd/server/v3/embed"

eCfg := embed.NewConfig()
eCfg.Name = "node1"
eCfg.Dir = "/tmp/mqtt/node1/etcd"
eCfg.ClusterState = "new"  // or "existing"

etcd, err := embed.StartEtcd(eCfg)
```

**Key Insight**: Each broker node IS an etcd node. The etcd cluster and broker cluster are the same thing.

### Raft Consensus

etcd uses the Raft consensus algorithm for:
- **Leader election**: One node becomes leader
- **Log replication**: All nodes agree on operation order
- **Strong consistency**: Reads guaranteed to see latest writes

**Raft Roles**:
- **Leader**: Handles all writes, replicates to followers
- **Follower**: Replicates log, votes in elections
- **Candidate**: Follower requesting votes during election

```
Initial Cluster Formation:
┌──────────┐   ┌──────────┐   ┌──────────┐
│  Node1   │   │  Node2   │   │  Node3   │
│ (Leader) │──▶│(Follower)│──▶│(Follower)│
└──────────┘   └──────────┘   └──────────┘
      │              │              │
      └──────────────┴──────────────┘
         Raft Log Replication

Write Path:
Client → Leader → Replicate to Majority → Commit → Respond
         Node1  →  Node2, Node3 (2/3)  →    ✓    →   OK
```

### Configuration

**Bootstrap Node** (`node1.yaml`):
```yaml
cluster:
  enabled: true
  node_id: "node1"
  etcd:
    data_dir: "/tmp/mqtt/node1/etcd"
    bind_addr: "127.0.0.1:2380"      # Raft peer communication
    client_addr: "127.0.0.1:2379"    # Client API
    initial_cluster: "node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2480,node3=http://127.0.0.1:2580"
    bootstrap: true  # cluster_state = "new"
```

**Joining Node** (`node2.yaml`):
```yaml
cluster:
  enabled: true
  node_id: "node2"
  etcd:
    data_dir: "/tmp/mqtt/node2/etcd"
    bind_addr: "127.0.0.1:2480"
    client_addr: "127.0.0.1:2479"
    initial_cluster: "node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2480,node3=http://127.0.0.1:2580"
    bootstrap: true  # Same for initial cluster formation
```

**Port Mapping**:
- `bind_addr`: Raft protocol (node-to-node)
- `client_addr`: etcd API (used by broker locally)

**Important**: For initial cluster formation, ALL nodes set `bootstrap: true`. Only use `bootstrap: false` when adding a node to an already-running cluster.

### Data Model

etcd stores data as key-value pairs with optional TTL (lease).

#### Session Ownership

**Write**:
```go
// Acquire session ownership
lease, _ := client.Grant(ctx, 30) // 30 second TTL
txn := client.Txn(ctx)
txn.If(clientv3.Compare(clientv3.CreateRevision("/mqtt/sessions/client1"), "=", 0))
txn.Then(clientv3.OpPut("/mqtt/sessions/client1", `{"node_id":"node2"}`, clientv3.WithLease(lease)))
txn.Commit()
```

**Auto-Renewal**:
```go
ch, _ := client.KeepAlive(ctx, leaseID)
go func() {
    for range ch {
        // Lease kept alive
    }
}()
```

If the broker crashes, the lease expires after 30 seconds, automatically releasing ownership.

#### Subscriptions

**Write**:
```go
key := fmt.Sprintf("/mqtt/subscriptions/%s/%s", clientID, filter)
value, _ := json.Marshal(Subscription{
    ClientID: clientID,
    Filter:   filter,
    QoS:      1,
    Options:  {...},
})
client.Put(ctx, key, string(value))
```

**Read** (for routing):
```go
resp, _ := client.Get(ctx, "/mqtt/subscriptions/", clientv3.WithPrefix())
for _, kv := range resp.Kvs {
    var sub Subscription
    json.Unmarshal(kv.Value, &sub)
    // Check if topic matches filter
}
```

#### Retained Messages

**Write**:
```go
key := "/mqtt/retained/sensor/temperature"
value, _ := json.Marshal(Message{
    Topic:   "sensor/temperature",
    Payload: []byte("22.5"),
    QoS:     1,
})
client.Put(ctx, key, string(value))
```

**Delete** (empty retained):
```go
client.Delete(ctx, key)
```

### Leadership and Background Tasks

Some tasks should run on exactly one node (e.g., processing pending will messages):

```go
// Create election
election := concurrency.NewElection(session, "/mqtt/leader")

// Campaign to become leader
err := election.Campaign(ctx, nodeID)
if err == nil {
    // This node is the leader
    go processWillMessages()
}

// Check leadership
if election.Leader(ctx) == nodeID {
    // Still leader
}
```

**Use Cases**:
- Processing pending will messages
- Cleaning up expired sessions
- Garbage collection tasks

### Performance Considerations

**Reads**:
- Linearizable reads: Contact leader (slower, guaranteed latest)
- Serializable reads: Local replica (faster, may be stale)

```go
// Fast read (may be stale)
resp, _ := client.Get(ctx, key)

// Guaranteed latest (slower)
resp, _ := client.Get(ctx, key, clientv3.WithSerializable())
```

**Writes**:
- All writes go through leader
- Require quorum (majority) to commit
- Latency: ~1-5ms in LAN, ~10-100ms WAN

**Best Practices**:
- Batch small writes when possible
- Use transactions for atomic operations
- Keep values small (<1KB recommended)
- Use leases for automatic cleanup

### Failure Handling

**Node Failure**:
- Surviving nodes detect via heartbeat
- If leader fails, new election triggered
- Majority required to elect new leader

**Split Brain Prevention**:
- Requires majority (quorum) for all operations
- Minority partition cannot accept writes
- Prevents inconsistent state

**Example**:
```
3-node cluster: [A, B, C]
Network partition: [A, B] | [C]

Partition [A, B]:
- Has majority (2/3)
- Can elect leader
- Accepts writes ✓

Partition [C]:
- No majority (1/3)
- Read-only mode
- Rejects writes ✗
```

## gRPC Transport

### Overview

gRPC provides the inter-broker communication layer for routing messages between nodes.

**Protocol**: gRPC over HTTP/2
**Serialization**: Protocol Buffers
**Pattern**: Request-response (not streaming)

### Service Definition

`cluster/broker.proto`:
```protobuf
syntax = "proto3";

package cluster;

service BrokerService {
  rpc RoutePublish(PublishRequest) returns (PublishResponse);
  rpc TakeoverSession(TakeoverRequest) returns (TakeoverResponse);
}

message PublishRequest {
  string client_id = 1;
  string topic = 2;
  bytes payload = 3;
  uint32 qos = 4;
  bool retain = 5;
  bool dup = 6;
  map<string, string> properties = 7;
}

message PublishResponse {
  bool success = 1;
  string error = 2;
}
```

**Generation**:
```bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       cluster/broker.proto
```

Generates:
- `broker.pb.go`: Message types
- `broker_grpc.pb.go`: Service interfaces

### Transport Implementation

`cluster/transport.go`:

**Server Side**:
```go
type Transport struct {
    UnimplementedBrokerServiceServer  // Embed for forward compatibility

    nodeID     string
    grpcServer *grpc.Server
    handler    TransportHandler
}

func NewTransport(nodeID, bindAddr string, handler TransportHandler) (*Transport, error) {
    listener, _ := net.Listen("tcp", bindAddr)
    grpcServer := grpc.NewServer()

    t := &Transport{
        nodeID:     nodeID,
        grpcServer: grpcServer,
        handler:    handler,
    }

    RegisterBrokerServiceServer(grpcServer, t)
    return t, nil
}

func (t *Transport) Start() error {
    go t.grpcServer.Serve(t.listener)
    return nil
}
```

**Client Side**:
```go
type Transport struct {
    peerClients map[string]BrokerServiceClient  // nodeID -> client
}

func (t *Transport) ConnectPeer(nodeID, addr string) error {
    conn, _ := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    client := NewBrokerServiceClient(conn)
    t.peerClients[nodeID] = client
    return nil
}

func (t *Transport) SendPublish(ctx context.Context, nodeID, clientID, topic string, ...) error {
    client := t.peerClients[nodeID]
    req := &PublishRequest{
        ClientId: clientID,
        Topic:    topic,
        Payload:  payload,
        Qos:      uint32(qos),
        // ...
    }
    resp, err := client.RoutePublish(ctx, req)
    return err
}
```

### RPC Implementation

**RoutePublish**:
```go
func (t *Transport) RoutePublish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
    // Called by remote broker to deliver message
    err := t.handler.HandlePublish(
        ctx,
        req.ClientId,
        req.Topic,
        req.Payload,
        byte(req.Qos),
        req.Retain,
        req.Dup,
        req.Properties,
    )

    if err != nil {
        return &PublishResponse{Success: false, Error: err.Error()}, nil
    }

    return &PublishResponse{Success: true}, nil
}
```

**Handler Interface**:
```go
type TransportHandler interface {
    HandlePublish(ctx context.Context, clientID, topic string, payload []byte, ...) error
    HandleTakeover(ctx context.Context, clientID, fromNode, toNode string, ...) error
}
```

Implemented by `broker.Broker`:
```go
func (b *Broker) HandlePublish(ctx context.Context, clientID, topic string, ...) error {
    session := b.Get(clientID)
    if session == nil {
        return fmt.Errorf("session not found: %s", clientID)
    }

    msg := Message{Topic: topic, Payload: payload, QoS: qos, ...}
    _, err := b.DeliverToSession(session, msg)
    return err
}
```

### Connection Management

**Peer Discovery**:
```yaml
cluster:
  transport:
    bind_addr: "127.0.0.1:7948"
    peers:
      node2: "127.0.0.1:7949"
      node3: "127.0.0.1:7950"
```

**Startup Sequence**:
```go
// 1. Start gRPC server
transport.Start()  // Listen on bind_addr

// 2. Connect to all peers
for nodeID, addr := range config.Peers {
    transport.ConnectPeer(nodeID, addr)
}

// 3. Bidirectional connections established
// Each node connects to all others
```

**Connection Topology**:
```
3-node cluster:

Node1 ←→ Node2
  ↕        ↕
Node3 ←→ Node3

Fully connected mesh: N*(N-1)/2 connections
3 nodes: 3 connections
10 nodes: 45 connections
```

### Error Handling

**Network Errors**:
```go
resp, err := client.RoutePublish(ctx, req)
if err != nil {
    // Connection error, node down, timeout
    log.Printf("Failed to route to %s: %v", nodeID, err)
    // Message lost - not retried
    return err
}

if !resp.Success {
    // Application error (session not found, etc.)
    log.Printf("Route failed: %s", resp.Error)
    return fmt.Errorf(resp.Error)
}
```

**Timeouts**:
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

resp, err := client.RoutePublish(ctx, req)
// Fails if remote node doesn't respond in 5s
```

### Performance Considerations

**Latency**:
- LAN: ~1-2ms per RPC
- WAN: 50-200ms depending on distance

**Throughput**:
- HTTP/2 multiplexing: Multiple RPCs per connection
- Protobuf serialization: ~10x faster than JSON
- No TLS (for now): Lower CPU overhead

**Optimization**:
- Reuse connections (done via connection pooling)
- Batch if possible (not currently implemented)
- Use compression for large payloads (not currently enabled)

## BadgerDB Storage

### Overview

BadgerDB is an embedded key-value store designed for high performance:
- **LSM tree** architecture (like RocksDB, LevelDB)
- **Embedded**: No server process, library only
- **Written in Go**: No CGO, pure Go
- **Fast writes**: Sequential writes optimized
- **TTL support**: Automatic expiration

### Architecture

```
BadgerDB Structure:

Write Path:
  Put(key, value) → MemTable (in-memory) → WAL (disk)
                       ↓
                   (when full)
                       ↓
                   SSTable (disk, immutable)

Read Path:
  Get(key) → MemTable → L0 SSTables → L1 SSTables → ... → Ln SSTables
             (newest)     (newer)       (older)
```

**LSM Tree Levels**:
- **L0**: Recently flushed from MemTable
- **L1-Ln**: Compacted and merged SSTables
- **Compaction**: Merge and sort SSTables to reduce read amplification

### Configuration

```go
import "github.com/dgraph-io/badger/v4"

opts := badger.DefaultOptions("/tmp/mqtt/data")
opts.Logger = nil  // Disable badger's logger
opts.SyncWrites = false  // Async writes (faster, less durable)

db, err := badger.Open(opts)
defer db.Close()
```

**Options**:
- `SyncWrites`: `false` for performance, `true` for durability
- `ValueLogFileSize`: Size of value log files (default 1GB)
- `NumMemtables`: Number of MemTables to keep (default 5)
- `NumLevelZeroTables`: L0 tables before compaction (default 5)

### Data Model

BadgerDB is a **pure key-value store** - no tables, no indexes, just `[]byte → []byte`.

#### Session Store

**Schema**:
```
Key:   session:{clientID}
Value: JSON{
  "client_id": "client1",
  "clean_start": false,
  "expiry_interval": 300,
  "keep_alive": 60,
  ...
}
```

**Implementation** (`storage/badger/session.go`):
```go
func (s *SessionStore) Save(info storage.SessionInfo) error {
    key := []byte("session:" + info.ClientID)
    value, _ := json.Marshal(info)

    entry := badger.NewEntry(key, value)
    if info.ExpiryInterval > 0 {
        entry = entry.WithTTL(time.Duration(info.ExpiryInterval) * time.Second)
    }

    return s.db.Update(func(txn *badger.Txn) error {
        return txn.SetEntry(entry)
    })
}

func (s *SessionStore) Get(clientID string) (*storage.SessionInfo, error) {
    key := []byte("session:" + clientID)
    var info storage.SessionInfo

    err := s.db.View(func(txn *badger.Txn) error {
        item, err := txn.Get(key)
        if err != nil {
            return err
        }
        return item.Value(func(val []byte) error {
            return json.Unmarshal(val, &info)
        })
    })

    return &info, err
}
```

#### Message Store

**Schema**:
```
Inflight: {clientID}/inflight/{packetID} → Message
Queue:    {clientID}/queue/{sequence}    → Message
```

**Example**:
```
client1/inflight/5   → {topic: "sensor/temp", qos: 1, ...}
client1/queue/0      → {topic: "alerts/high", qos: 2, ...}
client1/queue/1      → {topic: "alerts/low", qos: 1, ...}
```

**Prefix Scan**:
```go
func (s *MessageStore) GetByPrefix(prefix string) ([]*storage.Message, error) {
    var messages []*storage.Message

    err := s.db.View(func(txn *badger.Txn) error {
        opts := badger.DefaultIteratorOptions
        opts.Prefix = []byte(prefix)

        it := txn.NewIterator(opts)
        defer it.Close()

        for it.Rewind(); it.Valid(); it.Next() {
            item := it.Item()
            err := item.Value(func(val []byte) error {
                var msg storage.Message
                json.Unmarshal(val, &msg)
                messages = append(messages, &msg)
                return nil
            })
        }
        return nil
    })

    return messages, err
}
```

#### Subscription Store

**Schema**:
```
Key:   sub:{clientID}:{filter}
Value: JSON{qos: 1, no_local: false, ...}
```

**Count Tracking**:
```go
// Increment
txn.Set([]byte("sub:count"), []byte(strconv.Itoa(newCount)))

// Get count
item, _ := txn.Get([]byte("sub:count"))
item.Value(func(val []byte) error {
    count, _ = strconv.Atoi(string(val))
})
```

### Transactions

**Read-Write Transaction**:
```go
err := db.Update(func(txn *badger.Txn) error {
    // Read
    item, _ := txn.Get(key)
    var value int
    item.Value(func(val []byte) error {
        value = decodeInt(val)
        return nil
    })

    // Write
    newValue := value + 1
    return txn.Set(key, encodeInt(newValue))
})
```

**Read-Only Transaction**:
```go
err := db.View(func(txn *badger.Txn) error {
    item, _ := txn.Get(key)
    // Read only
    return nil
})
```

**ACID Properties**:
- **Atomicity**: Transaction commits all or nothing
- **Consistency**: Invariants maintained
- **Isolation**: Snapshot isolation (reads see consistent snapshot)
- **Durability**: WAL ensures durability (if SyncWrites=true)

### Garbage Collection

BadgerDB requires periodic GC to reclaim space:

```go
ticker := time.NewTicker(5 * time.Minute)
for range ticker.C {
    again := true
    for again {
        again = db.RunValueLogGC(0.5)  // Discard 50% garbage
    }
}
```

**Why GC?**
- Values stored separately in value log
- Deleted/updated values leave garbage
- GC compacts and reclaims space

**Currently**: Not implemented - TODO for production use.

### Performance Characteristics

**Write Performance**:
- Sequential writes to WAL: ~100K writes/sec
- MemTable flush: Batched, amortized cost
- Async mode (SyncWrites=false): Even faster, less durable

**Read Performance**:
- MemTable hit: ~1-2μs
- SSTable hit: ~10-100μs (depending on level)
- Bloom filters reduce unnecessary reads

**Space Amplification**:
- LSM tree: ~1.5-3x raw data size
- Compaction reduces over time
- GC reclaims deleted space

**Comparison**:
| Store | Write | Read | Durability |
|-------|-------|------|------------|
| Memory | Fastest | Fastest | None |
| BadgerDB (async) | Fast | Fast | Medium |
| BadgerDB (sync) | Medium | Fast | High |
| PostgreSQL | Slow | Medium | Highest |

## Integration Points

### Broker ↔ Cluster

**Session Connect**:
```go
// broker/broker.go
func (b *Broker) CreateSession(clientID string, opts SessionOptions) (*session.Session, bool, error) {
    // ... create session locally ...

    // Register ownership in cluster
    if b.cluster != nil {
        ctx := context.Background()
        nodeID := b.cluster.NodeID()
        b.cluster.AcquireSession(ctx, clientID, nodeID)
    }

    return session, true, nil
}
```

**Subscribe**:
```go
func (b *Broker) subscribeInternal(s *session.Session, filter string, opts SubscriptionOptions) error {
    // Add to local router
    b.router.Subscribe(s.ID, filter, opts.QoS, storeOpts)

    // Persist to local storage
    b.subscriptions.Add(sub)

    // Register in cluster for routing
    if b.cluster != nil {
        b.cluster.AddSubscription(ctx, s.ID, filter, opts.QoS, storeOpts)
    }

    return nil
}
```

**Publish**:
```go
func (b *Broker) distribute(topic string, payload []byte, qos byte, ...) error {
    // Deliver to local subscribers
    matched, _ := b.router.Match(topic)
    for _, sub := range matched {
        // ... deliver to local session ...
    }

    // Route to remote subscribers
    if b.cluster != nil {
        b.cluster.RoutePublish(ctx, topic, payload, qos, retain, props)
    }

    return nil
}
```

### Cluster ↔ etcd

**Session Ownership**:
```go
// cluster/etcd_cluster.go
func (c *EtcdCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
    key := "/mqtt/sessions/" + clientID
    value, _ := json.Marshal(map[string]string{"node_id": nodeID})

    txn := c.client.Txn(ctx)
    txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
    txn.Then(clientv3.OpPut(key, string(value), clientv3.WithLease(c.sessionLease)))

    resp, err := txn.Commit()
    if !resp.Succeeded {
        return fmt.Errorf("session already owned")
    }

    return err
}
```

**Subscription Query**:
```go
func (c *EtcdCluster) GetSubscribersForTopic(ctx context.Context, topic string) ([]*storage.Subscription, error) {
    // Get all subscriptions
    resp, _ := c.client.Get(ctx, "/mqtt/subscriptions/", clientv3.WithPrefix())

    var subs []*storage.Subscription
    for _, kv := range resp.Kvs {
        var sub storage.Subscription
        json.Unmarshal(kv.Value, &sub)

        // Check if topic matches filter
        if topicMatchesFilter(topic, sub.Filter) {
            subs = append(subs, &sub)
        }
    }

    return subs, nil
}
```

### Cluster ↔ gRPC

**Route Publish**:
```go
func (c *EtcdCluster) RoutePublish(ctx context.Context, topic string, payload []byte, ...) error {
    // Get subscribers
    subs, _ := c.GetSubscribersForTopic(ctx, topic)

    // Group by owner node
    nodeClients := make(map[string][]string)
    for _, sub := range subs {
        nodeID, exists, _ := c.GetSessionOwner(ctx, sub.ClientID)
        if exists && nodeID != c.nodeID {
            nodeClients[nodeID] = append(nodeClients[nodeID], sub.ClientID)
        }
    }

    // Send gRPC to each node
    for nodeID, clientIDs := range nodeClients {
        for _, clientID := range clientIDs {
            c.transport.SendPublish(ctx, nodeID, clientID, topic, payload, qos, ...)
        }
    }

    return nil
}
```

**Receive Publish**:
```go
func (c *EtcdCluster) HandlePublish(ctx context.Context, clientID, topic string, ...) error {
    // Called by gRPC transport
    // Delegate to broker
    return c.msgHandler.DeliverToClient(ctx, clientID, topic, payload, ...)
}
```

### Broker ↔ BadgerDB

**Session Persistence**:
```go
// Create session
session := broker.CreateSession(clientID, opts)

// Saved to BadgerDB
storage.Sessions().Save(session.Info())

// Disconnect
session.Disconnect()

// Reconnect
info, _ := storage.Sessions().Get(clientID)
session := broker.RestoreSession(info)
```

**Message Persistence**:
```go
// Queue offline message
broker.QueueMessage(clientID, message)

// Saved to BadgerDB
storage.Messages().Store(clientID+"/queue/"+seq, message)

// Reconnect and deliver
messages, _ := storage.Messages().GetByPrefix(clientID + "/queue/")
for _, msg := range messages {
    session.DeliverMessage(msg)
}
```

## Summary

The clustering infrastructure combines three technologies:

**etcd (Coordination)**:
- Strong consistency via Raft
- Session ownership tracking
- Subscription registry
- Leader election

**gRPC (Communication)**:
- Low-latency RPC between brokers
- Protobuf serialization
- Targeted message delivery

**BadgerDB (Persistence)**:
- Fast embedded storage
- Session state persistence
- Offline message queuing
- No external dependencies

Together, they provide:
- **Scalability**: Linear scaling with node count
- **Reliability**: Node failures handled gracefully
- **Performance**: Sub-millisecond local ops, ~1-5ms cluster ops
- **Simplicity**: Single binary, no external services

For more details, see:
- [Clustering Architecture](clustering-architecture.md) - Overall design
- [Broker & Routing](broker-routing.md) - Message routing internals
- [Configuration](configuration.md) - Setup and tuning
