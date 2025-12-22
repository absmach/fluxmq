# Clustering Architecture

This document provides a comprehensive overview of the MQTT broker's clustering architecture, explaining how multiple broker nodes work together to provide a distributed, highly available message broker.

## Table of Contents

- [Overview](#overview)
- [Design Goals](#design-goals)
- [Architecture Components](#architecture-components)
- [Data Distribution Model](#data-distribution-model)
- [Message Flow](#message-flow)
- [Failure Scenarios](#failure-scenarios)
- [Comparison with Other Approaches](#comparison-with-other-approaches)

## Overview

The MQTT broker implements a **shared-nothing architecture with distributed coordination**. Each broker node:

- Maintains its own local state (connected sessions, in-memory routing)
- Stores persistent data in its own BadgerDB instance
- Coordinates with other nodes through embedded etcd
- Routes messages to remote subscribers via gRPC

This design provides:
- **High availability**: Clients can connect to any node
- **Load distribution**: Sessions spread across nodes
- **No single point of failure**: All components are embedded
- **Simple deployment**: Single binary, no external dependencies

### Key Insight

Unlike traditional message brokers that use a shared database or message queue, this broker uses a **hybrid approach**:
- **Strong consistency** (via etcd Raft): Session ownership, subscriptions
- **Direct communication** (via gRPC): Message routing between nodes
- **Local storage** (via BadgerDB): Session state, offline messages

## Design Goals

### 1. Embedded Everything
**Goal**: Deploy as a single Go binary with no external dependencies.

**Implementation**:
- Embedded etcd server (not etcd client to external cluster)
- Embedded gRPC server for inter-broker communication
- Embedded BadgerDB for local persistence

**Benefit**: Simplified operations, consistent deployment model

### 2. Automatic Coordination
**Goal**: Nodes automatically discover and coordinate with each other.

**Implementation**:
- etcd handles leader election for background tasks
- Automatic session ownership tracking
- Cluster-wide subscription visibility

**Benefit**: No manual intervention for node failures or additions

### 3. Protocol Compliance
**Goal**: Full MQTT semantics in clustered mode.

**Implementation**:
- Session takeover across nodes
- QoS guarantees maintained
- Retained messages visible cluster-wide
- Will messages processed by single leader

**Benefit**: Clients see identical behavior regardless of cluster size

### 4. Linear Scalability
**Goal**: Adding nodes increases capacity proportionally.

**Implementation**:
- Sessions distributed across nodes
- Messages routed directly (no broadcast storms)
- Local topic routing (trie per node)

**Benefit**: Scale from 1 to N nodes without architectural changes

## Architecture Components

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        MQTT Broker Cluster                              │
│                                                                         │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐         │
│  │   Node 1     │      │   Node 2     │      │   Node 3     │         │
│  │              │      │              │      │              │         │
│  │ ┌──────────┐ │      │ ┌──────────┐ │      │ ┌──────────┐ │         │
│  │ │  Broker  │ │      │ │  Broker  │ │      │ │  Broker  │ │         │
│  │ │  Core    │ │      │ │  Core    │ │      │ │  Core    │ │         │
│  │ └────┬─────┘ │      │ └────┬─────┘ │      │ └────┬─────┘ │         │
│  │      │       │      │      │       │      │      │       │         │
│  │ ┌────▼─────┐ │      │ ┌────▼─────┐ │      │ ┌────▼─────┐ │         │
│  │ │ BadgerDB │ │      │ │ BadgerDB │ │      │ │ BadgerDB │ │         │
│  │ │(local)   │ │      │ │(local)   │ │      │ │(local)   │ │         │
│  │ └──────────┘ │      │ └──────────┘ │      │ └──────────┘ │         │
│  │              │      │              │      │              │         │
│  │ ┌──────────┐ │      │ ┌──────────┐ │      │ ┌──────────┐ │         │
│  │ │ etcd     │◄├──────┤►│ etcd     │◄├──────┤►│ etcd     │ │         │
│  │ │(embedded)│ │      │ │(embedded)│ │      │ │(embedded)│ │         │
│  │ └──────────┘ │      │ └──────────┘ │      │ └──────────┘ │         │
│  │              │      │              │      │              │         │
│  │ ┌──────────┐ │      │ ┌──────────┐ │      │ ┌──────────┐ │         │
│  │ │  gRPC    │◄├──────┤►│  gRPC    │◄├──────┤►│  gRPC    │ │         │
│  │ │Transport │ │      │ │Transport │ │      │ │Transport │ │         │
│  │ └──────────┘ │      │ └──────────┘ │      │ └──────────┘ │         │
│  │              │      │              │      │              │         │
│  │   :1883      │      │   :1884      │      │   :1885      │         │
│  └──────▲───────┘      └──────▲───────┘      └──────▲───────┘         │
│         │                     │                     │                 │
└─────────┼─────────────────────┼─────────────────────┼─────────────────┘
          │                     │                     │
    ┌─────▼─────┐         ┌─────▼─────┐         ┌─────▼─────┐
    │  Client1  │         │  Client2  │         │  Client3  │
    │  (sub)    │         │  (pub)    │         │  (sub)    │
    └───────────┘         └───────────┘         └───────────┘
```

### Component Responsibilities

#### 1. Broker Core
- Session lifecycle management
- Local message routing (trie-based)
- Protocol handling (MQTT v3/v5)
- Instrumentation (logging, metrics)

**Cluster Integration**:
- Registers session ownership on connect
- Adds subscriptions to cluster on subscribe
- Routes publishes to remote nodes
- Implements `SessionManager` interface for session takeover callbacks

**SessionManager Interface**:
```go
type SessionManager interface {
    // GetSessionStateAndClose captures session state and closes it
    // Called by remote node during takeover
    GetSessionStateAndClose(ctx context.Context, clientID string) (*SessionState, error)

    // RestoreSessionState applies captured state when creating session
    // Called after taking over a session from another node
    RestoreSessionState(ctx context.Context, clientID string, state *SessionState) error
}
```

The broker implements this interface, allowing the cluster layer to request session state transfer during takeover operations.

#### 2. etcd (Embedded)
- **Raft consensus** for strong consistency
- **Key-value store** for cluster metadata
- **Leader election** for singleton tasks
- **Watch API** for change notifications

**Stored Data**:
- Session ownership: `clientID → nodeID`
- Subscriptions: `clientID:filter → {qos, options}`
- Retained messages: `topic → message`
- Will messages: `clientID → will`

#### 3. gRPC Transport
- **Bidirectional connections** between all nodes
- **Protobuf serialization** for efficiency
- **Request/response** semantics for routing

**RPC Methods**:
- `RoutePublish(clientID, topic, payload, qos, ...)`: Forward PUBLISH to remote subscriber
- `TakeoverSession(clientID, fromNode, toNode)`: Migrate session between nodes with full state transfer

**Session State Transfer**:
The `TakeoverSession` RPC returns a complete `SessionState` protobuf containing:
- Inflight messages (QoS 1/2 pending acknowledgments)
- Offline queue (messages queued while disconnected)
- Subscriptions with QoS levels
- Will message configuration
- Session expiry interval

#### 4. BadgerDB (Local)
- **LSM tree storage** for high write throughput
- **Embedded** - no server process
- **Per-node** - each node has its own database

**Stored Data**:
- Session state (local only)
- Inflight messages (QoS 1/2)
- Offline message queue
- Local subscriptions (backup)

## Data Distribution Model

### Metadata (Strong Consistency via etcd)

**Session Ownership**
```
Key:   /mqtt/sessions/{clientID}
Value: {"node_id": "node2", "lease_id": "7587869134..."}
TTL:   30 seconds (auto-renewed while connected)
```

When a client connects:
1. Node attempts `AcquireSession(clientID, nodeID)`
2. etcd transaction: put if not exists
3. Auto-renewing lease maintains ownership
4. On disconnect, ownership released

**Subscriptions**
```
Key:   /mqtt/subscriptions/{clientID}/{filter}
Value: {"qos": 1, "no_local": false, ...}
```

When a client subscribes:
1. Added to local router (trie)
2. Stored in local BadgerDB
3. Registered in etcd for cluster visibility

**Why Both Local and etcd?**
- **Local router**: Fast O(log n) matching for local subscribers
- **etcd**: Cluster-wide visibility for routing to remote nodes

### Messages (Direct Routing via gRPC)

Messages are **never** stored in etcd. Instead:

1. **Local delivery**: Matched against local router trie
2. **Remote routing**: Query etcd for subscribers, send via gRPC

```
Publisher (Node3) → Broker Core → distribute()
                                   ├─→ Local Router → Local Subscribers
                                   └─→ cluster.RoutePublish()
                                       ├─→ Query etcd for subscribers
                                       ├─→ Group by owner node
                                       └─→ gRPC call per node
                                           └─→ Node2.HandlePublish()
                                               └─→ DeliverToClient()
```

### Retained Messages (Full Replication via etcd)

Retained messages stored in etcd for cluster-wide visibility:

```
Key:   /mqtt/retained/{topic}
Value: {"payload": "...", "qos": 1, "properties": {...}}
```

On subscription, any node can query etcd for matching retained messages.

**Trade-off**: etcd not designed for large payloads. Future optimization: store pointer in etcd, payload in BadgerDB.

### Will Messages (Leader-Only Processing)

Will messages stored in etcd with disconnect timestamp:

```
Key:   /mqtt/wills/{clientID}
Value: {
  "will": {"topic": "...", "payload": "...", ...},
  "disconnected_at": "2025-12-17T18:00:00Z"
}
```

Only the **elected leader** processes pending wills to avoid duplicates.

## Message Flow

### Scenario: Cross-Node Publish

**Setup**:
- Client A connected to Node1, subscribed to `sensor/#`
- Client B connected to Node2, subscribed to `sensor/+/temp`
- Client C connected to Node3, publishes to `sensor/living/temp`

**Flow**:

```
1. Client C → Node3
   PUBLISH(topic="sensor/living/temp", payload="22.5", qos=1)

2. Node3: Broker.Publish()
   ├─ Store retained message (if retain flag set)
   └─ distribute("sensor/living/temp", "22.5", 1, false, {})

3. Node3: Local Delivery
   ├─ router.Match("sensor/living/temp")
   └─ No local subscribers

4. Node3: Cluster Routing
   ├─ cluster.RoutePublish(ctx, "sensor/living/temp", ...)
   │
   ├─ cluster.GetSubscribersForTopic(ctx, "sensor/living/temp")
   │  ├─ Check local subscription cache (loaded from etcd on startup)
   │  ├─ O(N) scan of cached subscriptions (fast - in memory)
   │  ├─ Filter matches: "sensor/#", "sensor/+/temp"
   │  └─ Returns: [
   │      {clientID: "clientA", filter: "sensor/#"},
   │      {clientID: "clientB", filter: "sensor/+/temp"}
   │    ]
   │  Note: Cache updated in real-time via etcd watch
   │
   ├─ For each subscriber, get session owner:
   │  ├─ GetSessionOwner(ctx, "clientA") → "node1"
   │  └─ GetSessionOwner(ctx, "clientB") → "node2"
   │
   ├─ Group by node:
   │  ├─ node1: [clientA]
   │  └─ node2: [clientB]
   │
   └─ Send gRPC messages:
      ├─ transport.SendPublish(ctx, "node1", "clientA", ...)
      └─ transport.SendPublish(ctx, "node2", "clientB", ...)

5. Node1: Receives gRPC
   ├─ transport.RoutePublish(req{clientId: "clientA", ...})
   ├─ broker.DeliverToClient(ctx, "clientA", "sensor/living/temp", ...)
   └─ Session.DeliverMessage() → Send to Client A

6. Node2: Receives gRPC
   ├─ transport.RoutePublish(req{clientId: "clientB", ...})
   ├─ broker.DeliverToClient(ctx, "clientB", "sensor/living/temp", ...)
   └─ Session.DeliverMessage() → Send to Client B
```

**Key Points**:
- Message never touches etcd (subscriptions cached locally)
- No broadcast - targeted delivery only
- Each node independently delivers to its local clients
- Subscription cache updated via etcd watch (real-time)

### Scenario: Session Takeover

**Setup**:
- Client D connected to Node1 with `clean_start=false`
- Client D reconnects to Node2 (same clientID)

**Flow**:

```
1. Client D disconnects from Node1
   ├─ session.Disconnect()
   ├─ broker.handleDisconnect()
   │  ├─ Save session state to BadgerDB
   │  ├─ Save offline queue
   │  └─ Session ownership lease expires after 30s
   └─ Connection closed

2. Client D connects to Node2
   ├─ CONNECT(clientId="clientD", clean_start=false)
   ├─ broker.CreateSession("clientD", opts{clean_start: false})
   │
   ├─ cluster.GetSessionOwner(ctx, "clientD")
   │  └─ Returns "node1" (if within 30s) or not found (if lease expired)
   │
   ├─ IF session owned by Node1:
   │  ├─ cluster.TakeoverSession(ctx, "clientD", "node1", "node2")
   │  │  ├─ gRPC call: transport.SendTakeover("node1", "clientD", ...)
   │  │  ├─ Node1.HandleTakeover()
   │  │  │  ├─ broker.GetSessionStateAndClose("clientD")
   │  │  │  ├─ Disconnect client (TCP close)
   │  │  │  ├─ Capture session state:
   │  │  │  │  ├─ Inflight messages (QoS 1/2)
   │  │  │  │  ├─ Offline queue
   │  │  │  │  ├─ Subscriptions with QoS
   │  │  │  │  └─ Will message
   │  │  │  └─ Return SessionState
   │  │  └─ Node2 receives SessionState
   │  │
   │  └─ cluster.AcquireSession(ctx, "clientD", "node2")
   │      └─ Update /mqtt/sessions/clientD = {node_id: "node2"}
   │
   ├─ IF takeover state received:
   │  ├─ Restore inflight messages
   │  ├─ Restore offline queue
   │  ├─ Restore subscriptions (add to local router + etcd)
   │  └─ Restore will message
   │
   └─ CONNACK(session_present=1)

3. Client D on Node2
   ├─ All subscriptions active (restored from Node1)
   ├─ Inflight messages redelivered
   ├─ Offline queue preserved
   └─ Full session continuity maintained
```

**Implementation Status**: ✅ **FULLY IMPLEMENTED**

The session takeover protocol is complete:
- ✅ gRPC `TakeoverSession` RPC with `SessionState` transfer
- ✅ `broker.GetSessionStateAndClose()` captures full session state
- ✅ State includes inflight, queue, subscriptions, will message
- ✅ `broker.CreateSession()` detects remote ownership and triggers takeover
- ✅ Session state restoration on new node
- ✅ Client experiences seamless handoff with session continuity

## Failure Scenarios

### Node Failure

**Scenario**: Node2 crashes while clients are connected.

**Impact**:
- Clients on Node2 lose connection
- Session ownership leases expire (30s TTL)
- Other nodes unaffected

**Recovery**:
1. Clients reconnect to Node1 or Node3
2. Session ownership acquired by new node
3. Subscriptions re-established
4. Offline messages lost (if not persisted)

**Mitigation**:
- Client auto-reconnect with exponential backoff
- Load balancer distributes connections
- Future: Session state replication

### etcd Partition

**Scenario**: Network partition splits etcd cluster.

**Impact**:
- Minority partition: Nodes can't acquire sessions (read-only)
- Majority partition: Continues normally

**Recovery**:
- Partition heals automatically
- etcd re-synchronizes
- Minority nodes resume operations

**Design Choice**: CAP theorem - chose **CP** (Consistency + Partition tolerance)
- Availability sacrificed in minority partition
- Prevents split-brain scenarios

### Message Loss

**Scenarios**:
1. **QoS 0**: Best effort - may lose on any failure
2. **QoS 1**: Persisted to BadgerDB, safe unless disk failure
3. **QoS 2**: Two-phase commit, safe unless both nodes fail

**Guarantees**:
- Messages in transit during node failure: lost
- Messages in offline queue: persisted to BadgerDB
- Inflight messages: persisted, redelivered on reconnect

### Graceful Shutdown

**Scenario**: Node receives SIGTERM (e.g., during deployment).

**Process**:
1. **Drain Phase**: Stop accepting new connections, wait for active sessions to disconnect
   - Configurable drain timeout (default: 30s)
   - Sessions can gracefully close
2. **Session Transfer**: Release ownership of remaining sessions in etcd
   - Enables other nodes to immediately take over
   - Clients reconnect to healthy nodes within seconds
3. **Cleanup**: Final BadgerDB GC pass, close all resources
   - Idempotent shutdown (safe to call multiple times)

**Impact**:
- Zero session loss in cluster mode
- Connected clients experience brief reconnection
- Persistent sessions preserved across shutdown
- Offline messages retained in BadgerDB

**Configuration**:
```yaml
server:
  shutdown_timeout: 30s  # Drain period
```

**Recovery**:
- Clients automatically reconnect to other nodes
- Session ownership acquired by new node
- Subscriptions and offline messages restored

## Comparison with Other Approaches

### vs. Shared Database

**Traditional Approach**:
- All nodes connect to PostgreSQL/MySQL
- Sessions, subscriptions, messages in DB
- Lock-based coordination

**Our Approach**:
- No shared database
- etcd for coordination only
- Local storage for session state

**Trade-offs**:
| Aspect | Shared DB | Our Approach |
|--------|-----------|--------------|
| Deployment | DB + Brokers | Single binary |
| Scalability | DB bottleneck | Linear scaling |
| Failure Domain | DB SPOF | Distributed |
| Latency | DB query per op | Local + etcd KV |
| Complexity | Simple | More components |

### vs. Message Queue (Kafka/NATS)

**Traditional Approach**:
- Brokers publish to message queue
- All brokers subscribe to queue
- Queue handles message routing

**Our Approach**:
- Direct gRPC between brokers
- No intermediate queue
- Targeted delivery only

**Trade-offs**:
| Aspect | Message Queue | Our Approach |
|--------|---------------|--------------|
| Message Ordering | Guaranteed | Per-client |
| Broadcast | Efficient | Not needed |
| Dependencies | Queue service | None |
| Latency | Queue hop | Direct |
| Replay | Built-in | Not needed |

### vs. Gossip Protocol (Consul/Memberlist)

**Traditional Approach**:
- Eventual consistency
- Broadcast state updates
- No leader

**Our Approach**:
- Strong consistency (etcd Raft)
- Direct targeted communication
- Leader election

**Trade-offs**:
| Aspect | Gossip | Our Approach |
|--------|--------|--------------|
| Consistency | Eventual | Strong |
| Network | Broadcast | Point-to-point |
| Complexity | Low | Medium |
| Latency | Variable | Predictable |
| MQTT Semantics | Tricky | Natural |

## Summary

The clustering architecture balances several concerns:

**Embedded First**: All components run in single process
- etcd server (not client)
- gRPC server
- BadgerDB storage

**Hybrid Consistency**:
- Strong (etcd): Session ownership, subscriptions
- Direct (gRPC): Message routing
- Local (BadgerDB): Session state

**MQTT Compliance**:
- Session takeover semantics
- QoS guarantees
- Retained message delivery
- Will message processing

**Scalability**:
- Linear capacity scaling
- No shared resources
- Targeted message routing

**Operational Simplicity**:
- Single binary deployment
- Automatic coordination
- No external dependencies

For implementation details, see:
- [Clustering Infrastructure](clustering-infrastructure.md) - etcd, gRPC, BadgerDB deep dive
- [Broker & Routing](broker-routing.md) - Message routing internals
- [Configuration](configuration.md) - Cluster setup and tuning
