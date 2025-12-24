# Broker & Message Routing

This document explains the broker's internal architecture, message routing mechanisms, session management, and how clustering integrates with the core broker logic.

## Table of Contents

- [Broker Architecture](#broker-architecture)
- [Session Lifecycle](#session-lifecycle)
- [Message Routing](#message-routing)
- [Topic Matching](#topic-matching)
- [QoS Handling](#qos-handling)
- [Cluster Integration](#cluster-integration)

## Broker Architecture

### Core Broker Structure

```go
// broker/broker.go
type Broker struct {
    mu          sync.RWMutex
    sessionsMap session.Cache              // In-memory session cache

    router      Router                     // Topic matching trie

    // Storage backends
    messages      storage.MessageStore     // Inflight & queued messages
    sessions      storage.SessionStore     // Session persistence
    subscriptions storage.SubscriptionStore // Subscription persistence
    retained      storage.RetainedStore    // Retained messages
    wills         storage.WillStore        // Will messages

    cluster cluster.Cluster               // Cluster coordination (nil if single-node)

    auth   *AuthEngine                    // Authentication
    logger *slog.Logger                   // Structured logging
    stats  *Stats                         // Metrics collection

    stopCh chan struct{}
    wg     sync.WaitGroup
}
```

**Key Design Principles**:

1. **Protocol-Agnostic**: Broker knows nothing about MQTT packets
2. **Storage-Agnostic**: Works with any storage implementation
3. **Cluster-Aware**: Seamlessly works in single-node or clustered mode
4. **Instrumented**: Built-in logging and metrics

### Initialization

```go
func NewBroker(store storage.Store, clust cluster.Cluster, logger *slog.Logger, stats *Stats) *Broker {
    if store == nil {
        store = memory.New()  // Default to in-memory
    }

    b := &Broker{
        sessionsMap:   session.NewMapCache(),
        router:        NewRouter(),
        messages:      store.Messages(),
        sessions:      store.Sessions(),
        subscriptions: store.Subscriptions(),
        retained:      store.Retained(),
        wills:         store.Wills(),
        cluster:       clust,  // Can be nil for single-node
        logger:        logger,
        stats:         stats,
        stopCh:        make(chan struct{}),
    }

    // Start background tasks
    go b.expiryLoop()
    go b.statsLoop()

    return b
}
```

**Dependency Injection**:
- `store`: Pluggable storage (memory, BadgerDB, future: PostgreSQL)
- `clust`: Cluster coordination (nil, NoopCluster, or EtcdCluster)
- `logger`: Structured logging (slog)
- `stats`: Metrics collection

### Domain Methods

The broker exposes clean, protocol-agnostic methods:

```go
// Session Management
CreateSession(clientID string, opts SessionOptions) (*session.Session, bool, error)
DestroySession(clientID string) error
Get(clientID string) *session.Session

// Publishing
Publish(msg Message) error
PublishWill(clientID string) error

// Subscribing
Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error
Unsubscribe(clientID string, filter string) error

// Delivery
DeliverToSession(s *session.Session, msg Message) (uint16, error)
AckMessage(clientID string, packetID uint16, qos byte) error

// Cluster Integration (implements cluster.MessageHandler)
DeliverToClient(ctx context.Context, clientID, topic string, ...) error
```

Protocol handlers (V3Handler, V5Handler) translate MQTT packets into these domain operations.

## Session Lifecycle

### Connection Flow

```
Client            Handler          Broker            Cluster           Storage
  │                 │                │                  │                 │
  ├─CONNECT────────▶│                │                  │                 │
  │                 ├─CreateSession─▶│                  │                 │
  │                 │                ├─Check existing   │                 │
  │                 │                │  in cache        │                 │
  │                 │                │                  │                 │
  │                 │                ├─AcquireSession──▶│                 │
  │                 │                │                  ├─Put etcd        │
  │                 │                │                  │  /sessions/X    │
  │                 │                │◀─────────────────┤                 │
  │                 │                │                  │                 │
  │                 │                ├─Save session────────────────────▶│
  │                 │                │                  │                 │
  │                 │◀─session───────┤                  │                 │
  │                 │                │                  │                 │
  │◀──CONNACK───────┤                │                  │                 │
  │                 │                │                  │                 │
```

**CreateSession Implementation**:

```go
func (b *Broker) CreateSession(clientID string, opts SessionOptions) (*session.Session, bool, error) {
    b.mu.Lock()
    defer b.mu.Unlock()

    // Check if session exists
    existing := b.sessionsMap.Get(clientID)
    if opts.CleanStart && existing != nil {
        // Clean start: destroy old session
        b.destroySessionLocked(existing)
        existing = nil
    }

    if existing != nil {
        // Resume existing session
        return existing, false, nil
    }

    // Create new session
    session := session.New(clientID, 0, sessionOpts, inflight, offlineQueue)

    // Restore from storage if persistent
    if !opts.CleanStart {
        b.restoreSessionFromStorage(session, clientID, sessionOpts)
    }

    // Add to cache
    b.sessionsMap.Set(clientID, session)

    // Persist to storage
    if b.sessions != nil {
        b.sessions.Save(session.Info())
    }

    // Register in cluster
    if b.cluster != nil {
        ctx := context.Background()
        nodeID := b.cluster.NodeID()
        b.cluster.AcquireSession(ctx, clientID, nodeID)
    }

    return session, true, nil
}
```

**Key Steps**:
1. Check cache for existing session
2. Handle clean_start flag (destroy or resume)
3. Create new session with inflight tracker and queue
4. Restore state from BadgerDB if persistent session
5. Register ownership in etcd cluster
6. Set disconnect callback

### Disconnection Flow

```
Client            Session          Broker            Cluster           Storage
  │                 │                │                  │                 │
  ├─DISCONNECT─────▶│                │                  │                 │
  │                 ├─Disconnect()   │                  │                 │
  │                 │                │                  │                 │
  │                 ├─OnDisconnect──▶│                  │                 │
  │                 │  callback      │                  │                 │
  │                 │                ├─Save state──────────────────────▶│
  │                 │                │  (sessions)      │                 │
  │                 │                ├─Save queue──────────────────────▶│
  │                 │                │  (messages)      │                 │
  │                 │                ├─Save inflight───────────────────▶│
  │                 │                │  (messages)      │                 │
  │                 │                │                  │                 │
  │                 │                ├─ReleaseSession──▶│                 │
  │                 │                │                  ├─Delete etcd     │
  │                 │                │                  │  /sessions/X    │
  │                 │                │◀─────────────────┤                 │
  │                 │                │                  │                 │
  │                 ├──(if clean)    │                  │                 │
  │                 │  DestroySession│                  │                 │
  │                 │                ├─Delete all──────────────────────▶│
  │                 │                │                  │                 │
```

**handleDisconnect Implementation**:

```go
func (b *Broker) handleDisconnect(s *session.Session, graceful bool) {
    // Save session state
    if b.sessions != nil {
        b.sessions.Save(s.Info())
    }

    // Handle will message
    if b.wills != nil {
        will := s.GetWill()
        if !graceful && will != nil {
            b.wills.Set(s.ID, will)  // Trigger will
        } else if graceful {
            b.wills.Delete(s.ID)  // Cancel will
        }
    }

    // Save offline queue and inflight
    if b.messages != nil {
        // Save queued messages
        msgs := s.OfflineQueue().Drain()
        for i, msg := range msgs {
            key := fmt.Sprintf("%s/queue/%d", s.ID, i)
            b.messages.Store(key, msg)
        }

        // Save inflight messages
        for _, inf := range s.Inflight().GetAll() {
            key := fmt.Sprintf("%s/inflight/%d", s.ID, inf.PacketID)
            b.messages.Store(key, inf.Message)
        }
    }

    // Clean start with no expiry: destroy immediately
    if s.CleanStart && s.ExpiryInterval == 0 {
        b.mu.Lock()
        b.destroySessionLocked(s)
        b.mu.Unlock()
    } else {
        // Persistent session: release ownership
        if b.cluster != nil {
            ctx := context.Background()
            b.cluster.ReleaseSession(ctx, s.ID)
        }
    }
}
```

**Persistent vs Clean Sessions**:

| Aspect | Clean Session | Persistent Session |
|--------|---------------|-------------------|
| On Connect | Destroy old | Resume old |
| Subscriptions | Lost | Restored |
| Offline Queue | Lost | Restored |
| Inflight | Lost | Restored |
| On Disconnect | Destroyed | Saved to storage |
| Cluster Ownership | Released on destroy | Released on disconnect |

## Message Routing

### Publish Flow

```
Publisher         Broker            Router            Cluster           Remote Broker
  │                 │                  │                  │                 │
  ├─PUBLISH────────▶│                  │                  │                 │
  │  topic=a/b/c    │                  │                  │                 │
  │                 ├─Publish()        │                  │                 │
  │                 │                  │                  │                 │
  │                 ├─Store retained   │                  │                 │
  │                 │  (if flag set)   │                  │                 │
  │                 │                  │                  │                 │
  │                 ├─distribute()─────┤                  │                 │
  │                 │                  │                  │                 │
  │                 │                  ├─Match(a/b/c)     │                 │
  │                 │                  │  returns:        │                 │
  │                 │                  │  [client1, ...]  │                 │
  │                 │                  │                  │                 │
  │                 │◀─────────────────┤                  │                 │
  │                 │  local subs      │                  │                 │
  │                 │                  │                  │                 │
  │                 ├─DeliverToSession()                  │                 │
  │                 │  (for each local)                   │                 │
  │                 │                  │                  │                 │
  │                 ├─RoutePublish()──────────────────────▶│                 │
  │                 │                  │                  │                 │
  │                 │                  │                  ├─GetSubscribers  │
  │                 │                  │                  │  (query etcd)   │
  │                 │                  │                  │                 │
  │                 │                  │                  ├─GetSessionOwner │
  │                 │                  │                  │  (for each)     │
  │                 │                  │                  │                 │
  │                 │                  │                  ├─SendPublish────▶│
  │                 │                  │                  │  (gRPC)         │
  │                 │                  │                  │                 │
  │                 │                  │                  │                 ├─HandlePublish()
  │                 │                  │                  │                 │
  │                 │                  │                  │                 ├─DeliverToClient()
  │                 │                  │                  │                 │
  │                 │                  │                  │                 ├─→ Subscriber
  │                 │                  │                  │                 │
  │◀──PUBACK────────┤                  │                  │                 │
  │  (if QoS>0)     │                  │                  │                 │
```

### distribute() Implementation

```go
func (b *Broker) distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
    // 1. Deliver to local subscribers
    matched, err := b.router.Match(topic)
    if err != nil {
        return err
    }

    for _, sub := range matched {
        session := b.sessionsMap.Get(sub.ClientID)
        if session == nil {
            continue  // Session not connected locally
        }

        // Downgrade QoS if subscription QoS is lower
        deliverQoS := qos
        if sub.QoS < deliverQoS {
            deliverQoS = sub.QoS
        }

        msg := Message{
            Topic:      topic,
            Payload:    payload,
            QoS:        deliverQoS,
            Retain:     retain,
            Properties: props,
        }

        b.DeliverToSession(session, msg)
    }

    // 2. Route to remote subscribers in cluster
    if b.cluster != nil {
        ctx := context.Background()
        err := b.cluster.RoutePublish(ctx, topic, payload, qos, retain, props)
        if err != nil {
            b.logError("cluster_route_publish", err, slog.String("topic", topic))
        }
    }

    return nil
}
```

**Two-Phase Delivery**:
1. **Local Phase**: Match topic against local router, deliver immediately
2. **Cluster Phase**: Query etcd for remote subscribers, send via gRPC

**Why Not Broadcast?**
- Broadcasting to all nodes is wasteful
- Most topics have few subscribers
- Targeted delivery scales better

### Retained Messages

```go
func (b *Broker) Publish(msg Message) error {
    b.stats.IncrementPublishReceived()

    // Handle retained messages
    if msg.Retain {
        if len(msg.Payload) == 0 {
            // Empty payload: delete retained
            b.retained.Delete(msg.Topic)
        } else {
            // Store retained message
            storeMsg := &storage.Message{
                Topic:      msg.Topic,
                Payload:    msg.Payload,
                QoS:        msg.QoS,
                Retain:     true,
                Properties: msg.Properties,
            }
            b.retained.Set(msg.Topic, storeMsg)

            // Also store in cluster (etcd)
            if b.cluster != nil {
                b.cluster.SetRetained(ctx, msg.Topic, storeMsg)
            }
        }
    }

    // Distribute to subscribers
    return b.distribute(msg.Topic, msg.Payload, msg.QoS, false, msg.Properties)
}
```

**Retained Message Delivery on Subscribe**:

```go
func (b *Broker) subscribeInternal(s *session.Session, filter string, opts SubscriptionOptions) error {
    // ... add to router and storage ...

    // Deliver matching retained messages
    var retained []*storage.Message

    if b.cluster != nil {
        // Query cluster for retained
        retained, _ = b.cluster.GetRetainedMatching(ctx, filter)
    } else {
        // Query local storage
        retained, _ = b.retained.Match(filter)
    }

    for _, msg := range retained {
        b.DeliverToSession(s, Message{
            Topic:      msg.Topic,
            Payload:    msg.Payload,
            QoS:        msg.QoS,
            Retain:     true,  // RETAIN flag in delivery
            Properties: msg.Properties,
        })
    }

    return nil
}
```

## Topic Matching

### Router Implementation

The router uses a **Trie (prefix tree)** for efficient topic matching:

```go
type Router struct {
    mu   sync.RWMutex
    root *trieNode
}

type trieNode struct {
    children     map[string]*trieNode  // Regular level: "sensor", "home"
    wildcard     *trieNode             // + wildcard
    multiWildcard *trieNode            // # wildcard
    subscribers  []*Subscriber         // Subscribers at this node
}

type Subscriber struct {
    ClientID string
    Filter   string
    QoS      byte
    Options  storage.SubscribeOptions
}
```

**Example Trie**:

```
Subscriptions: sensor/+/temp, home/#, sensor/living/temp

         [root]
           │
        sensor ─── home
           │         │
        [+]──────── [#]
           │       (home/#)
         temp
      (sensor/+/temp)
           │
        living
           │
         temp
    (sensor/living/temp)
```

### Subscribe

```go
func (r *Router) Subscribe(clientID, filter string, qos byte, opts storage.SubscribeOptions) {
    r.mu.Lock()
    defer r.mu.Unlock()

    levels := strings.Split(filter, "/")
    node := r.root

    for _, level := range levels {
        switch level {
        case "+":
            if node.wildcard == nil {
                node.wildcard = &trieNode{children: make(map[string]*trieNode)}
            }
            node = node.wildcard

        case "#":
            if node.multiWildcard == nil {
                node.multiWildcard = &trieNode{}
            }
            node = node.multiWildcard
            // # must be last level
            break

        default:
            if node.children[level] == nil {
                node.children[level] = &trieNode{children: make(map[string]*trieNode)}
            }
            node = node.children[level]
        }
    }

    // Add subscriber at this node
    node.subscribers = append(node.subscribers, &Subscriber{
        ClientID: clientID,
        Filter:   filter,
        QoS:      qos,
        Options:  opts,
    })
}
```

### Match

```go
func (r *Router) Match(topic string) ([]*Subscriber, error) {
    r.mu.RLock()
    defer r.mu.RUnlock()

    levels := strings.Split(topic, "/")
    var result []*Subscriber

    r.matchRecursive(r.root, levels, 0, &result)

    return result, nil
}

func (r *Router) matchRecursive(node *trieNode, levels []string, depth int, result *[]*Subscriber) {
    if node == nil {
        return
    }

    // Reached end of topic
    if depth == len(levels) {
        *result = append(*result, node.subscribers...)
        return
    }

    level := levels[depth]

    // Match exact level
    if child := node.children[level]; child != nil {
        r.matchRecursive(child, levels, depth+1, result)
    }

    // Match + wildcard
    if node.wildcard != nil {
        r.matchRecursive(node.wildcard, levels, depth+1, result)
    }

    // Match # wildcard (consumes all remaining levels)
    if node.multiWildcard != nil {
        *result = append(*result, node.multiWildcard.subscribers...)
    }
}
```

**Complexity**:
- Subscribe: O(L) where L = levels in filter
- Match: O(L * B) where B = branching factor (usually small)
- Space: O(N * L) where N = number of subscriptions

**Example Matching**:

```
Topic: sensor/living/temp

Trie traversal:
1. root
2. root.children["sensor"]
3. root.children["sensor"].children["living"]  ← Match: sensor/living/temp
4. root.children["sensor"].wildcard            ← Match: sensor/+/temp
5. root.children["sensor"].children["living"].children["temp"]
6. At each step, check multiWildcard           ← Match: sensor/#, #

Result: [sensor/living/temp, sensor/+/temp, sensor/#, #]
```

## QoS Handling

### QoS Levels

| QoS | Guarantee | Flow |
|-----|-----------|------|
| 0 | At most once | PUBLISH → (done) |
| 1 | At least once | PUBLISH → PUBACK |
| 2 | Exactly once | PUBLISH → PUBREC → PUBREL → PUBCOMP |

### QoS 0 (Fire and Forget)

```go
func (b *Broker) DeliverToSession(s *session.Session, msg Message) (uint16, error) {
    if msg.QoS == 0 {
        // Immediate delivery, no tracking
        return 0, s.DeliverMessage(msg, 0)
    }
    // ... QoS 1/2 handling ...
}
```

**Characteristics**:
- No acknowledgment
- No retransmission
- No persistence
- Fastest delivery

### QoS 1 (At Least Once)

```go
func (b *Broker) DeliverToSession(s *session.Session, msg Message) (uint16, error) {
    if msg.QoS == 1 {
        packetID := s.NextPacketID()

        // Track as inflight
        s.Inflight().Add(packetID, &messages.InflightEntry{
            PacketID: packetID,
            Message:  msg,
            State:    messages.StatePublished,
            SentAt:   time.Now(),
        })

        // Persist to storage
        if b.messages != nil {
            key := fmt.Sprintf("%s/inflight/%d", s.ID, packetID)
            b.messages.Store(key, msg)
        }

        // Deliver
        return packetID, s.DeliverMessage(msg, packetID)
    }
    // ... QoS 2 handling ...
}
```

**Acknowledgment**:

```go
func (b *Broker) AckMessage(clientID string, packetID uint16, qos byte) error {
    s := b.Get(clientID)
    if s == nil {
        return ErrSessionNotFound
    }

    // Remove from inflight
    s.Inflight().Remove(packetID)

    // Delete from storage
    if b.messages != nil {
        key := fmt.Sprintf("%s/inflight/%d", clientID, packetID)
        b.messages.Delete(key)
    }

    return nil
}
```

**Retransmission** (on reconnect):

```go
func (b *Broker) restoreInflightFromStorage(clientID string, tracker messages.Inflight) error {
    msgs, _ := b.messages.GetByPrefix(clientID + "/inflight/")

    for _, msg := range msgs {
        packetID := extractPacketID(msg)
        tracker.Add(packetID, &messages.InflightEntry{
            PacketID: packetID,
            Message:  msg,
            State:    messages.StatePublished,
            SentAt:   time.Now(),
        })
    }

    return nil
}
```

### QoS 2 (Exactly Once)

```go
func (b *Broker) DeliverToSession(s *session.Session, msg Message) (uint16, error) {
    if msg.QoS == 2 {
        packetID := s.NextPacketID()

        // Track as inflight with QoS 2 state machine
        s.Inflight().Add(packetID, &messages.InflightEntry{
            PacketID: packetID,
            Message:  msg,
            State:    messages.StatePublished,  // → PUBREC → PUBREL → PUBCOMP
            SentAt:   time.Now(),
        })

        return packetID, s.DeliverMessage(msg, packetID)
    }
}
```

**State Machine**:

```
PUBLISH (→ PUBREC) → Receive PUBREC
    ↓
  Store PUBREL state
    ↓
PUBREL (→ PUBCOMP) → Receive PUBCOMP
    ↓
  Remove from inflight
```

## Cluster Integration

### Session Ownership Registration

```go
func (b *Broker) CreateSession(clientID string, opts SessionOptions) (*session.Session, bool, error) {
    // ... create session locally ...

    // Register ownership in cluster
    if b.cluster != nil {
        ctx := context.Background()
        nodeID := b.cluster.NodeID()
        err := b.cluster.AcquireSession(ctx, clientID, nodeID)
        if err != nil {
            b.logError("cluster_acquire_session", err, slog.String("client_id", clientID))
            // Don't fail connection, log and continue
        }
    }

    return session, true, nil
}
```

### Subscription Registration

```go
func (b *Broker) subscribeInternal(s *session.Session, filter string, opts SubscriptionOptions) error {
    // Add to local router
    b.router.Subscribe(s.ID, filter, opts.QoS, storeOpts)

    // Persist to local storage
    b.subscriptions.Add(sub)

    // Register in cluster for routing
    if b.cluster != nil {
        ctx := context.Background()
        err := b.cluster.AddSubscription(ctx, s.ID, filter, opts.QoS, storeOpts)
        if err != nil {
            b.logError("cluster_add_subscription", err,
                slog.String("client_id", s.ID),
                slog.String("filter", filter))
        }
    }

    return nil
}
```

### Cross-Node Message Delivery

```go
// Implements cluster.MessageHandler interface
func (b *Broker) DeliverToClient(ctx context.Context, clientID, topic string, payload []byte, qos byte, retain bool, dup bool, properties map[string]string) error {
    // Called by cluster transport when message routed from remote node
    session := b.Get(clientID)
    if session == nil {
        return fmt.Errorf("session not found: %s", clientID)
    }

    msg := Message{
        Topic:      topic,
        Payload:    payload,
        QoS:        qos,
        Retain:     retain,
        Properties: properties,
    }

    _, err := b.DeliverToSession(session, msg)
    return err
}
```

**Flow**:
1. Remote node calls `transport.SendPublish(nodeID, clientID, ...)`
2. gRPC client sends `RoutePublish` RPC
3. Local transport receives RPC, calls `HandlePublish()`
4. Cluster layer calls `broker.DeliverToClient()`
5. Broker delivers to local session

### Subscription Cleanup

```go
func (b *Broker) destroySessionLocked(s *session.Session) error {
    // ... delete from storage ...

    // Remove subscriptions from cluster
    subs := s.GetSubscriptions()
    for filter := range subs {
        b.router.Unsubscribe(s.ID, filter)

        if b.cluster != nil {
            ctx := context.Background()
            b.cluster.RemoveSubscription(ctx, s.ID, filter)
        }
    }

    // Release session ownership in cluster
    if b.cluster != nil {
        ctx := context.Background()
        b.cluster.ReleaseSession(ctx, s.ID)
    }

    b.sessionsMap.Delete(s.ID)

    return nil
}
```

## Summary

The broker architecture provides:

**Clean Separation**:
- Protocol handlers translate packets
- Broker contains pure domain logic
- Storage and cluster are pluggable

**Efficient Routing**:
- Trie-based topic matching: O(L)
- Local delivery: immediate
- Remote delivery: targeted via etcd query

**QoS Guarantees**:
- QoS 0: Fire and forget
- QoS 1: At least once, with retransmission
- QoS 2: Exactly once, with state machine

**Cluster Integration**:
- Session ownership tracked in etcd
- Subscriptions visible cluster-wide
- Messages routed directly via gRPC
- Seamless local/remote delivery

For more details, see:
- [Clustering Architecture](clustering-architecture.md) - Overall design
- [Clustering Infrastructure](clustering-infrastructure.md) - etcd, gRPC, BadgerDB
- [Configuration](configuration.md) - Setup and tuning
