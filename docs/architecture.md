# MQTT Broker Architecture

## Overview

This document describes the architecture of a multi-protocol MQTT broker designed with a **Plugin Architecture**. The broker core is decoupled from the network transport layer, allowing various protocols (HTTP, WebSocket, CoAP) to plug in via standardized interfaces.

**Design Philosophy:**
1.  **Plugin-First** - The broker exposes `ConnectionHandler` and `OperationHandler` interfaces for any protocol to use.
2.  **Protocol Agnostic Core** - The core logic deals only with MQTT semantics; transport details are handled by specific plugins.
3.  **Leverage Ecosystem** - Use existing, robust libraries for external protocols (net/http, gorilla/websocket) rather than reinventing them.
4.  **Core Packets** - Pure MQTT packet encoding/decoding (the essential MQTT implementation).
5.  **Extensibility** - Storage, authentication, and routing are pluggable.

## Architecture Diagram

```
              ┌─────────────────────────────────────────────────────────────┐
              │                     Protocol Plugins                        │
              │                                                             │
              │  ┌────────────┐   ┌────────────┐   ┌────────────┐           │
              │  │ TCP Server │   │ HTTP Server│   │  WS Server │           │
              │  │ (net.Conn) │   │ (net/http) │   │ (gorilla)  │           │
              │  └─────┬──────┘   └─────┬──────┘   └─────┬──────┘           │
              └────────│────────────────│────────────────│──────────────────┘
                       │                │                │
            Stream     │                │ Stateless      │ Stream
            Connection │                │ Operation      │ Connection
                       │                │                │
     ┌─────────────────▼────────────────▼────────────────▼────────────────────┐
     │                           Broker Interface                             │
     │                 (ConnectionHandler / OperationHandler)                 │
     └──────────────────────────────────┬─────────────────────────────────────┘
                                        │
                 ┌──────────────────────┼──────────────────────┐
                 │                      │                      │
                 ▼                      ▼                      ▼
          ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
          │   Handlers  │       │   Session   │       │   Topic     │
          │  (dispatch) │◄─────►│   Manager   │◄─────►│   Router    │
          └──────┬──────┘       └──────┬──────┘       └──────┬──────┘
                 │                     │                     │
                 └──────────────┬──────┴─────────────────────┘
                                │
                                ▼
                    ┌───────────────────────┐
                    │        Store          │
                    │  (sessions, retained, │
                    │   subscriptions)      │
                    └───────────────────────┘
```

## Component Layers

### Layer 1: Protocol Frontends & Plugins

Instead of a monolithic server, the broker supports multiple "Frontends" or "Plugins".

#### 1. Connection-Oriented (TCP, WebSocket)
For protocols that maintain a continuous stream (MQTT over TCP, MQTT over WebSocket), plugins use the `ConnectionHandler` interface. They pass a `broker.Connection` (which can encapsulate `net.Conn` or `websocket.Conn`) to the broker.

**Key Interface:**

```go
type ConnectionHandler interface {
    // HandleConnection runs the main MQTT protocol loop on a connection.
    // It blocks until the connection is closed.
    HandleConnection(conn Connection)
}

// Connection abstracts the packet stream.
type Connection interface {
    ReadPacket() (packets.ControlPacket, error)
    WritePacket(p packets.ControlPacket) error
    Close() error
    RemoteAddr() net.Addr
}
```

The broker provides a `StreamConnection` helper to easily wrap a standard `net.Conn`.

#### 2. Stateless Operations (HTTP, CoAP)
For request/response protocols, plugins use the `OperationHandler` interface to inject messages or subscribe to topics without maintaining a full MQTT session state in the protocol layer.

**Key Interface:**

```go
type OperationHandler interface {
    // Publish injects a message directly into the broker.
    Publish(ctx context.Context, clientID string, topic string, payload []byte, qos byte, retain bool) error
    
    // SubscribeToTopic allows a stateless client to listen for messages.
    // (Implementation pending for streaming response)
    SubscribeToTopic(ctx context.Context, clientID string, topicFilter string) (<-chan *store.Message, error)
}
```

### Layer 2: Core Packets (`packets/`)

The essential MQTT implementation. Pure packet encoding/decoding with no I/O.

```
packets/
├── packets.go        # Interfaces: ControlPacket, Encoder, Decoder
├── codec/            # Binary encoding primitives
│   ├── encode.go     # VBI, string, uint16/32 encoding
│   ├── decode.go     # VBI, string, uint16/32 decoding
│   └── zerocopy.go   # Zero-allocation parsing
├── sniffer.go        # Protocol version detection
├── validate.go       # Packet validation
├── v3/               # MQTT 3.1.1 packets
│   ├── connect.go
│   ├── publish.go
│   └── pool/         # Object pooling
└── v5/               # MQTT 5.0 packets (with properties)
    ├── connect.go
    ├── publish.go
    └── pool/
```

### Layer 3: Broker Core (`broker/`)

The broker orchestrates sessions, routing, and storage. It implements the `ConnectionHandler` and `OperationHandler` interfaces.

```
broker/
├── server.go         # Main broker, implements handler interfaces
├── interfaces.go     # Definition of Frontend, ConnectionHandler, etc.
├── connection.go     # StreamConnection wrapper logic
└── router.go         # Topic subscription trie
```

### Layer 4: Handlers (`handlers/`)

Packet handlers implement simple MQTT protocol logic (processing a PUBLISH, returning a PUBACK).

```
handlers/
├── handler.go        # Handler interface
├── broker.go         # BrokerHandler implementation
├── dispatcher.go     # Routes packets to handlers
└── handlers_test.go
```

### Layer 5: Session Management (`session/`)

Sessions track client state independent of connection.

```
session/
├── session.go        # Session struct with full state
├── manager.go        # SessionManager for lifecycle
├── inflight.go       # QoS 1/2 message tracking
└── queue.go          # Offline message queue
```

### Layer 6: Topic Router (`topics/`)

Subscription matching using a topic trie.

```
topics/
├── trie.go           # Subscription trie
├── match.go          # Wildcard matching
└── validate.go       # Topic validation
```

### Layer 7: Storage (`store/`)

Pluggable storage backends.

```
store/
├── store.go          # Interfaces
├── memory/           # In-memory (default)
└── other/            # (Future: Redis, Etcd)
```

## Data Flow Examples

### MQTT Client Publishing (via TCP)

1.  **Transport**: `transport/tcp.go` accepts `net.Conn`.
2.  **Wrap**: Transport wraps `net.Conn` into `broker.StreamConnection`.
3.  **Handoff**: Transport calls `server.HandleConnection(streamConn)`.
4.  **Loop**: Server starts read loop on `streamConn`.
5.  **Packet**: `StreamConnection` detects version, reads `CONNECT` then `PUBLISH`.
6.  **Dispatch**: Server passes packet to `handlers.HandlePublish`.
7.  **Route**: Handler finds subscribers via `topics.Router`.
8.  **Distribute**: Message sent to subscriber sessions.

### HTTP Client Publishing (via HTTP Adapter)

1.  **Transport**: `adapter/http.go` (standard `http.Handler`) receives POST request.
2.  **Parse**: Adapter parses JSON body and query params.
3.  **Inject**: Adapter calls `server.Publish(...)` (OperationHandler).
4.  **Route**: Server distributes message to matching subscribers internally.
5.  **Response**: Adapter returns `200 OK`.

## Current Package Structure

```
mqtt/
├── packets/              # Core MQTT packet encoding
├── broker/               # Broker core (Server, Interfaces)
├── handlers/             # Protocol Logic
├── session/              # Session State
├── topics/               # Topic Trie
├── store/                # Storage Interfaces & Impl
├── adapter/              # HTTP Adapter
├── transport/            # TCP/WS Transports
├── client/               # Testing Client
└── integration/          # Integration tests
```
