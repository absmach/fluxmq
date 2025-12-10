# Plan: Refactor Broker to Version-Specific Handlers

## Overview

Refactor the MQTT broker architecture to support version-specific handlers (v3 and v5) while keeping broker.Broker as the entry point. The broker will be instantiated for a specific MQTT version at startup using `broker.New(version byte)`.

## User Requirements

1. **Entry Point**: Keep broker.Broker as entry point, make it version-aware
2. **API**: Unified constructor `broker.New(version byte)` called once at startup
3. **Version Detection**: Connection/codec detects version and validates against broker's expected version
4. **Code Reuse**: Shared core logic with version-specific adapters (composition pattern)
5. **Focus**: Implement v3 fully, prepare architecture for v5 addition

## Current Architecture Issues

- `broker/broker.go` hardcodes v3 packet types in HandleConnection()
- `handlers/broker.go` (BrokerHandler) is completely v3-specific with all type assertions to v3 packets
- No abstraction for version-specific packet handling
- Protocol detection exists but version isn't validated or used for handler selection

## Target Architecture

```
broker.New(version=3) → Broker (version-aware)
                          ├─ v3Handler (handlers/v3/handler.go)
                          │   └─ uses core.PubSubEngine
                          ├─ sessionManager (shared)
                          ├─ router (shared)
                          └─ store (shared)

HandleConnection flow:
1. Connection detects protocol version from CONNECT packet
2. Broker validates detected version == broker.version
3. Broker delegates to version-specific handler
4. Handler processes CONNECT and enters packet loop
```

## File Structure Changes

### New Files to Create

```
handlers/v3/
  handler.go          # V3-specific handler implementation (moves logic from handlers/broker.go)
  packets.go          # V3 packet parsing/creation helpers

handlers/v5/          # Placeholder for future v5 support
  handler.go          # (future) V5-specific handler
  packets.go          # (future) V5 packet helpers

handlers/core/
  pubsub.go          # Shared pub/sub logic (QoS handling, message distribution)
  auth.go            # Shared authentication/authorization logic
  subscriptions.go   # Shared subscription management logic
```

### Files to Modify

```
broker/broker.go
  - Add version field to Broker struct
  - Replace HandleConnection logic to validate version and delegate
  - Add broker.New(version byte) constructor

broker/connection.go
  - Expose detected version via connection.Version() method
  - Keep existing DetectProtocolVersion logic

handlers/broker.go → handlers/v3/handler.go
  - Move existing BrokerHandler to handlers/v3/handler.go
  - Rename to V3Handler
  - Extract shared logic to handlers/core/

handlers/handler.go
  - Keep Handler interface unchanged (works for both v3 and v5)
  - Add documentation about version-specific implementations
```

## Detailed Implementation Plan

### Phase 1: Create Core Shared Logic (No Breaking Changes)

**Goal**: Extract version-agnostic business logic into reusable components.

#### Step 1.1: Create handlers/core/pubsub.go - **DONE**

Extract shared pub/sub logic:
- QoS 0/1/2 message handling workflows
- Inflight message tracking integration
- Offline queue management
- Retained message handling

**Shared Functions/Types:**
```go
// handlers/core/pubsub.go

type PubSubEngine struct {
    router     Router
    publisher  Publisher
    retained   RetainedStore
}

// HandleQoS0Publish - fire and forget
func (e *PubSubEngine) HandleQoS0Publish(topic string, payload []byte, retain bool) error

// HandleQoS1Publish - publish with ack
func (e *PubSubEngine) HandleQoS1Publish(sess *session.Session, topic string, payload []byte, retain bool, packetID uint16) error

// HandleQoS2Publish - publish with 2-phase commit
func (e *PubSubEngine) HandleQoS2Publish(sess *session.Session, topic string, payload []byte, retain bool, packetID uint16, dup bool) error

// ProcessSubscription - handle subscription request
func (e *PubSubEngine) ProcessSubscription(sess *session.Session, filter string, qos byte) error

// SendRetainedMessages - deliver retained messages for a subscription
func (e *PubSubEngine) SendRetainedMessages(sess *session.Session, filter string, maxQoS byte, deliver func(msg *store.Message) error) error
```

**Files to Reference:**
- Current logic in `handlers/broker.go` lines 68-122 (HandlePublish)
- Lines 124-148 (publishMessage)
- Lines 189-235 (HandleSubscribe)

#### Step 1.2: Create handlers/core/auth.go - **DONE**

Extract auth/authz checks:
```go
// handlers/core/auth.go

type AuthEngine struct {
    auth  Authenticator
    authz Authorizer
}

func (e *AuthEngine) CanPublish(clientID, topic string) bool
func (e *AuthEngine) CanSubscribe(clientID, topic string) bool
```

#### Step 1.3: Update handlers/broker.go to Use Core Logic

Refactor existing BrokerHandler to use core.PubSubEngine and core.AuthEngine:
- Replace inline logic with calls to core functions
- Keep packet parsing/sending v3-specific
- Add tests to ensure no behavior changes

**This phase keeps current API intact - tests should still pass.**

---

### Phase 2: Create Version-Specific Handler Structure

**Goal**: Move v3 handler into dedicated package, prepare for v5.

#### Step 2.1: Create handlers/v3/handler.go

Move `handlers/broker.go` → `handlers/v3/handler.go`:
```go
// handlers/v3/handler.go
package v3

import (
    "github.com/dborovcanin/mqtt/handlers"
    "github.com/dborovcanin/mqtt/handlers/core"
    "github.com/dborovcanin/mqtt/session"
    v3packets "github.com/dborovcanin/mqtt/packets/v3"
)

type V3Handler struct {
    sessionMgr *session.Manager
    pubsub     *core.PubSubEngine
    auth       *core.AuthEngine
}

func NewV3Handler(cfg handlers.BrokerHandlerConfig) *V3Handler {
    return &V3Handler{
        sessionMgr: cfg.SessionManager,
        pubsub: core.NewPubSubEngine(cfg.Router, cfg.Publisher, cfg.Retained),
        auth: core.NewAuthEngine(cfg.Authenticator, cfg.Authorizer),
    }
}

// Implement handlers.Handler interface with v3-specific packet handling
func (h *V3Handler) HandleConnect(s *session.Session, pkt packets.ControlPacket) error {
    // Cast to v3.Connect
    // Existing logic from handlers/broker.go
}

func (h *V3Handler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
    p := pkt.(*v3packets.Publish)
    // Use h.pubsub for business logic
    // V3-specific: packet parsing, response creation
}

// ... other handler methods
```

#### Step 2.2: Create handlers/v3/packets.go

V3-specific packet helpers:
```go
// handlers/v3/packets.go
package v3

import v3packets "github.com/dborovcanin/mqtt/packets/v3"

// SendPubAck creates and sends a v3 PUBACK
func SendPubAck(s *session.Session, packetID uint16) error

// SendConnAck creates and sends a v3 CONNACK
func SendConnAck(s *session.Session, returnCode byte) error

// ... other v3 packet senders
```

#### Step 2.3: Create handlers/v5/ Placeholder

```go
// handlers/v5/handler.go
package v5

// Placeholder for future v5 implementation
// Will implement handlers.Handler interface with v5 packet types
```

#### Step 2.4: Update handlers/broker.go as Compatibility Wrapper (Temporary)

Keep old import path working temporarily:
```go
// handlers/broker.go
package handlers

import "github.com/dborovcanin/mqtt/handlers/v3"

// Deprecated: Use v3.NewV3Handler directly
type BrokerHandler = v3.V3Handler

// Deprecated: Use v3.NewV3Handler directly
func NewBrokerHandler(cfg BrokerHandlerConfig) *BrokerHandler {
    return v3.NewV3Handler(cfg)
}
```

**Tests should still pass - only imports changed.**

---

### Phase 3: Make Broker Version-Aware

**Goal**: Add broker.New(version) API and version validation.

#### Step 3.1: Update broker/broker.go - Add Version Field

```go
// broker/broker.go
type Broker struct {
    version    byte // MQTT protocol version (3, 4, 5)
    sessionMgr *session.Manager
    router     *Router
    store      store.Store
    handler    handlers.Handler // Version-specific handler
    dispatcher *handlers.Dispatcher
    logger     *slog.Logger
}
```

#### Step 3.2: Add broker.New(version byte) Constructor

```go
// broker/broker.go

// New creates a new MQTT broker for the specified protocol version.
// version: 3 (MQTT 3.1), 4 (MQTT 3.1.1), or 5 (MQTT 5.0)
// Only v3/v4 are currently supported. v5 will return error.
func New(version byte, logger *slog.Logger) (*Broker, error) {
    if logger == nil {
        logger = slog.Default()
    }

    // Validate version
    switch version {
    case 3, 4:
        // v3.1 and v3.1.1 supported
    case 5:
        return nil, fmt.Errorf("MQTT v5.0 not yet implemented")
    default:
        return nil, fmt.Errorf("unsupported MQTT version: %d", version)
    }

    st := memory.New()
    sessionMgr := session.NewManager(st)
    router := NewRouter()

    b := &Broker{
        version:    version,
        sessionMgr: sessionMgr,
        router:     router,
        store:      st,
        logger:     logger,
    }

    // Create version-specific handler
    switch version {
    case 3, 4:
        b.handler = v3.NewV3Handler(handlers.BrokerHandlerConfig{
            SessionManager: sessionMgr,
            Router:         b,
            Publisher:      b,
            Retained:       st.Retained(),
        })
    case 5:
        // Future: b.handler = v5.NewV5Handler(...)
        return nil, fmt.Errorf("v5 handler not implemented")
    }

    b.dispatcher = handlers.NewDispatcher(b.handler)

    sessionMgr.SetOnWillTrigger(func(will *store.WillMessage) {
        b.Distribute(will.Topic, will.Payload, will.QoS, will.Retain, will.Properties)
    })

    return b, nil
}

// NewBroker is deprecated. Use New() instead.
// Defaults to v3.1.1 (version 4).
func NewBroker(logger *slog.Logger) *Broker {
    b, err := New(4, logger)
    if err != nil {
        panic(err) // Should never happen for v4
    }
    return b
}
```

#### Step 3.3: Update broker/connection.go - Expose Version

Add method to expose detected version:
```go
// broker/connection.go

// Version returns the detected MQTT protocol version.
// Returns 0 if version hasn't been detected yet.
func (c *mqttCodec) Version() byte {
    return byte(c.version)
}
```

Update Connection interface:
```go
// broker/interfaces.go

type Connection interface {
    ReadPacket() (packets.ControlPacket, error)
    WritePacket(p packets.ControlPacket) error
    Close() error
    RemoteAddr() net.Addr
    Version() byte // Add this method
}
```

#### Step 3.4: Update HandleConnection to Validate Version

```go
// broker/broker.go - HandleConnection method

func (b *Broker) HandleConnection(netConn net.Conn) {
    conn := NewConnection(netConn)

    // Read CONNECT packet (triggers version detection in connection)
    pkt, err := conn.ReadPacket()
    if err != nil {
        b.logger.Error("Failed to read CONNECT packet", ...)
        conn.Close()
        return
    }

    // Validate packet type
    if pkt.Type() != packets.ConnectType {
        b.logger.Warn("Expected CONNECT packet", ...)
        conn.Close()
        return
    }

    // Get detected version from connection
    detectedVersion := conn.Version()

    // Validate version matches broker's expected version
    // Allow v3 (3.1) and v4 (3.1.1) to be compatible
    compatible := false
    switch b.version {
    case 3:
        compatible = (detectedVersion == 3)
    case 4:
        compatible = (detectedVersion == 3 || detectedVersion == 4)
    case 5:
        compatible = (detectedVersion == 5)
    }

    if !compatible {
        b.logger.Warn("Protocol version mismatch",
            slog.Int("expected", int(b.version)),
            slog.Int("detected", int(detectedVersion)),
            slog.String("remote_addr", netConn.RemoteAddr().String()))

        // Send CONNACK with error (version-specific)
        // For v3/v4: return code 0x01 (unacceptable protocol version)
        // For v5: reason code 0x84 (unsupported protocol version)
        b.sendConnAckError(conn, detectedVersion)
        conn.Close()
        return
    }

    // Parse CONNECT packet (version-specific)
    // Delegate to handler for version-specific parsing
    sess, err := b.createSessionFromConnect(pkt, detectedVersion)
    if err != nil {
        b.logger.Error("Failed to create session", ...)
        conn.Close()
        return
    }

    // Rest of HandleConnection logic remains similar
    // Attach connection, send CONNACK, deliver offline messages, run session
    ...
}
```

---

### Phase 4: Update Main Entry Point

**Goal**: Use new broker.New() API in cmd/broker/main.go.

#### Step 4.1: Update cmd/broker/main.go

```go
// cmd/broker/main.go

func main() {
    // Load config
    cfg, err := config.Load("")
    if err != nil {
        log.Fatal(err)
    }

    // Determine MQTT version from config (default to v4/3.1.1)
    mqttVersion := byte(4) // Default: MQTT 3.1.1
    // Future: read from cfg.Broker.ProtocolVersion

    logger := setupLogger(cfg)

    // Create version-specific broker
    broker, err := broker.New(mqttVersion, logger)
    if err != nil {
        logger.Error("Failed to create broker", slog.String("error", err.Error()))
        log.Fatal(err)
    }
    defer broker.Close()

    // Start TCP server
    server := tcp.NewServer(cfg.Server.TCPAddr, broker)
    logger.Info("Starting MQTT broker",
        slog.String("version", fmt.Sprintf("v%d", mqttVersion)),
        slog.String("addr", cfg.Server.TCPAddr))

    if err := server.ListenAndServe(); err != nil {
        logger.Error("Server error", slog.String("error", err.Error()))
        log.Fatal(err)
    }
}
```

#### Step 4.2: Add Version to Config (Optional)

```go
// config/config.go - Add to BrokerConfig

type BrokerConfig struct {
    // MQTT protocol version to support (3, 4, or 5)
    ProtocolVersion byte `yaml:"protocol_version"`

    // ... existing fields
}

// Update Default() to set ProtocolVersion: 4
```

---

### Phase 5: Testing and Validation

**Goal**: Ensure v3 functionality is preserved and architecture supports v5.

#### Step 5.1: Update Existing Tests

- Update imports in test files: `handlers.NewBrokerHandler` → `v3.NewV3Handler`
- Add tests for `broker.New(version)` validation
- Test version mismatch scenarios (v3 client connects to v5 broker)

#### Step 5.2: Add Integration Tests

```go
// broker/broker_test.go

func TestBrokerVersionValidation(t *testing.T) {
    // Test creating broker with supported versions
    // Test version detection and validation in HandleConnection
}

func TestBrokerRejectsWrongVersion(t *testing.T) {
    // Create v3 broker
    // Send v5 CONNECT packet
    // Verify CONNACK error and connection close
}
```

#### Step 5.3: Run Full Test Suite

```bash
go test ./...
```

Ensure all existing tests pass with refactored architecture.

---

## Migration Steps Summary

1. **Phase 1**: Extract shared logic → No breaking changes, tests pass
2. **Phase 2**: Create v3 package → Old API still works via wrapper
3. **Phase 3**: Add broker.New(version) → New API available, old API deprecated
4. **Phase 4**: Update main.go → Start using new API
5. **Phase 5**: Test thoroughly → Verify v3 works, validate v5 hooks

## Future v5 Support Roadmap

When implementing v5 in the future:

1. **Create handlers/v5/handler.go**:
   - Implement Handler interface with v5 packet types
   - Handle v5-specific packets (AUTH)
   - Process properties in all packets

2. **Extend core logic**:
   - Add property handling in core.PubSubEngine
   - Support topic aliases, subscription IDs
   - Enhanced authentication flows

3. **Update broker.New()**:
   - Enable `case 5:` branch
   - Create v5.NewV5Handler(...)

4. **Add v5 tests**:
   - Properties validation
   - Enhanced authentication
   - Topic alias handling

## Critical Files to Modify

**Phase 1 (Core Extraction):**
- [ ] Create `handlers/core/pubsub.go`
- [ ] Create `handlers/core/auth.go`
- [ ] Update `handlers/broker.go` to use core

**Phase 2 (V3 Package):**
- [ ] Create `handlers/v3/handler.go`
- [ ] Create `handlers/v3/packets.go`
- [ ] Create `handlers/v5/handler.go` (placeholder)
- [ ] Update `handlers/broker.go` (compatibility wrapper)

**Phase 3 (Version-Aware Broker):**
- [ ] Update `broker/broker.go` - add version field and New()
- [ ] Update `broker/connection.go` - expose Version()
- [ ] Update `broker/interfaces.go` - add Version() to Connection interface
- [ ] Update `broker/broker.go` - HandleConnection validation

**Phase 4 (Entry Point):**
- [ ] Update `cmd/broker/main.go`
- [ ] Update `config/config.go` (optional)

**Phase 5 (Testing):**
- [ ] Update test imports
- [ ] Add version validation tests
- [ ] Run integration tests

## Risk Mitigation

- **Incremental approach**: Each phase is independently testable
- **Backward compatibility**: Old API kept until Phase 4
- **Shared logic first**: Extract before splitting reduces duplication risk
- **Version validation**: Fail fast on version mismatch prevents silent bugs
- **V5 placeholder**: Architecture ready, implementation deferred

## Success Criteria

- [ ] All existing tests pass
- [ ] broker.New(3) and broker.New(4) create working v3 brokers
- [ ] broker.New(5) returns clear "not implemented" error
- [ ] Version mismatch detected and handled gracefully
- [ ] Code duplication minimized via handlers/core
- [ ] Integration tests demonstrate v3 functionality
- [ ] Architecture clearly supports adding v5 implementation
