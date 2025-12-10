package session

import (
	"log/slog"
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/packets"
)

// ConnectionHandler manages the low-level connection, I/O, and keep-alive.
type ConnectionHandler struct {
	mu sync.RWMutex

	clientID string
	conn     Connection
	state    State

	connectedAt    time.Time
	disconnectedAt time.Time
	lastActivity   time.Time

	keepAlive       uint16
	keepAliveExpiry time.Duration
	keepAliveTimer  *time.Timer

	onDisconnect func(graceful bool)
	stopCh       chan struct{}
}

// NewConnectionHandler creates a new connection handler.
func NewConnectionHandler(clientID string, keepAlive uint16) *ConnectionHandler {
	h := &ConnectionHandler{
		clientID: clientID,
		state:    StateNew,
		stopCh:   make(chan struct{}),
	}
	h.SetKeepAlive(keepAlive)
	return h
}

// Connect attaches a connection.
func (h *ConnectionHandler) Connect(conn Connection) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.conn = conn
	h.state = StateConnected
	h.connectedAt = time.Now()
	h.lastActivity = time.Now()

	if h.keepAliveExpiry > 0 {
		h.startKeepAliveTimer()
	}

	return nil
}

// Disconnect performs disconnection logic.
func (h *ConnectionHandler) Disconnect(graceful bool) {
	h.mu.Lock()

	if h.state != StateConnected {
		h.mu.Unlock()
		return
	}

	h.state = StateDisconnecting
	h.cleanupConnectionResources(graceful)
	callback := h.onDisconnect

	h.mu.Unlock()

	// Reset stopCh for potential reconnection
	h.mu.Lock()
	h.stopCh = make(chan struct{})
	h.mu.Unlock()

	if callback != nil {
		go callback(graceful)
	}
}

func (h *ConnectionHandler) cleanupConnectionResources(graceful bool) {
	if h.keepAliveTimer != nil {
		h.keepAliveTimer.Stop()
		h.keepAliveTimer = nil
	}

	if h.conn != nil {
		h.conn.Close()
		h.conn = nil
	}

	h.state = StateDisconnected
	h.disconnectedAt = time.Now()
	close(h.stopCh)
}

// WritePacket writes a packet to the connection.
func (h *ConnectionHandler) WritePacket(pkt packets.ControlPacket) error {
	h.mu.RLock()
	conn := h.conn
	h.mu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	return conn.WritePacket(pkt)
}

// ReadPacket reads a packet from the connection.
func (h *ConnectionHandler) ReadPacket() (packets.ControlPacket, error) {
	h.mu.RLock()
	conn := h.conn
	h.mu.RUnlock()

	if conn == nil {
		return nil, ErrNotConnected
	}

	return conn.ReadPacket()
}

// TouchActivity updates the last activity timestamp.
func (h *ConnectionHandler) TouchActivity() {
	h.mu.Lock()
	h.lastActivity = time.Now()
	h.mu.Unlock()
}

// SetKeepAlive updates the keepalive interval.
func (h *ConnectionHandler) SetKeepAlive(keepAlive uint16) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.keepAlive = keepAlive
	if keepAlive > 0 {
		h.keepAliveExpiry = time.Duration(keepAlive) * time.Second * 3 / 2
	} else {
		h.keepAliveExpiry = 0
	}
}

// SetOnDisconnect sets the disconnect callback.
func (h *ConnectionHandler) SetOnDisconnect(fn func(bool)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.onDisconnect = fn
}

// State returns current state.
func (h *ConnectionHandler) State() State {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state
}

// IsConnected returns true if connected.
func (h *ConnectionHandler) IsConnected() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.state == StateConnected && h.conn != nil
}

// Conn returns the underlying connection.
func (h *ConnectionHandler) Conn() Connection {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conn
}

// ConnectedAt returns connection time.
func (h *ConnectionHandler) ConnectedAt() time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.connectedAt
}

// DisconnectedAt returns disconnection time.
func (h *ConnectionHandler) DisconnectedAt() time.Time {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.disconnectedAt
}

// StopChan returns a channel that is closed on disconnect.
func (h *ConnectionHandler) StopChan() <-chan struct{} {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.stopCh
}

func (h *ConnectionHandler) startKeepAliveTimer() {
	if h.keepAliveTimer != nil {
		h.keepAliveTimer.Stop()
	}

	h.keepAliveTimer = time.AfterFunc(h.keepAliveExpiry, func() {
		h.checkKeepAlive()
	})
}

func (h *ConnectionHandler) checkKeepAlive() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.state != StateConnected {
		return
	}

	elapsed := time.Since(h.lastActivity)
	if elapsed >= h.keepAliveExpiry {
		slog.Info("Keepalive expired", "client_id", h.clientID)

		// Logic similar to Disconnect but internal
		h.state = StateDisconnecting
		h.cleanupConnectionResources(false) // Not graceful

		if h.onDisconnect != nil {
			go h.onDisconnect(false)
		}
		return
	}

	remaining := h.keepAliveExpiry - elapsed
	h.keepAliveTimer = time.AfterFunc(remaining, func() {
		h.checkKeepAlive()
	})
}
