package session

import (
	"fmt"
	"sync"
	"time"

	"github.com/dborovcanin/mqtt/store"
)

// InflightState represents the state of an inflight message.
type InflightState int

const (
	// StatePublishSent means PUBLISH was sent, waiting for PUBACK (QoS 1) or PUBREC (QoS 2)
	StatePublishSent InflightState = iota
	// StatePubRecReceived means PUBREC was received, PUBREL sent, waiting for PUBCOMP (QoS 2)
	StatePubRecReceived
)

// InflightMessage represents a message in flight (waiting for acknowledgment).
type InflightMessage struct {
	PacketID  uint16
	Message   *store.Message
	State     InflightState
	SentAt    time.Time
	Retries   int
	Direction Direction // Inbound or outbound
}

// Direction indicates message direction.
type Direction int

const (
	Outbound Direction = iota // Sent by broker to client
	Inbound                   // Received from client
)

// InflightTracker tracks QoS 1 and QoS 2 messages in flight.
type InflightTracker struct {
	mu       sync.RWMutex
	messages map[uint16]*InflightMessage
	maxSize  int

	// For QoS 2 inbound: track received packet IDs to detect duplicates
	receivedIDs map[uint16]time.Time
}

// NewInflightTracker creates a new inflight tracker.
func NewInflightTracker(maxSize int) *InflightTracker {
	if maxSize <= 0 {
		maxSize = 65535
	}
	return &InflightTracker{
		messages:    make(map[uint16]*InflightMessage),
		maxSize:     maxSize,
		receivedIDs: make(map[uint16]time.Time),
	}
}

// Add adds a message to the inflight tracker.
func (t *InflightTracker) Add(packetID uint16, msg *store.Message, dir Direction) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.messages) >= t.maxSize {
		return ErrInflightFull
	}

	t.messages[packetID] = &InflightMessage{
		PacketID:  packetID,
		Message:   msg,
		State:     StatePublishSent,
		SentAt:    time.Now(),
		Retries:   0,
		Direction: dir,
	}
	return nil
}

// Get retrieves an inflight message by packet ID.
func (t *InflightTracker) Get(packetID uint16) (*InflightMessage, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	msg, ok := t.messages[packetID]
	if !ok {
		return nil, false
	}

	// Return a copy to prevent races
	cp := *msg
	return &cp, true
}

// Has returns true if the packet ID is in the tracker.
func (t *InflightTracker) Has(packetID uint16) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.messages[packetID]
	return ok
}

// UpdateState updates the state of an inflight message.
func (t *InflightTracker) UpdateState(packetID uint16, state InflightState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[packetID]
	if !ok {
		return fmt.Errorf("update state for packet ID %d: %w", packetID, ErrPacketNotFound)
	}
	msg.State = state
	return nil
}

// Ack acknowledges and removes a message (for QoS 1 PUBACK or QoS 2 PUBCOMP).
func (t *InflightTracker) Ack(packetID uint16) (*store.Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[packetID]
	if !ok {
		return nil, fmt.Errorf("ack packet ID %d: %w", packetID, ErrPacketNotFound)
	}

	delete(t.messages, packetID)
	return msg.Message, nil
}

// Remove removes an inflight message.
func (t *InflightTracker) Remove(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.messages, packetID)
}

// GetExpired returns messages that have exceeded the retry timeout.
func (t *InflightTracker) GetExpired(timeout time.Duration) []*InflightMessage {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	var expired []*InflightMessage

	for _, msg := range t.messages {
		if now.Sub(msg.SentAt) >= timeout {
			cp := *msg
			expired = append(expired, &cp)
		}
	}
	return expired
}

// MarkRetry marks a message as retried and updates sent time.
func (t *InflightTracker) MarkRetry(packetID uint16) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[packetID]
	if !ok {
		return fmt.Errorf("mark retry for packet ID %d: %w", packetID, ErrPacketNotFound)
	}

	msg.SentAt = time.Now()
	msg.Retries++
	return nil
}

// Count returns the number of inflight messages.
func (t *InflightTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages)
}

// IsFull returns true if the tracker is at capacity.
func (t *InflightTracker) IsFull() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages) >= t.maxSize
}

// GetAll returns all inflight messages.
func (t *InflightTracker) GetAll() []*InflightMessage {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*InflightMessage, 0, len(t.messages))
	for _, msg := range t.messages {
		cp := *msg
		result = append(result, &cp)
	}
	return result
}

// Clear removes all inflight messages.
func (t *InflightTracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.messages = make(map[uint16]*InflightMessage)
	t.receivedIDs = make(map[uint16]time.Time)
}

// --- QoS 2 inbound tracking ---

// MarkReceived marks a packet ID as received (for QoS 2 duplicate detection).
func (t *InflightTracker) MarkReceived(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.receivedIDs[packetID] = time.Now()
}

// WasReceived returns true if the packet ID was previously received.
func (t *InflightTracker) WasReceived(packetID uint16) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.receivedIDs[packetID]
	return ok
}

// ClearReceived clears a received packet ID (after PUBCOMP sent).
func (t *InflightTracker) ClearReceived(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.receivedIDs, packetID)
}

// CleanupExpiredReceived removes old received IDs.
func (t *InflightTracker) CleanupExpiredReceived(olderThan time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	for id, received := range t.receivedIDs {
		if received.Before(cutoff) {
			delete(t.receivedIDs, id)
		}
	}
}
