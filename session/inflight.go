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

// inflightTracker tracks QoS 1 and QoS 2 messages in flight.
type inflightTracker struct {
	mu       sync.RWMutex
	messages map[uint16]*InflightMessage
	maxSize  int

	// For QoS 2 inbound: track received packet IDs to detect duplicates
	receivedIDs map[uint16]time.Time
}

// NewInflightTracker creates a new inflight tracker.
func NewInflightTracker(maxSize int) *inflightTracker {
	if maxSize <= 0 {
		maxSize = 65535
	}
	return &inflightTracker{
		messages:    make(map[uint16]*InflightMessage),
		maxSize:     maxSize,
		receivedIDs: make(map[uint16]time.Time),
	}
}

// add adds a message to the inflight tracker.
func (t *inflightTracker) add(packetID uint16, msg *store.Message, dir Direction) error {
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

// get retrieves an inflight message by packet ID.
func (t *inflightTracker) get(packetID uint16) (*InflightMessage, bool) {
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

// has returns true if the packet ID is in the tracker.
func (t *inflightTracker) has(packetID uint16) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.messages[packetID]
	return ok
}

// updateState updates the state of an inflight message.
func (t *inflightTracker) updateState(packetID uint16, state InflightState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[packetID]
	if !ok {
		return fmt.Errorf("update state for packet ID %d: %w", packetID, ErrPacketNotFound)
	}
	msg.State = state
	return nil
}

// ack acknowledges and removes a message (for QoS 1 PUBACK or QoS 2 PUBCOMP).
func (t *inflightTracker) ack(packetID uint16) (*store.Message, error) {
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
func (t *inflightTracker) Remove(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.messages, packetID)
}

// getExpired returns messages that have exceeded the retry timeout.
func (t *inflightTracker) getExpired(timeout time.Duration) []*InflightMessage {
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

// markRetry marks a message as retried and updates sent time.
func (t *inflightTracker) markRetry(packetID uint16) error {
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
func (t *inflightTracker) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages)
}

// IsFull returns true if the tracker is at capacity.
func (t *inflightTracker) IsFull() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages) >= t.maxSize
}

// getAll returns all inflight messages.
func (t *inflightTracker) getAll() []*InflightMessage {
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
func (t *inflightTracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.messages = make(map[uint16]*InflightMessage)
	t.receivedIDs = make(map[uint16]time.Time)
}

// --- QoS 2 inbound tracking ---

// markReceived marks a packet ID as received (for QoS 2 duplicate detection).
func (t *inflightTracker) markReceived(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.receivedIDs[packetID] = time.Now()
}

// wasReceived returns true if the packet ID was previously received.
func (t *inflightTracker) wasReceived(packetID uint16) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.receivedIDs[packetID]
	return ok
}

// clearReceived clears a received packet ID (after PUBCOMP sent).
func (t *inflightTracker) clearReceived(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.receivedIDs, packetID)
}

// CleanupExpiredReceived removes old received IDs.
func (t *inflightTracker) CleanupExpiredReceived(olderThan time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	for id, received := range t.receivedIDs {
		if received.Before(cutoff) {
			delete(t.receivedIDs, id)
		}
	}
}

// Inflight message operations
func (s *Session) AddInflight(packetID uint16, msg *store.Message, direction Direction) error {
	return s.inflight.add(packetID, msg, direction)
}

func (s *Session) AckInflight(packetID uint16) {
	s.inflight.ack(packetID)
}

func (s *Session) GetInflight(packetID uint16) (*InflightMessage, bool) {
	return s.inflight.get(packetID)
}

func (s *Session) GetAllInflight() []*InflightMessage {
	return s.inflight.getAll()
}

func (s *Session) HasInflight(packetID uint16) bool {
	return s.inflight.has(packetID)
}

func (s *Session) WasReceivedInflight(packetID uint16) bool {
	return s.inflight.wasReceived(packetID)
}

func (s *Session) MarkReceivedInflight(packetID uint16) {
	s.inflight.markReceived(packetID)
}

func (s *Session) UpdateStateInflight(packetID uint16, state InflightState) {
	s.inflight.updateState(packetID, state)
}

func (s *Session) ClearReceivedInflight(packetID uint16) {
	s.inflight.clearReceived(packetID)
}

func (s *Session) GetExpiredInflight(expiry time.Duration) []*InflightMessage {
	return s.inflight.getExpired(expiry)
}

func (s *Session) MarkRetryInflight(packetID uint16) {
	s.inflight.markRetry(packetID)
}
