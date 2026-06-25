// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"fmt"
	"sync"
	"time"

	"github.com/absmach/fluxmq/storage"
)

// InflightState represents the state of an inflight message.
type InflightState int

const (
	// StatePublishSent means PUBLISH was sent, waiting for PUBACK (QoS 1) or PUBREC (QoS 2).
	StatePublishSent InflightState = iota
	// StatePubRecReceived means PUBREC was received, PUBREL sent, waiting for PUBCOMP (QoS 2).
	StatePubRecReceived
)

// InflightMessage represents a message in flight (waiting for acknowledgment).
type InflightMessage struct {
	// DeliveryAttemptedAt is set when delivery is first attempted (before write).
	// Used to retry messages where the send queue was full and SentAt was never set.
	DeliveryAttemptedAt time.Time
	// SentAt is set only after a successful socket write (via onSent callback).
	SentAt    time.Time
	Message   *storage.Message
	State     InflightState
	Retries   int
	Direction Direction
	PacketID  uint16
}

// Direction indicates message direction.
type Direction int

const (
	Outbound Direction = iota // Sent by broker to client
	Inbound                   // Received from client
)

// Inflight defines operations on inflight messages.
type Inflight interface {
	Add(packetID uint16, msg *storage.Message, direction Direction) error
	Ack(packetID uint16) (*storage.Message, error)
	Get(packetID uint16) (*InflightMessage, bool)
	Has(packetID uint16) bool
	WasReceived(packetID uint16) bool
	MarkReceived(packetID uint16)
	UpdateState(packetID uint16, state InflightState) error
	ClearReceived(packetID uint16)
	GetExpired(expiry time.Duration) []*InflightMessage
	MarkSent(packetID uint16)
	MarkDeliveryAttempted(packetID uint16)
	MarkRetry(packetID uint16) error
	GetAll() []*InflightMessage
	CleanupExpiredReceived(olderThan time.Duration)
}

// inflight tracks QoS 1 and QoS 2 messages in flight.
type inflight struct {
	mu       sync.RWMutex
	messages map[uint16]*InflightMessage
	maxSize  int

	// For QoS 2 inbound: track received packet IDs to detect duplicates
	receivedIDs map[uint16]time.Time
}

// NewInflightTracker creates a new inflight tracker.
func NewInflightTracker(maxSize int) *inflight {
	if maxSize <= 0 {
		maxSize = 65535
	}
	return &inflight{
		messages:    make(map[uint16]*InflightMessage),
		maxSize:     maxSize,
		receivedIDs: make(map[uint16]time.Time),
	}
}

// Add adds a message to the inflight tracker.
func (t *inflight) Add(packetID uint16, msg *storage.Message, direction Direction) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.messages) >= t.maxSize {
		return ErrInflightFull
	}

	t.messages[packetID] = &InflightMessage{
		PacketID: packetID,
		Message:  msg,
		State:    StatePublishSent,
		// SentAt is intentionally set only after successful socket write.
		SentAt:    time.Time{},
		Retries:   0,
		Direction: direction,
	}
	return nil
}

// Get retrieves an inflight message by packet ID.
func (t *inflight) Get(packetID uint16) (*InflightMessage, bool) {
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
func (t *inflight) Has(packetID uint16) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.messages[packetID]
	return ok
}

// UpdateState updates the state of an inflight message.
func (t *inflight) UpdateState(packetID uint16, state InflightState) error {
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
func (t *inflight) Ack(packetID uint16) (*storage.Message, error) {
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
func (t *inflight) Remove(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.messages, packetID)
}

// neverSentRetryDelay is the time to wait before retrying a message that was
// never written to the wire (e.g., send queue was full on first delivery attempt).
const neverSentRetryDelay = 500 * time.Millisecond

// GetExpired returns messages that should be retried.
// A message is eligible if:
//   - SentAt is set and the retry timeout has elapsed, or
//   - SentAt is zero (never written to wire) but DeliveryAttemptedAt was set
//     and the neverSentRetryDelay has elapsed.
func (t *inflight) GetExpired(timeout time.Duration) []*InflightMessage {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	var expired []*InflightMessage

	for _, msg := range t.messages {
		if msg.SentAt.IsZero() {
			// Never written to wire; retry after neverSentRetryDelay if delivery was attempted.
			if !msg.DeliveryAttemptedAt.IsZero() && now.Sub(msg.DeliveryAttemptedAt) >= neverSentRetryDelay {
				cp := *msg
				expired = append(expired, &cp)
			}
			continue
		}
		if now.Sub(msg.SentAt) >= timeout {
			cp := *msg
			expired = append(expired, &cp)
		}
	}
	return expired
}

// MarkSent marks a message as successfully written to the connection.
func (t *inflight) MarkSent(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if msg, ok := t.messages[packetID]; ok {
		msg.SentAt = time.Now()
	}
}

// MarkDeliveryAttempted records the time of the most recent delivery attempt.
// Each call overwrites the previous timestamp, resetting the neverSentRetryDelay
// backoff so a failed retry doesn't immediately re-appear in GetExpired.
func (t *inflight) MarkDeliveryAttempted(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if msg, ok := t.messages[packetID]; ok {
		msg.DeliveryAttemptedAt = time.Now()
	}
}

// MarkRetry marks a message as retried and updates sent time.
func (t *inflight) MarkRetry(packetID uint16) error {
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
func (t *inflight) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages)
}

// IsFull returns true if the tracker is at capacity.
func (t *inflight) IsFull() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages) >= t.maxSize
}

// GetAll returns all inflight messages.
func (t *inflight) GetAll() []*InflightMessage {
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
func (t *inflight) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.messages = make(map[uint16]*InflightMessage)
	t.receivedIDs = make(map[uint16]time.Time)
}

// MarkReceived marks a packet ID as received (for QoS 2 duplicate detection).
func (t *inflight) MarkReceived(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.receivedIDs[packetID] = time.Now()
}

// WasReceived returns true if the packet ID was previously received.
func (t *inflight) WasReceived(packetID uint16) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.receivedIDs[packetID]
	return ok
}

// ClearReceived clears a received packet ID (after PUBCOMP sent).
func (t *inflight) ClearReceived(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.receivedIDs, packetID)
}

// CleanupExpiredReceived removes old received IDs.
func (t *inflight) CleanupExpiredReceived(olderThan time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	cutoff := time.Now().Add(-olderThan)
	for id, received := range t.receivedIDs {
		if received.Before(cutoff) {
			delete(t.receivedIDs, id)
		}
	}
}
