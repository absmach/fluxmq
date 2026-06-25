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
//
// MQTT packet identifiers are independent in each direction, so entries are
// keyed by (direction, packetID). Methods that are inherently one-directional
// operate on the outbound entries (Get, Has, UpdateState, MarkSent, MarkRetry,
// MarkDeliveryAttempted, GetExpired); the inbound QoS 2 receive path uses Add
// with Inbound, Ack with Inbound, and the *Received methods. Ack therefore takes
// the direction explicitly.
type Inflight interface {
	Add(packetID uint16, msg *storage.Message, direction Direction) error
	Ack(packetID uint16, direction Direction) (*storage.Message, error)
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

// inflightKey identifies an inflight entry. Inbound and outbound flows have
// independent packet-ID spaces, so the direction is part of the key.
type inflightKey struct {
	direction Direction
	packetID  uint16
}

// inflight tracks QoS 1 and QoS 2 messages in flight.
type inflight struct {
	mu       sync.RWMutex
	messages map[inflightKey]*InflightMessage
	// counts is the number of entries per direction. Each direction is capped at
	// maxSize independently, so outbound delivery cannot consume the inbound
	// Receive Maximum the broker advertises, and vice versa.
	counts  [2]int
	maxSize int

	// For QoS 2 inbound: track received packet IDs to detect duplicates
	receivedIDs map[uint16]time.Time
}

// NewInflightTracker creates a new inflight tracker.
func NewInflightTracker(maxSize int) *inflight {
	if maxSize <= 0 {
		maxSize = 65535
	}
	return &inflight{
		messages:    make(map[inflightKey]*InflightMessage),
		maxSize:     maxSize,
		receivedIDs: make(map[uint16]time.Time),
	}
}

// Add adds a message to the inflight tracker. Each direction is capped at
// maxSize independently.
func (t *inflight) Add(packetID uint16, msg *storage.Message, direction Direction) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.counts[direction] >= t.maxSize {
		return ErrInflightFull
	}

	key := inflightKey{direction: direction, packetID: packetID}
	if _, exists := t.messages[key]; !exists {
		t.counts[direction]++
	}
	t.messages[key] = &InflightMessage{
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

// Get retrieves an outbound inflight message by packet ID.
func (t *inflight) Get(packetID uint16) (*InflightMessage, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	msg, ok := t.messages[inflightKey{direction: Outbound, packetID: packetID}]
	if !ok {
		return nil, false
	}

	// Return a copy to prevent races
	cp := *msg
	return &cp, true
}

// Has returns true if the outbound packet ID is in the tracker. Used to avoid
// reusing an outbound packet identifier.
func (t *inflight) Has(packetID uint16) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.messages[inflightKey{direction: Outbound, packetID: packetID}]
	return ok
}

// UpdateState updates the state of an outbound inflight message.
func (t *inflight) UpdateState(packetID uint16, state InflightState) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[inflightKey{direction: Outbound, packetID: packetID}]
	if !ok {
		return fmt.Errorf("update state for packet ID %d: %w", packetID, ErrPacketNotFound)
	}
	msg.State = state
	return nil
}

// Ack acknowledges and removes a message in the given direction (outbound for
// PUBACK/PUBCOMP, inbound for PUBREL).
func (t *inflight) Ack(packetID uint16, direction Direction) (*storage.Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := inflightKey{direction: direction, packetID: packetID}
	msg, ok := t.messages[key]
	if !ok {
		return nil, fmt.Errorf("ack packet ID %d: %w", packetID, ErrPacketNotFound)
	}

	delete(t.messages, key)
	t.counts[direction]--
	return msg.Message, nil
}

// Remove removes an outbound inflight message.
func (t *inflight) Remove(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := inflightKey{direction: Outbound, packetID: packetID}
	if _, ok := t.messages[key]; ok {
		delete(t.messages, key)
		t.counts[Outbound]--
	}
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

	// Only outbound messages are retransmitted by the broker; inbound QoS 2
	// completion is driven by the client's PUBREL.
	for key, msg := range t.messages {
		if key.direction != Outbound {
			continue
		}
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

// MarkSent marks an outbound message as successfully written to the connection.
func (t *inflight) MarkSent(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if msg, ok := t.messages[inflightKey{direction: Outbound, packetID: packetID}]; ok {
		msg.SentAt = time.Now()
	}
}

// MarkDeliveryAttempted records the time of the most recent outbound delivery
// attempt. Each call overwrites the previous timestamp, resetting the
// neverSentRetryDelay backoff so a failed retry doesn't immediately re-appear in
// GetExpired.
func (t *inflight) MarkDeliveryAttempted(packetID uint16) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if msg, ok := t.messages[inflightKey{direction: Outbound, packetID: packetID}]; ok {
		msg.DeliveryAttemptedAt = time.Now()
	}
}

// MarkRetry marks an outbound message as retried and updates sent time.
func (t *inflight) MarkRetry(packetID uint16) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	msg, ok := t.messages[inflightKey{direction: Outbound, packetID: packetID}]
	if !ok {
		return fmt.Errorf("mark retry for packet ID %d: %w", packetID, ErrPacketNotFound)
	}

	msg.SentAt = time.Now()
	msg.Retries++
	return nil
}

// Count returns the total number of inflight messages across both directions.
func (t *inflight) Count() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.messages)
}

// IsFull reports whether the outbound direction is at capacity.
func (t *inflight) IsFull() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.counts[Outbound] >= t.maxSize
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
	t.messages = make(map[inflightKey]*InflightMessage)
	t.counts = [2]int{}
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
