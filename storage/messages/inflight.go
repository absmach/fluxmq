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
// MarkDeliveryAttempted, GetExpired). The live inbound QoS 2 receive path uses
// the optional InboundAdder and InboundAcker extensions so external
// implementations that do not isolate directions fail explicitly.
type Inflight interface {
	Add(packetID uint16, msg *storage.Message, direction Direction) error
	// Ack acknowledges and removes an outbound message (PUBACK/PUBCOMP).
	Ack(packetID uint16) (*storage.Message, error)
	Get(packetID uint16) (*InflightMessage, bool)
	Has(packetID uint16) bool
	UpdateState(packetID uint16, state InflightState) error
	GetExpired(expiry time.Duration) []*InflightMessage
	MarkSent(packetID uint16)
	MarkDeliveryAttempted(packetID uint16)
	MarkRetry(packetID uint16) error
	GetAll() []*InflightMessage
}

// InboundAdder is an optional extension of Inflight for atomic inbound QoS 2
// admission. accepted reports whether the tracker took ownership of msg. A
// duplicate returns accepted=false with no error and preserves the first
// accepted transaction. Keeping this out of the base Inflight interface
// preserves source compatibility for external implementations.
type InboundAdder interface {
	AddInbound(packetID uint16, msg *storage.Message) (accepted bool, err error)
}

// InboundAcker is an optional extension of Inflight for directional inbound
// acknowledgement (PUBREL completing an inbound QoS 2 receive). Keeping it out
// of the base Inflight interface preserves source compatibility for external
// implementations. The built-in tracker implements it.
type InboundAcker interface {
	AckInbound(packetID uint16) (*storage.Message, error)
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
}

// NewInflightTracker creates a new inflight tracker.
func NewInflightTracker(maxSize int) *inflight {
	if maxSize <= 0 {
		maxSize = 65535
	}
	return &inflight{
		messages: make(map[inflightKey]*InflightMessage),
		maxSize:  maxSize,
	}
}

// Add adds a message to the inflight tracker. Each direction is capped at
// maxSize independently.
func (t *inflight) Add(packetID uint16, msg *storage.Message, direction Direction) error {
	if direction != Outbound && direction != Inbound {
		return fmt.Errorf("add packet ID %d: %w", packetID, ErrInvalidDirection)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	key := inflightKey{direction: direction, packetID: packetID}
	_, exists := t.messages[key]
	// Capacity only gates new keys. An existing key (e.g. a retransmitted QoS 2
	// PUBLISH with a known packet ID) must be accepted even when the direction
	// is at capacity, so duplicates are not rejected with ErrInflightFull.
	if !exists {
		if t.counts[direction] >= t.maxSize {
			return ErrInflightFull
		}
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

// AddInbound atomically admits an inbound QoS 2 transaction, rejecting a
// duplicate by packet ID. It never replaces an existing transaction with the
// same packet ID. accepted is true only when the tracker takes ownership of
// msg.
func (t *inflight) AddInbound(packetID uint16, msg *storage.Message) (bool, error) {
	return t.addInbound(packetID, msg, Inbound)
}

func (t *inflight) addInbound(packetID uint16, msg *storage.Message, direction Direction) (bool, error) {
	if direction != Inbound {
		return false, fmt.Errorf("add inbound packet ID %d: %w", packetID, ErrInvalidDirection)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	key := inflightKey{direction: direction, packetID: packetID}
	if _, exists := t.messages[key]; exists {
		return false, nil
	}
	if t.counts[direction] >= t.maxSize {
		return false, ErrInflightFull
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
	t.counts[direction]++
	return true, nil
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

// Ack acknowledges and removes an outbound message (PUBACK/PUBCOMP).
func (t *inflight) Ack(packetID uint16) (*storage.Message, error) {
	return t.ackDirection(packetID, Outbound)
}

// AckInbound acknowledges and removes an inbound message (PUBREL).
func (t *inflight) AckInbound(packetID uint16) (*storage.Message, error) {
	return t.ackDirection(packetID, Inbound)
}

func (t *inflight) ackDirection(packetID uint16, direction Direction) (*storage.Message, error) {
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
}
