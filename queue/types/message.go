// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"time"

	"github.com/absmach/fluxmq/core"
)

// MessageState represents the lifecycle state of a stream message.
type MessageState string

const (
	StateQueued    MessageState = "queued"
	StateDelivered MessageState = "delivered"
	StateAcked     MessageState = "acked"
	StateRetry     MessageState = "retry"
	StateDLQ       MessageState = "dlq"
)

// Message represents a message in the stream system.
type Message struct {
	ID         string
	Payload    []byte                 // Deprecated: Use PayloadBuf for zero-copy
	PayloadBuf *core.RefCountedBuffer // Zero-copy payload buffer (preferred)
	Topic      string
	Sequence   uint64
	Properties map[string]string

	// Lifecycle tracking
	State       MessageState
	CreatedAt   time.Time
	DeliveredAt time.Time
	NextRetryAt time.Time
	RetryCount  int

	// DLQ metadata
	FailureReason string
	FirstAttempt  time.Time
	LastAttempt   time.Time
	MovedToDLQAt  time.Time
	ExpiresAt     time.Time
}

// GetPayload returns the message payload, preferring PayloadBuf if available.
// This provides backward compatibility during migration to zero-copy.
func (m *Message) GetPayload() []byte {
	if m.PayloadBuf != nil {
		return m.PayloadBuf.Bytes()
	}
	return m.Payload
}

// SetPayloadFromBuffer sets the payload from a RefCountedBuffer.
// The message takes ownership of one reference.
func (m *Message) SetPayloadFromBuffer(buf *core.RefCountedBuffer) {
	if m.PayloadBuf != nil {
		m.PayloadBuf.Release() // Release previous buffer
	}
	m.PayloadBuf = buf
	m.Payload = nil // Clear legacy field
}

// SetPayloadFromBytes creates a new buffer from bytes (for backward compatibility).
// This will eventually be phased out in favor of direct buffer creation.
func (m *Message) SetPayloadFromBytes(data []byte) {
	if m.PayloadBuf != nil {
		m.PayloadBuf.Release()
	}
	if len(data) > 0 {
		m.PayloadBuf = core.GetBufferWithData(data)
	} else {
		m.PayloadBuf = nil
	}
	m.Payload = nil
}

// ReleasePayload releases the buffer reference if PayloadBuf is set.
// This should be called when the message is no longer needed.
func (m *Message) ReleasePayload() {
	if m.PayloadBuf != nil {
		m.PayloadBuf.Release()
		m.PayloadBuf = nil
	}
	m.Payload = nil
}
