// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"time"

	"github.com/absmach/mqtt/core"
)

// Common errors.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrLocked        = errors.New("resource is locked")
)

// Store is the composite storage interface providing access to all storage backends.
type Store interface {
	// Messages returns the message store for QoS offline queue.
	Messages() MessageStore

	// Sessions returns the session store.
	Sessions() SessionStore

	// Subscriptions returns the subscription store.
	Subscriptions() SubscriptionStore

	// Retained returns the retained message store.
	Retained() RetainedStore

	// Wills returns the will message store.
	Wills() WillStore

	// Close closes all storage backends.
	Close() error
}

// Message represents a stored MQTT message.
type Message struct {
	Expiry          time.Time
	PublishTime     time.Time
	Payload         []byte                 // Deprecated: Use PayloadBuf for zero-copy
	PayloadBuf      *core.RefCountedBuffer // Zero-copy payload buffer (preferred)
	CorrelationData []byte
	SubscriptionIDs []uint32
	Topic           string
	ContentType     string
	ResponseTopic   string
	Properties      map[string]string
	UserProperties  map[string]string
	MessageExpiry   *uint32
	PayloadFormat   *byte
	PacketID        uint16
	QoS             byte
	Retain          bool
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

// ReleasePayload releases the payload buffer if using zero-copy.
// Must be called when message is no longer needed.
func (m *Message) ReleasePayload() {
	if m.PayloadBuf != nil {
		m.PayloadBuf.Release()
		m.PayloadBuf = nil
	}
}

// RetainPayload increments the reference count for sharing the message.
// Must be called before passing message to another goroutine.
func (m *Message) RetainPayload() {
	if m.PayloadBuf != nil {
		m.PayloadBuf.Retain()
	}
}

// CopyMessage creates a deep copy of a message.
func CopyMessage(msg *Message) *Message {
	if msg == nil {
		return nil
	}

	cp := &Message{
		Topic:         msg.Topic,
		QoS:           msg.QoS,
		Retain:        msg.Retain,
		PacketID:      msg.PacketID,
		Expiry:        msg.Expiry,
		ContentType:   msg.ContentType,
		ResponseTopic: msg.ResponseTopic,
		PublishTime:   msg.PublishTime,
	}

	if msg.MessageExpiry != nil {
		exp := *msg.MessageExpiry
		cp.MessageExpiry = &exp
	}

	if msg.PayloadFormat != nil {
		pf := *msg.PayloadFormat
		cp.PayloadFormat = &pf
	}

	// Zero-copy: Share the buffer instead of copying
	if msg.PayloadBuf != nil {
		msg.PayloadBuf.Retain() // Retain for the copy
		cp.PayloadBuf = msg.PayloadBuf
	} else if len(msg.Payload) > 0 {
		// Fallback for legacy code still using Payload field
		cp.Payload = make([]byte, len(msg.Payload))
		copy(cp.Payload, msg.Payload)
	}

	if len(msg.CorrelationData) > 0 {
		cp.CorrelationData = make([]byte, len(msg.CorrelationData))
		copy(cp.CorrelationData, msg.CorrelationData)
	}

	if len(msg.Properties) > 0 {
		cp.Properties = make(map[string]string, len(msg.Properties))
		for k, v := range msg.Properties {
			cp.Properties[k] = v
		}
	}

	if len(msg.UserProperties) > 0 {
		cp.UserProperties = make(map[string]string, len(msg.UserProperties))
		for k, v := range msg.UserProperties {
			cp.UserProperties[k] = v
		}
	}

	if len(msg.SubscriptionIDs) > 0 {
		cp.SubscriptionIDs = make([]uint32, len(msg.SubscriptionIDs))
		copy(cp.SubscriptionIDs, msg.SubscriptionIDs)
	}

	return cp
}

// Session represents persisted session state.
type Session struct {
	ConnectedAt     time.Time
	DisconnectedAt  time.Time
	ClientID        string
	ExpiryInterval  uint32 // Session expiry in seconds (0 = no expiry when disconnected)
	MaxPacketSize   uint32
	ReceiveMaximum  uint16
	TopicAliasMax   uint16
	Version         byte // MQTT version (3, 4, or 5)
	CleanStart      bool
	Connected       bool
	RequestResponse bool
	RequestProblem  bool
}

// Subscription represents a stored subscription.
type Subscription struct {
	ClientID       string
	Filter         string
	SubscriptionID *uint32
	Options        SubscribeOptions
	QoS            byte
}

// CopySubscription creates a copy of a subscription.
func CopySubscription(sub *Subscription) *Subscription {
	if sub == nil {
		return nil
	}
	cp := &Subscription{
		ClientID: sub.ClientID,
		Filter:   sub.Filter,
		QoS:      sub.QoS,
		Options:  sub.Options,
	}
	if sub.SubscriptionID != nil {
		id := *sub.SubscriptionID
		cp.SubscriptionID = &id
	}
	return cp
}

// SubscribeOptions holds MQTT 5.0 subscription options.
type SubscribeOptions struct {
	NoLocal           bool   // Don't receive own messages
	RetainAsPublished bool   // Keep original retain flag
	RetainHandling    byte   // 0=send, 1=new only, 2=none
	ConsumerGroup     string // Queue consumer group (MQTT v5 User Property)
}

// WillMessage represents a stored will message.
type WillMessage struct {
	Payload    []byte
	ClientID   string
	Topic      string
	Properties map[string]string
	Delay      uint32
	Expiry     uint32
	QoS        byte
	Retain     bool
}

// MessageStore handles message persistence for QoS offline queue.
type MessageStore interface {
	// Store stores a message with optional TTL.
	// key format: "{clientID}/{packetID}" for inflight, "{clientID}/queue/{seq}" for offline queue
	Store(key string, msg *Message) error

	// Get retrieves a message by key.
	Get(key string) (*Message, error)

	// Delete removes a message.
	Delete(key string) error

	// List returns all messages matching a key prefix.
	List(prefix string) ([]*Message, error)

	// DeleteByPrefix removes all messages matching a prefix.
	DeleteByPrefix(prefix string) error
}

// SessionStore handles session persistence.
type SessionStore interface {
	// Get retrieves a session by client ID.
	Get(clientID string) (*Session, error)

	// Save persists a session.
	Save(session *Session) error

	// Delete removes a session.
	Delete(clientID string) error

	// GetExpired returns client IDs of sessions that have expired.
	GetExpired(before time.Time) ([]string, error)

	// List returns all sessions (for debugging/metrics).
	List() ([]*Session, error)
}

// SubscriptionStore handles subscription persistence.
type SubscriptionStore interface {
	// Add adds or updates a subscription.
	Add(sub *Subscription) error

	// Remove removes a subscription.
	Remove(clientID, filter string) error

	// RemoveAll removes all subscriptions for a client.
	RemoveAll(clientID string) error

	// GetForClient returns all subscriptions for a client.
	GetForClient(clientID string) ([]*Subscription, error)

	// Match returns all subscriptions matching a topic.
	// This is the core routing operation.
	Match(topic string) ([]*Subscription, error)

	// Count returns total subscription count.
	Count() int
}

// RetainedStore handles retained message persistence.
type RetainedStore interface {
	// Set stores or updates a retained message.
	// Empty payload deletes the retained message.
	Set(ctx context.Context, topic string, msg *Message) error

	// Get retrieves a retained message by exact topic.
	Get(ctx context.Context, topic string) (*Message, error)

	// Delete removes a retained message.
	Delete(ctx context.Context, topic string) error

	// Match returns all retained messages matching a filter (supports wildcards).
	Match(ctx context.Context, filter string) ([]*Message, error)
}

// WillStore handles will message persistence.
type WillStore interface {
	// Set stores a will message for a client.
	Set(ctx context.Context, clientID string, will *WillMessage) error

	// Get retrieves the will message for a client.
	Get(ctx context.Context, clientID string) (*WillMessage, error)

	// Delete removes the will message for a client.
	Delete(ctx context.Context, clientID string) error

	// GetPending returns will messages that should be triggered.
	// (will delay elapsed and client still disconnected)
	GetPending(ctx context.Context, before time.Time) ([]*WillMessage, error)
}
