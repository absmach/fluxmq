// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"errors"
	"time"

	"github.com/absmach/fluxmq/message"
)

// Common errors.
var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrLocked        = errors.New("resource is locked")
	ErrClosed        = errors.New("store is closed")
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

	// Ping verifies the storage backend is reachable and operational.
	Ping() error

	// Close closes all storage backends. It is safe to call while operations
	// are in flight and safe to call more than once. Operations that race with
	// Close either complete or return ErrClosed; they never panic.
	Close() error
}

// Session represents persisted session state.
type Session struct {
	ConnectedAt     time.Time
	DisconnectedAt  time.Time
	ClientID        string
	ExternalID      string
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
	// Store borrows msg only for the duration of the call.
	Store(key string, msg *message.Envelope) error

	// Get retrieves an owned envelope. The caller must release it.
	Get(key string) (*message.Envelope, error)

	// Delete removes a message.
	Delete(key string) error

	// List returns owned envelopes. The caller must release every element.
	List(prefix string) ([]*message.Envelope, error)

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

	// GetByFilter returns all subscriptions for an exact topic filter.
	GetByFilter(filter string) ([]*Subscription, error)

	// Count returns total subscription count.
	Count() int
}

// RetainedStore handles retained message persistence.
type RetainedStore interface {
	// Set stores or updates a retained message.
	// Empty payload deletes the retained message.
	// Set borrows msg only for the duration of the call.
	Set(ctx context.Context, topic string, msg *message.Envelope) error

	// Get retrieves an owned retained envelope. The caller must release it.
	Get(ctx context.Context, topic string) (*message.Envelope, error)

	// Delete removes a retained message.
	Delete(ctx context.Context, topic string) error

	// Match returns owned retained envelopes matching a filter. The caller must
	// release every element.
	Match(ctx context.Context, filter string) ([]*message.Envelope, error)
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
