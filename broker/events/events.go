// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Event type constants.
const (
	TypeClientConnected     = "client.connected"
	TypeClientDisconnected  = "client.disconnected"
	TypeSessionTakeover     = "client.session_takeover"
	TypeMessagePublished    = "message.published"
	TypeMessageDelivered    = "message.delivered"
	TypeRetainedMessageSet  = "message.retained"
	TypeSubscriptionCreated = "subscription.created"
	TypeSubscriptionRemoved = "subscription.removed"
)

// Event is the common interface for all webhook events.
type Event interface {
	// Type returns the event type identifier (e.g., "client.connected")
	Type() string

	// Topic returns the MQTT topic for message events, empty for others
	Topic() string

	// Wrap wraps the event in a common envelope with metadata
	Wrap(brokerID string) *Envelope
}

// Envelope is the common wrapper for all webhook events.
type Envelope struct {
	EventType string `json:"event_type"`
	EventID   string `json:"event_id"`
	Timestamp string `json:"timestamp"`
	BrokerID  string `json:"broker_id"`
	Data      any    `json:"data"`
}

// MarshalJSON serializes the envelope to JSON.
func (e *Envelope) MarshalJSON() ([]byte, error) {
	return json.Marshal(*e)
}

// ClientConnected is emitted when a client successfully connects.
type ClientConnected struct {
	ClientID   string `json:"client_id"`
	Protocol   string `json:"protocol"` // "mqtt3" or "mqtt5"
	CleanStart bool   `json:"clean_start"`
	KeepAlive  uint16 `json:"keep_alive"`
	RemoteAddr string `json:"remote_addr"`
}

func (e ClientConnected) Type() string  { return TypeClientConnected }
func (e ClientConnected) Topic() string { return "" }
func (e ClientConnected) Wrap(brokerID string) *Envelope {
	return &Envelope{
		EventType: e.Type(),
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		BrokerID:  brokerID,
		Data:      e,
	}
}

// ClientDisconnected is emitted when a client disconnects.
type ClientDisconnected struct {
	ClientID   string `json:"client_id"`
	Reason     string `json:"reason"` // "normal", "error", "timeout", "takeover"
	RemoteAddr string `json:"remote_addr"`
}

func (e ClientDisconnected) Type() string  { return TypeClientDisconnected }
func (e ClientDisconnected) Topic() string { return "" }
func (e ClientDisconnected) Wrap(brokerID string) *Envelope {
	return &Envelope{
		EventType: e.Type(),
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		BrokerID:  brokerID,
		Data:      e,
	}
}

// SessionTakeover is emitted when a session migrates from one node to another.
type SessionTakeover struct {
	ClientID string `json:"client_id"`
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
}

func (e SessionTakeover) Type() string  { return TypeSessionTakeover }
func (e SessionTakeover) Topic() string { return "" }
func (e SessionTakeover) Wrap(brokerID string) *Envelope {
	return &Envelope{
		EventType: e.Type(),
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		BrokerID:  brokerID,
		Data:      e,
	}
}

// MessagePublished is emitted when a message is published to the broker.
type MessagePublished struct {
	ClientID     string `json:"client_id"`
	MessageTopic string `json:"topic"`
	QoS          byte   `json:"qos"`
	Retained     bool   `json:"retained"`
	PayloadSize  int    `json:"payload_size"`
	Payload      string `json:"payload,omitempty"` // base64 encoded, optional
}

func (e MessagePublished) Type() string  { return TypeMessagePublished }
func (e MessagePublished) Topic() string { return e.MessageTopic }
func (e MessagePublished) Wrap(brokerID string) *Envelope {
	return &Envelope{
		EventType: e.Type(),
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		BrokerID:  brokerID,
		Data:      e,
	}
}

// MessageDelivered is emitted when a message is delivered to a subscriber.
type MessageDelivered struct {
	ClientID     string `json:"client_id"` // subscriber
	MessageTopic string `json:"topic"`
	QoS          byte   `json:"qos"`
	PayloadSize  int    `json:"payload_size"`
}

func (e MessageDelivered) Type() string  { return TypeMessageDelivered }
func (e MessageDelivered) Topic() string { return e.MessageTopic }
func (e MessageDelivered) Wrap(brokerID string) *Envelope {
	return &Envelope{
		EventType: e.Type(),
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		BrokerID:  brokerID,
		Data:      e,
	}
}

// RetainedMessageSet is emitted when a retained message is set or cleared.
type RetainedMessageSet struct {
	MessageTopic string `json:"topic"`
	PayloadSize  int    `json:"payload_size"` // 0 if cleared
	Cleared      bool   `json:"cleared"`
}

func (e RetainedMessageSet) Type() string  { return TypeRetainedMessageSet }
func (e RetainedMessageSet) Topic() string { return e.MessageTopic }
func (e RetainedMessageSet) Wrap(brokerID string) *Envelope {
	return &Envelope{
		EventType: e.Type(),
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		BrokerID:  brokerID,
		Data:      e,
	}
}

// SubscriptionCreated is emitted when a client subscribes to a topic.
type SubscriptionCreated struct {
	ClientID       string `json:"client_id"`
	TopicFilter    string `json:"topic_filter"`
	QoS            byte   `json:"qos"`
	SubscriptionID uint32 `json:"subscription_id,omitempty"` // MQTT 5.0 only
}

func (e SubscriptionCreated) Type() string  { return TypeSubscriptionCreated }
func (e SubscriptionCreated) Topic() string { return e.TopicFilter }
func (e SubscriptionCreated) Wrap(brokerID string) *Envelope {
	return &Envelope{
		EventType: e.Type(),
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		BrokerID:  brokerID,
		Data:      e,
	}
}

// SubscriptionRemoved is emitted when a client unsubscribes from a topic.
type SubscriptionRemoved struct {
	ClientID    string `json:"client_id"`
	TopicFilter string `json:"topic_filter"`
}

func (e SubscriptionRemoved) Type() string  { return TypeSubscriptionRemoved }
func (e SubscriptionRemoved) Topic() string { return e.TopicFilter }
func (e SubscriptionRemoved) Wrap(brokerID string) *Envelope {
	return &Envelope{
		EventType: e.Type(),
		EventID:   uuid.New().String(),
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		BrokerID:  brokerID,
		Data:      e,
	}
}

