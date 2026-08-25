// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package message defines the broker's single canonical internal message.
package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/absmach/fluxmq/payload"
)

// Version identifies the persisted internal envelope schema.
type Version uint16

const (
	// Version1 is the only envelope schema understood by this codebase.
	Version1 Version = 1
)

// ErrUnsupportedVersion is returned for zero, legacy, and future envelope
// versions. This implementation deliberately has no compatibility decoder.
var ErrUnsupportedVersion = errors.New("unsupported message envelope version")

// Protocol identifies the ingress protocol without leaking protocol-specific
// types into the broker core.
type Protocol string

const (
	ProtocolMQTT    Protocol = "mqtt"
	ProtocolAMQP091 Protocol = "amqp"
	ProtocolAMQP1   Protocol = "amqp1"
	ProtocolHTTP    Protocol = "http"
	ProtocolCoAP    Protocol = "coap"
)

// QueueState is the broker-owned lifecycle of a durable queue record.
type QueueState string

const (
	QueueStateQueued    QueueState = "queued"
	QueueStateDelivered QueueState = "delivered"
	QueueStateAcked     QueueState = "acked"
	QueueStateRetry     QueueState = "retry"
	QueueStateDLQ       QueueState = "dlq"
)

// UserMetadata contains publisher-owned message metadata. Protocol adapters
// may project only fields their wire format supports.
type UserMetadata struct {
	Key             []byte            `json:"key,omitempty"`
	Headers         map[string][]byte `json:"headers,omitempty"`
	Properties      map[string]string `json:"properties,omitempty"`
	ContentType     string            `json:"content_type,omitempty"`
	ContentEncoding string            `json:"content_encoding,omitempty"`
	ResponseTopic   string            `json:"response_topic,omitempty"`
	CorrelationData []byte            `json:"correlation_data,omitempty"`
	PayloadFormat   *byte             `json:"payload_format,omitempty"`
	MessageExpiry   *uint32           `json:"message_expiry,omitempty"`
}

// SourceMetadata identifies the authenticated origin. It is broker-owned and
// never accepted from an untrusted peer as user metadata.
type SourceMetadata struct {
	ClientID   string   `json:"client_id,omitempty"`
	ExternalID string   `json:"external_id,omitempty"`
	Protocol   Protocol `json:"protocol,omitempty"`
	Topic      string   `json:"topic,omitempty"`
}

// DeliveryMetadata contains broker delivery and MQTT transaction state.
type DeliveryMetadata struct {
	PublishedAt       time.Time `json:"published_at,omitempty"`
	ExpiresAt         time.Time `json:"expires_at,omitempty"`
	SubscriptionIDs   []uint32  `json:"subscription_ids,omitempty"`
	PacketID          uint16    `json:"packet_id,omitempty"`
	QoS               byte      `json:"qos,omitempty"`
	InflightDirection byte      `json:"inflight_direction,omitempty"`
	InflightState     byte      `json:"inflight_state,omitempty"`
	Retain            bool      `json:"retain,omitempty"`
	Duplicate         bool      `json:"duplicate,omitempty"`
}

// StreamMetadata records broker-owned stream projection state.
type StreamMetadata struct {
	Offset             uint64 `json:"offset,omitempty"`
	Timestamp          int64  `json:"timestamp,omitempty"`
	CommittedOffset    uint64 `json:"committed_offset,omitempty"`
	HasCommittedOffset bool   `json:"has_committed_offset,omitempty"`
	WorkAcknowledged   bool   `json:"work_acknowledged,omitempty"`
	WorkGroup          string `json:"work_group,omitempty"`
}

// QueueMetadata contains durable-queue identity and lifecycle state.
type QueueMetadata struct {
	MessageID   string          `json:"message_id,omitempty"`
	Name        string          `json:"name,omitempty"`
	GroupID     string          `json:"group_id,omitempty"`
	Offset      uint64          `json:"offset,omitempty"`
	State       QueueState      `json:"state,omitempty"`
	CreatedAt   time.Time       `json:"created_at,omitempty"`
	DeliveredAt time.Time       `json:"delivered_at,omitempty"`
	NextRetryAt time.Time       `json:"next_retry_at,omitempty"`
	RetryCount  int             `json:"retry_count,omitempty"`
	ExpiresAt   time.Time       `json:"expires_at,omitempty"`
	Stream      *StreamMetadata `json:"stream,omitempty"`
}

// TransferMetadata records a recoverable broker-owned message transfer such
// as a move to a dead-letter queue.
type TransferMetadata struct {
	ID            string    `json:"id,omitempty"`
	FailureReason string    `json:"failure_reason,omitempty"`
	FirstAttempt  time.Time `json:"first_attempt,omitempty"`
	LastAttempt   time.Time `json:"last_attempt,omitempty"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	SourceQueue   string    `json:"source_queue,omitempty"`
	SourceGroup   string    `json:"source_group,omitempty"`
	SourceOffset  uint64    `json:"source_offset,omitempty"`
	DeliveryCount int       `json:"delivery_count,omitempty"`
}

// TraceMetadata is a typed broker-owned trace context. It is not a bag of
// arbitrary user-visible properties.
type TraceMetadata struct {
	TraceParent string `json:"trace_parent,omitempty"`
	TraceState  string `json:"trace_state,omitempty"`
	TraceID     string `json:"trace_id,omitempty"`
}

// BrokerMetadata contains state owned exclusively by the broker.
type BrokerMetadata struct {
	Source   SourceMetadata   `json:"source,omitempty"`
	Delivery DeliveryMetadata `json:"delivery,omitempty"`
	Queue    QueueMetadata    `json:"queue,omitempty"`
	Transfer TransferMetadata `json:"transfer,omitempty"`
	Trace    TraceMetadata    `json:"trace,omitempty"`
}

// Envelope is the canonical in-memory and persisted broker message. Payload
// has exactly one representation: an immutable reference-counted buffer.
type Envelope struct {
	Version Version         `json:"version"`
	Topic   string          `json:"topic"`
	Payload *payload.Buffer `json:"-"`
	User    UserMetadata    `json:"user,omitempty"`
	Broker  BrokerMetadata  `json:"broker,omitempty"`
}

// New constructs a Version1 envelope and copies payload into the broker pool.
func New(topic string, data []byte) *Envelope {
	return &Envelope{Version: Version1, Topic: topic, Payload: payload.FromBytes(data)}
}

// NewDelivery constructs a Version1 envelope with protocol delivery metadata.
func NewDelivery(topic string, data []byte, qos byte, retain bool) *Envelope {
	envelope := New(topic, data)
	envelope.Broker.Delivery.QoS = qos
	envelope.Broker.Delivery.Retain = retain
	return envelope
}

// NewWithBuffer constructs a Version1 envelope and takes ownership of buf's
// existing reference.
func NewWithBuffer(topic string, buf *payload.Buffer) *Envelope {
	return &Envelope{Version: Version1, Topic: topic, Payload: buf}
}

// Validate rejects every schema other than the current one.
func (e *Envelope) Validate() error {
	if e == nil {
		return errors.New("message envelope is nil")
	}
	if e.Version != Version1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, e.Version)
	}
	return nil
}

// PayloadBytes returns an immutable view valid while e owns its reference.
func (e *Envelope) PayloadBytes() []byte {
	if e == nil || e.Payload == nil {
		return nil
	}
	return e.Payload.Bytes()
}

// StablePayload returns a copy whose lifetime is independent of e.
func (e *Envelope) StablePayload() []byte {
	return bytes.Clone(e.PayloadBytes())
}

// SetPayload replaces the payload with a pooled copy of data.
func (e *Envelope) SetPayload(data []byte) {
	if e.Payload != nil {
		e.Payload.Release()
	}
	e.Payload = payload.FromBytes(data)
}

// SetPayloadBuffer replaces the payload and takes ownership of buf's existing
// reference.
func (e *Envelope) SetPayloadBuffer(buf *payload.Buffer) {
	if e.Payload != nil {
		e.Payload.Release()
	}
	e.Payload = buf
}

// RetainPayload adds one payload reference before an envelope is shared.
func (e *Envelope) RetainPayload() {
	if e != nil && e.Payload != nil {
		e.Payload.Retain()
	}
}

// ReleasePayload drops this envelope's payload reference.
func (e *Envelope) ReleasePayload() {
	if e != nil && e.Payload != nil {
		e.Payload.Release()
		e.Payload = nil
	}
}

// IsExpired reports whether the queue or delivery expiry has passed.
func (e *Envelope) IsExpired() bool {
	if e == nil {
		return false
	}
	expiresAt := e.Broker.Queue.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = e.Broker.Delivery.ExpiresAt
	}
	return !expiresAt.IsZero() && time.Now().After(expiresAt)
}

// Clone returns a metadata-deep copy sharing the immutable payload.
func (e *Envelope) Clone() *Envelope {
	if e == nil {
		return nil
	}
	cp := *e
	cp.User = cloneUserMetadata(e.User)
	cp.Broker = cloneBrokerMetadata(e.Broker)
	if cp.Payload != nil {
		cp.Payload.Retain()
	}
	return &cp
}

func cloneUserMetadata(src UserMetadata) UserMetadata {
	dst := src
	dst.Key = bytes.Clone(src.Key)
	dst.CorrelationData = bytes.Clone(src.CorrelationData)
	dst.Properties = maps.Clone(src.Properties)
	if src.Headers != nil {
		dst.Headers = make(map[string][]byte, len(src.Headers))
		for key, value := range src.Headers {
			dst.Headers[key] = bytes.Clone(value)
		}
	}
	if src.PayloadFormat != nil {
		value := *src.PayloadFormat
		dst.PayloadFormat = &value
	}
	if src.MessageExpiry != nil {
		value := *src.MessageExpiry
		dst.MessageExpiry = &value
	}
	return dst
}

func cloneBrokerMetadata(src BrokerMetadata) BrokerMetadata {
	dst := src
	dst.Delivery.SubscriptionIDs = append([]uint32(nil), src.Delivery.SubscriptionIDs...)
	if src.Queue.Stream != nil {
		stream := *src.Queue.Stream
		dst.Queue.Stream = &stream
	}
	return dst
}

type persistedEnvelope struct {
	Version Version        `json:"version"`
	Topic   string         `json:"topic"`
	Payload []byte         `json:"payload,omitempty"`
	User    UserMetadata   `json:"user,omitempty"`
	Broker  BrokerMetadata `json:"broker,omitempty"`
}

// MarshalJSON persists only Version1. The payload bytes are read synchronously
// from their sole in-memory representation without an intermediate copy.
func (e Envelope) MarshalJSON() ([]byte, error) {
	if err := (&e).Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(persistedEnvelope{
		Version: e.Version,
		Topic:   e.Topic,
		Payload: e.PayloadBytes(),
		User:    e.User,
		Broker:  e.Broker,
	})
}

// UnmarshalJSON accepts only Version1 and never attempts a legacy decode.
func (e *Envelope) UnmarshalJSON(data []byte) error {
	var stored persistedEnvelope
	if err := json.Unmarshal(data, &stored); err != nil {
		return err
	}
	if stored.Version != Version1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, stored.Version)
	}
	if e.Payload != nil {
		e.Payload.Release()
	}
	e.Version = stored.Version
	e.Topic = stored.Topic
	e.Payload = payload.FromBytes(stored.Payload)
	e.User = stored.User
	e.Broker = stored.Broker
	return nil
}
