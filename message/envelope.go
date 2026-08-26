// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package message defines the broker's single canonical internal message.
package message

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"strconv"
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

// PublisherMetadata contains publisher-owned message metadata. Protocol adapters
// may project only fields their wire format supports.
type PublisherMetadata struct {
	Key             []byte
	Headers         map[string][]byte
	Properties      map[string]string
	ContentType     string
	ContentEncoding string
	ResponseTopic   string
	CorrelationData []byte
	PayloadFormat   *byte
	MessageExpiry   *uint32

	// MessageID is the publisher's own identifier, as carried by AMQP's
	// message-id property. It is user metadata: the broker never reads it and a
	// publisher may set it to anything. The broker's own handle for a durable
	// delivery is QueueMetadata.DeliveryID, derived from the queue and offset.
	MessageID string
}

// SourceMetadata identifies the authenticated origin. It is broker-owned and
// never accepted from an untrusted peer as user metadata.
type SourceMetadata struct {
	ClientID   string
	ExternalID string
	Protocol   Protocol
	Topic      string
}

// DeliveryMetadata contains broker delivery and MQTT transaction state.
type DeliveryMetadata struct {
	PublishedAt       time.Time
	ExpiresAt         time.Time
	SubscriptionIDs   []uint32
	PacketID          uint16
	QoS               byte
	InflightDirection byte
	InflightState     byte
	Retain            bool
	Duplicate         bool
}

// StreamMetadata records broker-owned stream projection state.
type StreamMetadata struct {
	Offset             uint64
	Timestamp          int64
	CommittedOffset    uint64
	HasCommittedOffset bool
	WorkAcknowledged   bool
	WorkGroup          string
}

// QueueMetadata contains durable-queue identity and lifecycle state.
//
// It stores no message identifier. A durable record is named by its queue and
// offset, and the string form clients see is derived from those by DeliveryID
// at the protocol boundary. The stored field it replaces held two different
// things at two different times — a delivery handle written at delivery, and
// whatever a publisher had put in a message-id property — so neither could be
// trusted to mean the other.
type QueueMetadata struct {
	Name        string
	GroupID     string
	Offset      uint64
	State       QueueState
	CreatedAt   time.Time
	DeliveredAt time.Time
	NextRetryAt time.Time
	RetryCount  int
	ExpiresAt   time.Time
	Stream      *StreamMetadata
}

// DeliveryID is the broker's handle for a durable delivery, "<queue>:<offset>".
// It is derived rather than stored: the queue and the offset are the identity,
// and any string form is a rendering of them for a protocol that needs one.
// A record with no queue has no delivery handle.
func (q QueueMetadata) DeliveryID() string {
	if q.Name == "" {
		return ""
	}
	return q.Name + ":" + strconv.FormatUint(q.Offset, 10)
}

// TransferMetadata records a recoverable broker-owned message transfer such
// as a move to a dead-letter queue.
type TransferMetadata struct {
	ID            string
	FailureReason string
	FirstAttempt  time.Time
	LastAttempt   time.Time
	CompletedAt   time.Time
	SourceQueue   string
	SourceGroup   string
	SourceOffset  uint64
	DeliveryCount int
}

// TraceMetadata is a typed broker-owned trace context. It is not a bag of
// arbitrary user-visible properties.
type TraceMetadata struct {
	TraceParent string
	TraceState  string
	TraceID     string
}

// BrokerMetadata contains state owned exclusively by the broker.
type BrokerMetadata struct {
	Source   SourceMetadata
	Delivery DeliveryMetadata
	Queue    QueueMetadata
	Transfer TransferMetadata
	Trace    TraceMetadata
}

// Envelope is the canonical in-memory and persisted broker message. Payload
// has exactly one representation: an immutable reference-counted buffer.
type Envelope struct {
	Version       Version
	Topic         string
	Payload       *payload.Buffer
	PublisherMeta PublisherMetadata
	BrokerMeta    BrokerMetadata
}

// New constructs a Version1 envelope and copies payload into the broker pool.
func New(topic string, data []byte) *Envelope {
	envelope := Acquire()
	envelope.Topic = topic
	envelope.Payload = payload.FromBytes(data)
	return envelope
}

// NewDelivery constructs a Version1 envelope with protocol delivery metadata.
func NewDelivery(topic string, data []byte, qos byte, retain bool) *Envelope {
	envelope := New(topic, data)
	envelope.BrokerMeta.Delivery.QoS = qos
	envelope.BrokerMeta.Delivery.Retain = retain
	return envelope
}

// NewWithBuffer constructs a Version1 envelope and takes ownership of buf's
// existing reference.
func NewWithBuffer(topic string, buf *payload.Buffer) *Envelope {
	envelope := Acquire()
	envelope.Topic = topic
	envelope.Payload = buf
	return envelope
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
	expiresAt := e.BrokerMeta.Queue.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = e.BrokerMeta.Delivery.ExpiresAt
	}
	return !expiresAt.IsZero() && time.Now().After(expiresAt)
}

// Clone returns a metadata-deep copy sharing the immutable payload.
func (e *Envelope) Clone() *Envelope {
	if e == nil {
		return nil
	}
	cp := Acquire()
	cp.Version = e.Version
	cp.Topic = e.Topic
	cp.Payload = e.Payload
	if cp.Payload != nil {
		cp.Payload.Retain()
	}
	if hasUserMetadata(e.PublisherMeta) {
		cp.PublisherMeta = cloneUserMetadata(e.PublisherMeta)
	}
	if e.BrokerMeta.Source != (SourceMetadata{}) {
		cp.BrokerMeta.Source = e.BrokerMeta.Source
	}
	if hasDeliveryMetadata(e.BrokerMeta.Delivery) {
		cp.BrokerMeta.Delivery = cloneDeliveryMetadata(e.BrokerMeta.Delivery)
	}
	if e.BrokerMeta.Queue != (QueueMetadata{}) {
		cp.BrokerMeta.Queue = cloneQueueMetadata(e.BrokerMeta.Queue)
	}
	if e.BrokerMeta.Transfer != (TransferMetadata{}) {
		cp.BrokerMeta.Transfer = e.BrokerMeta.Transfer
	}
	if e.BrokerMeta.Trace != (TraceMetadata{}) {
		cp.BrokerMeta.Trace = e.BrokerMeta.Trace
	}
	return cp
}

func hasUserMetadata(user PublisherMetadata) bool {
	return len(user.Key) > 0 || len(user.Headers) > 0 || len(user.Properties) > 0 ||
		user.ContentType != "" || user.ContentEncoding != "" || user.ResponseTopic != "" ||
		len(user.CorrelationData) > 0 || user.PayloadFormat != nil || user.MessageExpiry != nil ||
		user.MessageID != ""
}

func hasDeliveryMetadata(delivery DeliveryMetadata) bool {
	return !delivery.PublishedAt.IsZero() || !delivery.ExpiresAt.IsZero() || len(delivery.SubscriptionIDs) > 0 ||
		delivery.PacketID != 0 || delivery.QoS != 0 || delivery.InflightDirection != 0 ||
		delivery.InflightState != 0 || delivery.Retain || delivery.Duplicate
}

func cloneUserMetadata(src PublisherMetadata) PublisherMetadata {
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

func cloneDeliveryMetadata(src DeliveryMetadata) DeliveryMetadata {
	dst := src
	dst.SubscriptionIDs = append([]uint32(nil), src.SubscriptionIDs...)
	return dst
}

func cloneQueueMetadata(src QueueMetadata) QueueMetadata {
	dst := src
	if src.Stream != nil {
		stream := *src.Stream
		dst.Stream = &stream
	}
	return dst
}
