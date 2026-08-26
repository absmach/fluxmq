// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

// Package message defines the broker's single canonical internal message.
package message

import (
	"bytes"
	"errors"
	"fmt"
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

// ErrUnsupportedProtocol and ErrUnsupportedQueueState identify enum values
// that cannot be represented by the persisted v1 schema.
var (
	ErrUnsupportedProtocol   = errors.New("unsupported message protocol")
	ErrUnsupportedQueueState = errors.New("unsupported message queue state")
)

// A metadata blob describes a log record whose value and key the record itself
// owns. Carrying either inside the blob would leave two sources for one field,
// with the embedded one silently losing.
var (
	ErrMetadataCarriesPayload = errors.New("message envelope metadata carries a payload")
	ErrMetadataCarriesKey     = errors.New("message envelope metadata carries a publisher key")
)

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
	Key             Binary
	Headers         HeaderMap
	Properties      PropertyMap
	ContentType     string
	ContentEncoding string
	ResponseTopic   string
	CorrelationData Binary
	PayloadFormat   Optional[byte]
	MessageExpiry   Optional[uint32]

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
	SubscriptionIDs   Uint32List
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
	Stream      Optional[StreamMetadata]
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
	PublisherMeta PublisherMetadata
	BrokerMeta    BrokerMetadata
	payload       *payload.Buffer
}

// New constructs a Version1 envelope and copies payload into the broker pool.
func New(topic string, data []byte) *Envelope {
	envelope := Acquire()
	envelope.Topic = topic
	envelope.payload = payload.FromBytes(data)
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
	envelope.payload = buf
	return envelope
}

// Validate rejects every schema other than the current one.
func (e *Envelope) Validate() error {
	if e == nil {
		return errors.New("message envelope is nil")
	}
	if err := requireVersion1(e.Version); err != nil {
		return err
	}
	if _, err := protocolNumber(e.BrokerMeta.Source.Protocol); err != nil {
		return err
	}
	if _, err := queueStateNumber(e.BrokerMeta.Queue.State); err != nil {
		return err
	}
	return nil
}

func protocolNumber(protocol Protocol) (uint64, error) {
	switch protocol {
	case "":
		return 0, nil
	case ProtocolMQTT:
		return 1, nil
	case ProtocolAMQP091:
		return 2, nil
	case ProtocolAMQP1:
		return 3, nil
	case ProtocolHTTP:
		return 4, nil
	case ProtocolCoAP:
		return 5, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrUnsupportedProtocol, protocol)
	}
}

func protocolFromNumber(number uint64) (Protocol, error) {
	switch number {
	case 0:
		return "", nil
	case 1:
		return ProtocolMQTT, nil
	case 2:
		return ProtocolAMQP091, nil
	case 3:
		return ProtocolAMQP1, nil
	case 4:
		return ProtocolHTTP, nil
	case 5:
		return ProtocolCoAP, nil
	default:
		return "", fmt.Errorf("%w: %d", ErrUnsupportedProtocol, number)
	}
}

func queueStateNumber(state QueueState) (uint64, error) {
	switch state {
	case "":
		return 0, nil
	case QueueStateQueued:
		return 1, nil
	case QueueStateDelivered:
		return 2, nil
	case QueueStateAcked:
		return 3, nil
	case QueueStateRetry:
		return 4, nil
	case QueueStateDLQ:
		return 5, nil
	default:
		return 0, fmt.Errorf("%w: %q", ErrUnsupportedQueueState, state)
	}
}

func queueStateFromNumber(number uint64) (QueueState, error) {
	switch number {
	case 0:
		return "", nil
	case 1:
		return QueueStateQueued, nil
	case 2:
		return QueueStateDelivered, nil
	case 3:
		return QueueStateAcked, nil
	case 4:
		return QueueStateRetry, nil
	case 5:
		return QueueStateDLQ, nil
	default:
		return "", fmt.Errorf("%w: %d", ErrUnsupportedQueueState, number)
	}
}

// requireVersion1 is the one place a version is checked. It was written out
// three times, once per decoder, which is three chances for the message or the
// comparison to drift.
func requireVersion1(version Version) error {
	if version != Version1 {
		return fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}
	return nil
}

// PayloadBytes returns an immutable view valid while e owns its reference.
func (e *Envelope) PayloadBytes() []byte {
	if e == nil || e.payload == nil {
		return nil
	}
	return e.payload.Bytes()
}

// StablePayload returns a copy whose lifetime is independent of e.
func (e *Envelope) StablePayload() []byte {
	return bytes.Clone(e.PayloadBytes())
}

// SetPayload replaces the payload with a pooled copy of data.
func (e *Envelope) SetPayload(data []byte) {
	if e.payload != nil {
		e.payload.Release()
	}
	e.payload = payload.FromBytes(data)
}

// SetPayloadBuffer replaces the payload and takes ownership of buf's existing
// reference.
func (e *Envelope) SetPayloadBuffer(buf *payload.Buffer) {
	if e.payload != nil {
		e.payload.Release()
	}
	e.payload = buf
}

// RetainPayload returns an owned payload reference for a packet or other
// asynchronous holder. The caller must release the returned buffer.
func (e *Envelope) RetainPayload() *payload.Buffer {
	if e != nil && e.payload != nil {
		e.payload.Retain()
		return e.payload
	}
	return nil
}

// ReleasePayload drops this envelope's payload reference.
func (e *Envelope) ReleasePayload() {
	if e != nil && e.payload != nil {
		e.payload.Release()
		e.payload = nil
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

// Clone returns an O(1) metadata copy sharing the immutable payload and
// immutable metadata collections. Subsequent metadata mutation is copy-on-write.
func (e *Envelope) Clone() *Envelope {
	if e == nil {
		return nil
	}
	cp := Acquire()
	cp.Version = e.Version
	cp.Topic = e.Topic
	cp.payload = e.payload
	if cp.payload != nil {
		cp.payload.Retain()
	}
	cp.PublisherMeta = e.PublisherMeta
	cp.BrokerMeta = e.BrokerMeta
	return cp
}
