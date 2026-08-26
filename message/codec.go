// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/absmach/fluxmq/payload"
	"google.golang.org/protobuf/encoding/protowire"
)

// The binary envelope codec is the broker's strict persisted v1 contract. It
// uses protobuf wire primitives without exposing a generated public API. Field
// numbers in this file are therefore append-only and must never be reused.

// MarshalBinary encodes a complete Version1 envelope, including its payload.
func MarshalBinary(envelope *Envelope) ([]byte, error) {
	return marshalBinary(envelope, encodeFull)
}

// MarshalMetadata encodes a Version1 envelope without its payload or key. Log
// storage already owns those as the record value and key, so duplicating them
// in metadata wastes CPU, memory, and disk bandwidth.
func MarshalMetadata(envelope *Envelope) ([]byte, error) {
	return marshalBinary(envelope, encodeMetadata)
}

func marshalBinary(envelope *Envelope, mode codecMode) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}

	capacity := estimateSize(envelope, mode)
	if mode == encodeFull {
		capacity += len(envelope.PayloadBytes())
	}
	encoded := make([]byte, 0, capacity)
	encoded = appendVarint(encoded, 1, uint64(envelope.Version))
	encoded = appendString(encoded, 2, envelope.Topic)
	if mode == encodeFull {
		encoded = appendBytes(encoded, 3, envelope.PayloadBytes())
	}
	encoded, publisher := beginNested(encoded, 4)
	encoded = appendPublisher(encoded, envelope.PublisherMeta, mode)
	encoded = endNested(encoded, publisher)

	encoded, broker := beginNested(encoded, 5)
	encoded = appendBroker(encoded, envelope.BrokerMeta)
	encoded = endNested(encoded, broker)

	return encoded, nil
}

// UnmarshalBinary decodes a complete strict Version1 envelope. The returned
// envelope owns its payload and must be released by the caller.
func UnmarshalBinary(encoded []byte) (*Envelope, error) {
	return unmarshalBinary(encoded, nil, nil, encodeFull)
}

// UnmarshalMetadata decodes strict Version1 log metadata and copies value and
// key into broker-owned memory. The returned envelope must be released.
func UnmarshalMetadata(encoded, value, key []byte) (*Envelope, error) {
	return unmarshalBinary(encoded, value, key, encodeMetadata)
}

func unmarshalBinary(encoded, externalPayload, externalKey []byte, mode codecMode) (*Envelope, error) {
	envelope := Acquire()
	// Acquire initializes new in-memory messages as Version1. Decoding must not
	// inherit that default: a persisted record without an explicit version is
	// unsupported, exactly like the JSON decoder.
	envelope.Version = 0
	var decodedPayload []byte
	err := walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, scalar uint64) error {
		switch number {
		case 1:
			if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
				return err
			}
			envelope.Version = Version(scalar)
		case 2:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			envelope.Topic = string(raw)
		case 3:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			// The log record owns the payload in metadata mode, so a blob
			// carrying one is rejected rather than having it silently dropped
			// in favour of the record's. MarshalMetadata omits this field, so
			// this asserts the existing invariant rather than adding one.
			if mode == encodeMetadata {
				return ErrMetadataCarriesPayload
			}
			decodedPayload = raw
		case 4:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			if mode == encodeMetadata {
				if err := rejectEmbeddedKey(raw); err != nil {
					return err
				}
			}
			return decodeUser(raw, &envelope.PublisherMeta)
		case 5:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			return decodeBroker(raw, &envelope.BrokerMeta)
		}
		return nil
	})
	if err != nil {
		Release(envelope)
		return nil, err
	}
	if err := requireVersion1(envelope.Version); err != nil {
		Release(envelope)
		return nil, err
	}
	if mode == encodeMetadata {
		decodedPayload = externalPayload
		envelope.PublisherMeta.Key = NewBinary(externalKey)
	}
	envelope.payload = payload.FromBytes(decodedPayload)
	return envelope, nil
}

func appendPublisher(encoded []byte, user PublisherMetadata, mode codecMode) []byte {
	if mode == encodeFull {
		encoded = appendBytes(encoded, 1, user.Key.value)
	}
	for key, value := range user.Headers.values {
		var entry nested
		encoded, entry = beginNested(encoded, 2)
		encoded = appendString(encoded, 1, key)
		encoded = appendBytes(encoded, 2, value.value)
		encoded = endNested(encoded, entry)
	}
	for key, value := range user.Properties.values {
		var entry nested
		encoded, entry = beginNested(encoded, 3)
		encoded = appendString(encoded, 1, key)
		encoded = appendString(encoded, 2, value)
		encoded = endNested(encoded, entry)
	}
	encoded = appendString(encoded, 4, user.ContentType)
	encoded = appendString(encoded, 5, user.ContentEncoding)
	encoded = appendString(encoded, 6, user.ResponseTopic)
	encoded = appendBytes(encoded, 7, user.CorrelationData.value)
	if value, ok := user.PayloadFormat.Value(); ok {
		encoded = appendVarint(encoded, 8, uint64(value))
	}
	if value, ok := user.MessageExpiry.Value(); ok {
		encoded = appendVarint(encoded, 9, uint64(value))
	}
	encoded = appendString(encoded, 10, user.MessageID)
	return encoded
}

func decodeUser(encoded []byte, user *PublisherMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, scalar uint64) error {
		switch number {
		case 1:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			user.Key = NewBinary(raw)
		case 2:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			key, value, err := decodeBytesMapEntry(raw)
			if err != nil {
				return err
			}
			if user.Headers.values == nil {
				user.Headers.values = make(map[string]Binary)
			}
			user.Headers.values[key] = Binary{value: value}
		case 3:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			key, value, err := decodeStringMapEntry(raw)
			if err != nil {
				return err
			}
			if user.Properties.values == nil {
				user.Properties.values = make(map[string]string)
			}
			user.Properties.values[key] = value
		case 4:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			user.ContentType = string(raw)
		case 5:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			user.ContentEncoding = string(raw)
		case 6:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			user.ResponseTopic = string(raw)
		case 7:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			user.CorrelationData = NewBinary(raw)
		case 8:
			if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
				return err
			}
			if scalar > 255 {
				return fmt.Errorf("message envelope payload format overflows byte: %d", scalar)
			}
			user.PayloadFormat = Some(byte(scalar))
		case 9:
			if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
				return err
			}
			if scalar > uint64(^uint32(0)) {
				return fmt.Errorf("message envelope expiry overflows uint32: %d", scalar)
			}
			user.MessageExpiry = Some(uint32(scalar))
		case 10:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			user.MessageID = string(raw)
		}
		return nil
	})
}

func appendBroker(encoded []byte, broker BrokerMetadata) []byte {
	var at nested

	encoded, at = beginNested(encoded, 1)
	encoded = appendSource(encoded, broker.Source)
	encoded = endNested(encoded, at)

	encoded, at = beginNested(encoded, 2)
	encoded = appendDelivery(encoded, broker.Delivery)
	encoded = endNested(encoded, at)

	encoded, at = beginNested(encoded, 3)
	encoded = appendQueue(encoded, broker.Queue)
	encoded = endNested(encoded, at)

	encoded, at = beginNested(encoded, 4)
	encoded = appendTransfer(encoded, broker.Transfer)
	encoded = endNested(encoded, at)

	encoded, at = beginNested(encoded, 5)
	encoded = appendTrace(encoded, broker.Trace)
	encoded = endNested(encoded, at)

	return encoded
}

func decodeBroker(encoded []byte, broker *BrokerMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, _ uint64) error {
		switch number {
		case 1:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			return decodeSource(raw, &broker.Source)
		case 2:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			return decodeDelivery(raw, &broker.Delivery)
		case 3:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			return decodeQueue(raw, &broker.Queue)
		case 4:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			return decodeTransfer(raw, &broker.Transfer)
		case 5:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			return decodeTrace(raw, &broker.Trace)
		}
		return nil
	})
}

func appendSource(encoded []byte, source SourceMetadata) []byte {
	encoded = appendString(encoded, 1, source.ClientID)
	encoded = appendString(encoded, 2, source.ExternalID)
	protocol, _ := protocolNumber(source.Protocol) // validated by marshalBinary
	encoded = appendNonZeroVarint(encoded, 3, protocol)
	encoded = appendString(encoded, 4, source.Topic)
	return encoded
}

func decodeSource(encoded []byte, source *SourceMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, scalar uint64) error {
		switch number {
		case 1:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			source.ClientID = string(raw)
		case 2:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			source.ExternalID = string(raw)
		case 3:
			if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
				return err
			}
			protocol, err := protocolFromNumber(scalar)
			if err != nil {
				return err
			}
			source.Protocol = protocol
		case 4:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			source.Topic = string(raw)
		}
		return nil
	})
}

func appendDelivery(encoded []byte, delivery DeliveryMetadata) []byte {
	encoded = appendTime(encoded, 1, delivery.PublishedAt)
	encoded = appendTime(encoded, 2, delivery.ExpiresAt)
	encoded = appendPackedVarints(encoded, 3, delivery.SubscriptionIDs.values)
	encoded = appendNonZeroVarint(encoded, 4, uint64(delivery.PacketID))
	encoded = appendNonZeroVarint(encoded, 5, uint64(delivery.QoS))
	encoded = appendNonZeroVarint(encoded, 6, uint64(delivery.InflightDirection))
	encoded = appendNonZeroVarint(encoded, 7, uint64(delivery.InflightState))
	encoded = appendBool(encoded, 8, delivery.Retain)
	encoded = appendBool(encoded, 9, delivery.Duplicate)
	return encoded
}

func decodeDelivery(encoded []byte, delivery *DeliveryMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, scalar uint64) error {
		if number < 1 || number > 9 {
			return nil
		}
		// Field 3 is a packed repeated field. A record written before it was
		// packed carries one varint-tagged entry per ID, and proto3 requires a
		// decoder to accept both forms, so both are read.
		if number == 3 && wireType == protowire.BytesType {
			return decodePackedSubscriptionIDs(raw, delivery)
		}
		if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
			return err
		}
		switch number {
		case 1:
			delivery.PublishedAt = decodeTime(scalar)
		case 2:
			delivery.ExpiresAt = decodeTime(scalar)
		case 3:
			if scalar > uint64(^uint32(0)) {
				return fmt.Errorf("message envelope subscription ID overflows uint32: %d", scalar)
			}
			delivery.SubscriptionIDs.values = append(delivery.SubscriptionIDs.values, uint32(scalar))
		case 4:
			if scalar > uint64(^uint16(0)) {
				return fmt.Errorf("message envelope packet ID overflows uint16: %d", scalar)
			}
			delivery.PacketID = uint16(scalar)
		case 5:
			if scalar > 255 {
				return fmt.Errorf("message envelope QoS overflows byte: %d", scalar)
			}
			delivery.QoS = byte(scalar)
		case 6:
			if scalar > 255 {
				return fmt.Errorf("message envelope inflight direction overflows byte: %d", scalar)
			}
			delivery.InflightDirection = byte(scalar)
		case 7:
			if scalar > 255 {
				return fmt.Errorf("message envelope inflight state overflows byte: %d", scalar)
			}
			delivery.InflightState = byte(scalar)
		case 8:
			delivery.Retain = scalar != 0
		case 9:
			delivery.Duplicate = scalar != 0
		}
		return nil
	})
}

// Field 1 is retired: it held a message identifier that is now derived from the
// queue and offset. Nothing writes it, and decodeQueue ignores it, so a record
// written by an older broker still decodes.
func appendQueue(encoded []byte, queue QueueMetadata) []byte {
	encoded = appendString(encoded, 2, queue.Name)
	encoded = appendString(encoded, 3, queue.GroupID)
	encoded = appendNonZeroVarint(encoded, 4, queue.Offset)
	state, _ := queueStateNumber(queue.State) // validated by marshalBinary
	encoded = appendNonZeroVarint(encoded, 5, state)
	encoded = appendTime(encoded, 6, queue.CreatedAt)
	encoded = appendTime(encoded, 7, queue.DeliveredAt)
	encoded = appendTime(encoded, 8, queue.NextRetryAt)
	encoded = appendNonZeroVarint(encoded, 9, uint64(queue.RetryCount))
	encoded = appendTime(encoded, 10, queue.ExpiresAt)
	if streamValue, ok := queue.Stream.Value(); ok {
		encoded, stream := beginNested(encoded, 11)
		encoded = appendStream(encoded, streamValue)
		return endNested(encoded, stream)
	}
	return encoded
}

func decodeQueue(encoded []byte, queue *QueueMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, scalar uint64) error {
		switch number {
		case 2, 3, 11:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
		case 4, 5, 6, 7, 8, 9, 10:
			if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
				return err
			}
		default:
			return nil
		}
		switch number {
		case 2:
			queue.Name = string(raw)
		case 3:
			queue.GroupID = string(raw)
		case 4:
			queue.Offset = scalar
		case 5:
			state, err := queueStateFromNumber(scalar)
			if err != nil {
				return err
			}
			queue.State = state
		case 6:
			queue.CreatedAt = decodeTime(scalar)
		case 7:
			queue.DeliveredAt = decodeTime(scalar)
		case 8:
			queue.NextRetryAt = decodeTime(scalar)
		case 9:
			queue.RetryCount = int(scalar)
		case 10:
			queue.ExpiresAt = decodeTime(scalar)
		case 11:
			var stream StreamMetadata
			if err := decodeStream(raw, &stream); err != nil {
				return err
			}
			queue.Stream = Some(stream)
		}
		return nil
	})
}

func appendStream(encoded []byte, stream StreamMetadata) []byte {
	encoded = appendNonZeroVarint(encoded, 1, stream.Offset)
	if stream.Timestamp != 0 {
		encoded = appendVarint(encoded, 2, protowire.EncodeZigZag(stream.Timestamp))
	}
	encoded = appendNonZeroVarint(encoded, 3, stream.CommittedOffset)
	encoded = appendBool(encoded, 4, stream.HasCommittedOffset)
	encoded = appendBool(encoded, 5, stream.WorkAcknowledged)
	encoded = appendString(encoded, 6, stream.WorkGroup)
	return encoded
}

func decodeStream(encoded []byte, stream *StreamMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, scalar uint64) error {
		if number == 6 {
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			stream.WorkGroup = string(raw)
			return nil
		}
		if number < 1 || number > 5 {
			return nil
		}
		if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
			return err
		}
		switch number {
		case 1:
			stream.Offset = scalar
		case 2:
			stream.Timestamp = protowire.DecodeZigZag(scalar)
		case 3:
			stream.CommittedOffset = scalar
		case 4:
			stream.HasCommittedOffset = scalar != 0
		case 5:
			stream.WorkAcknowledged = scalar != 0
		}
		return nil
	})
}

func appendTransfer(encoded []byte, transfer TransferMetadata) []byte {
	encoded = appendString(encoded, 1, transfer.ID)
	encoded = appendString(encoded, 2, transfer.FailureReason)
	encoded = appendTime(encoded, 3, transfer.FirstAttempt)
	encoded = appendTime(encoded, 4, transfer.LastAttempt)
	encoded = appendTime(encoded, 5, transfer.CompletedAt)
	encoded = appendString(encoded, 6, transfer.SourceQueue)
	encoded = appendString(encoded, 7, transfer.SourceGroup)
	encoded = appendNonZeroVarint(encoded, 8, transfer.SourceOffset)
	encoded = appendNonZeroVarint(encoded, 9, uint64(transfer.DeliveryCount))
	return encoded
}

func decodeTransfer(encoded []byte, transfer *TransferMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, scalar uint64) error {
		switch number {
		case 1, 2, 6, 7:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
		case 3, 4, 5, 8, 9:
			if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
				return err
			}
		default:
			return nil
		}
		switch number {
		case 1:
			transfer.ID = string(raw)
		case 2:
			transfer.FailureReason = string(raw)
		case 3:
			transfer.FirstAttempt = decodeTime(scalar)
		case 4:
			transfer.LastAttempt = decodeTime(scalar)
		case 5:
			transfer.CompletedAt = decodeTime(scalar)
		case 6:
			transfer.SourceQueue = string(raw)
		case 7:
			transfer.SourceGroup = string(raw)
		case 8:
			transfer.SourceOffset = scalar
		case 9:
			transfer.DeliveryCount = int(scalar)
		}
		return nil
	})
}

func appendTrace(encoded []byte, trace TraceMetadata) []byte {
	encoded = appendString(encoded, 1, trace.TraceParent)
	encoded = appendString(encoded, 2, trace.TraceState)
	encoded = appendString(encoded, 3, trace.TraceID)
	return encoded
}

func decodeTrace(encoded []byte, trace *TraceMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, _ uint64) error {
		switch number {
		case 1:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			trace.TraceParent = string(raw)
		case 2:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			trace.TraceState = string(raw)
		case 3:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			trace.TraceID = string(raw)
		}
		return nil
	})
}

func decodeBytesMapEntry(encoded []byte) (string, []byte, error) {
	var key string
	var value []byte
	err := walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, _ uint64) error {
		switch number {
		case 1:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			key = string(raw)
		case 2:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			value = bytes.Clone(raw)
		}
		return nil
	})
	return key, value, err
}

func decodeStringMapEntry(encoded []byte) (string, string, error) {
	var key, value string
	err := walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, _ uint64) error {
		switch number {
		case 1:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			key = string(raw)
		case 2:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			value = string(raw)
		}
		return nil
	})
	return key, value, err
}

func walkFields(encoded []byte, visit func(protowire.Number, protowire.Type, []byte, uint64) error) error {
	for len(encoded) > 0 {
		number, wireType, tagLen := protowire.ConsumeTag(encoded)
		if tagLen < 0 {
			return protowire.ParseError(tagLen)
		}
		encoded = encoded[tagLen:]
		switch wireType {
		case protowire.VarintType:
			value, valueLen := protowire.ConsumeVarint(encoded)
			if valueLen < 0 {
				return protowire.ParseError(valueLen)
			}
			if err := visit(number, wireType, nil, value); err != nil {
				return err
			}
			encoded = encoded[valueLen:]
		case protowire.BytesType:
			value, valueLen := protowire.ConsumeBytes(encoded)
			if valueLen < 0 {
				return protowire.ParseError(valueLen)
			}
			if err := visit(number, wireType, value, 0); err != nil {
				return err
			}
			encoded = encoded[valueLen:]
		default:
			valueLen := protowire.ConsumeFieldValue(number, wireType, encoded)
			if valueLen < 0 {
				return protowire.ParseError(valueLen)
			}
			encoded = encoded[valueLen:]
		}
	}
	return nil
}

func requireWireType(number protowire.Number, got, want protowire.Type) error {
	if got != want {
		return fmt.Errorf("message envelope field %d has wire type %d, want %d", number, got, want)
	}
	return nil
}

// estimateSize approximates the encoded size of everything but the payload, so
// a message carrying metadata does not grow its buffer two or three times on
// the way out. A realistic queue record encodes to about 600 bytes of metadata,
// against a flat 128-byte guess that grew three times to reach it.
//
// It is deliberately an estimate and not a size pass. An exact pass would have
// to restate every field's encoding rule, and could then disagree with the one
// that writes — the class of drift the schema of record exists to prevent. This
// one only sizes a buffer: too small and append grows it as before, too large
// and a few bytes go unused.
func estimateSize(envelope *Envelope, mode codecMode) int {
	const (
		// Tag plus length prefix for one field, and for one map entry, which
		// carries two fields of its own.
		fieldOverhead = 2
		entryOverhead = 8
		// Every namespace is a varint field or two beyond its strings. Ten
		// bytes is the widest a varint gets.
		namespaceOverhead = 10 * 10
	)

	size := fieldOverhead * 4 // version, topic, publisher, broker
	size += len(envelope.Topic)

	user := &envelope.PublisherMeta
	if mode == encodeFull {
		size += user.Key.Len() + fieldOverhead
	}
	for key, value := range user.Headers.values {
		size += len(key) + value.Len() + entryOverhead
	}
	for key, value := range user.Properties.values {
		size += len(key) + len(value) + entryOverhead
	}
	size += len(user.ContentType) + len(user.ContentEncoding) + len(user.ResponseTopic) +
		user.CorrelationData.Len() + len(user.MessageID) + 5*fieldOverhead

	broker := &envelope.BrokerMeta
	size += len(broker.Source.ClientID) + len(broker.Source.ExternalID) +
		len(broker.Source.Protocol) + len(broker.Source.Topic)
	size += len(broker.Queue.Name) + len(broker.Queue.GroupID) + len(broker.Queue.State)
	size += len(broker.Transfer.ID) + len(broker.Transfer.FailureReason) +
		len(broker.Transfer.SourceQueue) + len(broker.Transfer.SourceGroup)
	size += len(broker.Trace.TraceParent) + len(broker.Trace.TraceState) + len(broker.Trace.TraceID)
	size += namespaceOverhead
	if stream, ok := broker.Queue.Stream.Value(); ok {
		size += len(stream.WorkGroup) + namespaceOverhead
	}
	size += broker.Delivery.SubscriptionIDs.Len() * 6

	return size
}

// codecMode selects what an encoding carries. A metadata encoding describes a
// log record that holds the value and key itself, so both are left out; a full
// encoding is self-contained.
//
// It replaces a pair of booleans that were always passed the same value at every
// call site, where marshalBinary(envelope, false, false) said nothing about
// which of the two forms it meant.
type codecMode uint8

const (
	encodeFull codecMode = iota
	encodeMetadata
)

// rejectEmbeddedKey reports a metadata blob that carries the publisher key. The
// log record owns the key in metadata mode, so an embedded one would be dropped
// silently in favour of the record's.
func rejectEmbeddedKey(user []byte) error {
	return walkFields(user, func(number protowire.Number, _ protowire.Type, _ []byte, _ uint64) error {
		if number == 1 {
			return ErrMetadataCarriesKey
		}
		return nil
	})
}

// appendPackedVarints writes a repeated scalar field in proto3's default packed
// form: one length-delimited field holding the concatenated varints, rather
// than one tagged varint per element.
//
// The unpacked form this replaces was not what a .proto declaring
// `repeated uint32` produces, so nothing generated from the schema of record
// could have written a record this codec would read.
func appendPackedVarints(encoded []byte, number protowire.Number, values []uint32) []byte {
	if len(values) == 0 {
		return encoded
	}
	encoded, at := beginNested(encoded, number)
	for _, value := range values {
		encoded = protowire.AppendVarint(encoded, uint64(value))
	}
	return endNested(encoded, at)
}

func decodePackedSubscriptionIDs(raw []byte, delivery *DeliveryMetadata) error {
	for len(raw) > 0 {
		value, n := protowire.ConsumeVarint(raw)
		if n < 0 {
			return fmt.Errorf("message envelope packed subscription IDs: %w", protowire.ParseError(n))
		}
		if value > uint64(^uint32(0)) {
			return fmt.Errorf("message envelope subscription ID overflows uint32: %d", value)
		}
		delivery.SubscriptionIDs.values = append(delivery.SubscriptionIDs.values, uint32(value))
		raw = raw[n:]
	}
	return nil
}

// nested marks where a length-delimited field began, so its length prefix can
// be written after its body.
type nested struct {
	tagAt  int
	bodyAt int
}

// beginNested writes a nested message's tag and reserves one byte for its
// length. The body is then appended straight into the same buffer.
//
// Each nested message used to be encoded into a buffer of its own and copied in
// once complete. With nine namespaces and one buffer per map entry, that is
// what made a realistic envelope cost 44 allocations to marshal where the
// generated codec costs 17.
func beginNested(encoded []byte, number protowire.Number) ([]byte, nested) {
	at := nested{tagAt: len(encoded)}
	encoded = protowire.AppendTag(encoded, number, protowire.BytesType)
	// One byte covers a body up to 127 bytes, which is every namespace on an
	// ordinary message. A longer one widens the prefix in endNested.
	encoded = append(encoded, 0)
	at.bodyAt = len(encoded)
	return encoded, at
}

// endNested writes the length of the body appended since beginNested.
//
// An empty body removes the field outright, which is what encoding into a
// separate buffer did by skipping an empty byte slice — and is what keeps a
// message with no queue metadata from carrying an empty queue submessage.
func endNested(encoded []byte, at nested) []byte {
	size := len(encoded) - at.bodyAt
	if size == 0 {
		return encoded[:at.tagAt]
	}

	var prefix [binary.MaxVarintLen64]byte
	width := binary.PutUvarint(prefix[:], uint64(size))
	if width > 1 {
		// Make room for the wider prefix and shift the body down once. This is
		// a copy inside one buffer rather than an allocation, and it happens
		// only for a body over 127 bytes.
		encoded = append(encoded, prefix[:width-1]...)
		copy(encoded[at.bodyAt+width-1:], encoded[at.bodyAt:at.bodyAt+size])
	}
	copy(encoded[at.bodyAt-1:], prefix[:width])
	return encoded
}

func appendString(encoded []byte, number protowire.Number, value string) []byte {
	if value == "" {
		return encoded
	}
	encoded = protowire.AppendTag(encoded, number, protowire.BytesType)
	return protowire.AppendString(encoded, value)
}

func appendBytes(encoded []byte, number protowire.Number, value []byte) []byte {
	if len(value) == 0 {
		return encoded
	}
	encoded = protowire.AppendTag(encoded, number, protowire.BytesType)
	return protowire.AppendBytes(encoded, value)
}

func appendVarint(encoded []byte, number protowire.Number, value uint64) []byte {
	encoded = protowire.AppendTag(encoded, number, protowire.VarintType)
	return protowire.AppendVarint(encoded, value)
}

func appendNonZeroVarint(encoded []byte, number protowire.Number, value uint64) []byte {
	if value == 0 {
		return encoded
	}
	return appendVarint(encoded, number, value)
}

func appendBool(encoded []byte, number protowire.Number, value bool) []byte {
	if !value {
		return encoded
	}
	return appendVarint(encoded, number, 1)
}

func appendTime(encoded []byte, number protowire.Number, value time.Time) []byte {
	if value.IsZero() {
		return encoded
	}
	return appendVarint(encoded, number, protowire.EncodeZigZag(value.UnixNano()))
}

func decodeTime(value uint64) time.Time {
	return time.Unix(0, protowire.DecodeZigZag(value)).UTC()
}
