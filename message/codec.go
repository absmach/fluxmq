// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"bytes"
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
	return marshalBinary(envelope, true, true)
}

// MarshalMetadata encodes a Version1 envelope without its payload or key. Log
// storage already owns those as the record value and key, so duplicating them
// in metadata wastes CPU, memory, and disk bandwidth.
func MarshalMetadata(envelope *Envelope) ([]byte, error) {
	return marshalBinary(envelope, false, false)
}

func marshalBinary(envelope *Envelope, includePayload, includeKey bool) ([]byte, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}

	capacity := 128
	if includePayload {
		capacity += len(envelope.PayloadBytes())
	}
	encoded := make([]byte, 0, capacity)
	encoded = appendVarint(encoded, 1, uint64(envelope.Version))
	encoded = appendString(encoded, 2, envelope.Topic)
	if includePayload {
		encoded = appendBytes(encoded, 3, envelope.PayloadBytes())
	}
	encoded = appendMessage(encoded, 4, encodeUser(envelope.PublisherMeta, includeKey))
	encoded = appendMessage(encoded, 5, encodeBroker(envelope.BrokerMeta))
	return encoded, nil
}

// UnmarshalBinary decodes a complete strict Version1 envelope. The returned
// envelope owns its payload and must be released by the caller.
func UnmarshalBinary(encoded []byte) (*Envelope, error) {
	return unmarshalBinary(encoded, nil, nil, false)
}

// UnmarshalMetadata decodes strict Version1 log metadata and copies value and
// key into broker-owned memory. The returned envelope must be released.
func UnmarshalMetadata(encoded, value, key []byte) (*Envelope, error) {
	return unmarshalBinary(encoded, value, key, true)
}

func unmarshalBinary(encoded, externalPayload, externalKey []byte, metadataOnly bool) (*Envelope, error) {
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
			decodedPayload = raw
		case 4:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
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
	if envelope.Version != Version1 {
		version := envelope.Version
		Release(envelope)
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}
	if metadataOnly {
		decodedPayload = externalPayload
		envelope.PublisherMeta.Key = bytes.Clone(externalKey)
	}
	envelope.Payload = payload.FromBytes(decodedPayload)
	return envelope, nil
}

func encodeUser(user PublisherMetadata, includeKey bool) []byte {
	var encoded []byte
	if includeKey {
		encoded = appendBytes(encoded, 1, user.Key)
	}
	for key, value := range user.Headers {
		var entry []byte
		entry = appendString(entry, 1, key)
		entry = appendBytes(entry, 2, value)
		encoded = appendMessage(encoded, 2, entry)
	}
	for key, value := range user.Properties {
		var entry []byte
		entry = appendString(entry, 1, key)
		entry = appendString(entry, 2, value)
		encoded = appendMessage(encoded, 3, entry)
	}
	encoded = appendString(encoded, 4, user.ContentType)
	encoded = appendString(encoded, 5, user.ContentEncoding)
	encoded = appendString(encoded, 6, user.ResponseTopic)
	encoded = appendBytes(encoded, 7, user.CorrelationData)
	if user.PayloadFormat != nil {
		encoded = appendVarint(encoded, 8, uint64(*user.PayloadFormat))
	}
	if user.MessageExpiry != nil {
		encoded = appendVarint(encoded, 9, uint64(*user.MessageExpiry))
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
			user.Key = bytes.Clone(raw)
		case 2:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			key, value, err := decodeBytesMapEntry(raw)
			if err != nil {
				return err
			}
			if user.Headers == nil {
				user.Headers = make(map[string][]byte)
			}
			user.Headers[key] = value
		case 3:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			key, value, err := decodeStringMapEntry(raw)
			if err != nil {
				return err
			}
			if user.Properties == nil {
				user.Properties = make(map[string]string)
			}
			user.Properties[key] = value
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
			user.CorrelationData = bytes.Clone(raw)
		case 8:
			if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
				return err
			}
			if scalar > 255 {
				return fmt.Errorf("message envelope payload format overflows byte: %d", scalar)
			}
			value := byte(scalar)
			user.PayloadFormat = &value
		case 9:
			if err := requireWireType(number, wireType, protowire.VarintType); err != nil {
				return err
			}
			if scalar > uint64(^uint32(0)) {
				return fmt.Errorf("message envelope expiry overflows uint32: %d", scalar)
			}
			value := uint32(scalar)
			user.MessageExpiry = &value
		case 10:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			user.MessageID = string(raw)
		}
		return nil
	})
}

func encodeBroker(broker BrokerMetadata) []byte {
	var encoded []byte
	encoded = appendMessage(encoded, 1, encodeSource(broker.Source))
	encoded = appendMessage(encoded, 2, encodeDelivery(broker.Delivery))
	encoded = appendMessage(encoded, 3, encodeQueue(broker.Queue))
	encoded = appendMessage(encoded, 4, encodeTransfer(broker.Transfer))
	encoded = appendMessage(encoded, 5, encodeTrace(broker.Trace))
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

func encodeSource(source SourceMetadata) []byte {
	var encoded []byte
	encoded = appendString(encoded, 1, source.ClientID)
	encoded = appendString(encoded, 2, source.ExternalID)
	encoded = appendString(encoded, 3, string(source.Protocol))
	encoded = appendString(encoded, 4, source.Topic)
	return encoded
}

func decodeSource(encoded []byte, source *SourceMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, _ uint64) error {
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
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			source.Protocol = Protocol(raw)
		case 4:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
			source.Topic = string(raw)
		}
		return nil
	})
}

func encodeDelivery(delivery DeliveryMetadata) []byte {
	var encoded []byte
	encoded = appendTime(encoded, 1, delivery.PublishedAt)
	encoded = appendTime(encoded, 2, delivery.ExpiresAt)
	for _, id := range delivery.SubscriptionIDs {
		encoded = appendVarint(encoded, 3, uint64(id))
	}
	encoded = appendNonZeroVarint(encoded, 4, uint64(delivery.PacketID))
	encoded = appendNonZeroVarint(encoded, 5, uint64(delivery.QoS))
	encoded = appendNonZeroVarint(encoded, 6, uint64(delivery.InflightDirection))
	encoded = appendNonZeroVarint(encoded, 7, uint64(delivery.InflightState))
	encoded = appendBool(encoded, 8, delivery.Retain)
	encoded = appendBool(encoded, 9, delivery.Duplicate)
	return encoded
}

func decodeDelivery(encoded []byte, delivery *DeliveryMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, _ []byte, scalar uint64) error {
		if number < 1 || number > 9 {
			return nil
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
			delivery.SubscriptionIDs = append(delivery.SubscriptionIDs, uint32(scalar))
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
func encodeQueue(queue QueueMetadata) []byte {
	var encoded []byte
	encoded = appendString(encoded, 2, queue.Name)
	encoded = appendString(encoded, 3, queue.GroupID)
	encoded = appendNonZeroVarint(encoded, 4, queue.Offset)
	encoded = appendString(encoded, 5, string(queue.State))
	encoded = appendTime(encoded, 6, queue.CreatedAt)
	encoded = appendTime(encoded, 7, queue.DeliveredAt)
	encoded = appendTime(encoded, 8, queue.NextRetryAt)
	encoded = appendNonZeroVarint(encoded, 9, uint64(queue.RetryCount))
	encoded = appendTime(encoded, 10, queue.ExpiresAt)
	if queue.Stream != nil {
		encoded = appendMessage(encoded, 11, encodeStream(*queue.Stream))
	}
	return encoded
}

func decodeQueue(encoded []byte, queue *QueueMetadata) error {
	return walkFields(encoded, func(number protowire.Number, wireType protowire.Type, raw []byte, scalar uint64) error {
		switch number {
		case 2, 3, 5, 11:
			if err := requireWireType(number, wireType, protowire.BytesType); err != nil {
				return err
			}
		case 4, 6, 7, 8, 9, 10:
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
			queue.State = QueueState(raw)
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
			stream := &StreamMetadata{}
			if err := decodeStream(raw, stream); err != nil {
				return err
			}
			queue.Stream = stream
		}
		return nil
	})
}

func encodeStream(stream StreamMetadata) []byte {
	var encoded []byte
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

func encodeTransfer(transfer TransferMetadata) []byte {
	var encoded []byte
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

func encodeTrace(trace TraceMetadata) []byte {
	var encoded []byte
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

func appendMessage(encoded []byte, number protowire.Number, value []byte) []byte {
	return appendBytes(encoded, number, value)
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
