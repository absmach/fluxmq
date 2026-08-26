// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestBinaryEnvelopeRoundTrip(t *testing.T) {
	payloadFormat := byte(0)
	messageExpiry := uint32(0)
	now := time.Date(2026, time.August, 25, 8, 15, 30, 123456789, time.UTC)
	original := New("devices/1", []byte{0x00, 0x01, 0xfe, 0xff})
	defer Release(original)
	original.User = UserMetadata{
		Key:             []byte{0x00, 0xff},
		Headers:         map[string][]byte{"binary": {0x01, 0xfe}},
		Properties:      map[string]string{"tenant": "acme"},
		ContentType:     "application/octet-stream",
		ContentEncoding: "gzip",
		ResponseTopic:   "responses/1",
		CorrelationData: []byte{0x80, 0x00},
		PayloadFormat:   &payloadFormat,
		MessageExpiry:   &messageExpiry,
		MessageID:       "publisher-1",
	}
	original.Broker = BrokerMetadata{
		Source: SourceMetadata{ClientID: testClientID, ExternalID: testSubject, Protocol: ProtocolMQTT, Topic: "source/topic"},
		Delivery: DeliveryMetadata{
			PublishedAt:       now,
			ExpiresAt:         now.Add(time.Minute),
			SubscriptionIDs:   []uint32{1, 7},
			PacketID:          42,
			QoS:               2,
			InflightDirection: 1,
			InflightState:     2,
			Retain:            true,
			Duplicate:         true,
		},
		Queue: QueueMetadata{
			Name:        testQueueName,
			GroupID:     "group",
			Offset:      9,
			State:       QueueStateRetry,
			CreatedAt:   now,
			DeliveredAt: now.Add(time.Second),
			NextRetryAt: now.Add(2 * time.Second),
			RetryCount:  3,
			ExpiresAt:   now.Add(time.Hour),
			Stream: &StreamMetadata{
				Offset:             9,
				Timestamp:          now.UnixMilli(),
				CommittedOffset:    8,
				HasCommittedOffset: true,
				WorkAcknowledged:   true,
				WorkGroup:          testGroupID,
			},
		},
		Transfer: TransferMetadata{
			ID:            "transfer",
			FailureReason: "rejected",
			FirstAttempt:  now,
			LastAttempt:   now.Add(time.Second),
			CompletedAt:   now.Add(2 * time.Second),
			SourceQueue:   "source",
			SourceGroup:   "group",
			SourceOffset:  7,
			DeliveryCount: 4,
		},
		Trace: TraceMetadata{TraceParent: "parent", TraceState: "state", TraceID: "trace"},
	}

	encoded, err := MarshalBinary(original)
	if err != nil {
		t.Fatalf("marshal binary: %v", err)
	}
	decoded, err := UnmarshalBinary(encoded)
	if err != nil {
		t.Fatalf("unmarshal binary: %v", err)
	}
	defer Release(decoded)

	if decoded.Version != Version1 || decoded.Topic != original.Topic {
		t.Fatalf("decoded identity = version %d topic %q", decoded.Version, decoded.Topic)
	}
	if !reflect.DeepEqual(decoded.PayloadBytes(), original.PayloadBytes()) {
		t.Fatalf("decoded payload = %v, want %v", decoded.PayloadBytes(), original.PayloadBytes())
	}
	if !reflect.DeepEqual(decoded.User, original.User) {
		t.Fatalf("decoded user metadata = %#v, want %#v", decoded.User, original.User)
	}
	if !reflect.DeepEqual(decoded.Broker, original.Broker) {
		t.Fatalf("decoded broker metadata = %#v, want %#v", decoded.Broker, original.Broker)
	}
}

func TestBinaryEnvelopeMetadataUsesExternalPayloadAndKey(t *testing.T) {
	original := New("devices/1", []byte("embedded payload"))
	defer Release(original)
	original.User.Key = []byte("embedded key")

	encoded, err := MarshalMetadata(original)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	externalPayload := []byte("record payload")
	externalKey := []byte("record key")
	decoded, err := UnmarshalMetadata(encoded, externalPayload, externalKey)
	if err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	defer Release(decoded)

	externalPayload[0] = 'X'
	externalKey[0] = 'X'
	if string(decoded.PayloadBytes()) != "record payload" || string(decoded.User.Key) != "record key" {
		t.Fatalf("external record data was aliased: payload %q key %q", decoded.PayloadBytes(), decoded.User.Key)
	}
}

func TestBinaryEnvelopeRequiresVersion1(t *testing.T) {
	for _, encoded := range [][]byte{
		nil,
		{0x08, 0x00},
		{0x08, 0x02},
	} {
		decoded, err := UnmarshalBinary(encoded)
		if decoded != nil {
			Release(decoded)
		}
		if !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("unmarshal %v error = %v, want unsupported version", encoded, err)
		}
	}
}

func TestBinaryEnvelopeIgnoresUnknownFields(t *testing.T) {
	original := New("devices/1", []byte("payload"))
	defer Release(original)
	encoded, err := MarshalBinary(original)
	if err != nil {
		t.Fatalf("marshal binary: %v", err)
	}
	encoded = protowire.AppendTag(encoded, 100, protowire.VarintType)
	encoded = protowire.AppendVarint(encoded, 42)

	decoded, err := UnmarshalBinary(encoded)
	if err != nil {
		t.Fatalf("unmarshal with unknown field: %v", err)
	}
	defer Release(decoded)
	if decoded.Topic != original.Topic || string(decoded.PayloadBytes()) != "payload" {
		t.Fatalf("unknown field changed decoded envelope: %#v", decoded)
	}
}

func BenchmarkEnvelopeBinaryCodec(b *testing.B) {
	envelope := NewDelivery("devices/1", make([]byte, 256), 1, false)
	defer Release(envelope)
	b.ReportAllocs()
	for b.Loop() {
		encoded, err := MarshalBinary(envelope)
		if err != nil {
			b.Fatal(err)
		}
		decoded, err := UnmarshalBinary(encoded)
		if err != nil {
			b.Fatal(err)
		}
		Release(decoded)
	}
}
