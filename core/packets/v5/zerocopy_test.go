// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5_test

import (
	"bytes"
	"testing"

	"github.com/absmach/fluxmq/core/packets"
	. "github.com/absmach/fluxmq/core/packets/v5"
)

func TestPublishUnpackBytes(t *testing.T) {
	// Create a Publish packet
	original := &Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		TopicName:   "test/topic",
		ID:          12345,
		Payload:     []byte("hello world"),
	}

	// Encode it
	encoded := original.Encode()

	// Parse the header to get the fixed header
	var fh packets.FixedHeader
	fh.PacketType = encoded[0] >> 4
	fh.QoS = (encoded[0] >> 1) & 0x03

	// Find where the variable header starts (after fixed header)
	headerLen := 2 // 1 byte type + at least 1 byte remaining length
	if encoded[1]&0x80 != 0 {
		headerLen = 3 // 2-byte remaining length
	}

	// Create a new packet and unpack using zero-copy
	pkt := &Publish{FixedHeader: fh}
	err := pkt.UnpackBytes(encoded[headerLen:])
	if err != nil {
		t.Fatalf("UnpackBytes failed: %v", err)
	}

	// Verify
	if pkt.TopicName != original.TopicName {
		t.Errorf("TopicName: got %q, want %q", pkt.TopicName, original.TopicName)
	}
	if pkt.ID != original.ID {
		t.Errorf("ID: got %d, want %d", pkt.ID, original.ID)
	}
	if !bytes.Equal(pkt.Payload, original.Payload) {
		t.Errorf("Payload: got %v, want %v", pkt.Payload, original.Payload)
	}
}

func TestPublishUnpackBytesWithProperties(t *testing.T) {
	payloadFormat := byte(1)
	messageExpiry := uint32(3600)

	original := &Publish{
		FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
		TopicName:   "props/topic",
		ID:          100,
		Payload:     []byte("message with props"),
		Properties: &PublishProperties{
			PayloadFormat:   &payloadFormat,
			MessageExpiry:   &messageExpiry,
			ResponseTopic:   "response/topic",
			CorrelationData: []byte("corr-123"),
			ContentType:     "text/plain",
			User:            []User{{Key: "key1", Value: "value1"}},
		},
	}

	encoded := original.Encode()

	// Parse header
	var fh FixedHeader
	fh.PacketType = encoded[0] >> 4
	fh.QoS = (encoded[0] >> 1) & 0x03

	headerLen := 2 // Simplified - assumes remaining length fits in 1 byte
	if encoded[1]&0x80 != 0 {
		headerLen = 3
	}

	pkt := &Publish{FixedHeader: fh}
	err := pkt.UnpackBytes(encoded[headerLen:])
	if err != nil {
		t.Fatalf("UnpackBytes failed: %v", err)
	}

	if pkt.TopicName != original.TopicName {
		t.Errorf("TopicName: got %q, want %q", pkt.TopicName, original.TopicName)
	}
	if pkt.Properties == nil {
		t.Fatal("Properties is nil")
	}
	if pkt.Properties.PayloadFormat == nil || *pkt.Properties.PayloadFormat != payloadFormat {
		t.Errorf("PayloadFormat mismatch")
	}
	if pkt.Properties.MessageExpiry == nil || *pkt.Properties.MessageExpiry != messageExpiry {
		t.Errorf("MessageExpiry mismatch")
	}
	if pkt.Properties.ResponseTopic != original.Properties.ResponseTopic {
		t.Errorf("ResponseTopic: got %q, want %q", pkt.Properties.ResponseTopic, original.Properties.ResponseTopic)
	}
}

func TestReadPacketBytes(t *testing.T) {
	// Create and encode a Publish packet
	original := &Publish{
		FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
		TopicName:   "test/topic",
		Payload:     []byte("hello"),
	}
	encoded := original.Encode()

	// Parse using zero-copy
	pkt, consumed, err := ReadPacketBytes(encoded)
	if err != nil {
		t.Fatalf("ReadPacketBytes failed: %v", err)
	}

	if consumed != len(encoded) {
		t.Errorf("consumed: got %d, want %d", consumed, len(encoded))
	}

	pub, ok := pkt.(*Publish)
	if !ok {
		t.Fatalf("Expected *Publish, got %T", pkt)
	}

	if pub.TopicName != original.TopicName {
		t.Errorf("TopicName: got %q, want %q", pub.TopicName, original.TopicName)
	}
	if !bytes.Equal(pub.Payload, original.Payload) {
		t.Errorf("Payload: got %v, want %v", pub.Payload, original.Payload)
	}
}

func TestReadPacketBytesConnect(t *testing.T) {
	original := &Connect{
		FixedHeader:     FixedHeader{PacketType: ConnectType},
		ProtocolName:    "MQTT",
		ProtocolVersion: V5,
		CleanStart:      true,
		KeepAlive:       60,
		ClientID:        "test-client",
	}
	encoded := original.Encode()

	pkt, consumed, err := ReadPacketBytes(encoded)
	if err != nil {
		t.Fatalf("ReadPacketBytes failed: %v", err)
	}

	if consumed != len(encoded) {
		t.Errorf("consumed: got %d, want %d", consumed, len(encoded))
	}

	conn, ok := pkt.(*Connect)
	if !ok {
		t.Fatalf("Expected *Connect, got %T", pkt)
	}

	if conn.ClientID != original.ClientID {
		t.Errorf("ClientID: got %q, want %q", conn.ClientID, original.ClientID)
	}
	if conn.KeepAlive != original.KeepAlive {
		t.Errorf("KeepAlive: got %d, want %d", conn.KeepAlive, original.KeepAlive)
	}
}

func TestReadPacketBytesSubscribe(t *testing.T) {
	original := &Subscribe{
		FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
		ID:          1,
		Opts: []SubOption{
			{Topic: "topic/one", MaxQoS: 1},
			{Topic: "topic/two", MaxQoS: 2},
		},
	}
	encoded := original.Encode()

	pkt, consumed, err := ReadPacketBytes(encoded)
	if err != nil {
		t.Fatalf("ReadPacketBytes failed: %v", err)
	}

	if consumed != len(encoded) {
		t.Errorf("consumed: got %d, want %d", consumed, len(encoded))
	}

	sub, ok := pkt.(*Subscribe)
	if !ok {
		t.Fatalf("Expected *Subscribe, got %T", pkt)
	}

	if sub.ID != original.ID {
		t.Errorf("ID: got %d, want %d", sub.ID, original.ID)
	}
	if len(sub.Opts) != len(original.Opts) {
		t.Fatalf("Opts length: got %d, want %d", len(sub.Opts), len(original.Opts))
	}
	for i, opt := range sub.Opts {
		if opt.Topic != original.Opts[i].Topic {
			t.Errorf("Opts[%d].Topic: got %q, want %q", i, opt.Topic, original.Opts[i].Topic)
		}
	}
}

func TestReadPacketBytesTooShort(t *testing.T) {
	_, _, err := ReadPacketBytes([]byte{0x30}) // Just one byte
	if err == nil {
		t.Error("Expected error for too short buffer")
	}
}

func BenchmarkPublishUnpackBytes(b *testing.B) {
	original := &Publish{
		FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
		TopicName:   "test/topic/with/multiple/levels",
		ID:          12345,
		Payload:     make([]byte, 1024),
	}
	encoded := original.Encode()

	// Find header length
	headerLen := 2
	if encoded[1]&0x80 != 0 {
		headerLen = 3
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pkt := &Publish{FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1}}
		_ = pkt.UnpackBytes(encoded[headerLen:])
	}
}

func BenchmarkPublishUnpackReader(b *testing.B) {
	original := &Publish{
		FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
		TopicName:   "test/topic/with/multiple/levels",
		ID:          12345,
		Payload:     make([]byte, 1024),
	}
	encoded := original.Encode()

	b.ReportAllocs()

	for b.Loop() {
		reader := bytes.NewReader(encoded)
		_, _, _, _ = ReadPacket(reader)
	}
}

func BenchmarkReadPacketBytes(b *testing.B) {
	original := &Publish{
		FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
		TopicName:   "test/topic",
		ID:          123,
		Payload:     make([]byte, 256),
	}
	encoded := original.Encode()

	b.ReportAllocs()

	for b.Loop() {
		_, _, _ = ReadPacketBytes(encoded)
	}
}

func BenchmarkReadPacketReader(b *testing.B) {
	original := &Publish{
		FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
		TopicName:   "test/topic",
		ID:          123,
		Payload:     make([]byte, 256),
	}
	encoded := original.Encode()

	b.ReportAllocs()

	for b.Loop() {
		reader := bytes.NewReader(encoded)
		_, _, _, _ = ReadPacket(reader)
	}
}
