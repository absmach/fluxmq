// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3_test

import (
	"bytes"
	"testing"

	"github.com/absmach/mqtt/core/packets"
	. "github.com/absmach/mqtt/core/packets/v3"
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
		ProtocolVersion: 4,
		CleanSession:    true,
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
		Topics: []Topic{
			{Name: "topic/one", QoS: 1},
			{Name: "topic/two", QoS: 2},
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
	if len(sub.Topics) != len(original.Topics) {
		t.Fatalf("Topics length: got %d, want %d", len(sub.Topics), len(original.Topics))
	}
	for i, topic := range sub.Topics {
		if topic.Name != original.Topics[i].Name {
			t.Errorf("Topics[%d].Name: got %q, want %q", i, topic.Name, original.Topics[i].Name)
		}
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

	for b.Loop() {
		pkt := &Publish{FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1}}
		_ = pkt.UnpackBytes(encoded[headerLen:])
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
