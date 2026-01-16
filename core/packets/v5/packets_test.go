// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5_test

import (
	"bytes"
	"reflect"
	"testing"

	. "github.com/absmach/fluxmq/core/packets/v5"
)

// Helper to create pointer to value.
func ptr[T any](v T) *T {
	return &v
}

func TestConnectEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		pkt  *Connect
	}{
		{
			name: "basic connect",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				KeepAlive:       60,
				ClientID:        "test-client",
			},
		},
		{
			name: "connect with credentials",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				UsernameFlag:    true,
				PasswordFlag:    true,
				KeepAlive:       30,
				ClientID:        "client-with-creds",
				Username:        "testuser",
				Password:        []byte("testpass"),
			},
		},
		{
			name: "connect with will",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				WillFlag:        true,
				WillQoS:         1,
				WillRetain:      true,
				KeepAlive:       60,
				ClientID:        "will-client",
				WillTopic:       "will/topic",
				WillPayload:     []byte("client disconnected"),
			},
		},
		{
			name: "connect with properties",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: V5,
				CleanStart:      true,
				KeepAlive:       60,
				ClientID:        "props-client",
				Properties: &ConnectProperties{
					SessionExpiryInterval: ptr(uint32(3600)),
					ReceiveMaximum:        ptr(uint16(100)),
					MaximumPacketSize:     ptr(uint32(65535)),
					TopicAliasMaximum:     ptr(uint16(10)),
					User:                  []User{{Key: "key1", Value: "value1"}, {Key: "key2", Value: "value2"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.pkt.Encode()
			if len(encoded) == 0 {
				t.Fatal("Encode returned empty bytes")
			}

			// Decode using ReadPacket
			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if err != nil {
				t.Fatalf("ReadPacket failed: %v", err)
			}

			connect, ok := decoded.(*Connect)
			if !ok {
				t.Fatalf("Expected *Connect, got %T", decoded)
			}

			// Verify key fields
			if connect.ProtocolName != tt.pkt.ProtocolName {
				t.Errorf("ProtocolName: got %q, want %q", connect.ProtocolName, tt.pkt.ProtocolName)
			}
			if connect.ProtocolVersion != tt.pkt.ProtocolVersion {
				t.Errorf("ProtocolVersion: got %d, want %d", connect.ProtocolVersion, tt.pkt.ProtocolVersion)
			}
			if connect.ClientID != tt.pkt.ClientID {
				t.Errorf("ClientID: got %q, want %q", connect.ClientID, tt.pkt.ClientID)
			}
			if connect.CleanStart != tt.pkt.CleanStart {
				t.Errorf("CleanStart: got %v, want %v", connect.CleanStart, tt.pkt.CleanStart)
			}
			if connect.KeepAlive != tt.pkt.KeepAlive {
				t.Errorf("KeepAlive: got %d, want %d", connect.KeepAlive, tt.pkt.KeepAlive)
			}
			if connect.Username != tt.pkt.Username {
				t.Errorf("Username: got %q, want %q", connect.Username, tt.pkt.Username)
			}
			if !bytes.Equal(connect.Password, tt.pkt.Password) {
				t.Errorf("Password: got %v, want %v", connect.Password, tt.pkt.Password)
			}
			if connect.WillTopic != tt.pkt.WillTopic {
				t.Errorf("WillTopic: got %q, want %q", connect.WillTopic, tt.pkt.WillTopic)
			}
		})
	}
}

func TestConnAckEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		pkt  *ConnAck
	}{
		{
			name: "basic connack",
			pkt: &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: false,
				ReasonCode:     0,
			},
		},
		{
			name: "connack with session present",
			pkt: &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: true,
				ReasonCode:     0,
			},
		},
		{
			name: "connack with properties",
			pkt: &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: false,
				ReasonCode:     0,
				Properties: &ConnAckProperties{
					SessionExpiryInterval: ptr(uint32(3600)),
					ReceiveMax:            ptr(uint16(100)),
					MaxQoS:                ptr(byte(2)),
					MaximumPacketSize:     ptr(uint32(65535)),
					TopicAliasMax:         ptr(uint16(10)),
					AssignedClientID:      "server-assigned-id",
					User:                  []User{{Key: "server", Value: "info"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.pkt.Encode()
			if len(encoded) == 0 {
				t.Fatal("Encode returned empty bytes")
			}

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if err != nil {
				t.Fatalf("ReadPacket failed: %v", err)
			}

			connack, ok := decoded.(*ConnAck)
			if !ok {
				t.Fatalf("Expected *ConnAck, got %T", decoded)
			}

			if connack.SessionPresent != tt.pkt.SessionPresent {
				t.Errorf("SessionPresent: got %v, want %v", connack.SessionPresent, tt.pkt.SessionPresent)
			}
			if connack.ReasonCode != tt.pkt.ReasonCode {
				t.Errorf("ReasonCode: got %d, want %d", connack.ReasonCode, tt.pkt.ReasonCode)
			}
		})
	}
}

func TestPublishEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		pkt  *Publish
	}{
		{
			name: "qos0 publish",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 0},
				TopicName:   "test/topic",
				Payload:     []byte("hello world"),
			},
		},
		{
			name: "qos1 publish",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
				TopicName:   "test/topic",
				ID:          12345,
				Payload:     []byte("qos1 message"),
			},
		},
		{
			name: "qos2 publish with retain",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 2, Retain: true},
				TopicName:   "retained/topic",
				ID:          54321,
				Payload:     []byte("retained message"),
			},
		},
		{
			name: "publish with properties",
			pkt: &Publish{
				FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
				TopicName:   "props/topic",
				ID:          100,
				Payload:     []byte("message with props"),
				Properties: &PublishProperties{
					PayloadFormat:   ptr(byte(1)),
					MessageExpiry:   ptr(uint32(3600)),
					TopicAlias:      ptr(uint16(5)),
					ResponseTopic:   "response/topic",
					CorrelationData: []byte("correlation-123"),
					ContentType:     "application/json",
					User:            []User{{Key: "custom", Value: "header"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.pkt.Encode()
			if len(encoded) == 0 {
				t.Fatal("Encode returned empty bytes")
			}

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if err != nil {
				t.Fatalf("ReadPacket failed: %v", err)
			}

			pub, ok := decoded.(*Publish)
			if !ok {
				t.Fatalf("Expected *Publish, got %T", decoded)
			}

			if pub.TopicName != tt.pkt.TopicName {
				t.Errorf("TopicName: got %q, want %q", pub.TopicName, tt.pkt.TopicName)
			}
			if pub.QoS != tt.pkt.QoS {
				t.Errorf("QoS: got %d, want %d", pub.QoS, tt.pkt.QoS)
			}
			if pub.QoS > 0 && pub.ID != tt.pkt.ID {
				t.Errorf("ID: got %d, want %d", pub.ID, tt.pkt.ID)
			}
			if !bytes.Equal(pub.Payload, tt.pkt.Payload) {
				t.Errorf("Payload: got %v, want %v", pub.Payload, tt.pkt.Payload)
			}
			if pub.Retain != tt.pkt.Retain {
				t.Errorf("Retain: got %v, want %v", pub.Retain, tt.pkt.Retain)
			}
		})
	}
}

func TestSubscribeEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		pkt  *Subscribe
	}{
		{
			name: "single subscription",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          1,
				Opts: []SubOption{
					{Topic: "test/topic", MaxQoS: 1},
				},
			},
		},
		{
			name: "multiple subscriptions",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          2,
				Opts: []SubOption{
					{Topic: "topic/one", MaxQoS: 0},
					{Topic: "topic/two", MaxQoS: 1},
					{Topic: "topic/three", MaxQoS: 2},
				},
			},
		},
		{
			name: "subscription with options",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          3,
				Opts: []SubOption{
					{
						Topic:             "options/topic",
						MaxQoS:            2,
						NoLocal:           ptr(true),
						RetainAsPublished: ptr(true),
						RetainHandling:    ptr(byte(1)),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.pkt.Encode()
			if len(encoded) == 0 {
				t.Fatal("Encode returned empty bytes")
			}

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if err != nil {
				t.Fatalf("ReadPacket failed: %v", err)
			}

			sub, ok := decoded.(*Subscribe)
			if !ok {
				t.Fatalf("Expected *Subscribe, got %T", decoded)
			}

			if sub.ID != tt.pkt.ID {
				t.Errorf("ID: got %d, want %d", sub.ID, tt.pkt.ID)
			}
			if len(sub.Opts) != len(tt.pkt.Opts) {
				t.Fatalf("Opts length: got %d, want %d", len(sub.Opts), len(tt.pkt.Opts))
			}
			for i, opt := range sub.Opts {
				if opt.Topic != tt.pkt.Opts[i].Topic {
					t.Errorf("Opts[%d].Topic: got %q, want %q", i, opt.Topic, tt.pkt.Opts[i].Topic)
				}
				if opt.MaxQoS != tt.pkt.Opts[i].MaxQoS {
					t.Errorf("Opts[%d].MaxQoS: got %d, want %d", i, opt.MaxQoS, tt.pkt.Opts[i].MaxQoS)
				}
			}
		})
	}
}

func TestSubAckEncodeDecode(t *testing.T) {
	reasonCodes := []byte{0, 1, 2}
	pkt := &SubAck{
		FixedHeader: FixedHeader{PacketType: SubAckType},
		ID:          1,
		ReasonCodes: &reasonCodes,
	}

	encoded := pkt.Encode()
	if len(encoded) == 0 {
		t.Fatal("Encode returned empty bytes")
	}

	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	suback, ok := decoded.(*SubAck)
	if !ok {
		t.Fatalf("Expected *SubAck, got %T", decoded)
	}

	if suback.ID != pkt.ID {
		t.Errorf("ID: got %d, want %d", suback.ID, pkt.ID)
	}
	if suback.ReasonCodes == nil {
		t.Fatal("ReasonCodes is nil")
	}
	if !bytes.Equal(*suback.ReasonCodes, *pkt.ReasonCodes) {
		t.Errorf("ReasonCodes: got %v, want %v", *suback.ReasonCodes, *pkt.ReasonCodes)
	}
}

func TestUnsubscribeEncodeDecode(t *testing.T) {
	pkt := &Unsubscribe{
		FixedHeader: FixedHeader{PacketType: UnsubscribeType, QoS: 1},
		ID:          1,
		Topics:      []string{"topic/one", "topic/two"},
	}

	encoded := pkt.Encode()
	if len(encoded) == 0 {
		t.Fatal("Encode returned empty bytes")
	}

	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	unsub, ok := decoded.(*Unsubscribe)
	if !ok {
		t.Fatalf("Expected *Unsubscribe, got %T", decoded)
	}

	if unsub.ID != pkt.ID {
		t.Errorf("ID: got %d, want %d", unsub.ID, pkt.ID)
	}
	if !reflect.DeepEqual(unsub.Topics, pkt.Topics) {
		t.Errorf("Topics: got %v, want %v", unsub.Topics, pkt.Topics)
	}
}

func TestPubAckEncodeDecode(t *testing.T) {
	pkt := &PubAck{
		FixedHeader: FixedHeader{PacketType: PubAckType},
		ID:          12345,
		ReasonCode:  ptr(byte(0)),
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	puback, ok := decoded.(*PubAck)
	if !ok {
		t.Fatalf("Expected *PubAck, got %T", decoded)
	}

	if puback.ID != pkt.ID {
		t.Errorf("ID: got %d, want %d", puback.ID, pkt.ID)
	}
}

func TestPubRecEncodeDecode(t *testing.T) {
	pkt := &PubRec{
		FixedHeader: FixedHeader{PacketType: PubRecType},
		ID:          12345,
		ReasonCode:  ptr(byte(0)),
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	pubrec, ok := decoded.(*PubRec)
	if !ok {
		t.Fatalf("Expected *PubRec, got %T", decoded)
	}

	if pubrec.ID != pkt.ID {
		t.Errorf("ID: got %d, want %d", pubrec.ID, pkt.ID)
	}
}

func TestPubRelEncodeDecode(t *testing.T) {
	pkt := &PubRel{
		FixedHeader: FixedHeader{PacketType: PubRelType, QoS: 1},
		ID:          12345,
		ReasonCode:  ptr(byte(0)),
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	pubrel, ok := decoded.(*PubRel)
	if !ok {
		t.Fatalf("Expected *PubRel, got %T", decoded)
	}

	if pubrel.ID != pkt.ID {
		t.Errorf("ID: got %d, want %d", pubrel.ID, pkt.ID)
	}
}

func TestPubCompEncodeDecode(t *testing.T) {
	pkt := &PubComp{
		FixedHeader: FixedHeader{PacketType: PubCompType},
		ID:          12345,
		ReasonCode:  ptr(byte(0)),
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	pubcomp, ok := decoded.(*PubComp)
	if !ok {
		t.Fatalf("Expected *PubComp, got %T", decoded)
	}

	if pubcomp.ID != pkt.ID {
		t.Errorf("ID: got %d, want %d", pubcomp.ID, pkt.ID)
	}
}

func TestPingReqEncodeDecode(t *testing.T) {
	pkt := &PingReq{
		FixedHeader: FixedHeader{PacketType: PingReqType},
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	_, ok := decoded.(*PingReq)
	if !ok {
		t.Fatalf("Expected *PingReq, got %T", decoded)
	}
}

func TestPingRespEncodeDecode(t *testing.T) {
	pkt := &PingResp{
		FixedHeader: FixedHeader{PacketType: PingRespType},
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	_, ok := decoded.(*PingResp)
	if !ok {
		t.Fatalf("Expected *PingResp, got %T", decoded)
	}
}

func TestDisconnectEncodeDecode(t *testing.T) {
	tests := []struct {
		name string
		pkt  *Disconnect
	}{
		{
			name: "basic disconnect",
			pkt: &Disconnect{
				FixedHeader: FixedHeader{PacketType: DisconnectType},
				ReasonCode:  0,
			},
		},
		{
			name: "disconnect with properties",
			pkt: &Disconnect{
				FixedHeader: FixedHeader{PacketType: DisconnectType},
				ReasonCode:  0x04, // Disconnect with will message
				Properties: &DisconnectProperties{
					SessionExpiryInterval: ptr(uint32(0)),
					ReasonString:          "normal disconnect",
					ServerReference:       "other-server:1883",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.pkt.Encode()

			reader := bytes.NewReader(encoded)
			decoded, _, _, err := ReadPacket(reader)
			if err != nil {
				t.Fatalf("ReadPacket failed: %v", err)
			}

			disc, ok := decoded.(*Disconnect)
			if !ok {
				t.Fatalf("Expected *Disconnect, got %T", decoded)
			}

			if disc.ReasonCode != tt.pkt.ReasonCode {
				t.Errorf("ReasonCode: got %d, want %d", disc.ReasonCode, tt.pkt.ReasonCode)
			}
		})
	}
}

func TestAuthEncodeDecode(t *testing.T) {
	pkt := &Auth{
		FixedHeader: FixedHeader{PacketType: AuthType},
		ReasonCode:  0x00,
		Properties: &AuthProperties{
			AuthMethod:   "SCRAM-SHA-256",
			AuthData:     []byte("auth-challenge"),
			ReasonString: "continue auth",
		},
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	auth, ok := decoded.(*Auth)
	if !ok {
		t.Fatalf("Expected *Auth, got %T", decoded)
	}

	if auth.ReasonCode != pkt.ReasonCode {
		t.Errorf("ReasonCode: got %d, want %d", auth.ReasonCode, pkt.ReasonCode)
	}
	if auth.Properties == nil {
		t.Fatal("Properties is nil")
	}
	if auth.Properties.AuthMethod != pkt.Properties.AuthMethod {
		t.Errorf("AuthMethod: got %q, want %q", auth.Properties.AuthMethod, pkt.Properties.AuthMethod)
	}
}

func TestFixedHeaderEncodeDecode(t *testing.T) {
	tests := []struct {
		name   string
		header FixedHeader
	}{
		{
			name:   "simple header",
			header: FixedHeader{PacketType: PublishType, QoS: 0, Retain: false, Dup: false, RemainingLength: 10},
		},
		{
			name:   "header with flags",
			header: FixedHeader{PacketType: PublishType, QoS: 2, Retain: true, Dup: true, RemainingLength: 100},
		},
		{
			name:   "large remaining length",
			header: FixedHeader{PacketType: PublishType, QoS: 1, RemainingLength: 16384},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.header.Encode()
			if len(encoded) < 2 {
				t.Fatal("Encoded header too short")
			}

			var decoded FixedHeader
			reader := bytes.NewReader(encoded[1:]) // Skip first byte for Decode
			err := decoded.Decode(encoded[0], reader)
			if err != nil {
				t.Fatalf("Decode failed: %v", err)
			}

			if decoded.PacketType != tt.header.PacketType {
				t.Errorf("PacketType: got %d, want %d", decoded.PacketType, tt.header.PacketType)
			}
			if decoded.QoS != tt.header.QoS {
				t.Errorf("QoS: got %d, want %d", decoded.QoS, tt.header.QoS)
			}
			if decoded.Retain != tt.header.Retain {
				t.Errorf("Retain: got %v, want %v", decoded.Retain, tt.header.Retain)
			}
			if decoded.Dup != tt.header.Dup {
				t.Errorf("Dup: got %v, want %v", decoded.Dup, tt.header.Dup)
			}
			if decoded.RemainingLength != tt.header.RemainingLength {
				t.Errorf("RemainingLength: got %d, want %d", decoded.RemainingLength, tt.header.RemainingLength)
			}
		})
	}
}

func TestUserPropertiesEncodeDecode(t *testing.T) {
	// Test that multiple user properties are encoded correctly
	pkt := &Publish{
		FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1},
		TopicName:   "test",
		ID:          1,
		Payload:     []byte("test"),
		Properties: &PublishProperties{
			User: []User{
				{Key: "key1", Value: "value1"},
				{Key: "key2", Value: "value2"},
				{Key: "key3", Value: "value3"},
			},
		},
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, _, _, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	pub, ok := decoded.(*Publish)
	if !ok {
		t.Fatalf("Expected *Publish, got %T", decoded)
	}

	if pub.Properties == nil {
		t.Fatal("Properties is nil")
	}
	if len(pub.Properties.User) != len(pkt.Properties.User) {
		t.Fatalf("User properties count: got %d, want %d", len(pub.Properties.User), len(pkt.Properties.User))
	}
	for i, u := range pub.Properties.User {
		if u.Key != pkt.Properties.User[i].Key {
			t.Errorf("User[%d].Key: got %q, want %q", i, u.Key, pkt.Properties.User[i].Key)
		}
		if u.Value != pkt.Properties.User[i].Value {
			t.Errorf("User[%d].Value: got %q, want %q", i, u.Value, pkt.Properties.User[i].Value)
		}
	}
}
