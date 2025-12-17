// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3_test

import (
	"bytes"
	"reflect"
	"testing"

	. "github.com/absmach/mqtt/core/packets/v3"
)

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
				ProtocolVersion: 4,
				CleanSession:    true,
				KeepAlive:       60,
				ClientID:        "test-client",
			},
		},
		{
			name: "connect with credentials",
			pkt: &Connect{
				FixedHeader:     FixedHeader{PacketType: ConnectType},
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				CleanSession:    true,
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
				ProtocolVersion: 4,
				CleanSession:    true,
				WillFlag:        true,
				WillQoS:         1,
				WillRetain:      true,
				KeepAlive:       60,
				ClientID:        "will-client",
				WillTopic:       "will/topic",
				WillMessage:     []byte("client disconnected"),
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
			decoded, err := ReadPacket(reader)
			if err != nil {
				t.Fatalf("ReadPacket failed: %v", err)
			}

			connect, ok := decoded.(*Connect)
			if !ok {
				t.Fatalf("Expected *Connect, got %T", decoded)
			}

			if connect.ProtocolName != tt.pkt.ProtocolName {
				t.Errorf("ProtocolName: got %q, want %q", connect.ProtocolName, tt.pkt.ProtocolName)
			}
			if connect.ProtocolVersion != tt.pkt.ProtocolVersion {
				t.Errorf("ProtocolVersion: got %d, want %d", connect.ProtocolVersion, tt.pkt.ProtocolVersion)
			}
			if connect.ClientID != tt.pkt.ClientID {
				t.Errorf("ClientID: got %q, want %q", connect.ClientID, tt.pkt.ClientID)
			}
			if connect.CleanSession != tt.pkt.CleanSession {
				t.Errorf("CleanSession: got %v, want %v", connect.CleanSession, tt.pkt.CleanSession)
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
			if !bytes.Equal(connect.WillMessage, tt.pkt.WillMessage) {
				t.Errorf("WillMessage: got %v, want %v", connect.WillMessage, tt.pkt.WillMessage)
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
				ReturnCode:     0,
			},
		},
		{
			name: "connack with session present",
			pkt: &ConnAck{
				FixedHeader:    FixedHeader{PacketType: ConnAckType},
				SessionPresent: true,
				ReturnCode:     0,
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
			decoded, err := ReadPacket(reader)
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
			if connack.ReturnCode != tt.pkt.ReturnCode {
				t.Errorf("ReturnCode: got %d, want %d", connack.ReturnCode, tt.pkt.ReturnCode)
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := tt.pkt.Encode()
			if len(encoded) == 0 {
				t.Fatal("Encode returned empty bytes")
			}

			reader := bytes.NewReader(encoded)
			decoded, err := ReadPacket(reader)
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
				Topics: []Topic{
					{Name: "test/topic", QoS: 1},
				},
			},
		},
		{
			name: "multiple subscriptions",
			pkt: &Subscribe{
				FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1},
				ID:          2,
				Topics: []Topic{
					{Name: "topic/one", QoS: 0},
					{Name: "topic/two", QoS: 1},
					{Name: "topic/three", QoS: 2},
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
			decoded, err := ReadPacket(reader)
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
			if len(sub.Topics) != len(tt.pkt.Topics) {
				t.Fatalf("Topics length: got %d, want %d", len(sub.Topics), len(tt.pkt.Topics))
			}
			for i, topic := range sub.Topics {
				if topic.Name != tt.pkt.Topics[i].Name {
					t.Errorf("Topics[%d].Name: got %q, want %q", i, topic.Name, tt.pkt.Topics[i].Name)
				}
				if topic.QoS != tt.pkt.Topics[i].QoS {
					t.Errorf("Topics[%d].QoS: got %d, want %d", i, topic.QoS, tt.pkt.Topics[i].QoS)
				}
			}
		})
	}
}

func TestSubAckEncodeDecode(t *testing.T) {
	pkt := &SubAck{
		FixedHeader: FixedHeader{PacketType: SubAckType},
		ID:          1,
		ReturnCodes: []byte{0, 1, 2},
	}

	encoded := pkt.Encode()
	if len(encoded) == 0 {
		t.Fatal("Encode returned empty bytes")
	}

	reader := bytes.NewReader(encoded)
	decoded, err := ReadPacket(reader)
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
	if !bytes.Equal(suback.ReturnCodes, pkt.ReturnCodes) {
		t.Errorf("ReturnCodes: got %v, want %v", suback.ReturnCodes, pkt.ReturnCodes)
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
	decoded, err := ReadPacket(reader)
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
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, err := ReadPacket(reader)
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
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, err := ReadPacket(reader)
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
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, err := ReadPacket(reader)
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
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, err := ReadPacket(reader)
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
	decoded, err := ReadPacket(reader)
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
	decoded, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	_, ok := decoded.(*PingResp)
	if !ok {
		t.Fatalf("Expected *PingResp, got %T", decoded)
	}
}

func TestDisconnectEncodeDecode(t *testing.T) {
	pkt := &Disconnect{
		FixedHeader: FixedHeader{PacketType: DisconnectType},
	}

	encoded := pkt.Encode()
	reader := bytes.NewReader(encoded)
	decoded, err := ReadPacket(reader)
	if err != nil {
		t.Fatalf("ReadPacket failed: %v", err)
	}

	_, ok := decoded.(*Disconnect)
	if !ok {
		t.Fatalf("Expected *Disconnect, got %T", decoded)
	}
}
