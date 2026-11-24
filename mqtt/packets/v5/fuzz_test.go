// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"bytes"
	"testing"
)

// fuzzMaxPacket bounds the streaming reader so an advertised length cannot
// make a fuzz iteration allocate without limit.
const fuzzMaxPacket = 1 << 20

func fuzzSeedPackets() []ControlPacket {
	reasonCodes := []byte{0x00, 0x01, 0x80}
	return []ControlPacket{
		&Connect{
			FixedHeader: FixedHeader{PacketType: ConnectType}, ProtocolName: "MQTT", ProtocolVersion: V5,
			CleanStart: true, KeepAlive: 30, ClientID: "client",
			UsernameFlag: true, Username: "user", PasswordFlag: true, Password: []byte("pass"),
		},
		&ConnAck{FixedHeader: FixedHeader{PacketType: ConnAckType}, SessionPresent: true},
		&Publish{
			FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1}, TopicName: "a/b", ID: 9, Payload: []byte("p"),
			Properties: &PublishProperties{
				PayloadFormat: fuzzPtr(byte(1)), MessageExpiry: fuzzPtr(uint32(60)), TopicAlias: fuzzPtr(uint16(2)),
				ResponseTopic: "r/t", CorrelationData: []byte("c"), ContentType: "text/plain",
				User: []User{{Key: "k", Value: "v"}},
			},
		},
		&Publish{FixedHeader: FixedHeader{PacketType: PublishType}, TopicName: "a", Payload: []byte("q")},
		&PubAck{FixedHeader: FixedHeader{PacketType: PubAckType}, ID: 9, ReasonCode: fuzzPtr(byte(0x10))},
		&Subscribe{
			FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1}, ID: 3,
			Opts: []SubOption{
				{Topic: "a/+", MaxQoS: 1},
				{Topic: "#", MaxQoS: 2, NoLocal: fuzzPtr(true), RetainAsPublished: fuzzPtr(true), RetainHandling: fuzzPtr(byte(1))},
			},
		},
		&SubAck{FixedHeader: FixedHeader{PacketType: SubAckType}, ID: 3, ReasonCodes: &reasonCodes},
		&Unsubscribe{FixedHeader: FixedHeader{PacketType: UnsubscribeType, QoS: 1}, ID: 4, Topics: []string{"a/+", "#"}},
		&PingReq{FixedHeader: FixedHeader{PacketType: PingReqType}},
		&Disconnect{FixedHeader: FixedHeader{PacketType: DisconnectType}, ReasonCode: 0x04},
		&Auth{FixedHeader: FixedHeader{PacketType: AuthType}, ReasonCode: 0x18},
	}
}

// FuzzReadPacket checks that the streaming and zero-copy decoders agree on
// every input, and that whatever they accept re-encodes to bytes that decode
// back to the same packet.
func FuzzReadPacket(f *testing.F) {
	for _, pkt := range fuzzSeedPackets() {
		f.Add(pkt.Encode())
	}
	// Found by this target, each once accepted by one decoder only, or
	// re-encoded differently on a second pass.
	f.Add([]byte{0x32, 0x08, 0x00, 0x01, 'a', 0x00, 0x01, 0x02, 0x30, 0x00})                            // unknown publish property
	f.Add([]byte{0x30, 0x05, 0x00, 0x01, 'a', 0x05, 0x01})                                              // property block overruns the body
	f.Add([]byte{0x82, 0x0d, 0x30, 0x30, 0x00, 0x00, 0x03, '0', '0', '0', 0x30, 0x00, 0x01, '0', 0x30}) // reserved retain handling
	f.Add([]byte{0x82, 0x05, 0x30, 0x30, 0x00, 0x00, 0x00})                                             // topic without its options byte
	f.Add([]byte{0xc0, 0x01, 0x00})                                                                     // PINGREQ with a body
	f.Add([]byte{0x62, 0x07, 0x00, 0x01, 0x00, 0x03, 0x1f, 0x00, 0x00})                                 // properties that encode to nothing
	f.Add([]byte{0x30, 0xff, 0xff, 0xff, 0x7f})                                                         // maximum advertised length, no body

	f.Fuzz(func(t *testing.T, data []byte) {
		streamed, _, _, streamErr := ReadPacketLimit(bytes.NewReader(data), fuzzMaxPacket)
		zeroCopy, consumed, zeroCopyErr := ReadPacketBytes(data)

		if (streamErr == nil) != (zeroCopyErr == nil) {
			t.Fatalf("decoders disagree on % x: stream err=%v, zero-copy err=%v", data, streamErr, zeroCopyErr)
		}
		if streamErr != nil {
			return
		}
		if consumed <= 0 || consumed > len(data) {
			t.Fatalf("zero-copy consumed %d of %d bytes", consumed, len(data))
		}
		if streamed.Type() != zeroCopy.Type() {
			t.Fatalf("decoders disagree on type: %d vs %d", streamed.Type(), zeroCopy.Type())
		}

		encoded := streamed.Encode()
		if !bytes.Equal(encoded, zeroCopy.Encode()) {
			t.Fatalf("decoders produced different packets for % x:\n stream    % x\n zero-copy % x", data, encoded, zeroCopy.Encode())
		}
		again, _, _, err := ReadPacketLimit(bytes.NewReader(encoded), fuzzMaxPacket)
		if err != nil {
			t.Fatalf("re-encoded packet % x does not decode: %v", encoded, err)
		}
		if !bytes.Equal(again.Encode(), encoded) {
			t.Fatalf("encode is not stable:\n first  % x\n second % x", encoded, again.Encode())
		}
	})
}

func fuzzPtr[T any](v T) *T { return &v }
