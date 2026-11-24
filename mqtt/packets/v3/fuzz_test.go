// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"bytes"
	"testing"

	"github.com/absmach/fluxmq/mqtt/packets"
)

// fuzzMaxPacket bounds the streaming reader so an advertised length cannot
// make a fuzz iteration allocate without limit.
const fuzzMaxPacket = 1 << 20

func fuzzSeedPackets() []packets.ControlPacket {
	return []packets.ControlPacket{
		&Connect{
			FixedHeader: FixedHeader{PacketType: ConnectType}, ProtocolName: "MQTT", ProtocolVersion: 4,
			CleanSession: true, KeepAlive: 60, ClientID: "client",
			UsernameFlag: true, Username: "user", PasswordFlag: true, Password: []byte("pass"),
			WillFlag: true, WillQoS: 1, WillTopic: "will/t", WillMessage: []byte("bye"),
		},
		&ConnAck{FixedHeader: FixedHeader{PacketType: ConnAckType}, SessionPresent: true},
		&Publish{FixedHeader: FixedHeader{PacketType: PublishType}, TopicName: "a/b", Payload: []byte("x")},
		&Publish{FixedHeader: FixedHeader{PacketType: PublishType, QoS: 1, Retain: true}, TopicName: "a/b", ID: 7, Payload: []byte("y")},
		&PubAck{FixedHeader: FixedHeader{PacketType: PubAckType}, ID: 7},
		&Subscribe{FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1}, ID: 3, Topics: []Topic{{Name: "a/+", QoS: 1}, {Name: "#", QoS: 2}}},
		&SubAck{FixedHeader: FixedHeader{PacketType: SubAckType}, ID: 3, ReturnCodes: []byte{1, 2, 0x80}},
		&Unsubscribe{FixedHeader: FixedHeader{PacketType: UnsubscribeType, QoS: 1}, ID: 4, Topics: []string{"a/+"}},
		&UnSubAck{FixedHeader: FixedHeader{PacketType: UnsubAckType}, ID: 4},
		&PingReq{FixedHeader: FixedHeader{PacketType: PingReqType}},
		&Disconnect{FixedHeader: FixedHeader{PacketType: DisconnectType}},
	}
}

// FuzzReadPacket checks that the streaming and zero-copy decoders agree on
// every input, and that whatever they accept re-encodes to bytes that decode
// back to the same packet.
func FuzzReadPacket(f *testing.F) {
	for _, pkt := range fuzzSeedPackets() {
		f.Add(pkt.Encode())
	}
	// Found by this target: a PUBACK with a reserved flag set, accepted only by
	// the zero-copy reader; and a SUBSCRIBE whose topic is truncated, accepted
	// by the streaming reader as a packet with no topics.
	f.Add([]byte{0x41, 0x02, 0x30, 0x30})
	f.Add([]byte{0x82, 0x04, 0x30, 0x30, 0x30, 0x30})
	f.Add([]byte{0x30, 0xff, 0xff, 0xff, 0x7f}) // maximum advertised length, no body
	f.Add([]byte{0x30, 0x80, 0x80, 0x80, 0x80, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		streamed, streamErr := ReadPacketLimit(bytes.NewReader(data), fuzzMaxPacket)
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
		again, err := ReadPacketLimit(bytes.NewReader(encoded), fuzzMaxPacket)
		if err != nil {
			t.Fatalf("re-encoded packet % x does not decode: %v", encoded, err)
		}
		if !bytes.Equal(again.Encode(), encoded) {
			t.Fatalf("encode is not stable:\n first  % x\n second % x", encoded, again.Encode())
		}
	})
}
