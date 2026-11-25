// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"bufio"
	"io"
	"testing"

	"github.com/absmach/fluxmq/internal/allocguard"
	"github.com/absmach/fluxmq/mqtt/packets"
)

// Every PUBLISH is decoded once on the way in and packed once per subscriber
// on the way out; acknowledgements are packed once per QoS 1 or 2 message.
func TestAllocsPacketHotPaths(t *testing.T) {
	publish := &Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
		TopicName:   "devices/1/telemetry",
		ID:          7,
		Payload:     make([]byte, 1024),
	}
	encoded := publish.Encode()
	ack := &PubAck{FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType}, ID: 7}
	w := bufio.NewWriterSize(io.Discard, 64<<10)

	tests := []struct {
		name   string
		budget float64
		fn     func()
	}{
		// The decoded packet, its topic string and its properties.
		{"decode/publish", 3, func() {
			pkt, _, err := ReadPacketBytes(encoded)
			if err != nil {
				t.Fatal(err)
			}
			pkt.Release()
		}},
		{"pack/publish", 0, func() {
			if err := publish.Pack(w); err != nil {
				t.Fatal(err)
			}
		}},
		{"pack/puback", 0, func() {
			if err := ack.Pack(w); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allocguard.Max(t, tt.budget, tt.fn)
		})
	}
}
