// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"bufio"
	"fmt"
	"io"
	"testing"

	"github.com/absmach/fluxmq/mqtt/packets"
)

// BenchmarkPublishPack writes a PUBLISH the way a connection does: Pack into
// the connection's buffered writer.
func BenchmarkPublishPack(b *testing.B) {
	for _, size := range []int{64, 1024, 16 << 10} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			pkt := &Publish{
				FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
				TopicName:   "devices/1/telemetry",
				ID:          7,
				Payload:     make([]byte, size),
			}
			w := bufio.NewWriterSize(io.Discard, 64<<10)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			for b.Loop() {
				if err := pkt.Pack(w); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
