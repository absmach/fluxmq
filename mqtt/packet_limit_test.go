// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt_test

import (
	"bytes"
	"io"
	"net"
	"testing"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oversizedPublishHeader is a PUBLISH fixed header advertising remainingLength
// bytes of body that are never sent.
func oversizedPublishHeader(remainingLength int) []byte {
	fh := packets.FixedHeader{PacketType: packets.PublishType, RemainingLength: remainingLength}
	return fh.Encode()
}

// countingReader reports how many bytes past the fixed header were consumed, so
// a test can prove the decoder rejected the packet before touching the body.
type countingReader struct {
	data []byte
	pos  int
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func TestReadPacketLimitRejectsOversizedPacket(t *testing.T) {
	// The maximum an MQTT variable byte integer can express: the allocation an
	// unauthenticated peer could otherwise force with a 5-byte header.
	const protocolMax = 268435455

	tests := []struct {
		name string
		read func(io.Reader, int) error
		// publish encodes a small PUBLISH and reports its remaining length.
		publish func() ([]byte, int)
	}{
		{
			name: "v3",
			read: func(r io.Reader, maxSize int) error {
				_, err := v3.ReadPacketLimit(r, maxSize)
				return err
			},
			publish: func() ([]byte, int) {
				p := &v3.Publish{
					FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
					TopicName:   "a",
					Payload:     bytes.Repeat([]byte("x"), 16),
				}
				wire := p.Encode()
				return wire, p.FixedHeader.RemainingLength
			},
		},
		{
			name: "v5",
			read: func(r io.Reader, maxSize int) error {
				_, _, _, err := v5.ReadPacketLimit(r, maxSize)
				return err
			},
			publish: func() ([]byte, int) {
				p := &v5.Publish{
					FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
					TopicName:   "a",
					Properties:  &v5.PublishProperties{},
					Payload:     bytes.Repeat([]byte("x"), 16),
				}
				wire := p.Encode()
				return wire, p.FixedHeader.RemainingLength
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"/rejects_before_reading_body", func(t *testing.T) {
			header := oversizedPublishHeader(protocolMax)
			r := &countingReader{data: append(header, make([]byte, 64)...)}

			err := tt.read(r, 1024)
			require.ErrorIs(t, err, packets.ErrPacketTooLarge)
			assert.Equal(t, len(header), r.pos, "decoder must stop at the fixed header, not buffer the body")
		})

		t.Run(tt.name+"/accepts_packet_at_the_limit", func(t *testing.T) {
			wire, remaining := tt.publish()
			require.NoError(t, tt.read(bytes.NewReader(wire), remaining),
				"a packet exactly at the limit must be accepted")
		})

		t.Run(tt.name+"/rejects_packet_one_byte_over", func(t *testing.T) {
			wire, remaining := tt.publish()
			err := tt.read(bytes.NewReader(wire), remaining-1)
			require.ErrorIs(t, err, packets.ErrPacketTooLarge)
		})

		t.Run(tt.name+"/zero_limit_is_unlimited", func(t *testing.T) {
			// Kept small: an unlimited decoder reserves the advertised length up
			// front, which is exactly the behaviour a limit is meant to bound.
			header := oversizedPublishHeader(1 << 20)
			// Body never arrives, so an unlimited decoder fails on the read
			// rather than on the size check.
			err := tt.read(bytes.NewReader(header), 0)
			require.Error(t, err)
			assert.NotErrorIs(t, err, packets.ErrPacketTooLarge)
		})
	}
}

func TestConnectionMaxPacketSize(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		serverConn.Close() //nolint:errcheck // best-effort teardown
		clientConn.Close() //nolint:errcheck // best-effort teardown
	})

	conn := core.NewConnectionWithVersion(serverConn, 0, false, core.ProtocolV3, core.WithMaxPacketSize(1024))

	go func() {
		clientConn.Write(oversizedPublishHeader(10 * 1024 * 1024)) //nolint:errcheck // reader rejects before the body
	}()

	_, err := conn.ReadPacket()
	require.ErrorIs(t, err, packets.ErrPacketTooLarge)
}
