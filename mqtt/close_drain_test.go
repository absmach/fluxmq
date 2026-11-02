// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt_test

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tcpPair returns both ends of a loopback TCP connection.
func tcpPair(t *testing.T) (server, client net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close() //nolint:errcheck // listener only needed for one accept

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()

	client, err = net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	server, ok := <-accepted
	require.True(t, ok, "accept failed")
	t.Cleanup(func() {
		server.Close() //nolint:errcheck // best-effort teardown
		client.Close() //nolint:errcheck // best-effort teardown
	})
	return server, client
}

// readUntilEOF reads everything the peer sends until it closes the connection.
func readUntilEOF(t *testing.T, conn net.Conn) []byte {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	data, err := io.ReadAll(conn)
	require.NoError(t, err, "peer must close cleanly after the reply")
	return data
}

// TestAsyncCloseFlushesQueuedControlPackets covers the "send, then close"
// pattern used for CONNECT refusals and server-initiated DISCONNECTs: a control
// packet enqueued immediately before Close must reach the peer before EOF.
func TestAsyncCloseFlushesQueuedControlPackets(t *testing.T) {
	cases := []struct {
		name string
		pkt  func() packets.ControlPacket
		want []byte
	}{
		{
			name: "v3/connack/bad-credentials",
			pkt: func() packets.ControlPacket {
				return &v3.ConnAck{
					FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
					ReturnCode:  v3.ConnAckBadUsernameOrPassword,
				}
			},
			want: []byte{0x20, 0x02, 0x00, 0x04},
		},
		{
			name: "v5/connack/bad-credentials",
			pkt: func() packets.ControlPacket {
				return &v5.ConnAck{
					FixedHeader: packets.FixedHeader{PacketType: packets.ConnAckType},
					ReasonCode:  0x86,
					Properties:  &v5.ConnAckProperties{},
				}
			},
			want: []byte{0x20, 0x03, 0x00, 0x86, 0x00},
		},
		{
			name: "v5/disconnect/keepalive-timeout",
			pkt: func() packets.ControlPacket {
				return &v5.Disconnect{
					FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
					ReasonCode:  0x8D,
				}
			},
			want: []byte{0xE0, 0x02, 0x8D, 0x00},
		},
	}

	const attempts = 20
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for range attempts {
				serverConn, clientConn := tcpPair(t)
				conn := core.NewConnection(serverConn, 1000, false)

				require.NoError(t, conn.WritePacket(tc.pkt()))
				require.NoError(t, conn.Close())

				got := readUntilEOF(t, clientConn)
				require.True(t, bytes.Equal(tc.want, got), "want % x before EOF, got % x", tc.want, got)
			}
		})
	}
}

// TestAsyncCloseDoesNotBlockOnStalledPeer pins the bound on the close drain: a
// peer that never reads must not hold Close up, and its socket must still be
// closed once the drain timeout expires.
func TestAsyncCloseDoesNotBlockOnStalledPeer(t *testing.T) {
	rc := newRecordingConn(true)
	conn := core.NewConnection(rc, 4, false)

	require.NoError(t, conn.WritePacket(publishPacket(1)))
	rc.awaitWriteStart(t)
	require.NoError(t, conn.WritePacket(publishPacket(2)))

	start := time.Now()
	require.NoError(t, conn.Close())
	assert.Less(t, time.Since(start), 100*time.Millisecond, "Close must not wait for a stalled peer")

	assert.Eventually(t, rc.closed.Load, 3*time.Second, 10*time.Millisecond,
		"socket of a stalled peer must be closed after the drain timeout")
}
