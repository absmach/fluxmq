// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt_test

import (
	"net"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func pingResp() packets.ControlPacket {
	return &v3.PingResp{FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType}}
}

// TestWriteTimeoutUnblocksStalledPeer covers a peer that stops reading: without
// a write deadline the broker goroutine parks on the socket write for as long as
// the peer likes.
func TestWriteTimeoutUnblocksStalledPeer(t *testing.T) {
	serverConn, clientConn := net.Pipe() // unbuffered: nothing is written until read
	t.Cleanup(func() {
		serverConn.Close() //nolint:errcheck // best-effort teardown
		clientConn.Close() //nolint:errcheck // best-effort teardown
	})

	conn := core.NewConnection(serverConn, 0, false, core.WithWriteTimeout(50*time.Millisecond))

	done := make(chan error, 1)
	go func() { done <- conn.WritePacket(pingResp()) }()

	select {
	case err := <-done:
		require.Error(t, err, "write to a peer that never reads must fail")
		var netErr net.Error
		require.ErrorAs(t, err, &netErr)
		assert.True(t, netErr.Timeout(), "write must fail with a timeout, got %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("write never returned: no write deadline applied")
	}
}

// TestWriteWithoutTimeoutBlocks documents the opposite configuration: with no
// write timeout the same write stays parked, which is why the listener sets one.
func TestWriteWithoutTimeoutBlocks(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		serverConn.Close() //nolint:errcheck // best-effort teardown
		clientConn.Close() //nolint:errcheck // best-effort teardown
	})

	conn := core.NewConnection(serverConn, 0, false)

	done := make(chan error, 1)
	go func() { done <- conn.WritePacket(pingResp()) }()

	select {
	case err := <-done:
		t.Fatalf("expected the write to stay blocked, got %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Draining the peer lets the parked write complete.
	buf := make([]byte, 2)
	_, err := clientConn.Read(buf)
	require.NoError(t, err)
	require.NoError(t, <-done)
}

// TestWriteTimeoutDoesNotAccumulate verifies the deadline is refreshed per write
// rather than set once: a long-lived connection must keep writing successfully
// well past the timeout window.
func TestWriteTimeoutDoesNotAccumulate(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		serverConn.Close() //nolint:errcheck // best-effort teardown
		clientConn.Close() //nolint:errcheck // best-effort teardown
	})

	conn := core.NewConnection(serverConn, 0, false, core.WithWriteTimeout(500*time.Millisecond))

	reader := make(chan error, 1)
	go func() {
		buf := make([]byte, 2)
		for range 3 {
			if _, err := clientConn.Read(buf); err != nil {
				reader <- err
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		reader <- nil
	}()

	for i := range 3 {
		require.NoErrorf(t, conn.WritePacket(pingResp()), "write %d past the timeout window failed", i)
	}
	require.NoError(t, <-reader)
}
