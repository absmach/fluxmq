// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	"github.com/stretchr/testify/require"
)

// blockingConn is a mockConnection whose ReadPacket blocks until the
// connection is closed. It models a TCP socket with no inbound data — and, on
// a high-latency link, one whose FIN has not yet been processed when the
// client reconnects.
type blockingConn struct {
	mockConnection
	closeOnce sync.Once
	closeCh   chan struct{}
	closed    atomic.Bool
	reading   chan struct{} // closed when ReadPacket is first entered
	readOnce  sync.Once
}

func newBlockingConn() *blockingConn {
	return &blockingConn{
		closeCh: make(chan struct{}),
		reading: make(chan struct{}),
	}
}

func (c *blockingConn) ReadPacket() (packets.ControlPacket, error) {
	c.readOnce.Do(func() { close(c.reading) })
	<-c.closeCh
	return nil, io.EOF
}

func (c *blockingConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeCh)
	})
	return nil
}

const protocolNameMQTT = "MQTT"

func v3Connect(clientID string) *v3.Connect {
	return &v3.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    protocolNameMQTT,
		ProtocolVersion: 4,
		ClientID:        clientID,
		CleanSession:    false, // persistent session — the propeller#241 scenario
		KeepAlive:       60,
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// TestHandleConnect_LocalTakeoverHighLatencyReconnect reproduces propeller#241:
// a persistent-session client reconnects with the same client ID while its
// previous connection is still attached (the old FIN delayed by a high-latency
// link). The broker must close the old connection and keep the new one — the
// superseded runSession goroutine must NOT tear down the new connection.
func TestHandleConnect_LocalTakeoverHighLatencyReconnect(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	h := NewV3Handler(b)

	const clientID = "proplet-1"

	// First connection: enters runSession and blocks reading (idle socket).
	oldConn := newBlockingConn()
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		h.HandleConnect(oldConn, v3Connect(clientID)) //nolint:errcheck // returns io.EOF on takeover
	}()

	// Wait until the old connection's session is registered and reading.
	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "old session connected")

	// Second connection: same client ID, persistent session. This triggers the
	// local takeover — old conn must be closed, new conn becomes current.
	newConn := newBlockingConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		h.HandleConnect(newConn, v3Connect(clientID)) //nolint:errcheck // closed at cleanup
	}()

	// Old runSession goroutine unblocks (its socket was closed) and exits.
	oldWG.Wait()
	require.True(t, oldConn.closed.Load(), "takeover must close the old connection")

	// New connection must own the session and stay connected, even after the
	// stale old goroutine has fully torn down.
	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "new session connected and current")

	require.False(t, newConn.closed.Load(), "new connection must remain open after takeover")

	// Give any stale teardown a chance to (incorrectly) fire, then re-assert.
	time.Sleep(20 * time.Millisecond)
	s := b.sessionsMap.Get(clientID)
	require.NotNil(t, s)
	require.True(t, s.IsConnected(), "new connection must survive the stale goroutine's teardown")
	require.False(t, newConn.closed.Load())

	// Cleanup: release the new connection so its runSession exits.
	newConn.Close()
	newWG.Wait()
}
