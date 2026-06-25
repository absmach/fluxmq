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
	"github.com/absmach/fluxmq/mqtt/session"
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

// scriptedConn delivers exactly one packet, but only after releaseRead is
// closed. This lets a test sequence a read so it completes *after* a takeover
// has already swapped in the replacement connection — reproducing a stale
// in-flight packet.
type scriptedConn struct {
	mockConnection
	pkt         packets.ControlPacket
	releaseRead chan struct{}
	delivered   atomic.Bool
	closed      atomic.Bool
	reading     chan struct{}
	readOnce    sync.Once
}

func newScriptedConn(pkt packets.ControlPacket) *scriptedConn {
	return &scriptedConn{
		pkt:         pkt,
		releaseRead: make(chan struct{}),
		reading:     make(chan struct{}),
	}
}

func (c *scriptedConn) ReadPacket() (packets.ControlPacket, error) {
	c.readOnce.Do(func() { close(c.reading) })
	if !c.delivered.Swap(true) {
		<-c.releaseRead // deliver the scripted packet only when the test allows
		return c.pkt, nil
	}
	return nil, io.EOF
}

func (c *scriptedConn) Close() error {
	c.closed.Store(true)
	return nil
}

// blockingWriteConn delivers one packet, then blocks in WritePacket until it is
// closed — modelling a client that has stopped reading (full socket buffer) on
// a high-latency link. A takeover must not stall behind such a write.
type blockingWriteConn struct {
	mockConnection
	pkt         packets.ControlPacket
	releaseRead chan struct{}
	reading     chan struct{}
	writing     chan struct{} // closed when WritePacket is first entered
	closeCh     chan struct{}
	delivered   atomic.Bool
	closed      atomic.Bool
	readOnce    sync.Once
	writeOnce   sync.Once
	closeOnce   sync.Once
}

func newBlockingWriteConn(pkt packets.ControlPacket) *blockingWriteConn {
	return &blockingWriteConn{
		pkt:         pkt,
		releaseRead: make(chan struct{}),
		reading:     make(chan struct{}),
		writing:     make(chan struct{}),
		closeCh:     make(chan struct{}),
	}
}

func (c *blockingWriteConn) ReadPacket() (packets.ControlPacket, error) {
	c.readOnce.Do(func() { close(c.reading) })
	if !c.delivered.Swap(true) {
		select {
		case <-c.releaseRead:
			return c.pkt, nil
		case <-c.closeCh:
			return nil, io.EOF
		}
	}
	<-c.closeCh
	return nil, io.EOF
}

func (c *blockingWriteConn) WritePacket(packets.ControlPacket) error {
	if !c.delivered.Load() {
		return nil // let the CONNACK (written before the PINGREQ) through
	}
	c.writeOnce.Do(func() { close(c.writing) })
	<-c.closeCh // block until closed, like a peer that stopped reading
	return io.ErrClosedPipe
}

func (c *blockingWriteConn) WriteControlPacket(pkt packets.ControlPacket, _ func()) error {
	return c.WritePacket(pkt)
}

func (c *blockingWriteConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeCh)
	})
	return nil
}

// bindConn wraps a session in a connCtx bound to its current connection
// generation, for tests that call Handle* methods directly.
func bindConn(s *session.Session) *connCtx {
	return &connCtx{Session: s, conn: s.Conn(), epoch: s.Epoch()}
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

	// Exactly one active connection: the superseded goroutine must have
	// decremented the counter on its way out, not leaked it.
	require.Equal(t, uint64(1), b.telemetry.stats.GetCurrentConnections(),
		"active-connection count must be 1 after takeover")

	// Cleanup: release the new connection so its runSession exits.
	newConn.Close()
	newWG.Wait()
	waitFor(t, func() bool {
		return b.telemetry.stats.GetCurrentConnections() == 0
	}, "active-connection count drains to zero")
}

// TestHandleConnect_StaleDisconnectDoesNotCloseReplacement reproduces the
// second half of propeller#241: a DISCONNECT read on the old connection but
// processed *after* the takeover must not tear down the replacement
// connection. The processing guard skips dispatch for the superseded
// generation, so the new connection survives.
func TestHandleConnect_StaleDisconnectDoesNotCloseReplacement(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	h := NewV3Handler(b)

	const clientID = "proplet-2"

	// Old connection has a DISCONNECT queued, released only after takeover.
	disconnect := &v3.Disconnect{
		FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
	}
	oldConn := newScriptedConn(disconnect)
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		h.HandleConnect(oldConn, v3Connect(clientID)) //nolint:errcheck
	}()

	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "old session connected")

	// Take over with a new connection. This completes the swap before the old
	// goroutine ever returns from its read.
	newConn := newBlockingConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		h.HandleConnect(newConn, v3Connect(clientID)) //nolint:errcheck
	}()

	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "new session connected and current")

	// Now let the old connection deliver its stale DISCONNECT. Its connCtx is
	// bound to the superseded generation, so connCtx.Disconnect is a no-op via
	// DisconnectIf — without that binding it would call Disconnect and close
	// newConn.
	close(oldConn.releaseRead)
	oldWG.Wait()

	require.True(t, oldConn.closed.Load(), "old conn closed by takeover")

	// Give the stale DISCONNECT a chance to (incorrectly) tear down the session.
	time.Sleep(20 * time.Millisecond)
	s := b.sessionsMap.Get(clientID)
	require.NotNil(t, s)
	require.True(t, s.IsConnected(), "stale DISCONNECT must not disconnect the replacement")
	require.Equal(t, newConn, s.Conn())
	require.False(t, newConn.closed.Load())
	require.Equal(t, uint64(1), b.telemetry.stats.GetCurrentConnections(),
		"active-connection count must be 1 after stale-DISCONNECT takeover")

	newConn.Close()
	newWG.Wait()
}

// TestHandleConnect_TakeoverNotBlockedByStalledWrite guards finding #1: a
// takeover must complete even while the superseded connection's handler is
// blocked writing to a peer that stopped reading. The previous design held a
// processing lock across handler work, so the takeover deadlocked behind the
// blocked write; generation-safe writes hold no such lock.
func TestHandleConnect_TakeoverNotBlockedByStalledWrite(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	h := NewV3Handler(b)

	const clientID = "proplet-stalled"

	// Old connection: delivers a PINGREQ, whose PINGRESP write then blocks.
	pingreq := &v3.PingReq{FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType}}
	oldConn := newBlockingWriteConn(pingreq)
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		h.HandleConnect(oldConn, v3Connect(clientID)) //nolint:errcheck
	}()

	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "old session connected")

	// Deliver the PINGREQ and wait until the handler is stuck in the write.
	close(oldConn.releaseRead)
	<-oldConn.writing

	// Take over while the old handler is blocked writing. This must not hang.
	newConn := newBlockingConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		h.HandleConnect(newConn, v3Connect(clientID)) //nolint:errcheck
	}()

	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "takeover completes despite stalled write on old connection")

	// The takeover closed the old connection, unblocking its write so its
	// goroutine exits.
	oldWG.Wait()
	require.True(t, oldConn.closed.Load())

	newConn.Close()
	newWG.Wait()
}

// TestHandleConnect_RepeatedTakeoversDoNotLeakConnectionCount guards finding #2:
// every superseded runSession goroutine must decrement the active-connection
// counter exactly once. Repeated reconnects of the same client ID must leave
// the count at one live connection, never inflating it.
func TestHandleConnect_RepeatedTakeoversDoNotLeakConnectionCount(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()
	h := NewV3Handler(b)

	const clientID = "proplet-loop"
	const rounds = 8

	var wg sync.WaitGroup
	conns := make([]*blockingConn, rounds)

	for i := range rounds {
		conns[i] = newBlockingConn()
		wg.Add(1)
		go func(c *blockingConn) {
			defer wg.Done()
			h.HandleConnect(c, v3Connect(clientID)) //nolint:errcheck
		}(conns[i])

		<-conns[i].reading
		// Wait until this connection owns the session and the previous
		// goroutine has exited (count back to exactly one).
		current := conns[i]
		waitFor(t, func() bool {
			s := b.sessionsMap.Get(clientID)
			return s != nil && s.IsConnected() && s.Conn() == current &&
				b.telemetry.stats.GetCurrentConnections() == 1
		}, "single live connection after takeover round")
	}

	require.Equal(t, uint64(1), b.telemetry.stats.GetCurrentConnections())

	// Close the surviving connection; count must drain to zero.
	conns[rounds-1].Close()
	wg.Wait()
	waitFor(t, func() bool {
		return b.telemetry.stats.GetCurrentConnections() == 0
	}, "active-connection count drains to zero")
}
