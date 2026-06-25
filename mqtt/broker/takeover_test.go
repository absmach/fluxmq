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
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
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

// syncConn is a blocking-read connection that records written packets under a
// mutex, so a test can safely inspect packets written from another goroutine
// (e.g. the takeover drain goroutine).
type syncConn struct {
	mockConnection
	mu        sync.Mutex
	written   []packets.ControlPacket
	closeOnce sync.Once
	closeCh   chan struct{}
	closed    atomic.Bool
	reading   chan struct{}
	readOnce  sync.Once
}

func newSyncConn() *syncConn {
	return &syncConn{closeCh: make(chan struct{}), reading: make(chan struct{})}
}

func (c *syncConn) record(p packets.ControlPacket, onSent func()) error {
	c.mu.Lock()
	c.written = append(c.written, p)
	c.mu.Unlock()
	if onSent != nil {
		onSent()
	}
	return nil
}

func (c *syncConn) WritePacket(p packets.ControlPacket) error { return c.record(p, nil) }
func (c *syncConn) WriteControlPacket(p packets.ControlPacket, onSent func()) error {
	return c.record(p, onSent)
}

func (c *syncConn) WriteDataPacket(p packets.ControlPacket, onSent func()) error {
	return c.record(p, onSent)
}

func (c *syncConn) TryWriteDataPacket(p packets.ControlPacket, onSent func()) error {
	return c.record(p, onSent)
}

func (c *syncConn) ReadPacket() (packets.ControlPacket, error) {
	c.readOnce.Do(func() { close(c.reading) })
	<-c.closeCh
	return nil, io.EOF
}

func (c *syncConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		close(c.closeCh)
	})
	return nil
}

func (c *syncConn) writtenPackets() []packets.ControlPacket {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]packets.ControlPacket(nil), c.written...)
}

func v5Connect(clientID, willTopic string, willPayload []byte) *v5.Connect {
	return v5ConnectWillDelay(clientID, willTopic, willPayload, 0)
}

func v5ConnectWillDelay(clientID, willTopic string, willPayload []byte, willDelay uint32) *v5.Connect {
	c := &v5.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ProtocolName:    protocolNameMQTT,
		ProtocolVersion: 5,
		ClientID:        clientID,
		CleanStart:      false,
		KeepAlive:       60,
	}
	if willTopic != "" {
		c.WillFlag = true
		c.WillTopic = willTopic
		c.WillPayload = willPayload
		if willDelay > 0 {
			d := willDelay
			c.WillProperties = &v5.WillProperties{WillDelayInterval: &d}
		}
	}
	return c
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
	h := newV3Handler(b)

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
	waitFor(t, func() bool { return oldConn.closed.Load() }, "takeover must close the old connection")

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
	h := newV3Handler(b)

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

	waitFor(t, func() bool { return oldConn.closed.Load() }, "old conn closed by takeover")

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
	h := newV3Handler(b)

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
	waitFor(t, func() bool { return oldConn.closed.Load() }, "takeover closes the stalled connection")

	// The superseded goroutine's blocked PINGRESP write failed against its own
	// closed socket. That is expected teardown and must not be counted as a
	// protocol error on the broker.
	require.Equal(t, uint64(0), b.telemetry.stats.GetProtocolErrors(),
		"stale write failure must not be counted as a protocol error")

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
	h := newV3Handler(b)

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

// TestHandleConnect_StalePublishNotDispatched guards finding #1: a PUBLISH read
// on the old connection but processed after a takeover must not mutate shared
// state — here, it must not be routed to subscribers through the broker. The
// generation check drops the packet before dispatch.
func TestHandleConnect_StalePublishNotDispatched(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV3Handler(b)

	const pubID = "publisher"
	const topic = "devices/telemetry"

	// A subscriber on a separate session; its connection captures deliveries.
	sub, _, err := b.CreateSession("subscriber", 4, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := &captureConnection{}
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, topic, 0, storage.SubscribeOptions{}))

	// Publisher's old connection has a QoS0 PUBLISH queued, released only after
	// the takeover.
	pub := &v3.Publish{
		FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
		TopicName:   topic,
		Payload:     []byte("stale"),
	}
	oldConn := newScriptedConn(pub)
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		h.HandleConnect(oldConn, v3Connect(pubID)) //nolint:errcheck
	}()

	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(pubID)
		return s != nil && s.IsConnected()
	}, "old publisher session connected")

	// Take over the publisher's client ID before the PUBLISH is processed.
	newConn := newBlockingConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		h.HandleConnect(newConn, v3Connect(pubID)) //nolint:errcheck
	}()

	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(pubID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "new publisher session current")

	// Release the stale PUBLISH. It must be dropped, not routed.
	close(oldConn.releaseRead)
	oldWG.Wait()

	time.Sleep(20 * time.Millisecond)
	require.Empty(t, subConn.packets, "stale PUBLISH must not be delivered to subscribers")

	newConn.Close()
	newWG.Wait()
}

// TestHandleConnect_V5TakeoverNotifiesAndPublishesWill guards finding #2: an
// MQTT 5 takeover must send the displaced client a DISCONNECT with reason 0x8E
// (session taken over) and publish that connection's zero-delay Will.
// References: OASIS MQTT 5.0, sections 3.1.4 and 3.1.2.5.
func TestHandleConnect_V5TakeoverNotifiesAndPublishesWill(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "proplet-v5"
	const willTopic = "devices/proplet-v5/status"

	// Subscriber that should receive the displaced connection's Will.
	sub, _, err := b.CreateSession("will-subscriber", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	// Old connection with a zero-delay Will.
	oldConn := newSyncConn()
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		h.HandleConnect(oldConn, v5Connect(clientID, willTopic, []byte("offline"))) //nolint:errcheck
	}()

	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "old v5 session connected")

	// Take over with a new connection (no Will).
	newConn := newSyncConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		h.HandleConnect(newConn, v5Connect(clientID, "", nil)) //nolint:errcheck
	}()

	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "new v5 session current")

	// The drain runs asynchronously: the old connection gets a DISCONNECT 0x8E.
	waitFor(t, func() bool {
		for _, p := range oldConn.writtenPackets() {
			if d, ok := p.(*v5.Disconnect); ok && d.ReasonCode == reasonSessionTakenOver {
				return true
			}
		}
		return false
	}, "displaced client receives DISCONNECT 0x8E")

	// The displaced connection's zero-delay Will is published to the subscriber.
	waitFor(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return true
			}
		}
		return false
	}, "displaced connection's Will is published")

	oldWG.Wait()
	newConn.Close()
	newWG.Wait()
}

// TestHandleConnect_V5TakeoverDelayedWillNotPublished guards finding #1: a Will
// with a non-zero Will Delay Interval must NOT be published when a takeover
// closes the connection — the session continues under the new connection, so
// the delayed Will is cancelled. [MQTT-3.1.2.5].
func TestHandleConnect_V5TakeoverDelayedWillNotPublished(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	h := newV5Handler(b)

	const clientID = "proplet-delay"
	const willTopic = "devices/proplet-delay/status"

	sub, _, err := b.CreateSession("delay-sub", 5, session.Options{CleanStart: true})
	require.NoError(t, err)
	subConn := newSyncConn()
	_, err = sub.Connect(subConn)
	require.NoError(t, err)
	require.NoError(t, b.subscribe(sub, willTopic, 0, storage.SubscribeOptions{}))

	// Old connection with a 60s-delayed Will.
	oldConn := newSyncConn()
	var oldWG sync.WaitGroup
	oldWG.Add(1)
	go func() {
		defer oldWG.Done()
		h.HandleConnect(oldConn, v5ConnectWillDelay(clientID, willTopic, []byte("offline"), 60)) //nolint:errcheck
	}()

	<-oldConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected()
	}, "old v5 session connected")

	newConn := newSyncConn()
	var newWG sync.WaitGroup
	newWG.Add(1)
	go func() {
		defer newWG.Done()
		h.HandleConnect(newConn, v5Connect(clientID, "", nil)) //nolint:errcheck
	}()

	<-newConn.reading
	waitFor(t, func() bool {
		s := b.sessionsMap.Get(clientID)
		return s != nil && s.IsConnected() && s.Conn() == newConn
	}, "new v5 session current")

	// The old connection is still notified and closed, but the delayed Will
	// must not be published.
	waitFor(t, func() bool { return oldConn.closed.Load() }, "old connection closed by takeover")
	time.Sleep(30 * time.Millisecond)
	for _, p := range subConn.writtenPackets() {
		if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
			t.Fatal("delayed Will must not be published on takeover")
		}
	}

	oldWG.Wait()
	newConn.Close()
	newWG.Wait()
}
