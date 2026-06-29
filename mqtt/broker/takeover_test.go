// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/absmach/fluxmq/storage/messages"
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

	// oldWG.Wait above already guaranteed the superseded goroutine ran its
	// teardown (DisconnectIf) and exited, so this re-assert is deterministic: the
	// stale teardown has had its chance to (incorrectly) fire.
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

	// oldWG.Wait above guaranteed the stale DISCONNECT was dispatched (a no-op via
	// connCtx.Disconnect/DisconnectIf) and the goroutine exited, so this assert is
	// deterministic.
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

	// Release the stale PUBLISH. It must be dropped, not routed. The drop happens
	// synchronously in the old runSession goroutine (the cc.current() check
	// precedes dispatch), so oldWG.Wait guarantees the decision is final.
	close(oldConn.releaseRead)
	oldWG.Wait()

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
			if d, ok := p.(*v5.Disconnect); ok && d.ReasonCode == v5.DisconnectSessionTakenOver {
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

	// The Will is published (if at all) by the asynchronous drainSuperseded
	// goroutine, which the test cannot join. require.Never polls the subscriber
	// throughout a settle window, asserting the delayed Will never arrives —
	// deterministic where a single sleep+check would only sample one instant.
	require.Never(t, func() bool {
		for _, p := range subConn.writtenPackets() {
			if pub, ok := p.(*v5.Publish); ok && pub.TopicName == willTopic {
				return true
			}
		}
		return false
	}, 100*time.Millisecond, 5*time.Millisecond, "delayed Will must not be published on takeover")

	oldWG.Wait()
	newConn.Close()
	newWG.Wait()
}

// TestCreateSession_InflightTrackerSizedByServerLimit guards finding #2: the
// persistent bidirectional inflight store is sized by the server limit, not the
// client's outbound Receive Maximum, so inbound QoS 2 transactions are not
// starved by a small advertised Receive Maximum.
func TestCreateSession_InflightTrackerSizedByServerLimit(t *testing.T) {
	b := NewBroker(memory.New(), nil, WithSessionConfig(config.SessionConfig{
		MaxInflightMessages: 10,
		InflightOverflow:    config.InflightOverflowBackpressure,
	}))
	defer b.Close()

	s, _, err := b.CreateSession("c", 5, session.Options{CleanStart: true, ReceiveMaximum: 1})
	require.NoError(t, err)
	require.Equal(t, uint16(1), s.ReceiveMaximum, "outbound send quota is clamped to the client Receive Maximum")

	// The store accepts several inbound QoS 2 transactions even though the
	// outbound send quota is one.
	for i := uint16(1); i <= 5; i++ {
		m := &storage.Message{Topic: "t", QoS: 2}
		require.NoError(t, s.Inflight().Add(i, m, messages.Inbound),
			"inbound QoS 2 must not be limited by the client's outbound Receive Maximum")
	}
}

// TestDeliverMessage_EncodesForLeaseVersion guards finding #1: the PUBLISH is
// encoded for the version captured in the delivery lease, not the session's
// mutable version, so a cross-version takeover cannot encode for one protocol
// and write to a connection of the other.
func TestDeliverMessage_EncodesForLeaseVersion(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	v3msg := &storage.Message{Topic: "t", QoS: 1, PacketID: 1}
	v3msg.SetPayloadFromBytes([]byte("x"))
	v3conn := newSyncConn()
	require.NoError(t, b.DeliverMessage(v3conn, 4, v3msg, nil))
	require.Len(t, v3conn.writtenPackets(), 1)
	require.IsType(t, &v3.Publish{}, v3conn.writtenPackets()[0])

	v5msg := &storage.Message{Topic: "t", QoS: 1, PacketID: 2}
	v5msg.SetPayloadFromBytes([]byte("x"))
	v5conn := newSyncConn()
	require.NoError(t, b.DeliverMessage(v5conn, 5, v5msg, nil))
	require.Len(t, v5conn.writtenPackets(), 1)
	require.IsType(t, &v5.Publish{}, v5conn.writtenPackets()[0])
}

// TestDeliverQoS_SupersededLeaseRetriesCurrentGeneration guards finding #2: a
// delivery whose lease was superseded must retry against the current generation
// and reach the connection, not be parked where nothing drains it.
func TestDeliverQoS_SupersededLeaseRetriesCurrentGeneration(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	s, _, err := b.CreateSession("c", 5, session.Options{CleanStart: true, ReceiveMaximum: 4})
	require.NoError(t, err)
	conn := newSyncConn()
	_, err = s.Connect(conn)
	require.NoError(t, err)
	gen := s.Epoch()

	msg := &storage.Message{Topic: "t", QoS: 1}
	msg.SetPayloadFromBytes([]byte("x"))

	// Deliver under a stale (superseded) generation: it must re-lease against the
	// current generation and deliver, not fall back to the offline/pending path.
	pid, err := b.deliverQoS(context.Background(), s, msg, conn, 5, gen-1, 0)
	require.NoError(t, err)
	require.NotZero(t, pid, "delivery must succeed against the current generation")
	require.Len(t, conn.writtenPackets(), 1, "message must reach the live connection")
}

// TestConnAck_AdvertisesServerReceiveMaximum guards finding #3: CONNACK
// advertises the server's actual inbound Receive Maximum, not the protocol
// default of 65535.
func TestConnAck_AdvertisesServerReceiveMaximum(t *testing.T) {
	b := NewBroker(memory.New(), nil, WithSessionConfig(config.SessionConfig{
		MaxInflightMessages: 50,
		InflightOverflow:    config.InflightOverflowBackpressure,
	}))
	defer b.Close()
	h := newV5Handler(b)

	conn := newSyncConn()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.HandleConnect(conn, v5Connect("c", "", nil)) //nolint:errcheck
	}()

	<-conn.reading
	var ack *v5.ConnAck
	waitFor(t, func() bool {
		for _, p := range conn.writtenPackets() {
			if a, ok := p.(*v5.ConnAck); ok {
				ack = a
				return true
			}
		}
		return false
	}, "CONNACK written")

	require.NotNil(t, ack.Properties)
	require.NotNil(t, ack.Properties.ReceiveMax)
	require.Equal(t, uint16(50), *ack.Properties.ReceiveMax)

	conn.Close()
	wg.Wait()
}

// deferConn captures the onSent callback of the last data write without invoking
// it, so a test can fire it after a takeover to simulate an asynchronous flush
// on the displaced connection.
type deferConn struct {
	mockConnection
	mu        sync.Mutex
	onSent    func()
	reading   chan struct{}
	closeCh   chan struct{}
	readOnce  sync.Once
	closeOnce sync.Once
}

func newDeferConn() *deferConn {
	return &deferConn{reading: make(chan struct{}), closeCh: make(chan struct{})}
}

func (c *deferConn) capture(onSent func()) error {
	c.mu.Lock()
	c.onSent = onSent
	c.mu.Unlock()
	return nil
}
func (c *deferConn) WritePacket(packets.ControlPacket) error                    { return nil }
func (c *deferConn) WriteControlPacket(_ packets.ControlPacket, f func()) error { return c.capture(f) }
func (c *deferConn) WriteDataPacket(_ packets.ControlPacket, f func()) error    { return c.capture(f) }
func (c *deferConn) TryWriteDataPacket(_ packets.ControlPacket, f func()) error { return c.capture(f) }
func (c *deferConn) ReadPacket() (packets.ControlPacket, error) {
	c.readOnce.Do(func() { close(c.reading) })
	<-c.closeCh
	return nil, io.EOF
}

func (c *deferConn) Close() error {
	c.closeOnce.Do(func() { close(c.closeCh) })
	return nil
}

func (c *deferConn) fireOnSent() {
	c.mu.Lock()
	f := c.onSent
	c.mu.Unlock()
	if f != nil {
		f()
	}
}

// TestDelivery_StaleOnSentDoesNotMarkSent guards finding #3: if a queued packet
// flushes on the displaced connection after a takeover, its onSent must not mark
// the shared inflight entry as recently sent, or the replacement connection
// would wait out the retry timeout before retransmitting.
func TestDelivery_StaleOnSentDoesNotMarkSent(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	s, _, err := b.CreateSession("c", 5, session.Options{CleanStart: true, ReceiveMaximum: 4})
	require.NoError(t, err)
	oldConn := newDeferConn()
	_, err = s.Connect(oldConn)
	require.NoError(t, err)

	msg := &storage.Message{Topic: "t", QoS: 1}
	msg.SetPayloadFromBytes([]byte("x"))
	pid, err := b.DeliverToSession(context.Background(), s, msg)
	require.NoError(t, err)
	require.NotZero(t, pid)

	// Takeover: the inflight entry now belongs to the new generation.
	newConn := newDeferConn()
	_, _ = s.ConnectWithOptions(newConn, session.ConnectOptions{Version: 5, ReceiveMaximum: 4})

	// The old connection flushes the queued PUBLISH after the takeover.
	oldConn.fireOnSent()

	inf, ok := s.Inflight().Get(pid)
	require.True(t, ok)
	require.True(t, inf.SentAt.IsZero(),
		"a stale-generation onSent must not mark the inflight entry as sent")

	oldConn.Close()
	newConn.Close()
}

// TestRestoreInflightFromStorage_PreservesDirection guards finding #1: inbound
// and outbound inflight entries that share a packet ID are persisted under
// direction-qualified keys and restored to their original directions.
func TestRestoreInflightFromStorage_PreservesDirection(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	const clientID = "c"
	out := &storage.Message{Topic: "out", PacketID: 5, QoS: 2, InflightDirection: byte(messages.Outbound)}
	in := &storage.Message{Topic: "in", PacketID: 5, QoS: 2, InflightDirection: byte(messages.Inbound)}
	require.NoError(t, b.stores.messages.Store(fmt.Sprintf("%s%s%d/%d", clientID, inflightPrefix, messages.Outbound, 5), out))
	require.NoError(t, b.stores.messages.Store(fmt.Sprintf("%s%s%d/%d", clientID, inflightPrefix, messages.Inbound, 5), in))

	tracker := messages.NewInflightTracker(16)
	require.NoError(t, b.restoreInflightFromStorage(clientID, tracker))

	gotOut, err := tracker.Ack(5)
	require.NoError(t, err)
	require.Equal(t, "out", gotOut.Topic)
	gotIn, err := tracker.AckInbound(5)
	require.NoError(t, err)
	require.Equal(t, "in", gotIn.Topic)
}

// TestRestoreInflightFromTakeover_PreservesDirection guards finding #1 for the
// cluster transfer path: the takeover state carries direction, and restoration
// keeps inbound and outbound entries distinct.
func TestRestoreInflightFromTakeover_PreservesDirection(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	state := &clusterv1.SessionState{
		InflightMessages: []*clusterv1.InflightMessage{
			{PacketId: 5, Topic: "out", Qos: 2, Direction: uint32(messages.Outbound), Payload: []byte("op")},
			{PacketId: 5, Topic: "in", Qos: 2, Direction: uint32(messages.Inbound), Payload: []byte("ip")},
		},
	}
	tracker := messages.NewInflightTracker(16)
	require.NoError(t, b.restoreInflightFromTakeover(state, tracker))

	gotOut, err := tracker.Ack(5)
	require.NoError(t, err)
	require.Equal(t, "out", gotOut.Topic)
	require.Equal(t, "op", string(gotOut.GetPayload()), "payload must survive cluster transfer")
	gotIn, err := tracker.AckInbound(5)
	require.NoError(t, err)
	require.Equal(t, "in", gotIn.Topic)
	require.Equal(t, "ip", string(gotIn.GetPayload()))
}

// TestRestoreInflightFromTakeover_SkipsInvalidDirection guards finding #2: a
// corrupt direction from transferred state is skipped, not panicking.
func TestRestoreInflightFromTakeover_SkipsInvalidDirection(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()

	state := &clusterv1.SessionState{
		InflightMessages: []*clusterv1.InflightMessage{
			{PacketId: 1, Topic: "ok", Qos: 1, Direction: uint32(messages.Outbound)},
			{PacketId: 2, Topic: "bad", Qos: 1, Direction: 99}, // corrupt
		},
	}
	tracker := messages.NewInflightTracker(16)
	require.NoError(t, b.restoreInflightFromTakeover(state, tracker))

	require.True(t, tracker.Has(1))
	require.False(t, tracker.Has(2), "entry with invalid direction must be skipped")
}

// TestSession_AckInbound_UsesDirectionalAck guards finding #3: inbound
// acknowledgement is reachable without the base Inflight interface declaring it.
func TestSession_AckInbound_UsesDirectionalAck(t *testing.T) {
	b := NewBroker(memory.New(), nil)
	defer b.Close()
	s, _, err := b.CreateSession("c", 5, session.Options{CleanStart: true})
	require.NoError(t, err)

	require.NoError(t, s.Inflight().Add(9, &storage.Message{Topic: "in"}, messages.Inbound))
	got, err := s.AckInbound(9)
	require.NoError(t, err)
	require.Equal(t, "in", got.Topic)
}
