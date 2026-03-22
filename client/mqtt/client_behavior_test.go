// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testAddr string

func (a testAddr) Network() string { return "tcp" }
func (a testAddr) String() string  { return string(a) }

type packetCaptureConn struct {
	mu     sync.Mutex
	writes [][]byte
	closed bool
}

func (c *packetCaptureConn) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *packetCaptureConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	copyBuf := append([]byte(nil), p...)
	c.writes = append(c.writes, copyBuf)

	return len(p), nil
}

func (c *packetCaptureConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *packetCaptureConn) LocalAddr() net.Addr                { return testAddr("127.0.0.1:1883") }
func (c *packetCaptureConn) RemoteAddr() net.Addr               { return testAddr("127.0.0.1:1883") }
func (c *packetCaptureConn) SetDeadline(_ time.Time) error      { return nil }
func (c *packetCaptureConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *packetCaptureConn) SetWriteDeadline(_ time.Time) error { return nil }

type blockingFailWriteConn struct {
	mu                sync.Mutex
	writes            int
	firstWriteStarted chan struct{}
	releaseFirstWrite chan struct{}
}

func (c *blockingFailWriteConn) Read(_ []byte) (int, error) { return 0, io.EOF }

func (c *blockingFailWriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writes++
	writeNum := c.writes
	c.mu.Unlock()

	if writeNum == 1 {
		close(c.firstWriteStarted)
		<-c.releaseFirstWrite
		return 0, io.ErrClosedPipe
	}

	return len(p), nil
}

func (c *blockingFailWriteConn) Close() error                       { return nil }
func (c *blockingFailWriteConn) LocalAddr() net.Addr                { return testAddr("127.0.0.1:1883") }
func (c *blockingFailWriteConn) RemoteAddr() net.Addr               { return testAddr("127.0.0.1:1883") }
func (c *blockingFailWriteConn) SetDeadline(_ time.Time) error      { return nil }
func (c *blockingFailWriteConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *blockingFailWriteConn) SetWriteDeadline(_ time.Time) error { return nil }

type blockFirstWriteConn struct {
	mu                sync.Mutex
	writes            [][]byte
	writeCount        int
	firstWriteStarted chan struct{}
	releaseFirstWrite chan struct{}
}

func (c *blockFirstWriteConn) Read(_ []byte) (int, error) { return 0, io.EOF }

func (c *blockFirstWriteConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writeCount++
	writeNum := c.writeCount
	c.mu.Unlock()

	if writeNum == 1 {
		close(c.firstWriteStarted)
		<-c.releaseFirstWrite
	}

	c.mu.Lock()
	c.writes = append(c.writes, append([]byte(nil), p...))
	c.mu.Unlock()
	return len(p), nil
}

func (c *blockFirstWriteConn) snapshotWrites() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.writes))
	for i := range c.writes {
		out[i] = append([]byte(nil), c.writes[i]...)
	}
	return out
}

func (c *blockFirstWriteConn) Close() error                       { return nil }
func (c *blockFirstWriteConn) LocalAddr() net.Addr                { return testAddr("127.0.0.1:1883") }
func (c *blockFirstWriteConn) RemoteAddr() net.Addr               { return testAddr("127.0.0.1:1883") }
func (c *blockFirstWriteConn) SetDeadline(_ time.Time) error      { return nil }
func (c *blockFirstWriteConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *blockFirstWriteConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *packetCaptureConn) packetTypeCounts() map[byte]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	counts := make(map[byte]int)
	for _, frame := range c.writes {
		if len(frame) == 0 {
			continue
		}
		counts[frame[0]>>4]++
	}

	return counts
}

// setupWriteLoop wires up the write infrastructure so tests that bypass Connect() can use send methods.
// Cleanup is handled by the client's own cleanup() or Disconnect().
func setupWriteLoop(c *Client, conn net.Conn) {
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	close(doneCh) // no readLoop in tests — mark as already exited
	writeCh := make(chan writeRequest, 256)
	controlWriteCh := make(chan writeRequest, 64)
	qos0Wake := make(chan struct{}, 1)
	writeDone := make(chan struct{})
	c.writeRT.Store(&writeRuntime{
		conn:           conn,
		stopCh:         stopCh,
		doneCh:         doneCh,
		writeCh:        writeCh,
		controlWriteCh: controlWriteCh,
		qos0Wake:       qos0Wake,
		writeDone:      writeDone,
	})
	go c.writeLoop()
}

func TestRestoreStateReplaysSubscriptionsQueueAndOutbound(t *testing.T) {
	opts := NewOptions().
		SetClientID("restore-client").
		SetProtocolVersion(4).
		SetAckTimeout(50 * time.Millisecond)

	c, err := New(opts)
	require.NoError(t, err)

	c.state.set(StateConnected)
	conn := &packetCaptureConn{}
	setupWriteLoop(c, conn)

	c.subscriptions.setBasic("sensors/basic", 1)
	c.subscriptions.setOption(&SubscribeOption{Topic: "sensors/advanced", QoS: 2, NoLocal: true})
	c.queueSubs.add("$queue/jobs", &queueSubscription{queueName: "jobs", consumerGroup: "workers"})

	require.NoError(t, c.store.StoreOutbound(9, &Message{Topic: "outbound/qos1", Payload: []byte("qos1"), QoS: 1, PacketID: 9}))
	require.NoError(t, c.store.StoreOutbound(10, &Message{Topic: "outbound/qos0", Payload: []byte("qos0"), QoS: 0, PacketID: 10}))

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				for _, op := range c.pending.getAll() {
					if op.opType == pendingSubscribe {
						c.pending.complete(op.id, nil, []byte{0x00})
					}
				}
			}
		}
	}()

	c.restoreState()
	close(done)

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup

	counts := conn.packetTypeCounts()
	assert.Equal(t, 3, counts[packets.SubscribeType], "should replay regular, advanced, and queue subscriptions")
	assert.Equal(t, 2, counts[packets.PublishType], "should replay all stored outbound messages")

	outbound := c.store.GetAllOutbound()
	require.Len(t, outbound, 1, "only QoS>0 messages should remain stored after replay")

	replay := outbound[0]
	assert.Equal(t, "outbound/qos1", replay.Topic)
	assert.Equal(t, byte(1), replay.QoS)
	assert.True(t, replay.Dup)
	assert.NotZero(t, replay.PacketID)
	assert.NotEqual(t, uint16(9), replay.PacketID)
}

func TestSubscriptionRegistrySnapshotIsolation(t *testing.T) {
	r := newSubscriptionRegistry()

	r.setBasic("metrics/#", 1)
	opt := &SubscribeOption{Topic: "alerts/#", QoS: 2, NoLocal: true, RetainHandling: 1, SubscriptionID: 7}
	r.setOption(opt)

	opt.QoS = 0
	opt.SubscriptionID = 99

	snap := r.snapshot()
	require.Len(t, snap, 2)

	var basicRec, advRec subscriptionRecord
	for _, rec := range snap {
		if rec.topic == "metrics/#" {
			basicRec = rec
		}
		if rec.topic == "alerts/#" { //nolint:goconst // test value
			advRec = rec
		}
	}

	assert.Equal(t, byte(1), basicRec.qos)
	require.NotNil(t, advRec.opt)
	assert.Equal(t, byte(2), advRec.opt.QoS)
	assert.Equal(t, uint32(7), advRec.opt.SubscriptionID)
	assert.NotSame(t, opt, advRec.opt)

	advRec.opt.QoS = 1
	again := r.snapshot()
	for _, rec := range again {
		if rec.topic == "alerts/#" {
			require.NotNil(t, rec.opt)
			assert.Equal(t, byte(2), rec.opt.QoS, "snapshot mutation should not affect registry")
		}
	}

	r.remove("metrics/#")
	afterRemove := r.snapshot()
	require.Len(t, afterRemove, 1)
	assert.Equal(t, "alerts/#", afterRemove[0].topic)
}

func TestHandlePubRelFallsBackToStore(t *testing.T) {
	received := make(chan *Message, 1)
	opts := NewOptions().
		SetClientID("qos2-fallback").
		SetProtocolVersion(5).
		SetOnMessageV2(func(msg *Message) {
			received <- msg
		})

	c, err := New(opts)
	require.NoError(t, err)

	msg := &Message{Topic: "events/qos2", Payload: []byte("payload"), QoS: 2, PacketID: 42}
	require.NoError(t, c.store.StoreInbound(42, msg))

	c.handlePubRel(&v5.PubRel{ID: 42})

	select {
	case got := <-received:
		require.NotNil(t, got)
		assert.Equal(t, "events/qos2", got.Topic)
		assert.Equal(t, []byte("payload"), got.Payload)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected message delivery from inbound store fallback")
	}

	_, ok := c.store.GetInbound(42)
	assert.False(t, ok, "inbound store entry should be deleted after PUBREL")
}

func TestSendPingTriggersConnectionLostOnTimeout(t *testing.T) {
	lost := make(chan error, 1)
	opts := NewOptions().
		SetClientID("ping-timeout").
		SetAutoReconnect(false).
		SetPingTimeout(5 * time.Millisecond).
		SetOnConnectionLost(func(err error) {
			lost <- err
		})

	c, err := New(opts)
	require.NoError(t, err)

	c.state.set(StateConnected)
	setupWriteLoop(c, &packetCaptureConn{})

	c.pingMu.Lock()
	c.waitingPing = true
	c.lastPingSent = time.Now().Add(-50 * time.Millisecond)
	c.pingMu.Unlock()

	c.sendPing()

	select {
	case err := <-lost:
		assert.Equal(t, ErrPingTimeout, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected OnConnectionLost callback")
	}

	assert.Equal(t, StateDisconnected, c.State())

	c.pingMu.Lock()
	waitingPing := c.waitingPing
	c.pingMu.Unlock()
	assert.False(t, waitingPing)
}

func TestDeliverMessageOrderMattersBlocksWhenChannelFull(t *testing.T) {
	c := &Client{
		opts:      NewOptions().SetOrderMatters(true),
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
	}

	c.msgCh = make(chan *Message, 1)
	c.msgCh <- &Message{Topic: "prefill"}

	done := make(chan struct{})
	go func() {
		c.deliverMessage(&Message{Topic: "second"})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("deliverMessage should block when OrderMatters=true and channel is full")
	case <-time.After(25 * time.Millisecond):
	}

	<-c.msgCh

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("deliverMessage should unblock after channel has capacity")
	}
}

func TestDeliverMessageOrderMattersFalseDropsWithoutBlocking(t *testing.T) {
	c := &Client{
		opts: NewOptions().
			SetOrderMatters(false),
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
	}

	c.msgCh = make(chan *Message, 1)
	c.msgCh <- &Message{Topic: "prefill"}

	start := time.Now()
	c.deliverMessage(&Message{Topic: "dropped"})
	assert.Less(t, time.Since(start), 50*time.Millisecond, "deliverMessage should not block when OrderMatters=false")

	msg := <-c.msgCh
	assert.Equal(t, "prefill", msg.Topic, "original message should still be in channel")
}

func TestAsyncAPIsReturnUnderlyingErrors(t *testing.T) {
	c, err := New(NewOptions().SetClientID("async-errors"))
	require.NoError(t, err)

	assert.Equal(t, ErrNotConnected, c.PublishAsync(context.TODO(), "events/test", []byte("payload"), 0, false).Wait())
	assert.Equal(t, ErrInvalidMessage, c.PublishMessageAsync(context.TODO(), nil).Wait())
	assert.Equal(t, ErrNotConnected, c.SubscribeAsync(context.TODO(), map[string]byte{"events/#": 1}).Wait())
	assert.Equal(t, ErrNotConnected, c.UnsubscribeAsync(context.TODO(), "events/#").Wait())

	c.state.set(StateConnected)
	assert.Equal(t, ErrInvalidSubscribeOpt, c.SubscribeWithOptionsAsync(context.TODO(), nil).Wait())
}

func TestSendAuthRequiresV5AndConnection(t *testing.T) {
	// v4 client should reject auth
	c4, err := New(NewOptions().SetClientID("auth-v4").SetProtocolVersion(4))
	require.NoError(t, err)
	assert.Equal(t, ErrAuthNotV5, c4.SendAuth(0x18, []byte("data")))

	// v5 client not connected should reject auth
	c5, err := New(NewOptions().SetClientID("auth-v5").SetProtocolVersion(5).SetAuthMethod("SCRAM-SHA-256"))
	require.NoError(t, err)
	assert.Equal(t, ErrNotConnected, c5.SendAuth(0x18, []byte("data")))

	// v5 client without auth method should reject
	c5nomethod, err := New(NewOptions().SetClientID("auth-v5-no").SetProtocolVersion(5))
	require.NoError(t, err)
	c5nomethod.state.set(StateConnected)
	assert.Equal(t, ErrAuthMethodMissing, c5nomethod.SendAuth(0x18, []byte("data")))
}

func TestHandleAuthCallsOnAuthCallback(t *testing.T) {
	authCalled := make(chan struct{}, 1)
	opts := NewOptions().
		SetClientID("auth-session").
		SetProtocolVersion(5).
		SetAuthMethod("PLAIN").
		SetOnAuth(func(reasonCode byte, authMethod string, authData []byte) ([]byte, error) {
			assert.Equal(t, byte(0x00), reasonCode)
			assert.Equal(t, "PLAIN", authMethod)
			assert.Equal(t, []byte("challenge"), authData)
			authCalled <- struct{}{}
			return []byte("response"), nil
		})

	c, err := New(opts)
	require.NoError(t, err)

	c.state.set(StateConnected)
	conn := &packetCaptureConn{}
	setupWriteLoop(c, conn)

	authPkt := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  0x00, // Success — no response needed
		Properties: &v5.AuthProperties{
			AuthMethod: "PLAIN",
			AuthData:   []byte("challenge"),
		},
	}

	c.handleAuth(authPkt)

	select {
	case <-authCalled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected OnAuth callback to be called")
	}

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestHandleAuthWithoutCallbackDoesNotPanic(t *testing.T) {
	opts := NewOptions().
		SetClientID("auth-no-cb").
		SetProtocolVersion(5)

	c, err := New(opts)
	require.NoError(t, err)

	c.state.set(StateConnected)

	authPkt := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  0x18,
	}

	// Should not panic
	c.handleAuth(authPkt)
}

func TestHandleConnectAuthRequiresMethod(t *testing.T) {
	opts := NewOptions().
		SetClientID("auth-connect-no-method").
		SetProtocolVersion(5).
		SetOnAuth(func(reasonCode byte, authMethod string, authData []byte) ([]byte, error) {
			return []byte("response"), nil
		})

	c, err := New(opts)
	require.NoError(t, err)

	conn := &packetCaptureConn{}
	authPkt := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  0x18,
		Properties: &v5.AuthProperties{
			AuthData: []byte("challenge"),
		},
	}

	assert.Equal(t, ErrAuthMethodMissing, c.handleConnectAuth(conn, authPkt))
}

func TestHandleConnectAuthMethodMismatch(t *testing.T) {
	opts := NewOptions().
		SetClientID("auth-connect-mismatch").
		SetProtocolVersion(5).
		SetAuthMethod("PLAIN").
		SetOnAuth(func(reasonCode byte, authMethod string, authData []byte) ([]byte, error) {
			return []byte("response"), nil
		})

	c, err := New(opts)
	require.NoError(t, err)

	conn := &packetCaptureConn{}
	authPkt := &v5.Auth{
		FixedHeader: packets.FixedHeader{PacketType: packets.AuthType},
		ReasonCode:  0x18,
		Properties: &v5.AuthProperties{
			AuthMethod: "SCRAM-SHA-256",
			AuthData:   []byte("challenge"),
		},
	}

	assert.Equal(t, ErrAuthMethodMismatch, c.handleConnectAuth(conn, authPkt))
}

func TestQueueWriteReturnsWhenWriteLoopExitsWithPendingWrites(t *testing.T) {
	c, err := New(NewOptions().SetClientID("write-loop-exit"))
	require.NoError(t, err)
	c.state.set(StateConnected)

	conn := &blockingFailWriteConn{
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
	setupWriteLoop(c, conn)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- c.queueWrite([]byte("first"), time.Now().Add(time.Second))
	}()

	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first write did not start")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- c.queueWrite([]byte("second"), time.Now().Add(time.Second))
	}()

	close(conn.releaseFirstWrite)

	select {
	case err := <-firstDone:
		require.Error(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first write should not hang")
	}

	select {
	case err := <-secondDone:
		require.ErrorIs(t, err, ErrNotConnected)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second write should not hang when write loop exits")
	}

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestQueueWriteRequestTracksEnqueueRuntimeAcrossRuntimeSwap(t *testing.T) {
	c, err := New(NewOptions().SetClientID("write-runtime-swap"))
	require.NoError(t, err)

	oldWriteCh := make(chan writeRequest, 1)
	oldWriteCh <- writeRequest{data: []byte("prefill")}
	oldRT := &writeRuntime{
		writeCh:        oldWriteCh,
		controlWriteCh: make(chan writeRequest, 1),
		qos0Wake:       make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		writeDone:      make(chan struct{}),
	}
	c.writeRT.Store(oldRT)

	newRT := &writeRuntime{
		writeCh:        make(chan writeRequest, 1),
		controlWriteCh: make(chan writeRequest, 1),
		qos0Wake:       make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
		writeDone:      make(chan struct{}),
	}

	done := make(chan error, 1)
	go func() {
		done <- c.queueWrite([]byte("payload"), time.Now().Add(time.Second))
	}()

	time.Sleep(20 * time.Millisecond)
	c.writeRT.Store(newRT)

	select {
	case <-oldWriteCh:
	default:
		t.Fatal("expected prefilled old write channel")
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for len(oldWriteCh) == 0 && time.Now().Before(deadline) {
		time.Sleep(1 * time.Millisecond)
	}
	if len(oldWriteCh) == 0 {
		t.Fatal("request was not enqueued onto old runtime")
	}

	close(oldRT.stopCh)
	close(oldRT.writeDone)

	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrNotConnected)
	case <-time.After(250 * time.Millisecond):
		close(newRT.stopCh)
		close(newRT.writeDone)
		select {
		case <-done:
		case <-time.After(250 * time.Millisecond):
			t.Fatal("queueWrite should unblock on old runtime shutdown, not wait on swapped runtime")
		}
		t.Fatal("queueWrite waited on swapped runtime channels")
	}
}

func TestWriteLoopResolvesOrphanedQueuedWritesOnExit(t *testing.T) {
	c, err := New(NewOptions().SetClientID("write-loop-orphaned"))
	require.NoError(t, err)
	c.state.set(StateConnected)

	conn := &blockingFailWriteConn{
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
	setupWriteLoop(c, conn)

	firstErr := make(chan error, 1)
	_, err = c.enqueueWrite(writeRequest{
		data:  []byte("first"),
		errCh: firstErr,
	})
	require.NoError(t, err)

	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first write did not start")
	}

	orphanErr := make(chan error, 1)
	_, err = c.enqueueWrite(writeRequest{
		data:  []byte("orphan"),
		errCh: orphanErr,
	})
	require.NoError(t, err)

	close(conn.releaseFirstWrite)

	select {
	case err := <-firstErr:
		require.Error(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first write should not hang")
	}

	select {
	case err := <-orphanErr:
		require.ErrorIs(t, err, ErrNotConnected)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("orphaned queued write should be resolved when write loop exits")
	}

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestControlWriteNoWaitDropsWhenFull(t *testing.T) {
	c := &Client{}
	controlCh := make(chan writeRequest, 1)
	c.writeRT.Store(&writeRuntime{
		writeCh:        make(chan writeRequest, 1),
		controlWriteCh: controlCh,
		stopCh:         make(chan struct{}),
		writeDone:      make(chan struct{}),
	})

	// Fill the control channel.
	c.queueControlWriteNoWait([]byte("first"))

	// Second call should return immediately (drop), not block.
	done := make(chan struct{})
	go func() {
		c.queueControlWriteNoWait([]byte("second"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("queueControlWriteNoWait should not block when channel is full")
	}

	// Only one message should be in the channel.
	assert.Len(t, controlCh, 1)
}

func TestSendPingDroppedControlWriteClearsPingState(t *testing.T) {
	c, err := New(NewOptions().SetClientID("ping-control-drop"))
	require.NoError(t, err)

	controlCh := make(chan writeRequest, 1)
	controlCh <- writeRequest{data: []byte("full")}
	c.writeRT.Store(&writeRuntime{
		controlWriteCh: controlCh,
		stopCh:         make(chan struct{}),
		writeDone:      make(chan struct{}),
	})

	c.sendPing()

	c.pingMu.Lock()
	waiting := c.waitingPing
	lastSent := c.lastPingSent
	c.pingMu.Unlock()

	require.False(t, waiting, "ping should not remain pending if enqueue fails")
	require.True(t, lastSent.IsZero(), "lastPingSent should be cleared when ping enqueue fails")
}

func TestControlWriteNoWaitConcurrentWithCleanupDoesNotPanic(t *testing.T) {
	c, err := New(NewOptions().SetClientID("cleanup-race"))
	require.NoError(t, err)
	c.state.set(StateConnected)
	setupWriteLoop(c, &packetCaptureConn{})

	const workers = 32
	const loops = 200

	start := make(chan struct{})
	panicCh := make(chan any, workers)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
			}()

			<-start
			payload := []byte{byte(id), 0xAA, 0x55}
			for j := 0; j < loops; j++ {
				c.queueControlWriteNoWait(payload)
			}
		}(i)
	}

	close(start)
	time.Sleep(10 * time.Millisecond)

	var cleanupWG sync.WaitGroup
	for i := 0; i < 8; i++ {
		cleanupWG.Add(1)
		go func() {
			defer cleanupWG.Done()
			c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
		}()
	}

	cleanupWG.Wait()
	wg.Wait()
	close(panicCh)

	for p := range panicCh {
		t.Fatalf("unexpected panic during concurrent cleanup/write: %v", p)
	}
}

func TestPublishBufferedWhileDisconnectedAndFlushedOnReconnect(t *testing.T) {
	c, err := New(
		NewOptions().
			SetClientID("reconnect-buffer").
			SetReconnectBufferSize(1024),
	)
	require.NoError(t, err)

	require.NoError(t, c.Publish(nil, "events/a", []byte("hello"), 0, false))

	c.reconnectBufMu.Lock()
	require.Len(t, c.reconnectBuf, 1)
	c.reconnectBufMu.Unlock()

	c.state.set(StateConnected)
	conn := &packetCaptureConn{}
	setupWriteLoop(c, conn)

	c.flushReconnectBuffer()
	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup

	counts := conn.packetTypeCounts()
	assert.Equal(t, 1, counts[packets.PublishType], "buffered publish should be flushed after reconnect")

	c.reconnectBufMu.Lock()
	assert.Len(t, c.reconnectBuf, 0)
	assert.Equal(t, 0, c.reconnectBufBytes)
	c.reconnectBufMu.Unlock()
}

func TestReconnectBufferFullReportsAsyncError(t *testing.T) {
	asyncErr := make(chan error, 1)
	dropped := make(chan *DroppedMessage, 1)
	c, err := New(
		NewOptions().
			SetClientID("reconnect-buffer-full").
			SetReconnectBufferSize(16).
			SetOnAsyncError(func(err error) {
				select {
				case asyncErr <- err:
				default:
				}
			}).
			SetOnDroppedMessage(func(msg *DroppedMessage) {
				select {
				case dropped <- msg:
				default:
				}
			}),
	)
	require.NoError(t, err)

	err = c.Publish(nil, "events/big", []byte("this-payload-is-too-large"), 0, false)
	require.ErrorIs(t, err, ErrReconnectBufferFull)

	select {
	case got := <-asyncErr:
		require.ErrorIs(t, got, ErrReconnectBufferFull)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected reconnect buffer async error callback")
	}

	select {
	case got := <-dropped:
		require.NotNil(t, got)
		assert.Equal(t, DroppedMessageOutbound, got.Direction)
		assert.Equal(t, DroppedReasonReconnectBufferFull, got.Reason)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected reconnect buffer dropped-message callback")
	}
}

func TestSlowConsumerPendingLimitReportsAsyncError(t *testing.T) {
	asyncErr := make(chan error, 2)
	c := &Client{
		opts: NewOptions().
			SetClientID("slow-consumer").
			SetMaxPendingMessages(1).
			SetOnAsyncError(func(err error) {
				select {
				case asyncErr <- err:
				default:
				}
			}),
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
	}

	c.msgCh = make(chan *Message, 1)

	c.deliverMessage(&Message{Topic: "events/1", Payload: []byte("one")})
	c.deliverMessage(&Message{Topic: "events/2", Payload: []byte("two")})
	c.deliverMessage(&Message{Topic: "events/3", Payload: []byte("three")})

	assert.Equal(t, uint64(2), c.DroppedMessages())

	select {
	case got := <-asyncErr:
		require.ErrorIs(t, got, ErrSlowConsumer)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected slow consumer async error callback")
	}

	// Repeated drops while pressure persists should not spam callback.
	select {
	case got := <-asyncErr:
		t.Fatalf("unexpected extra async slow-consumer callback: %v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestControlWritePriorityOverDataQueue(t *testing.T) {
	c, err := New(NewOptions().SetClientID("control-priority"))
	require.NoError(t, err)
	c.state.set(StateConnected)

	conn := &blockFirstWriteConn{
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
	setupWriteLoop(c, conn)

	_, err = c.enqueueWrite(writeRequest{data: []byte{0x01}})
	require.NoError(t, err)
	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first write did not start")
	}

	_, err = c.enqueueWrite(writeRequest{data: []byte{0x02}})
	require.NoError(t, err)
	c.queueControlWriteNoWait([]byte{0x09})

	close(conn.releaseFirstWrite)

	deadline := time.Now().Add(500 * time.Millisecond)
	for len(conn.snapshotWrites()) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	writes := conn.snapshotWrites()
	require.Len(t, writes, 3)
	assert.Equal(t, []byte{0x01}, writes[0])
	assert.Equal(t, []byte{0x09}, writes[1], "control write should preempt queued data writes")
	assert.Equal(t, []byte{0x02}, writes[2])

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestPublishAsyncTokenCompletesFromPendingAck(t *testing.T) {
	c, err := New(NewOptions().SetClientID("async-pending"))
	require.NoError(t, err)
	c.state.set(StateConnected)

	setupWriteLoop(c, &packetCaptureConn{})

	tok := c.PublishAsync(context.Background(), "events/async", []byte("payload"), 1, false)
	require.NotZero(t, tok.MessageID)
	require.ErrorIs(t, tok.WaitTimeout(20*time.Millisecond), ErrTimeout)

	c.pending.complete(tok.MessageID, nil, nil)
	require.NoError(t, tok.Wait())

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestDrainWaitsForInflightPublishes(t *testing.T) {
	c, err := New(NewOptions().SetClientID("drain"))
	require.NoError(t, err)
	c.state.set(StateConnected)
	setupWriteLoop(c, &packetCaptureConn{})

	packetID := c.pending.nextPacketID()
	require.NotZero(t, packetID)
	_, err = c.pending.add(packetID, pendingPublish, &Message{Topic: "events/drain", QoS: 1})
	require.NoError(t, err)

	go func() {
		time.Sleep(30 * time.Millisecond)
		c.pending.complete(packetID, nil, nil)
	}()

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, c.Drain(ctx))
	assert.GreaterOrEqual(t, time.Since(start), 25*time.Millisecond)
	assert.Equal(t, StateDisconnected, c.State())
}

func TestDrainUnblocksWhenPendingPublishIsRemoved(t *testing.T) {
	c, err := New(NewOptions().SetClientID("drain-remove"))
	require.NoError(t, err)
	c.state.set(StateConnected)
	setupWriteLoop(c, &packetCaptureConn{})

	packetID := c.pending.nextPacketID()
	require.NotZero(t, packetID)
	_, err = c.pending.add(packetID, pendingPublish, &Message{Topic: "events/drain-remove", QoS: 1})
	require.NoError(t, err)

	drainDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		drainDone <- c.Drain(ctx)
	}()

	time.Sleep(25 * time.Millisecond)
	c.pending.remove(packetID)

	select {
	case err := <-drainDone:
		require.NoError(t, err)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("drain should unblock when pending publish is removed")
	}
	assert.Equal(t, StateDisconnected, c.State())
}

func TestDrainUnblocksWhenClientIsClosed(t *testing.T) {
	c, err := New(NewOptions().SetClientID("drain-close"))
	require.NoError(t, err)
	c.state.set(StateConnected)
	setupWriteLoop(c, &packetCaptureConn{})

	packetID := c.pending.nextPacketID()
	require.NotZero(t, packetID)
	_, err = c.pending.add(packetID, pendingPublish, &Message{Topic: "events/drain-close", QoS: 1})
	require.NoError(t, err)

	drainDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() {
		drainDone <- c.Drain(ctx)
	}()

	time.Sleep(25 * time.Millisecond)
	require.NoError(t, c.Close(context.Background()))

	select {
	case err := <-drainDone:
		require.ErrorIs(t, err, ErrConnectionLost)
	case <-time.After(300 * time.Millisecond):
		t.Fatal("drain should unblock when client is closed")
	}
}

func TestConnectFailureConcurrentCloseKeepsStateClosed(t *testing.T) {
	for i := 0; i < 256; i++ {
		c, err := New(
			NewOptions().
				SetClientID("connect-close-race").
				SetServers("127.0.0.1:1").
				SetConnectTimeout(15 * time.Millisecond),
		)
		require.NoError(t, err)

		connectDone := make(chan struct{})
		go func() {
			_ = c.Connect(context.Background())
			close(connectDone)
		}()

		closeDone := make(chan struct{})
		go func() {
			defer close(closeDone)
			for {
				select {
				case <-connectDone:
					return
				default:
					_ = c.Close(context.Background())
					runtime.Gosched()
				}
			}
		}()

		select {
		case <-connectDone:
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: connect did not complete", i)
		}
		<-closeDone

		_ = c.Close(context.Background())
		if got := c.State(); got != StateClosed {
			t.Fatalf("iteration %d: expected closed state, got %v", i, got)
		}
	}
}

func TestPublishRejectedWhileDraining(t *testing.T) {
	c, err := New(NewOptions().SetClientID("draining"))
	require.NoError(t, err)
	c.state.set(StateConnected)
	c.draining.Store(true)

	err = c.Publish(nil, "events/draining", []byte("payload"), 0, false)
	require.ErrorIs(t, err, ErrDraining)
}

func TestSlowConsumerDropOldestReplacesQueuedMessage(t *testing.T) {
	c := &Client{
		opts: NewOptions().
			SetClientID("slow-consumer-drop-oldest").
			SetOrderMatters(false).
			SetMaxPendingMessages(1).
			SetSlowConsumerPolicy(SlowConsumerDropOldest),
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
	}
	c.pendingCond = sync.NewCond(&c.pendingMu)
	c.msgCh = make(chan *Message, 1)
	c.msgStop = make(chan struct{})

	c.deliverMessage(&Message{Topic: "events/old", Payload: []byte("old")})
	c.deliverMessage(&Message{Topic: "events/new", Payload: []byte("new")})

	msg := <-c.msgCh
	assert.Equal(t, "events/new", msg.Topic)
	assert.Equal(t, uint64(1), c.DroppedMessages())
}

func TestSlowConsumerBlockWithTimeoutWaitsForCapacity(t *testing.T) {
	c := &Client{
		opts: NewOptions().
			SetClientID("slow-consumer-block").
			SetOrderMatters(false).
			SetMaxPendingMessages(1).
			SetSlowConsumerPolicy(SlowConsumerBlockWithTimeout).
			SetSlowConsumerBlockTimeout(100 * time.Millisecond),
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
	}
	c.pendingCond = sync.NewCond(&c.pendingMu)
	c.msgCh = make(chan *Message, 1)
	c.msgStop = make(chan struct{})

	c.deliverMessage(&Message{Topic: "events/1", Payload: []byte("one")})

	done := make(chan struct{})
	go func() {
		c.deliverMessage(&Message{Topic: "events/2", Payload: []byte("two")})
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("second delivery should block until queue pressure is relieved")
	case <-time.After(25 * time.Millisecond):
	}

	first := <-c.msgCh
	c.releasePending(int64(mqttMessageSize(first)))

	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected blocked delivery to continue after freeing queue capacity")
	}

	second := <-c.msgCh
	assert.Equal(t, "events/2", second.Topic)
}

func TestSlowConsumerBlockWithTimeoutDropsOnTimeout(t *testing.T) {
	c := &Client{
		opts: NewOptions().
			SetClientID("slow-consumer-block-timeout").
			SetOrderMatters(false).
			SetMaxPendingMessages(1).
			SetSlowConsumerPolicy(SlowConsumerBlockWithTimeout).
			SetSlowConsumerBlockTimeout(30 * time.Millisecond),
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
	}
	c.pendingCond = sync.NewCond(&c.pendingMu)
	c.msgCh = make(chan *Message, 1)
	c.msgStop = make(chan struct{})

	c.deliverMessage(&Message{Topic: "events/1", Payload: []byte("one")})

	start := time.Now()
	c.deliverMessage(&Message{Topic: "events/2", Payload: []byte("two")})
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 25*time.Millisecond)
	assert.Less(t, elapsed, 150*time.Millisecond)

	msg := <-c.msgCh
	assert.Equal(t, "events/1", msg.Topic)
	assert.Equal(t, uint64(1), c.DroppedMessages())
}

func TestReconnectStopsAfterMaxAttemptsAndCallsFailureCallback(t *testing.T) {
	failed := make(chan error, 1)
	c, err := New(
		NewOptions().
			SetClientID("reconnect-limit").
			SetServers("127.0.0.1:1").
			SetAutoReconnect(true).
			SetReconnectBackoff(5 * time.Millisecond).
			SetMaxReconnectWait(5 * time.Millisecond).
			SetReconnectJitter(0).
			SetMaxReconnectAttempts(1).
			SetOnReconnectFailed(func(err error) {
				select {
				case failed <- err:
				default:
				}
			}),
	)
	require.NoError(t, err)
	defer c.Close(context.Background())

	c.state.set(StateConnected)
	c.handleConnectionLost(ErrConnectionLost)

	select {
	case got := <-failed:
		require.Error(t, got)
	case <-time.After(2 * time.Second):
		t.Fatal("expected reconnect failed callback")
	}

	assert.Equal(t, StateDisconnected, c.State())
}

func TestSendAuthUsesControlWriteLanePriority(t *testing.T) {
	c, err := New(
		NewOptions().
			SetClientID("send-auth-priority").
			SetProtocolVersion(5).
			SetAuthMethod("PLAIN"),
	)
	require.NoError(t, err)
	c.state.set(StateConnected)

	conn := &blockFirstWriteConn{
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
	setupWriteLoop(c, conn)

	_, err = c.enqueueWrite(writeRequest{data: []byte{0x01}})
	require.NoError(t, err)
	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first data write did not start")
	}

	_, err = c.enqueueWrite(writeRequest{data: []byte{0x02}})
	require.NoError(t, err)

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- c.SendAuth(0x18, []byte("auth-data"))
	}()

	select {
	case <-sendDone:
		t.Fatal("SendAuth should wait while first write is blocked")
	case <-time.After(20 * time.Millisecond):
	}

	close(conn.releaseFirstWrite)

	select {
	case err := <-sendDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SendAuth did not complete")
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for len(conn.snapshotWrites()) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	writes := conn.snapshotWrites()
	require.Len(t, writes, 3)
	assert.Equal(t, []byte{0x01}, writes[0])
	assert.EqualValues(t, packets.AuthType, writes[1][0]>>4, "AUTH should be written through control lane before queued data")
	assert.Equal(t, []byte{0x02}, writes[2])

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestDeliverMessageConcurrentWithStopDispatcherDoesNotPanic(t *testing.T) {
	c, err := New(NewOptions().SetClientID("dispatcher-stop-race").SetOrderMatters(false))
	require.NoError(t, err)

	c.startDispatcher()

	panicCh := make(chan any, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
		}()
		for i := 0; i < 200; i++ {
			c.deliverMessage(&Message{Topic: "events/race"})
		}
	}()

	time.Sleep(5 * time.Millisecond)
	c.stopDispatcher()
	<-done

	select {
	case p := <-panicCh:
		t.Fatalf("unexpected panic while stopping dispatcher: %v", p)
	default:
	}
}

func TestOutboundBackpressureDropNewReturnsErrorAndReportsDrop(t *testing.T) {
	dropped := make(chan *DroppedMessage, 1)
	asyncErr := make(chan error, 1)
	c, err := New(
		NewOptions().
			SetClientID("outbound-drop-new").
			SetMessageChanSize(1).
			SetMaxOutboundPendingMessages(1).
			SetOutboundBackpressurePolicy(OutboundBackpressureDropNew).
			SetOnAsyncError(func(err error) {
				select {
				case asyncErr <- err:
				default:
				}
			}).
			SetOnDroppedMessage(func(msg *DroppedMessage) {
				select {
				case dropped <- msg:
				default:
				}
			}),
	)
	require.NoError(t, err)
	c.state.set(StateConnected)

	conn := &blockFirstWriteConn{
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
	setupWriteLoop(c, conn)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- c.Publish(nil, "events/first", []byte("first"), 0, false)
	}()

	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first publish write did not start")
	}

	err = c.Publish(nil, "events/second", []byte("second"), 0, false)
	require.ErrorIs(t, err, ErrOutboundBackpressure)

	select {
	case got := <-asyncErr:
		require.ErrorIs(t, got, ErrOutboundBackpressure)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected async outbound backpressure error")
	}

	select {
	case got := <-dropped:
		require.NotNil(t, got)
		assert.Equal(t, DroppedMessageOutbound, got.Direction)
		assert.Equal(t, DroppedReasonOutboundBackpressure, got.Reason)
		assert.Equal(t, "events/second", got.Topic)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected outbound dropped-message callback")
	}

	close(conn.releaseFirstWrite)

	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first publish did not complete")
	}

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestOutboundBackpressureBlockWithTimeout(t *testing.T) {
	c, err := New(
		NewOptions().
			SetClientID("outbound-block-timeout").
			SetMessageChanSize(1).
			SetMaxOutboundPendingMessages(1).
			SetOutboundBackpressurePolicy(OutboundBackpressureBlockWithTimeout).
			SetOutboundBlockTimeout(30 * time.Millisecond),
	)
	require.NoError(t, err)
	c.state.set(StateConnected)

	conn := &blockFirstWriteConn{
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
	setupWriteLoop(c, conn)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- c.Publish(nil, "events/first", []byte("first"), 0, false)
	}()

	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first publish write did not start")
	}

	start := time.Now()
	err = c.Publish(nil, "events/second", []byte("second"), 0, false)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrOutboundBackpressure)
	assert.GreaterOrEqual(t, elapsed, 25*time.Millisecond)
	assert.Less(t, elapsed, 200*time.Millisecond)

	close(conn.releaseFirstWrite)
	<-firstDone
	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestOutboundBackpressureBlockWaitsAndSucceeds(t *testing.T) {
	c, err := New(
		NewOptions().
			SetClientID("outbound-block").
			SetMessageChanSize(1).
			SetMaxOutboundPendingMessages(1).
			SetOutboundBackpressurePolicy(OutboundBackpressureBlock),
	)
	require.NoError(t, err)
	c.state.set(StateConnected)

	conn := &blockFirstWriteConn{
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
	setupWriteLoop(c, conn)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- c.Publish(nil, "events/first", []byte("first"), 0, false)
	}()

	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first publish write did not start")
	}

	secondDone := make(chan error, 1)
	go func() {
		secondDone <- c.Publish(nil, "events/second", []byte("second"), 0, false)
	}()

	select {
	case <-secondDone:
		t.Fatal("second publish should block while outbound queue is full")
	case <-time.After(25 * time.Millisecond):
	}

	close(conn.releaseFirstWrite)

	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first publish did not complete")
	}

	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("second publish should complete after outbound capacity frees up")
	}

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestOutboundBackpressureBlockHonorsContextCancellation(t *testing.T) {
	c, err := New(
		NewOptions().
			SetClientID("outbound-block-context").
			SetMessageChanSize(1).
			SetMaxOutboundPendingMessages(1).
			SetOutboundBackpressurePolicy(OutboundBackpressureBlock),
	)
	require.NoError(t, err)
	c.state.set(StateConnected)

	conn := &blockFirstWriteConn{
		firstWriteStarted: make(chan struct{}),
		releaseFirstWrite: make(chan struct{}),
	}
	setupWriteLoop(c, conn)

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- c.Publish(nil, "events/first", []byte("first"), 0, false)
	}()

	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first publish write did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = c.Publish(ctx, "events/second", []byte("second"), 0, false)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, elapsed, 20*time.Millisecond)
	assert.Less(t, elapsed, 250*time.Millisecond)

	close(conn.releaseFirstWrite)
	select {
	case err := <-firstDone:
		require.NoError(t, err)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("first publish did not complete")
	}

	c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
}

func TestDroppedMessageCallbackForSlowConsumer(t *testing.T) {
	dropped := make(chan *DroppedMessage, 2)
	c := &Client{
		opts: NewOptions().
			SetClientID("drop-callback").
			SetOrderMatters(false).
			SetMaxPendingMessages(1).
			SetOnDroppedMessage(func(msg *DroppedMessage) {
				select {
				case dropped <- msg:
				default:
				}
			}),
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
	}
	c.pendingCond = sync.NewCond(&c.pendingMu)
	c.msgCh = make(chan *Message, 1)
	c.msgStop = make(chan struct{})

	c.deliverMessage(&Message{Topic: "events/ok", Payload: []byte("ok")})
	c.deliverMessage(&Message{Topic: "events/dropped", Payload: []byte("drop-me")})

	select {
	case got := <-dropped:
		require.NotNil(t, got)
		assert.Equal(t, DroppedMessageInbound, got.Direction)
		assert.Equal(t, DroppedReasonSlowConsumer, got.Reason)
		assert.Equal(t, "events/dropped", got.Topic)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected dropped-message callback for slow consumer")
	}
}

func TestDroppedMessageCallbackQueueIsBounded(t *testing.T) {
	block := make(chan struct{})
	c, err := New(
		NewOptions().
			SetClientID("drop-callback-bounded").
			SetOrderMatters(false).
			SetMaxPendingMessages(1).
			SetOnDroppedMessage(func(_ *DroppedMessage) {
				<-block
			}),
	)
	require.NoError(t, err)
	defer func() {
		close(block)
		c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup
	}()

	c.msgCh = make(chan *Message, 1)
	c.msgStop = make(chan struct{})
	c.msgCh <- &Message{Topic: "prefill", Payload: []byte("x")}

	before := runtime.NumGoroutine()
	for i := 0; i < 300; i++ {
		c.deliverMessage(&Message{Topic: "events/drop", Payload: []byte("y")})
	}

	time.Sleep(40 * time.Millisecond)
	after := runtime.NumGoroutine()
	assert.Less(t, after-before, 80, "dropped-message callbacks should be bounded and not spawn one goroutine per drop")
}

func TestSoakReconnectStormNoPanic(t *testing.T) {
	c, err := New(
		NewOptions().
			SetClientID("soak-reconnect").
			SetServers("127.0.0.1:1").
			SetAutoReconnect(true).
			SetReconnectBackoff(2 * time.Millisecond).
			SetMaxReconnectWait(4 * time.Millisecond).
			SetReconnectJitter(1 * time.Millisecond).
			SetMaxReconnectAttempts(2),
	)
	require.NoError(t, err)
	defer c.Close(context.Background())

	for i := 0; i < 20; i++ {
		c.state.set(StateConnected)
		c.handleConnectionLost(ErrConnectionLost)
		time.Sleep(5 * time.Millisecond)
	}

	assert.NotEqual(t, StateConnected, c.State())
}

func TestSoakBlockedHandlerPressureNoDeadlock(t *testing.T) {
	c, err := New(
		NewOptions().
			SetClientID("soak-blocked-handler").
			SetOrderMatters(false).
			SetMaxPendingMessages(32).
			SetSlowConsumerPolicy(SlowConsumerBlockWithTimeout).
			SetSlowConsumerBlockTimeout(5 * time.Millisecond).
			SetOnMessage(func(_ string, _ []byte, _ byte) {
				time.Sleep(3 * time.Millisecond)
			}),
	)
	require.NoError(t, err)
	defer c.cleanup(context.Background(), nil) //nolint:errcheck // test cleanup

	c.startDispatcher()

	const total = 300
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c.deliverMessage(&Message{
				Topic:   "events/blocked",
				Payload: []byte{byte(idx % 251)},
				QoS:     0,
			})
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("blocked-handler soak did not finish in time")
	}

	c.stopDispatcher()
}

func TestSoakDrainConcurrentPublishBursts(t *testing.T) {
	c, err := New(NewOptions().SetClientID("soak-drain").SetMessageChanSize(64))
	require.NoError(t, err)
	c.state.set(StateConnected)
	setupWriteLoop(c, &packetCaptureConn{})

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := []byte{byte(id), 0xAA}
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := c.Publish(nil, "events/soak", payload, 0, false)
				if err != nil && err != ErrDraining && err != ErrNotConnected {
					return
				}
			}
		}(i)
	}

	time.Sleep(25 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.Drain(ctx))

	close(stop)
	wg.Wait()
}
