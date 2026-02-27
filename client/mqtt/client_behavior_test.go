// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"io"
	"net"
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
	c.conn = conn
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	close(c.doneCh) // no readLoop in tests — mark as already exited
	c.writeCh = make(chan writeRequest, 256)
	c.controlWriteCh = make(chan writeRequest, 64)
	c.writeDone = make(chan struct{})
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

	c.cleanup(nil)

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
		if rec.topic == "alerts/#" {
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

	c.cleanup(nil)
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

	c.cleanup(nil)
}

func TestControlWriteNoWaitDropsWhenFull(t *testing.T) {
	c := &Client{
		writeCh:        make(chan writeRequest, 1),
		controlWriteCh: make(chan writeRequest, 1),
		stopCh:         make(chan struct{}),
		writeDone:      make(chan struct{}),
	}

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
	assert.Len(t, c.controlWriteCh, 1)
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
			c.cleanup(nil)
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
	c.cleanup(nil)

	counts := conn.packetTypeCounts()
	assert.Equal(t, 1, counts[packets.PublishType], "buffered publish should be flushed after reconnect")

	c.reconnectBufMu.Lock()
	assert.Len(t, c.reconnectBuf, 0)
	assert.Equal(t, 0, c.reconnectBufBytes)
	c.reconnectBufMu.Unlock()
}

func TestReconnectBufferFullReportsAsyncError(t *testing.T) {
	asyncErr := make(chan error, 1)
	c, err := New(
		NewOptions().
			SetClientID("reconnect-buffer-full").
			SetReconnectBufferSize(16).
			SetOnAsyncError(func(err error) {
				select {
				case asyncErr <- err:
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

	require.NoError(t, c.enqueueWrite(writeRequest{data: []byte{0x01}}))
	select {
	case <-conn.firstWriteStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("first write did not start")
	}

	require.NoError(t, c.enqueueWrite(writeRequest{data: []byte{0x02}}))
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

	c.cleanup(nil)
}
