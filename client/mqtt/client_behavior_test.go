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
// Cleanup is handled by the client's own cleanup() or Disconnect() — no explicit teardown needed,
// but callers that don't trigger cleanup should close stopCh+writeCh themselves.
func setupWriteLoop(c *Client, conn net.Conn) {
	c.conn = conn
	c.stopCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	close(c.doneCh) // no readLoop in tests — mark as already exited
	c.writeCh = make(chan writeRequest, 256)
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
