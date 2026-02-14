// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
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

func TestRestoreStateReplaysSubscriptionsQueueAndOutbound(t *testing.T) {
	opts := NewOptions().
		SetClientID("restore-client").
		SetProtocolVersion(4).
		SetAckTimeout(50 * time.Millisecond)

	c, err := New(opts)
	require.NoError(t, err)

	c.state.set(StateConnected)
	conn := &packetCaptureConn{}
	c.conn = conn

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
	c.conn = &packetCaptureConn{}

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

func TestDeliverMessageOrderMattersFalseFallsBackWithoutBlocking(t *testing.T) {
	delivered := make(chan *Message, 1)

	c := &Client{
		opts: NewOptions().
			SetOrderMatters(false).
			SetOnMessageV2(func(msg *Message) {
				delivered <- msg
			}),
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
	}

	c.msgCh = make(chan *Message, 1)
	c.msgCh <- &Message{Topic: "prefill"}

	start := time.Now()
	c.deliverMessage(&Message{Topic: "fallback"})
	assert.Less(t, time.Since(start), 50*time.Millisecond, "deliverMessage should not block when OrderMatters=false")

	select {
	case msg := <-delivered:
		require.NotNil(t, msg)
		assert.Equal(t, "fallback", msg.Topic)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("expected direct fallback delivery when message channel is full")
	}
}

func TestAsyncAPIsReturnUnderlyingErrors(t *testing.T) {
	c, err := New(NewOptions().SetClientID("async-errors"))
	require.NoError(t, err)

	assert.Equal(t, ErrNotConnected, c.PublishAsync("events/test", []byte("payload"), 0, false).Wait())
	assert.Equal(t, ErrInvalidMessage, c.PublishMessageAsync(nil).Wait())
	assert.Equal(t, ErrNotConnected, c.SubscribeAsync(map[string]byte{"events/#": 1}).Wait())
	assert.Equal(t, ErrNotConnected, c.UnsubscribeAsync("events/#").Wait())

	c.state.set(StateConnected)
	assert.Equal(t, ErrInvalidSubscribeOpt, c.SubscribeWithOptionsAsync(nil).Wait())
}
