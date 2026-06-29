// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/require"
)

type testAddr string

func (a testAddr) Network() string { return "tcp" }
func (a testAddr) String() string  { return string(a) }

type testConn struct {
	onDisconnect func(graceful bool)
}

func (c *testConn) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (c *testConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *testConn) Close() error                { return nil }
func (c *testConn) LocalAddr() net.Addr         { return testAddr("local") }
func (c *testConn) RemoteAddr() net.Addr        { return testAddr("remote") }
func (c *testConn) SetDeadline(_ time.Time) error {
	return nil
}

func (c *testConn) SetReadDeadline(_ time.Time) error {
	return nil
}

func (c *testConn) SetWriteDeadline(_ time.Time) error {
	return nil
}

func (c *testConn) ReadPacket() (packets.ControlPacket, error) {
	return nil, io.EOF
}
func (c *testConn) WritePacket(_ packets.ControlPacket) error { return nil }
func (c *testConn) WriteControlPacket(_ packets.ControlPacket, onSent func()) error {
	if onSent != nil {
		onSent()
	}
	return nil
}

func (c *testConn) WriteDataPacket(_ packets.ControlPacket, onSent func()) error {
	if onSent != nil {
		onSent()
	}
	return nil
}

func (c *testConn) TryWriteDataPacket(pkt packets.ControlPacket, onSent func()) error {
	return c.WriteDataPacket(pkt, onSent)
}
func (c *testConn) SetKeepAlive(_ time.Duration) error { return nil }
func (c *testConn) SetOnDisconnect(fn func(graceful bool)) {
	c.onDisconnect = fn
}
func (c *testConn) Touch() {}

var _ core.Connection = (*testConn)(nil)

func TestNew_SendWindowCapacityRespectsReceiveMaximum(t *testing.T) {
	inflight := messages.NewInflightTracker(16)
	msg := storage.AcquireMessage()
	msg.Topic = "topic"
	msg.QoS = 1
	msg.SetPayloadFromBytes([]byte("x"))
	require.NoError(t, inflight.Add(1, msg, messages.Outbound))

	s := New(
		"client-1",
		packets.V5,
		Options{CleanStart: true, ReceiveMaximum: 2},
		inflight,
		messages.NewMessageQueue(16, true),
		config.SessionConfig{
			MaxInflightMessages: 128,
			InflightOverflow:    config.InflightOverflowBackpressure,
		},
	)

	// The send window is the connection send quota, separate from the
	// persistent inflight store: capacity == negotiated Receive Maximum, and
	// no token is pre-consumed for restored inflight (those consume on
	// retransmission).
	require.NotNil(t, s.sendWindow)
	require.Equal(t, 2, s.sendWindow.capacity)
	require.Equal(t, 0, len(s.sendWindow.held))

	acked, err := inflight.Ack(1)
	require.NoError(t, err)
	require.NotNil(t, acked)
	acked.ReleasePayload()
	storage.ReleaseMessage(acked)
}

func TestAcquireSendQuota_UnblocksOnDisconnect(t *testing.T) {
	s := New(
		"client-2",
		packets.V5,
		Options{CleanStart: true, ReceiveMaximum: 1},
		messages.NewInflightTracker(8),
		messages.NewMessageQueue(8, true),
		config.SessionConfig{
			InflightOverflow: config.InflightOverflowBackpressure,
		},
	)
	_, errConn := s.Connect(&testConn{})
	require.NoError(t, errConn)
	gen := s.Epoch()

	// Consume the only token, then a second acquire must block until disconnect.
	require.True(t, s.AcquireSendQuota(1, gen))

	result := make(chan bool, 1)
	go func() {
		result <- s.AcquireSendQuota(2, gen)
	}()

	time.Sleep(20 * time.Millisecond)
	require.NoError(t, s.Disconnect(false, v5.DisconnectNormalDisconnection))

	select {
	case ok := <-result:
		require.False(t, ok)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("AcquireSendQuota did not unblock after disconnect")
	}
}

func TestDrainPendingToOffline_ReleasesOriginalMessage(t *testing.T) {
	s := New(
		"client-3",
		packets.V5,
		Options{CleanStart: true, ReceiveMaximum: 8},
		messages.NewInflightTracker(8),
		messages.NewMessageQueue(8, true),
		config.SessionConfig{
			InflightOverflow: config.InflightOverflowQueue,
			PendingQueueSize: 8,
		},
	)
	_, errConn := s.Connect(&testConn{})
	require.NoError(t, errConn)

	msg := storage.AcquireMessage()
	msg.Topic = "topic"
	msg.QoS = 1
	msg.SetPayloadFromBuffer(core.GetBufferWithData([]byte("payload")))
	buf := msg.PayloadBuf
	require.NotNil(t, buf)
	require.Equal(t, int32(1), buf.RefCount())

	require.True(t, s.TryEnqueuePending(msg, nil))
	require.NoError(t, s.Disconnect(false, v5.DisconnectNormalDisconnection))

	require.Equal(t, 1, s.OfflineQueue().Len())
	require.Equal(t, int32(1), buf.RefCount())

	queued := s.OfflineQueue().Dequeue()
	require.NotNil(t, queued)
	queued.ReleasePayload()
	storage.ReleaseMessage(queued)
	require.Equal(t, int32(0), buf.RefCount())
}
