// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt_test

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createMockTCPConnection(t *testing.T) (net.Conn, net.Conn) {
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		serverConn.Close()
		clientConn.Close()
	})
	return serverConn, clientConn
}

func TestNewConnection(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)
	require.NotNil(t, conn)
}

func TestConnectionReadPacketV3(t *testing.T) {
	serverConn, clientConn := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)

	go func() {
		connect := &v3.Connect{
			FixedHeader:     v3.FixedHeader{PacketType: v3.ConnectType},
			ProtocolName:    "MQTT",
			ProtocolVersion: 4,
			CleanSession:    true,
			KeepAlive:       60,
			ClientID:        "test-client",
		}
		encoded := connect.Encode()
		clientConn.Write(encoded)
	}()

	pkt, err := conn.ReadPacket()
	require.NoError(t, err)
	require.NotNil(t, pkt)

	connect, ok := pkt.(*v3.Connect)
	require.True(t, ok)
	assert.Equal(t, "test-client", connect.ClientID)
	assert.Equal(t, byte(4), connect.ProtocolVersion)
}

func TestConnectionReadPacketV5(t *testing.T) {
	serverConn, clientConn := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)

	go func() {
		connect := &v5.Connect{
			FixedHeader:     v5.FixedHeader{PacketType: v5.ConnectType},
			ProtocolName:    "MQTT",
			ProtocolVersion: v5.V5,
			CleanStart:      true,
			KeepAlive:       60,
			ClientID:        "test-client-v5",
		}
		encoded := connect.Encode()
		clientConn.Write(encoded)
	}()

	pkt, err := conn.ReadPacket()
	require.NoError(t, err)
	require.NotNil(t, pkt)

	connect, ok := pkt.(*v5.Connect)
	require.True(t, ok)
	assert.Equal(t, "test-client-v5", connect.ClientID)
	assert.Equal(t, v5.V5, connect.ProtocolVersion)
}

func TestConnectionWritePacket(t *testing.T) {
	serverConn, clientConn := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)

	decodedCh := make(chan *v3.ConnAck, 1)
	errCh := make(chan error, 1)
	go func() {
		pkt, err := v3.ReadPacket(clientConn)
		if err != nil {
			errCh <- err
			return
		}
		ack, ok := pkt.(*v3.ConnAck)
		if !ok {
			errCh <- assert.AnError
			return
		}
		decodedCh <- ack
	}()

	connack := &v3.ConnAck{
		FixedHeader:    v3.FixedHeader{PacketType: v3.ConnAckType},
		SessionPresent: false,
		ReturnCode:     0,
	}

	err := conn.WritePacket(connack)
	require.NoError(t, err)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case receivedConnack := <-decodedCh:
		assert.Equal(t, byte(0), receivedConnack.ReturnCode)
	}
}

func TestConnectionSetKeepAlive(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)

	err := conn.SetKeepAlive(30 * time.Second)
	assert.NoError(t, err)
}

func TestConnectionTouch(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)

	conn.Touch()
	conn.Touch()
}

func TestConnectionOnDisconnect(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)

	called := false
	conn.SetOnDisconnect(func(g bool) {
		called = true
	})

	assert.False(t, called)
}

func TestConnectionReadWriteDeadline(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)

	deadline := time.Now().Add(5 * time.Second)

	err := conn.SetReadDeadline(deadline)
	assert.NoError(t, err)

	err = conn.SetWriteDeadline(deadline)
	assert.NoError(t, err)

	err = conn.SetDeadline(deadline)
	assert.NoError(t, err)
}

func TestConnectionAddresses(t *testing.T) {
	serverConn, _ := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)

	localAddr := conn.LocalAddr()
	assert.NotNil(t, localAddr)

	remoteAddr := conn.RemoteAddr()
	assert.NotNil(t, remoteAddr)
}

func TestConnectionMultiplePackets(t *testing.T) {
	serverConn, clientConn := createMockTCPConnection(t)

	conn := core.NewConnection(serverConn, 0, false)
	go func() {
		_, _ = io.Copy(io.Discard, clientConn)
	}()

	cases := []struct {
		desc string
		pkt  interface {
			Encode() []byte
			Pack(io.Writer) error
		}
	}{
		{
			desc: "connack packet",
			pkt: &v3.ConnAck{
				FixedHeader:    v3.FixedHeader{PacketType: v3.ConnAckType},
				SessionPresent: false,
				ReturnCode:     0,
			},
		},
		{
			desc: "puback packet",
			pkt: &v3.PubAck{
				FixedHeader: v3.FixedHeader{PacketType: v3.PubAckType},
				ID:          1,
			},
		},
		{
			desc: "pingresp packet",
			pkt: &v3.PingResp{
				FixedHeader: v3.FixedHeader{PacketType: v3.PingRespType},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.pkt.Pack(conn)
			assert.NoError(t, err)
		})
	}
}

func TestAsyncConnectionControlPriorityOverData(t *testing.T) {
	rc := newRecordingConn(true)
	conn := core.NewConnection(rc, 8, false)

	pub1 := publishPacket(1)
	pub2 := publishPacket(2)
	ack := &v3.PubAck{FixedHeader: v3.FixedHeader{PacketType: v3.PubAckType}, ID: 99}

	require.NoError(t, conn.WriteDataPacket(pub1, nil))
	require.NoError(t, conn.WriteDataPacket(pub2, nil))
	require.NoError(t, conn.WritePacket(ack))

	rc.unblockWrites()

	first := rc.nextWrite(t)
	second := rc.nextWrite(t)
	third := rc.nextWrite(t)

	types := []byte{packetType(first), packetType(second), packetType(third)}
	ackIdx := -1
	for i, typ := range types {
		if typ == byte(v3.PubAckType) {
			ackIdx = i
			break
		}
	}
	require.NotEqual(t, -1, ackIdx, "control packet was not written")
	assert.Less(t, ackIdx, 2, "control packet should be sent before all queued data packets")
}

func TestAsyncConnectionFairnessUnderControlBurst(t *testing.T) {
	rc := newRecordingConn(false)
	conn := core.NewConnection(rc, 64, false)

	require.NoError(t, conn.WriteDataPacket(publishPacket(1), nil))
	for i := 0; i < 64; i++ {
		ack := &v3.PubAck{FixedHeader: v3.FixedHeader{PacketType: v3.PubAckType}, ID: uint16(100 + i)}
		require.NoError(t, conn.WritePacket(ack))
	}

	seenData := false
	for i := 0; i < 65; i++ {
		if packetType(rc.nextWrite(t)) == byte(v3.PublishType) {
			seenData = true
			break
		}
	}
	require.True(t, seenData, "data packet should not starve under control burst")
}

func TestAsyncConnectionBackpressure(t *testing.T) {
	rc := newRecordingConn(true)
	conn := core.NewConnection(rc, 1, false)

	require.NoError(t, conn.WriteDataPacket(publishPacket(1), nil))
	rc.awaitWriteStart(t)
	require.NoError(t, conn.WriteDataPacket(publishPacket(2), nil))

	done := make(chan error, 1)
	go func() {
		done <- conn.WriteDataPacket(publishPacket(3), nil)
	}()

	select {
	case err := <-done:
		t.Fatalf("expected blocked write, got %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	rc.unblockWrites()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("blocked writer did not unblock")
	}
}

func TestAsyncConnectionDisconnectOnFull(t *testing.T) {
	rc := newRecordingConn(true)
	conn := core.NewConnection(rc, 1, true)

	require.NoError(t, conn.WriteDataPacket(publishPacket(1), nil))
	rc.awaitWriteStart(t)
	require.NoError(t, conn.WriteDataPacket(publishPacket(2), nil))

	err := conn.WriteDataPacket(publishPacket(3), nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, core.ErrSendQueueFull))
}

func TestAsyncConnectionCloseUnblocksBlockedWriter(t *testing.T) {
	rc := newRecordingConn(true)
	conn := core.NewConnection(rc, 1, false)

	require.NoError(t, conn.WriteDataPacket(publishPacket(1), nil))
	rc.awaitWriteStart(t)
	require.NoError(t, conn.WriteDataPacket(publishPacket(2), nil))

	done := make(chan error, 1)
	go func() {
		done <- conn.WriteDataPacket(publishPacket(3), nil)
	}()

	select {
	case <-done:
		t.Fatal("writer unexpectedly unblocked early")
	case <-time.After(100 * time.Millisecond):
	}

	require.NoError(t, conn.Close())

	select {
	case err := <-done:
		require.Error(t, err)
		assert.True(t, errors.Is(err, net.ErrClosed))
	case <-time.After(2 * time.Second):
		t.Fatal("blocked writer did not return after close")
	}
}

func TestAsyncConnectionWriteAfterCloseReturnsClosed(t *testing.T) {
	rc := newRecordingConn(false)
	conn := core.NewConnection(rc, 4, false)
	require.NoError(t, conn.Close())

	err := conn.WriteDataPacket(publishPacket(1), nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, net.ErrClosed))
}

func TestAsyncConnectionCallbackRunsAfterWrite(t *testing.T) {
	rc := newRecordingConn(true)
	conn := core.NewConnection(rc, 4, false)

	called := make(chan struct{}, 1)
	require.NoError(t, conn.WriteDataPacket(publishPacket(10), func() {
		called <- struct{}{}
	}))

	select {
	case <-called:
		t.Fatal("callback fired before wire write")
	case <-time.After(100 * time.Millisecond):
	}

	rc.unblockWrites()
	_ = rc.nextWrite(t)

	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not fire after write")
	}
}

func publishPacket(id uint16) *v3.Publish {
	return &v3.Publish{
		FixedHeader: v3.FixedHeader{PacketType: v3.PublishType, QoS: 1},
		TopicName:   "test/topic",
		ID:          id,
		Payload:     []byte("payload"),
	}
}

func packetType(encoded []byte) byte {
	if len(encoded) == 0 {
		return 0
	}
	return encoded[0] >> 4
}

type recordingConn struct {
	gate         chan struct{}
	gateOnce     sync.Once
	writeStarted chan struct{}
	writes       chan []byte
	closed       atomic.Bool
}

func newRecordingConn(block bool) *recordingConn {
	rc := &recordingConn{
		writeStarted: make(chan struct{}, 16),
		writes:       make(chan []byte, 64),
	}
	if block {
		rc.gate = make(chan struct{})
	}
	return rc
}

func (c *recordingConn) awaitWriteStart(t *testing.T) {
	t.Helper()
	select {
	case <-c.writeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for write start")
	}
}

func (c *recordingConn) unblockWrites() {
	if c.gate == nil {
		return
	}
	c.gateOnce.Do(func() {
		close(c.gate)
	})
}

func (c *recordingConn) nextWrite(t *testing.T) []byte {
	t.Helper()
	select {
	case w := <-c.writes:
		return w
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for wire write")
		return nil
	}
}

func (c *recordingConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (c *recordingConn) Write(b []byte) (int, error) {
	if c.closed.Load() {
		return 0, net.ErrClosed
	}

	select {
	case c.writeStarted <- struct{}{}:
	default:
	}

	if c.gate != nil {
		<-c.gate
	}

	if c.closed.Load() {
		return 0, net.ErrClosed
	}

	cp := make([]byte, len(b))
	copy(cp, b)
	c.writes <- cp
	return len(b), nil
}

func (c *recordingConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.unblockWrites()
	}
	return nil
}

func (c *recordingConn) LocalAddr() net.Addr  { return dummyAddr("local") }
func (c *recordingConn) RemoteAddr() net.Addr { return dummyAddr("remote") }

func (c *recordingConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (d dummyAddr) Network() string { return "tcp" }
func (d dummyAddr) String() string  { return string(d) }
