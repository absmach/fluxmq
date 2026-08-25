// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	payloadbuf "github.com/absmach/fluxmq/payload"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testTopic is the topic used by the payload-ownership tests.
const testTopic = "sensors/temp"

// TestEncodePublishRetainsPayloadBuffer verifies that an encoded PUBLISH keeps
// the message's payload buffer alive on its own: releasing the message must not
// recycle the buffer the packet still points into, and releasing the packet must.
func TestEncodePublishRetainsPayloadBuffer(t *testing.T) {
	for _, version := range []struct {
		name string
		id   byte
	}{
		{"v3", packets.V311},
		{"v5", packets.V5},
	} {
		t.Run(version.name, func(t *testing.T) {
			pool := payloadbuf.NewPoolWithCapacity(4, 4, 4)
			buf := pool.FromBytes([]byte("payload-bytes"))

			msg := message.Acquire()
			msg.Topic = testTopic
			msg.SetPayloadBuffer(buf)

			pkt := EncodePublish(msg, 7, version.id, false)

			// The publisher is done with the message as soon as the packet exists.
			msg.ReleasePayload()
			message.Release(msg)

			require.Equal(t, int32(1), buf.RefCount(),
				"packet must hold the only remaining reference after the message is released")
			assert.NotSame(t, buf, pool.Get(len("payload-bytes")),
				"buffer must not be back in the pool while a queued packet points into it")

			pkt.Release()
			assert.Equal(t, int32(0), buf.RefCount(), "releasing the packet must drop its reference")
		})
	}
}

// TestAsyncDeliveryKeepsPayloadIntact reproduces the corruption path end to end:
// with an asynchronous send queue the packet is serialized long after the
// publisher released the message, so a payload buffer recycled in between would
// be transmitted with another message's bytes.
func TestAsyncDeliveryKeepsPayloadIntact(t *testing.T) {
	const payload = "the-original-payload"

	conn := newBlockingConn()
	c := core.NewConnection(conn, 16, false)
	t.Cleanup(func() { c.Close() }) //nolint:errcheck // best-effort teardown

	// Park the send loop inside a socket write so the next packet stays queued
	// and unserialized for the rest of the test.
	require.NoError(t, c.TryWriteDataPacket(&v3.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}, nil))
	conn.awaitWrite(t)

	pool := payloadbuf.NewPoolWithCapacity(4, 4, 4)
	buf := pool.FromBytes([]byte(payload))

	msg := message.Acquire()
	msg.Topic = testTopic
	msg.SetPayloadBuffer(buf)

	pkt := EncodePublish(msg, 0, packets.V311, false)
	require.NoError(t, c.TryWriteDataPacket(pkt, nil))

	// QoS 0 delivery drops the message's reference immediately after queueing.
	msg.ReleasePayload()
	message.Release(msg)

	// A concurrent publish reusing a recycled buffer would overwrite the bytes
	// the queued packet points into.
	overwrite := pool.Get(len(payload))
	require.NotSame(t, buf, overwrite, "payload buffer must not be recycled while a queued packet points into it")
	copy(overwrite.Bytes(), bytes.Repeat([]byte("X"), len(payload)))

	conn.unblock()

	var sent []byte
	require.Eventually(t, func() bool {
		sent = conn.written()
		return bytes.Contains(sent, []byte(testTopic))
	}, 5*time.Second, time.Millisecond, "send loop never serialized the queued PUBLISH")
	require.NoError(t, c.Close())

	require.Contains(t, string(sent), payload, "queued PUBLISH must carry the payload it was encoded with")
	assert.NotContains(t, string(sent), "XXXX", "recycled buffer bytes must not reach the wire")
}

// blockingConn is a net.Conn whose first Write blocks until unblock is called,
// which parks the connection's send loop at a known point.
type blockingConn struct {
	mu       sync.Mutex
	buf      bytes.Buffer
	writing  chan struct{}
	release  chan struct{}
	once     sync.Once
	unblocks sync.Once
}

func newBlockingConn() *blockingConn {
	return &blockingConn{
		writing: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *blockingConn) Write(b []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	<-c.release

	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(b)
}

// awaitWrite blocks until the send loop has entered its first socket write.
func (c *blockingConn) awaitWrite(t *testing.T) {
	t.Helper()
	select {
	case <-c.writing:
	case <-time.After(5 * time.Second):
		t.Fatal("send loop never reached the socket write")
	}
}

func (c *blockingConn) unblock() {
	c.unblocks.Do(func() { close(c.release) })
}

func (c *blockingConn) written() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]byte(nil), c.buf.Bytes()...)
}

func (c *blockingConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *blockingConn) Close() error                     { c.unblock(); return nil }
func (c *blockingConn) LocalAddr() net.Addr              { return pipeAddr{} }
func (c *blockingConn) RemoteAddr() net.Addr             { return pipeAddr{} }
func (c *blockingConn) SetDeadline(time.Time) error      { return nil }
func (c *blockingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *blockingConn) SetWriteDeadline(time.Time) error { return nil }

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }
