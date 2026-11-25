// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt_test

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/stretchr/testify/require"
)

// A write's onSent callback takes session locks, and a disconnect writes its
// DISCONNECT while holding the session lock. If a synchronous connection ran
// onSent while still holding its own write lock, a delivery finishing as the
// client disconnected would hold one lock each and wait for the other.
func TestSyncWriteRunsOnSentAfterReleasingTheConnection(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	go func() { _, _ = io.Copy(io.Discard, client) }()
	conn := core.NewConnection(server, 0, false)

	var sessionLock sync.Mutex
	sessionLock.Lock()
	inOnSent := make(chan struct{})
	delivered := make(chan error, 1)
	go func() {
		publish := &v5.Publish{
			FixedHeader: packets.FixedHeader{PacketType: packets.PublishType, QoS: 1},
			TopicName:   "t",
			ID:          1,
			Payload:     []byte("p"),
		}
		delivered <- conn.WriteDataPacket(publish, func() {
			close(inOnSent)
			sessionLock.Lock() // what MarkSentIfEpoch does
			sessionLock.Unlock()
		})
	}()
	<-inOnSent

	// The disconnect, holding the session lock, writes to the same connection.
	disconnect := &v5.Disconnect{FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType}}
	written := make(chan error, 1)
	go func() { written <- conn.WritePacket(disconnect) }()
	select {
	case err := <-written:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("a write waited on a connection held by a callback waiting on the writer's lock")
	}
	sessionLock.Unlock()
	require.NoError(t, <-delivered)
}
