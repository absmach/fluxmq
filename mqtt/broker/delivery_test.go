// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	core "github.com/absmach/fluxmq/mqtt"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

func TestDeliverToSession_MarkSentAfterWireWrite(t *testing.T) {
	b := NewBroker(nil, nil, nil, nil, nil, nil, nil, config.SessionConfig{}, config.TransportConfig{}, config.BrokerConfig{})
	defer b.Close()

	s, _, err := b.CreateSession("test-client", 4, session.Options{CleanStart: true, ReceiveMaximum: 10})
	require.NoError(t, err)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})

	conn := core.NewConnection(serverConn, 1, false)
	require.NoError(t, s.Connect(conn))

	msg := storage.AcquireMessage()
	msg.Topic = "test/topic"
	msg.QoS = 1
	msg.SetPayloadFromBytes([]byte("payload"))

	packetID, err := b.DeliverToSession(s, msg)
	require.NoError(t, err)
	require.NotZero(t, packetID)

	inf, ok := s.Inflight().Get(packetID)
	require.True(t, ok)
	require.True(t, inf.SentAt.IsZero(), "SentAt should remain zero until socket write succeeds")

	readDone := make(chan struct{})
	go func() {
		_, _ = v3.ReadPacket(clientConn)
		close(readDone)
	}()

	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for packet write")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		updated, ok := s.Inflight().Get(packetID)
		require.True(t, ok)
		if !updated.SentAt.IsZero() {
			require.NoError(t, b.AckMessage(s, packetID))
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("SentAt did not update after successful wire write")
}
