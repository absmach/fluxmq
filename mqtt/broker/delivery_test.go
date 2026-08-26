// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
	core "github.com/absmach/fluxmq/mqtt"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/stretchr/testify/require"
)

func TestDeliverToSession_MarkSentAfterWireWrite(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()

	s, _, err := b.CreateSession("test-client", 4, session.Options{CleanStart: true, ReceiveMaximum: 10})
	require.NoError(t, err)

	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = clientConn.Close()
	})

	conn := core.NewConnection(serverConn, 1, false)
	_, errConn := s.Connect(conn)
	require.NoError(t, errConn)

	msg := message.NewDelivery(testTopic, []byte("payload"), 1, false)

	packetID, err := b.DeliverToSession(context.Background(), s, msg)
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

func TestConcurrentBorrowedQoS0AndOwnedQoSDeliveries(t *testing.T) {
	b := NewBroker(nil, nil)
	defer b.Close()

	s, _, err := b.CreateSession("concurrent-client", 5, session.Options{CleanStart: true, ReceiveMaximum: 64})
	require.NoError(t, err)
	conn := newSyncConn()
	t.Cleanup(func() { _ = conn.Close() })
	_, err = s.Connect(conn)
	require.NoError(t, err)

	source := message.NewDelivery(testTopic, []byte("shared-payload"), 2, true)
	source.PublisherMeta.Properties = message.NewPropertyMap(map[string]string{"tenant": "acme"})
	payload := source.RetainPayload()

	const borrowedDeliveries = 32
	errs := make(chan error, borrowedDeliveries+2)
	packetIDs := make(chan uint16, 2)
	var wg sync.WaitGroup
	for range borrowedDeliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- b.deliverSharedQoS0(context.Background(), s, source, false)
		}()
	}
	for _, qos := range []byte{1, 2} {
		owned := source.Clone()
		owned.BrokerMeta.Delivery.QoS = qos
		wg.Add(1)
		go func() {
			defer wg.Done()
			packetID, err := b.DeliverToSession(context.Background(), s, owned)
			if err == nil {
				packetIDs <- packetID
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	close(packetIDs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Equal(t, byte(2), source.BrokerMeta.Delivery.QoS)
	tenant, ok := source.PublisherMeta.Properties.Get("tenant")
	require.True(t, ok)
	require.Equal(t, "acme", tenant)

	for _, packet := range conn.writtenPackets() {
		packet.Release()
	}
	for packetID := range packetIDs {
		require.NotZero(t, packetID)
		require.NoError(t, b.AckMessage(s, packetID))
	}
	require.Equal(t, int32(2), payload.RefCount(), "only the source and test-held references should remain")
	message.Release(source)
	payload.Release()
}
