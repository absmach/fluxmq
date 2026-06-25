// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/packets"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/stretchr/testify/require"
)

func TestSendWindow_TryAcquireRespectsCapacity(t *testing.T) {
	w := newSendWindow(2, false)

	require.True(t, w.tryAcquire(1))
	require.True(t, w.tryAcquire(2))
	require.False(t, w.tryAcquire(3), "capacity 2 exhausted")

	// A retransmission of an already-held packet ID does not consume another.
	require.True(t, w.tryAcquire(1))

	// Releasing frees a token for a new packet ID.
	w.release(1)
	require.True(t, w.tryAcquire(3))
	require.False(t, w.tryAcquire(4))
}

func TestSendWindow_ResetClearsHoldsAndRefills(t *testing.T) {
	w := newSendWindow(1, false)
	require.True(t, w.tryAcquire(1))
	require.False(t, w.tryAcquire(2))

	// Reset to a larger capacity drops per-connection holds and refills quota.
	w.reset(3)
	require.True(t, w.tryAcquire(1)) // no longer held, re-acquired fresh
	require.True(t, w.tryAcquire(2))
	require.True(t, w.tryAcquire(3))
	require.False(t, w.tryAcquire(4))
}

func TestSendWindow_AcquireUnblocksOnRelease(t *testing.T) {
	w := newSendWindow(1, true)
	stop := make(chan struct{})
	require.True(t, w.acquire(1, stop))

	got := make(chan bool, 1)
	go func() { got <- w.acquire(2, stop) }()

	time.Sleep(20 * time.Millisecond)
	w.release(1) // frees a token; the blocked acquire must proceed

	select {
	case ok := <-got:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after release")
	}
}

// TestProcessRetries_GatedBySendQuota guards finding #1: a reconnect with a low
// Receive Maximum must not retransmit more unacknowledged PUBLISH packets at
// once than the quota allows. With three expired outbound messages and a quota
// of one, only one is retransmitted per cycle.
func TestProcessRetries_GatedBySendQuota(t *testing.T) {
	f := &fakeInflight{
		expired: []*messages.InflightMessage{
			{PacketID: 1, Direction: messages.Outbound, State: messages.StatePublishSent, Message: &storage.Message{Topic: "t", QoS: 1, Payload: []byte("a")}},
			{PacketID: 2, Direction: messages.Outbound, State: messages.StatePublishSent, Message: &storage.Message{Topic: "t", QoS: 1, Payload: []byte("a")}},
			{PacketID: 3, Direction: messages.Outbound, State: messages.StatePublishSent, Message: &storage.Message{Topic: "t", QoS: 1, Payload: []byte("a")}},
		},
	}
	h := newMessageHandler(f, nil, packets.V5)
	w := &mockWriter{}
	quota := newSendWindow(1, false)

	h.ProcessRetries(w, quota.tryAcquire)
	require.Len(t, w.data, 1, "only one of three expired messages fits the quota of one")

	// Even on a later cycle the quota of one still admits a single PUBLISH
	// (the one holding the token); packets 2 and 3 stay gated.
	w2 := &mockWriter{}
	h.ProcessRetries(w2, quota.tryAcquire)
	require.Len(t, w2.data, 1, "quota of one limits each retry cycle to one PUBLISH")
}

func TestNew_SendWindowClampedByServerMaxInflight(t *testing.T) {
	s := New(
		"client-cap",
		packets.V5,
		Options{CleanStart: true, ReceiveMaximum: 65535},
		messages.NewInflightTracker(16),
		messages.NewMessageQueue(16, true),
		config.SessionConfig{
			MaxInflightMessages: 256,
			InflightOverflow:    config.InflightOverflowBackpressure,
		},
	)
	require.Equal(t, 256, s.serverMaxInflight)

	// A reconnect advertising a huge Receive Maximum is clamped to the server cap.
	_, _ = s.ConnectWithOptions(&testConn{}, ConnectOptions{Version: 5, ReceiveMaximum: 65535})
	require.Equal(t, uint16(256), s.ReceiveMaximum)
	require.Equal(t, 256, s.sendWindow.capacity)
}
