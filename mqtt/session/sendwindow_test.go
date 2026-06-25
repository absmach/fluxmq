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
	const gen = 1
	w := newSendWindow(2, gen, false)

	require.True(t, w.tryAcquire(1, gen))
	require.True(t, w.tryAcquire(2, gen))
	require.False(t, w.tryAcquire(3, gen), "capacity 2 exhausted")

	// A retransmission of an already-held packet ID does not consume another.
	require.True(t, w.tryAcquire(1, gen))

	// Releasing frees a token for a new packet ID.
	w.release(1, gen)
	require.True(t, w.tryAcquire(3, gen))
	require.False(t, w.tryAcquire(4, gen))
}

func TestSendWindow_ResetClearsHoldsAndRefills(t *testing.T) {
	w := newSendWindow(1, 1, false)
	require.True(t, w.tryAcquire(1, 1))
	require.False(t, w.tryAcquire(2, 1))

	// Reset advances the generation, drops holds, and refills quota.
	w.reset(3, 2)
	require.False(t, w.tryAcquire(1, 1), "stale generation cannot acquire")
	require.True(t, w.tryAcquire(1, 2)) // re-acquired fresh in new generation
	require.True(t, w.tryAcquire(2, 2))
	require.True(t, w.tryAcquire(3, 2))
	require.False(t, w.tryAcquire(4, 2))
}

func TestSendWindow_StaleGenerationCannotAcquireOrRelease(t *testing.T) {
	w := newSendWindow(1, 1, false)
	require.True(t, w.tryAcquire(1, 1))

	// Takeover: new generation 2.
	w.reset(1, 2)
	require.True(t, w.tryAcquire(10, 2)) // gen 2 holds its one token

	// A leftover gen-1 release must not free gen-2's token.
	w.release(10, 1)
	require.False(t, w.tryAcquire(11, 2), "stale release must not free current-generation quota")

	// The current-generation release does free it.
	w.release(10, 2)
	require.True(t, w.tryAcquire(11, 2))
}

func TestSendWindow_AcquireUnblocksOnRelease(t *testing.T) {
	const gen = 1
	w := newSendWindow(1, gen, true)
	stop := make(chan struct{})
	require.True(t, w.acquire(1, gen, stop))

	got := make(chan bool, 1)
	go func() { got <- w.acquire(2, gen, stop) }()

	time.Sleep(20 * time.Millisecond)
	w.release(1, gen) // frees a token; the blocked acquire must proceed

	select {
	case ok := <-got:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after release")
	}
}

func TestSendWindow_AcquireUnblocksWhenSuperseded(t *testing.T) {
	const gen = 1
	w := newSendWindow(1, gen, true)
	stop := make(chan struct{})
	require.True(t, w.acquire(1, gen, stop))

	got := make(chan bool, 1)
	go func() { got <- w.acquire(2, gen, stop) }()

	time.Sleep(20 * time.Millisecond)
	w.reset(1, 2) // takeover: the blocked gen-1 acquire must return false

	select {
	case ok := <-got:
		require.False(t, ok, "a superseded acquire must not succeed")
	case <-time.After(time.Second):
		t.Fatal("acquire did not unblock after takeover")
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
	const gen = 1
	quota := newSendWindow(1, gen, false)
	acquire := func(packetID uint16) bool { return quota.tryAcquire(packetID, gen) }

	h.ProcessRetries(w, acquire)
	require.Len(t, w.data, 1, "only one of three expired messages fits the quota of one")

	// Even on a later cycle the quota of one still admits a single PUBLISH
	// (the one holding the token); packets 2 and 3 stay gated.
	w2 := &mockWriter{}
	h.ProcessRetries(w2, acquire)
	require.Len(t, w2.data, 1, "quota of one limits each retry cycle to one PUBLISH")
}

// TestSendQuota_LeaseIsGenerationBound guards finding #1: a delivery that
// acquired quota under one connection generation cannot, after a takeover,
// consume or free the replacement generation's quota. The session API carries
// the generation through acquire and release.
func TestSendQuota_LeaseIsGenerationBound(t *testing.T) {
	s := newTakeoverSession(t)

	_, err := s.Connect(&testConn{})
	require.NoError(t, err)
	gen0 := s.Epoch()

	conn0, version0, capturedGen := s.DeliveryLease()
	require.NotNil(t, conn0)
	require.Equal(t, byte(4), version0)
	require.Equal(t, gen0, capturedGen)
	require.True(t, s.TryAcquireSendQuota(1, gen0))

	// Takeover advances the generation and refills the quota.
	_, _ = s.ConnectWithOptions(&testConn{}, ConnectOptions{Version: 4, ReceiveMaximum: 1})
	gen1 := s.Epoch()
	require.Greater(t, gen1, gen0)

	// The old-generation delivery can neither acquire nor free new-generation
	// quota.
	require.False(t, s.TryAcquireSendQuota(2, gen0), "stale generation cannot acquire")
	s.ReleaseSendQuota(1, gen0) // stale release: no-op

	// The new generation has its full quota of one available.
	require.True(t, s.TryAcquireSendQuota(3, gen1))
	require.False(t, s.TryAcquireSendQuota(4, gen1), "new-generation quota of one is now exhausted")
}

// TestNew_ServerMaxInflightDefaultMatchesBroker guards finding #4: an unset
// MaxInflightMessages must default to the shared value, not 65535.
func TestNew_ServerMaxInflightDefaultMatchesBroker(t *testing.T) {
	s := New(
		"client-default",
		packets.V5,
		Options{CleanStart: true},
		messages.NewInflightTracker(16),
		messages.NewMessageQueue(16, true),
		config.SessionConfig{InflightOverflow: config.InflightOverflowBackpressure},
	)
	require.Equal(t, config.DefaultMaxInflightMessages, s.serverMaxInflight)
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

// TestMarkSentIfEpoch_StaleGenerationNoOp guards finding #2: marking sent is
// scoped to the connection generation under the session lock, so a stale onSent
// from a displaced connection cannot mark the shared inflight entry as sent.
func TestMarkSentIfEpoch_StaleGenerationNoOp(t *testing.T) {
	s := newTakeoverSession(t)
	_, err := s.Connect(&testConn{})
	require.NoError(t, err)
	gen := s.Epoch()

	require.NoError(t, s.Inflight().Add(1, &storage.Message{Topic: "t", QoS: 1}, messages.Outbound))

	// Stale generation: must not mark sent.
	s.MarkSentIfEpoch(1, gen-1)
	inf, ok := s.Inflight().Get(1)
	require.True(t, ok)
	require.True(t, inf.SentAt.IsZero(), "stale generation must not mark sent")

	// Current generation: marks sent.
	s.MarkSentIfEpoch(1, gen)
	inf, ok = s.Inflight().Get(1)
	require.True(t, ok)
	require.False(t, inf.SentAt.IsZero(), "current generation marks sent")
}
