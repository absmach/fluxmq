// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

const testTopic = "test"

func TestInflightAddInitialSentAtZero(t *testing.T) {
	tracker := NewInflightTracker(10)
	msg := &storage.Message{Topic: testTopic, QoS: 1}

	require.NoError(t, tracker.Add(1, msg, Outbound))

	inf, ok := tracker.Get(1)
	require.True(t, ok)
	require.True(t, inf.SentAt.IsZero())
}

func TestInflightGetExpiredSkipsUnsent(t *testing.T) {
	tracker := NewInflightTracker(10)
	msg := &storage.Message{Topic: testTopic, QoS: 1}

	require.NoError(t, tracker.Add(1, msg, Outbound))

	expired := tracker.GetExpired(0)
	require.Len(t, expired, 0)
}

func TestInflightMarkSentEnablesExpiry(t *testing.T) {
	tracker := NewInflightTracker(10)
	msg := &storage.Message{Topic: testTopic, QoS: 1}

	require.NoError(t, tracker.Add(1, msg, Outbound))
	tracker.MarkSent(1)

	inf, ok := tracker.Get(1)
	require.True(t, ok)
	require.False(t, inf.SentAt.IsZero())

	expired := tracker.GetExpired(0)
	require.Len(t, expired, 1)
}

func TestInflightMarkRetryUpdatesTimestampAndCount(t *testing.T) {
	tracker := NewInflightTracker(10)
	msg := &storage.Message{Topic: testTopic, QoS: 1}

	require.NoError(t, tracker.Add(1, msg, Outbound))
	tracker.MarkSent(1)

	before, ok := tracker.Get(1)
	require.True(t, ok)
	require.Equal(t, 0, before.Retries)

	time.Sleep(2 * time.Millisecond)
	require.NoError(t, tracker.MarkRetry(1))
	after, ok := tracker.Get(1)
	require.True(t, ok)
	require.Equal(t, 1, after.Retries)
	require.True(t, after.SentAt.After(before.SentAt))
}

func TestGetExpired_NeverSentNotEligibleWithoutDeliveryAttempt(t *testing.T) {
	tracker := NewInflightTracker(10)
	require.NoError(t, tracker.Add(1, &storage.Message{Topic: "t", QoS: 1}, Outbound))

	require.Empty(t, tracker.GetExpired(0))
}

func TestGetExpired_NeverSentEligibleAfterDelay(t *testing.T) {
	tracker := NewInflightTracker(10)
	require.NoError(t, tracker.Add(1, &storage.Message{Topic: "t", QoS: 1}, Outbound))
	tracker.MarkDeliveryAttempted(1)

	// Not eligible immediately after mark.
	require.Empty(t, tracker.GetExpired(20*time.Second))

	// Backdate to simulate neverSentRetryDelay elapsed.
	tracker.messages[inflightKey{direction: Outbound, packetID: 1}].DeliveryAttemptedAt = time.Now().Add(-neverSentRetryDelay - time.Millisecond)

	expired := tracker.GetExpired(20 * time.Second)
	require.Len(t, expired, 1)
	require.Equal(t, uint16(1), expired[0].PacketID)
}

func TestMarkDeliveryAttempted_ResetsBackoffTimer(t *testing.T) {
	tracker := NewInflightTracker(10)
	require.NoError(t, tracker.Add(1, &storage.Message{Topic: "t", QoS: 1}, Outbound))

	// Simulate: delay elapsed, message is eligible.
	tracker.MarkDeliveryAttempted(1)
	tracker.messages[inflightKey{direction: Outbound, packetID: 1}].DeliveryAttemptedAt = time.Now().Add(-neverSentRetryDelay - time.Millisecond)
	require.Len(t, tracker.GetExpired(20*time.Second), 1)

	// Simulate failed retry: reset backoff.
	tracker.MarkDeliveryAttempted(1)

	// No longer eligible — timer reset to now.
	require.Empty(t, tracker.GetExpired(20*time.Second))
}

func TestInflight_IndependentDirectionalCapacity(t *testing.T) {
	tr := NewInflightTracker(2)

	require.NoError(t, tr.Add(1, &storage.Message{Topic: "t"}, Outbound))
	require.NoError(t, tr.Add(2, &storage.Message{Topic: "t"}, Outbound))
	require.ErrorIs(t, tr.Add(3, &storage.Message{Topic: "t"}, Outbound), ErrInflightFull)

	// Inbound has its own capacity; a full outbound direction must not reject
	// inbound up to the advertised Receive Maximum.
	require.NoError(t, tr.Add(1, &storage.Message{Topic: "t"}, Inbound))
	require.NoError(t, tr.Add(2, &storage.Message{Topic: "t"}, Inbound))
	require.ErrorIs(t, tr.Add(3, &storage.Message{Topic: "t"}, Inbound), ErrInflightFull)
}

func TestInflight_DirectionalKeysDoNotCollide(t *testing.T) {
	tr := NewInflightTracker(8)

	out := &storage.Message{Topic: "outbound"}
	in := &storage.Message{Topic: "inbound"}
	require.NoError(t, tr.Add(5, out, Outbound))
	require.NoError(t, tr.Add(5, in, Inbound)) // same packet ID, opposite direction

	gotOut, err := tr.Ack(5)
	require.NoError(t, err)
	require.Equal(t, "outbound", gotOut.Topic, "inbound add must not overwrite the outbound entry")

	gotIn, err := tr.AckInbound(5)
	require.NoError(t, err)
	require.Equal(t, "inbound", gotIn.Topic)
}

func TestInflight_InvalidDirectionReturnsErrorNotPanic(t *testing.T) {
	tr := NewInflightTracker(4)
	// A corrupt/out-of-range direction must be rejected, not index counts[] out
	// of range and panic.
	err := tr.Add(1, &storage.Message{Topic: "t"}, Direction(99))
	require.ErrorIs(t, err, ErrInvalidDirection)
}

func TestInflight_DuplicateOnFullWindowAccepted(t *testing.T) {
	tr := NewInflightTracker(1)
	require.NoError(t, tr.Add(7, &storage.Message{Topic: "first"}, Inbound))

	// The inbound window is full (capacity 1). A retransmitted PUBLISH reusing
	// the same packet ID must be accepted (update), not rejected with
	// ErrInflightFull.
	require.NoError(t, tr.Add(7, &storage.Message{Topic: "second"}, Inbound))

	// A different packet ID is still rejected.
	require.ErrorIs(t, tr.Add(8, &storage.Message{Topic: "t"}, Inbound), ErrInflightFull)

	got, err := tr.AckInbound(7)
	require.NoError(t, err)
	require.Equal(t, "second", got.Topic)
}
