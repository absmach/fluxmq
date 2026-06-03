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
	tracker.messages[1].DeliveryAttemptedAt = time.Now().Add(-neverSentRetryDelay - time.Millisecond)

	expired := tracker.GetExpired(20 * time.Second)
	require.Len(t, expired, 1)
	require.Equal(t, uint16(1), expired[0].PacketID)
}

func TestMarkDeliveryAttempted_ResetsBackoffTimer(t *testing.T) {
	tracker := NewInflightTracker(10)
	require.NoError(t, tracker.Add(1, &storage.Message{Topic: "t", QoS: 1}, Outbound))

	// Simulate: delay elapsed, message is eligible.
	tracker.MarkDeliveryAttempted(1)
	tracker.messages[1].DeliveryAttemptedAt = time.Now().Add(-neverSentRetryDelay - time.Millisecond)
	require.Len(t, tracker.GetExpired(20*time.Second), 1)

	// Simulate failed retry: reset backoff.
	tracker.MarkDeliveryAttempted(1)

	// No longer eligible — timer reset to now.
	require.Empty(t, tracker.GetExpired(20*time.Second))
}
