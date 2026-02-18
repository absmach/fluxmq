// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package messages

import (
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

func TestInflightAddInitialSentAtZero(t *testing.T) {
	tracker := NewInflightTracker(10)
	msg := &storage.Message{Topic: "test", QoS: 1}

	require.NoError(t, tracker.Add(1, msg, Outbound))

	inf, ok := tracker.Get(1)
	require.True(t, ok)
	require.True(t, inf.SentAt.IsZero())
}

func TestInflightGetExpiredSkipsUnsent(t *testing.T) {
	tracker := NewInflightTracker(10)
	msg := &storage.Message{Topic: "test", QoS: 1}

	require.NoError(t, tracker.Add(1, msg, Outbound))

	expired := tracker.GetExpired(0)
	require.Len(t, expired, 0)
}

func TestInflightMarkSentEnablesExpiry(t *testing.T) {
	tracker := NewInflightTracker(10)
	msg := &storage.Message{Topic: "test", QoS: 1}

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
	msg := &storage.Message{Topic: "test", QoS: 1}

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
