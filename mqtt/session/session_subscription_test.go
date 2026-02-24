// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"fmt"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

func TestHasSubscription(t *testing.T) {
	s := &Session{
		subscriptions: map[string]storage.SubscribeOptions{
			"devices/+/status": {},
		},
	}

	require.True(t, s.HasSubscription("devices/+/status"))
	require.False(t, s.HasSubscription("devices/+/telemetry"))
}

func TestShouldUpdateHeartbeat(t *testing.T) {
	s := &Session{}
	base := time.Unix(100, 0)

	require.True(t, s.ShouldUpdateHeartbeat(base, time.Second))
	require.False(t, s.ShouldUpdateHeartbeat(base.Add(500*time.Millisecond), time.Second))
	require.True(t, s.ShouldUpdateHeartbeat(base.Add(1*time.Second), time.Second))
}

func BenchmarkSubscriptionLookup_GetSubscriptions(b *testing.B) {
	subs := make(map[string]storage.SubscribeOptions, 1000)
	for i := 0; i < 1000; i++ {
		subs[fmt.Sprintf("devices/%d/status", i)] = storage.SubscribeOptions{}
	}
	s := &Session{subscriptions: subs}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = s.GetSubscriptions()
	}
}

func BenchmarkSubscriptionLookup_HasSubscription(b *testing.B) {
	subs := make(map[string]storage.SubscribeOptions, 1000)
	for i := 0; i < 1000; i++ {
		subs[fmt.Sprintf("devices/%d/status", i)] = storage.SubscribeOptions{}
	}
	s := &Session{subscriptions: subs}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = s.HasSubscription("devices/999/status")
	}
}
