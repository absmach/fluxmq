// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"fmt"
	"sync"
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLockFreeRouter_BasicSubscribeMatch(t *testing.T) {
	r := NewLockFreeRouter()

	// Subscribe to a simple topic
	err := r.Subscribe("client1", "sensor/temperature", 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	// Match should find the subscription
	subs, err := r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.Equal(t, "client1", subs[0].ClientID)
	assert.Equal(t, "sensor/temperature", subs[0].Filter)
	assert.Equal(t, byte(1), subs[0].QoS)

	// Non-matching topic
	subs, err = r.Match("sensor/humidity")
	require.NoError(t, err)
	assert.Len(t, subs, 0)
}

func TestLockFreeRouter_MultipleSubscriptions(t *testing.T) {
	r := NewLockFreeRouter()

	// Multiple clients subscribe to same topic
	err := r.Subscribe("client1", "sensor/temperature", 1, storage.SubscribeOptions{})
	require.NoError(t, err)
	err = r.Subscribe("client2", "sensor/temperature", 2, storage.SubscribeOptions{})
	require.NoError(t, err)

	subs, err := r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 2)

	// Both clients should be in results
	clientIDs := []string{subs[0].ClientID, subs[1].ClientID}
	assert.Contains(t, clientIDs, "client1")
	assert.Contains(t, clientIDs, "client2")
}

func TestLockFreeRouter_SingleLevelWildcard(t *testing.T) {
	r := NewLockFreeRouter()

	// Subscribe with + wildcard
	err := r.Subscribe("client1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	// Should match
	subs, err := r.Match("sensor/room1/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.Equal(t, "client1", subs[0].ClientID)

	subs, err = r.Match("sensor/room2/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 1)

	// Should not match (different number of levels)
	subs, err = r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 0)

	subs, err = r.Match("sensor/room1/floor1/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 0)
}

func TestLockFreeRouter_MultiLevelWildcard(t *testing.T) {
	r := NewLockFreeRouter()

	// Subscribe with # wildcard
	err := r.Subscribe("client1", "sensor/#", 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	// Should match all under sensor/
	testCases := []string{
		"sensor/temperature",
		"sensor/room1/temperature",
		"sensor/room1/floor1/temperature",
	}

	for _, topic := range testCases {
		subs, err := r.Match(topic)
		require.NoError(t, err)
		assert.Len(t, subs, 1, "Failed for topic: %s", topic)
		assert.Equal(t, "client1", subs[0].ClientID)
	}

	// Should not match
	subs, err := r.Match("device/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 0)
}

func TestLockFreeRouter_CombinedWildcards(t *testing.T) {
	r := NewLockFreeRouter()

	// Multiple subscriptions with different wildcards
	err := r.Subscribe("client1", "sensor/+/temperature", 1, storage.SubscribeOptions{})
	require.NoError(t, err)
	err = r.Subscribe("client2", "sensor/#", 1, storage.SubscribeOptions{})
	require.NoError(t, err)
	err = r.Subscribe("client3", "sensor/room1/temperature", 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	// This should match all three
	subs, err := r.Match("sensor/room1/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 3)

	clientIDs := make(map[string]bool)
	for _, sub := range subs {
		clientIDs[sub.ClientID] = true
	}
	assert.True(t, clientIDs["client1"])
	assert.True(t, clientIDs["client2"])
	assert.True(t, clientIDs["client3"])
}

func TestLockFreeRouter_Unsubscribe(t *testing.T) {
	r := NewLockFreeRouter()

	// Subscribe
	err := r.Subscribe("client1", "sensor/temperature", 1, storage.SubscribeOptions{})
	require.NoError(t, err)
	err = r.Subscribe("client2", "sensor/temperature", 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	// Verify both subscribed
	subs, err := r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 2)

	// Unsubscribe client1
	err = r.Unsubscribe("client1", "sensor/temperature")
	require.NoError(t, err)

	// Only client2 should remain
	subs, err = r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.Equal(t, "client2", subs[0].ClientID)

	// Unsubscribe client2
	err = r.Unsubscribe("client2", "sensor/temperature")
	require.NoError(t, err)

	// No subscriptions remain
	subs, err = r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 0)
}

func TestLockFreeRouter_UnsubscribeNonExistent(t *testing.T) {
	r := NewLockFreeRouter()

	// Unsubscribe from non-existent subscription (should not error)
	err := r.Unsubscribe("client1", "sensor/temperature")
	require.NoError(t, err)

	// Unsubscribe from non-existent path
	err = r.Unsubscribe("client1", "does/not/exist")
	require.NoError(t, err)
}

func TestLockFreeRouter_ConcurrentReads(t *testing.T) {
	r := NewLockFreeRouter()

	// Setup subscriptions
	for i := 0; i < 100; i++ {
		err := r.Subscribe(fmt.Sprintf("client%d", i), "sensor/temperature", 1, storage.SubscribeOptions{})
		require.NoError(t, err)
	}

	// Concurrent reads
	const numReaders = 1000
	var wg sync.WaitGroup
	wg.Add(numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			subs, err := r.Match("sensor/temperature")
			assert.NoError(t, err)
			assert.Len(t, subs, 100)
		}()
	}

	wg.Wait()
}

func TestLockFreeRouter_ConcurrentWrites(t *testing.T) {
	r := NewLockFreeRouter()

	const numWriters = 100
	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Concurrent subscribes
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			err := r.Subscribe(fmt.Sprintf("client%d", id), "sensor/temperature", 1, storage.SubscribeOptions{})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// All subscriptions should be present
	subs, err := r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, numWriters)
}

func TestLockFreeRouter_ConcurrentReadWrite(t *testing.T) {
	r := NewLockFreeRouter()

	// Setup initial subscriptions
	for i := 0; i < 50; i++ {
		err := r.Subscribe(fmt.Sprintf("client%d", i), "sensor/temperature", 1, storage.SubscribeOptions{})
		require.NoError(t, err)
	}

	const numReaders = 500
	const numWriters = 50
	var wg sync.WaitGroup
	wg.Add(numReaders + numWriters)

	// Concurrent readers
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			subs, err := r.Match("sensor/temperature")
			assert.NoError(t, err)
			// Should have at least 50, may have up to 100
			assert.GreaterOrEqual(t, len(subs), 50)
			assert.LessOrEqual(t, len(subs), 100)
		}()
	}

	// Concurrent writers
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("client%d", 50+id)
			err := r.Subscribe(clientID, "sensor/temperature", 1, storage.SubscribeOptions{})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// Final count should be 100
	subs, err := r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, 100)
}

func TestLockFreeRouter_ConcurrentSubscribeUnsubscribe(t *testing.T) {
	r := NewLockFreeRouter()

	const numClients = 100
	var wg sync.WaitGroup
	wg.Add(numClients * 2)

	// Concurrent subscribe and unsubscribe
	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("client%d", id)
			err := r.Subscribe(clientID, "sensor/temperature", 1, storage.SubscribeOptions{})
			assert.NoError(t, err)
		}(i)

		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("client%d", id)
			// May or may not find the subscription depending on timing
			_ = r.Unsubscribe(clientID, "sensor/temperature")
		}(i)
	}

	wg.Wait()

	// Some subscriptions may remain (timing dependent)
	subs, err := r.Match("sensor/temperature")
	require.NoError(t, err)
	// Just verify it doesn't crash and returns valid results
	assert.GreaterOrEqual(t, len(subs), 0)
	assert.LessOrEqual(t, len(subs), numClients)
}

func TestLockFreeRouter_DeepHierarchy(t *testing.T) {
	r := NewLockFreeRouter()

	// Create a deep topic hierarchy
	topic := "a/b/c/d/e/f/g/h/i/j"
	err := r.Subscribe("client1", topic, 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	// Exact match
	subs, err := r.Match(topic)
	require.NoError(t, err)
	assert.Len(t, subs, 1)

	// Wildcard subscriptions
	err = r.Subscribe("client2", "a/+/c/+/e/+/g/+/i/+", 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	subs, err = r.Match(topic)
	require.NoError(t, err)
	assert.Len(t, subs, 2)

	// Multi-level wildcard
	err = r.Subscribe("client3", "a/b/#", 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	subs, err = r.Match(topic)
	require.NoError(t, err)
	assert.Len(t, subs, 3)
}

func TestLockFreeRouter_EmptyTopic(t *testing.T) {
	r := NewLockFreeRouter()

	// Subscribe to empty topic (edge case)
	err := r.Subscribe("client1", "", 1, storage.SubscribeOptions{})
	require.NoError(t, err)

	subs, err := r.Match("")
	require.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.Equal(t, "client1", subs[0].ClientID)
}

func TestLockFreeRouter_OptionsPreserved(t *testing.T) {
	r := NewLockFreeRouter()

	opts := storage.SubscribeOptions{
		NoLocal:           true,
		RetainAsPublished: true,
		RetainHandling:    2,
	}

	err := r.Subscribe("client1", "sensor/temperature", 1, opts)
	require.NoError(t, err)

	subs, err := r.Match("sensor/temperature")
	require.NoError(t, err)
	require.Len(t, subs, 1)

	// Verify options are preserved
	assert.True(t, subs[0].Options.NoLocal)
	assert.True(t, subs[0].Options.RetainAsPublished)
	assert.Equal(t, byte(2), subs[0].Options.RetainHandling)
}

// TestLockFreeRouter_CASRetry verifies that CAS retry logic works correctly
// when there's heavy contention on the same subscription path.
func TestLockFreeRouter_CASRetry(t *testing.T) {
	r := NewLockFreeRouter()

	const numGoroutines = 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// All goroutines try to subscribe to the SAME topic simultaneously
	// This maximizes CAS contention and exercises retry logic
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			clientID := fmt.Sprintf("client%d", id)
			err := r.Subscribe(clientID, "sensor/temperature", 1, storage.SubscribeOptions{})
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()

	// All subscriptions should be present (CAS retry succeeded)
	subs, err := r.Match("sensor/temperature")
	require.NoError(t, err)
	assert.Len(t, subs, numGoroutines)

	// Verify all clients are present
	clientSet := make(map[string]bool)
	for _, sub := range subs {
		clientSet[sub.ClientID] = true
	}
	assert.Len(t, clientSet, numGoroutines, "Duplicate subscriptions detected")
}
