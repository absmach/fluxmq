// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"strings"
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribe_ReplacesExistingClientFilter(t *testing.T) {
	r := NewRouter()

	require.NoError(t, r.Subscribe("client-a", "sensors/temperature", 0, storage.SubscribeOptions{}))
	require.NoError(t, r.Subscribe("client-a", "sensors/temperature", 2, storage.SubscribeOptions{}))

	matched, err := r.Match("sensors/temperature")
	require.NoError(t, err)
	require.Len(t, matched, 1)
	require.Equal(t, byte(2), matched[0].QoS)
}

func TestSubscribe_DifferentClientsRemainDistinct(t *testing.T) {
	r := NewRouter()

	require.NoError(t, r.Subscribe("client-a", "sensors/temperature", 0, storage.SubscribeOptions{}))
	require.NoError(t, r.Subscribe("client-b", "sensors/temperature", 1, storage.SubscribeOptions{}))

	matched, err := r.Match("sensors/temperature")
	require.NoError(t, err)
	require.Len(t, matched, 2)
}

func TestMatch_SingleSegmentTopic(t *testing.T) {
	r := NewRouter()

	require.NoError(t, r.Subscribe("c1", "status", 0, storage.SubscribeOptions{}))

	matched, err := r.Match("status")
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, "c1", matched[0].ClientID)
}

func TestMatch_EmptySegments(t *testing.T) {
	r := NewRouter()

	// Leading slash produces an empty first segment: "" / "a" / "b"
	require.NoError(t, r.Subscribe("c1", "/a/b", 0, storage.SubscribeOptions{}))
	matched, err := r.Match("/a/b")
	require.NoError(t, err)
	require.Len(t, matched, 1)

	// Consecutive slashes: "a" / "" / "b"
	require.NoError(t, r.Subscribe("c2", "a//b", 0, storage.SubscribeOptions{}))
	matched, err = r.Match("a//b")
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, "c2", matched[0].ClientID)

	// No cross-match: "a/b" should not match "a//b"
	matched, err = r.Match("a/b")
	require.NoError(t, err)
	assert.Empty(t, matched)
}

func TestMatch_DeepTopic_ExceedsPoolCapacity(t *testing.T) {
	r := NewRouter()

	// 20 segments — exceeds the 16-element pool capacity
	segments := make([]string, 20)
	for i := range segments {
		segments[i] = "level"
	}
	deep := strings.Join(segments, "/")

	require.NoError(t, r.Subscribe("c1", deep, 1, storage.SubscribeOptions{}))

	matched, err := r.Match(deep)
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, "c1", matched[0].ClientID)
	assert.Equal(t, byte(1), matched[0].QoS)
}

func TestMatch_DeepTopic_WildcardsAtDepth(t *testing.T) {
	r := NewRouter()

	segments := make([]string, 20)
	for i := range segments {
		segments[i] = "level"
	}
	deep := strings.Join(segments, "/")

	// + wildcard at the last segment
	wildcardFilter := strings.Join(segments[:19], "/") + "/+"
	require.NoError(t, r.Subscribe("c-plus", wildcardFilter, 0, storage.SubscribeOptions{}))

	// # wildcard partway through
	hashFilter := strings.Join(segments[:10], "/") + "/#"
	require.NoError(t, r.Subscribe("c-hash", hashFilter, 0, storage.SubscribeOptions{}))

	matched, err := r.Match(deep)
	require.NoError(t, err)
	require.Len(t, matched, 2)

	clientIDs := map[string]bool{}
	for _, s := range matched {
		clientIDs[s.ClientID] = true
	}
	assert.True(t, clientIDs["c-plus"])
	assert.True(t, clientIDs["c-hash"])
}

func TestMatch_NoSubscribers(t *testing.T) {
	r := NewRouter()

	matched, err := r.Match("no/one/here")
	require.NoError(t, err)
	assert.Empty(t, matched)
}

func TestMatch_OverlappingWildcards_DuplicateDelivery(t *testing.T) {
	r := NewRouter()

	// Same client subscribes to both exact and wildcard filters
	require.NoError(t, r.Subscribe("c1", "sensor/room1/temp", 0, storage.SubscribeOptions{}))
	require.NoError(t, r.Subscribe("c1", "sensor/+/temp", 1, storage.SubscribeOptions{}))

	matched, err := r.Match("sensor/room1/temp")
	require.NoError(t, err)
	// Router returns both matches — dedup is the caller's responsibility
	require.Len(t, matched, 2)
}

func TestUnsubscribe_NonexistentFilter(t *testing.T) {
	r := NewRouter()

	// Unsubscribing a filter that was never subscribed should not error
	require.NoError(t, r.Unsubscribe("c1", "does/not/exist"))
}

func TestUnsubscribe_PreservesOtherClients(t *testing.T) {
	r := NewRouter()

	require.NoError(t, r.Subscribe("c1", "a/b", 0, storage.SubscribeOptions{}))
	require.NoError(t, r.Subscribe("c2", "a/b", 1, storage.SubscribeOptions{}))
	require.NoError(t, r.Unsubscribe("c1", "a/b"))

	matched, err := r.Match("a/b")
	require.NoError(t, err)
	require.Len(t, matched, 1)
	assert.Equal(t, "c2", matched[0].ClientID)
}
