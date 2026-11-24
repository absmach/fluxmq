// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"strconv"
	"strings"
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
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

func TestMatch_DollarTopics(t *testing.T) {
	const uptime = "$SYS/broker/uptime"
	cases := []struct {
		name   string
		filter string
		topic  string
		want   bool
	}{
		{"hash/excludes $SYS", "#", uptime, false},
		{"plus/excludes $SYS", "+/broker/uptime", uptime, false},
		{"plus-hash/excludes $SYS", "+/#", uptime, false},
		{"hash/excludes single-level $ topic", "#", "$SYS", false},
		{"explicit/$SYS hash", "$SYS/#", uptime, true},
		{"explicit/$SYS plus", "$SYS/+/uptime", uptime, true},
		{"hash/matches ordinary topic", "#", "sensors/temp", true},
		{"hash/matches $ below the first level", "#", "sensors/$raw", true},
		{"plus/matches $ below the first level", "sensors/+", "sensors/$raw", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRouter()
			require.NoError(t, r.Subscribe("client", tc.filter, 0, storage.SubscribeOptions{}))

			matched, err := r.Match(tc.topic)
			require.NoError(t, err)
			assert.Equal(t, tc.want, len(matched) == 1)
			assert.Equal(t, topics.TopicMatch(tc.filter, tc.topic), tc.want,
				"the trie must agree with topics.TopicMatch")
		})
	}
}

func TestUnsubscribe_ReleasesRetainedState(t *testing.T) {
	sub := func(t *testing.T, r *TrieRouter, client, filter string) {
		t.Helper()
		require.NoError(t, r.Subscribe(client, filter, 0, storage.SubscribeOptions{}))
	}
	unsub := func(t *testing.T, r *TrieRouter, client, filter string) {
		t.Helper()
		require.NoError(t, r.Unsubscribe(client, filter))
	}

	t.Run("prune/last subscription removes its path", func(t *testing.T) {
		r := NewRouter()
		sub(t, r, "c1", "a/b/c")
		unsub(t, r, "c1", "a/b/c")
		assert.Empty(t, r.root.children)
	})

	t.Run("prune/wildcard filters", func(t *testing.T) {
		r := NewRouter()
		sub(t, r, "c1", "+/x/#")
		unsub(t, r, "c1", "+/x/#")
		assert.Empty(t, r.root.children)
	})

	t.Run("prune/stops at a node still subscribed", func(t *testing.T) {
		r := NewRouter()
		sub(t, r, "c1", "a/b")
		sub(t, r, "c2", "a/b/c")
		unsub(t, r, "c2", "a/b/c")

		ab := r.root.children["a"].children["b"]
		require.NotNil(t, ab)
		assert.Empty(t, ab.children)
		matched, err := r.Match("a/b")
		require.NoError(t, err)
		assert.Len(t, matched, 1)
	})

	t.Run("prune/stops at a node with other children", func(t *testing.T) {
		r := NewRouter()
		sub(t, r, "c1", "a/b/c")
		sub(t, r, "c1", "a/d")
		unsub(t, r, "c1", "a/b/c")

		assert.NotContains(t, r.root.children["a"].children, "b")
		matched, err := r.Match("a/d")
		require.NoError(t, err)
		assert.Len(t, matched, 1)
	})

	t.Run("prune/unknown client leaves path", func(t *testing.T) {
		r := NewRouter()
		sub(t, r, "c1", "a/b")
		unsub(t, r, "other", "a/b")
		matched, err := r.Match("a/b")
		require.NoError(t, err)
		assert.Len(t, matched, 1)
	})

	t.Run("churn/distinct topics do not accumulate", func(t *testing.T) {
		r := NewRouter()
		for i := range 10_000 {
			filter := "devices/" + strconv.Itoa(i) + "/telemetry"
			sub(t, r, "c1", filter)
			unsub(t, r, "c1", filter)
		}
		assert.Empty(t, r.root.children)
	})

	t.Run("slots/removed subscription is not retained", func(t *testing.T) {
		r := NewRouter()
		sub(t, r, "c1", "a")
		sub(t, r, "c2", "a")
		unsub(t, r, "c1", "a")

		subs := r.root.children["a"].subs
		require.Len(t, subs, 1)
		for _, slot := range subs[len(subs):cap(subs)] {
			assert.Nil(t, slot)
		}
	})
}

func TestReleasedPoolSlicesHoldNoReferences(t *testing.T) {
	t.Run("subscriptions", func(t *testing.T) {
		subs := make([]*storage.Subscription, 0, 8)
		for range 5 {
			subs = append(subs, &storage.Subscription{ClientID: "c"})
		}
		ReleaseSubscriptionSlice(&subs)
		assert.Empty(t, subs)
		for _, slot := range subs[:cap(subs)] {
			assert.Nil(t, slot)
		}
	})

	t.Run("oversized slices are not pooled", func(t *testing.T) {
		huge := make([]*storage.Subscription, 1, maxPooledSubscriptions+1)
		huge[0] = &storage.Subscription{ClientID: "c"}
		ReleaseSubscriptionSlice(&huge)
		for range 8 {
			got := AcquireSubscriptionSlice()
			assert.LessOrEqual(t, cap(*got), maxPooledSubscriptions)
			ReleaseSubscriptionSlice(got)
		}
	})
}
