// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	messagesPattern = "m/#"
	eventsPattern   = "t/+/events"
	bothQueue       = "both"
	bothQueueFilter = "$queue/both/#"
	qQueueFilter    = "$queue/q/#"
)

func sortedMatches(index *TopicIndex, topic string) []string {
	matches := index.FindMatching(topic)
	sort.Strings(matches)
	return matches
}

// The index files a pattern by whether it can match a "$"-prefixed topic, so
// every operation has to agree on which half a pattern belongs to. A queue
// holding one pattern of each kind exercises both halves at once.
func TestTopicIndexFindMatching(t *testing.T) {
	index := NewTopicIndex()
	index.AddQueue("orders", []string{"$queue/orders/#"})
	index.AddQueue("messages", []string{messagesPattern})
	index.AddQueue(bothQueue, []string{bothQueueFilter, eventsPattern})

	tests := []struct {
		name  string
		topic string
		want  []string
	}{
		{name: "queue address matches only its queue", topic: "$queue/orders/items", want: []string{"orders"}},
		{name: "ordinary topic matches an ordinary pattern", topic: "m/acme/temp", want: []string{"messages"}},
		{name: "ordinary topic does not match a queue address pattern", topic: "orders/items", want: nil},
		{name: "queue address does not match an ordinary pattern", topic: "$queue/messages/x", want: nil},
		{name: "queue with both pattern kinds matches on its queue address", topic: "$queue/both/x", want: []string{bothQueue}},
		{name: "queue with both pattern kinds matches on its ordinary pattern", topic: "t/acme/events", want: []string{bothQueue}},
		{name: "unmatched topic matches nothing", topic: "other/thing", want: nil},
		{name: "empty topic matches nothing", topic: "", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, sortedMatches(index, tt.topic))
		})
	}
}

func TestTopicIndexRemoveQueue(t *testing.T) {
	index := NewTopicIndex()
	index.AddQueue(bothQueue, []string{bothQueueFilter, eventsPattern})
	index.AddQueue("other", []string{eventsPattern})

	require.Equal(t, []string{bothQueue}, sortedMatches(index, "$queue/both/x"))
	require.Equal(t, []string{bothQueue, "other"}, sortedMatches(index, "t/acme/events"))

	index.RemoveQueue(bothQueue)

	t.Run("both halves of the index drop the queue", func(t *testing.T) {
		assert.Nil(t, sortedMatches(index, "$queue/both/x"))
		assert.Equal(t, []string{"other"}, sortedMatches(index, "t/acme/events"),
			"a queue sharing the pattern must keep it")
	})
	t.Run("queue metadata is dropped", func(t *testing.T) {
		_, exists := index.GetQueueTopics(bothQueue)
		assert.False(t, exists)
		assert.Equal(t, 1, index.QueueCount())
	})
	t.Run("removing an unknown queue is a no-op", func(t *testing.T) {
		index.RemoveQueue("missing")
		assert.Equal(t, 1, index.QueueCount())
	})
}

// AddQueue replaces a queue's patterns, so re-adding must not leave the old
// ones matching in either half.
func TestTopicIndexAddQueueReplacesPatterns(t *testing.T) {
	index := NewTopicIndex()
	index.AddQueue("q", []string{qQueueFilter, "old/#"})
	require.Equal(t, []string{"q"}, sortedMatches(index, "old/thing"))

	index.AddQueue("q", []string{"new/#"})

	assert.Nil(t, sortedMatches(index, "old/thing"), "replaced pattern must stop matching")
	assert.Nil(t, sortedMatches(index, "$queue/q/x"), "replaced queue-address pattern must stop matching")
	assert.Equal(t, []string{"q"}, sortedMatches(index, "new/thing"))
}

// A queue whose patterns both match one topic must be reported once. The
// deduplication switches representation once the result grows, so both sides of
// that threshold are covered.
func TestTopicIndexDeduplicatesQueues(t *testing.T) {
	t.Run("one queue matched by two of its own patterns", func(t *testing.T) {
		index := NewTopicIndex()
		index.AddQueue("q", []string{messagesPattern, "m/acme/+"})
		assert.Equal(t, []string{"q"}, sortedMatches(index, "m/acme/temp"))
	})

	t.Run("many queues past the set threshold", func(t *testing.T) {
		const queueCount = matchedQueuesMapThreshold * 3

		index := NewTopicIndex()
		want := make([]string, 0, queueCount)
		for i := range queueCount {
			name := fmt.Sprintf("q-%02d", i)
			// Two overlapping patterns per queue, so every queue is reached
			// twice and deduplication is exercised in both representations.
			index.AddQueue(name, []string{messagesPattern, fmt.Sprintf("m/acme/+/%d", i)})
			want = append(want, name)
		}
		sort.Strings(want)

		assert.Equal(t, want, sortedMatches(index, "m/acme/temp/1"))
	})
}

func TestTopicIndexGetQueues(t *testing.T) {
	index := NewTopicIndex()
	index.AddQueue("q", []string{qQueueFilter, messagesPattern})

	queues := index.GetQueues()
	require.Len(t, queues, 1)
	assert.Equal(t, []string{qQueueFilter, messagesPattern}, queues["q"])

	queues["q"][0] = "mutated"
	topics, exists := index.GetQueueTopics("q")
	require.True(t, exists)
	assert.Equal(t, []string{qQueueFilter, messagesPattern}, topics, "GetQueues must return a copy")

	topics[0] = "mutated"
	assert.Equal(t, []string{"q"}, sortedMatches(index, "$queue/q/x"),
		"GetQueueTopics must return a copy")
}
