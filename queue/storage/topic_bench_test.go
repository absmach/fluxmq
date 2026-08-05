// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"fmt"
	"testing"
)

const benchOrdinaryTopic = "m/acme/c/temp/reading"

// FindMatching runs on the publish path of every protocol: a queue's topic
// patterns are matched against ordinary pub/sub topics, not only "$queue/"
// addresses. The zero-match case is the one that must stay cheap, because a
// broker whose queues are all addressed through "$queue/" pays it on every
// publish while never matching anything.
func BenchmarkTopicIndexFindMatching(b *testing.B) {
	queuePattern := func(i int) string { return fmt.Sprintf("$queue/queue-%d/#", i) }
	topicPattern := func(i int) string { return fmt.Sprintf("t%d/+/events/#", i) }

	newIndex := func(queues, topicsBound int) *TopicIndex {
		index := NewTopicIndex()
		for i := range queues {
			index.AddQueue(fmt.Sprintf("queue-%d", i), []string{queuePattern(i)})
		}
		for i := range topicsBound {
			index.AddQueue(fmt.Sprintf("bound-%d", i), []string{topicPattern(i)})
		}
		return index
	}

	benchmarks := []struct {
		name        string
		queues      int
		topicsBound int
		topic       string
	}{
		{name: "queue_patterns_only/ordinary_topic_no_match", queues: 32, topic: benchOrdinaryTopic},
		{name: "queue_patterns_only/queue_topic_one_match", queues: 32, topic: "$queue/queue-7/items"},
		{name: "mixed/ordinary_topic_no_match", queues: 32, topicsBound: 8, topic: benchOrdinaryTopic},
		{name: "mixed/ordinary_topic_one_match", queues: 32, topicsBound: 8, topic: "t3/acme/events/temp"},
		{name: "many_queues/ordinary_topic_no_match", queues: 512, topic: benchOrdinaryTopic},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			index := newIndex(bm.queues, bm.topicsBound)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				index.FindMatching(bm.topic)
			}
		})
	}
}

// Scaling "$queue/" patterns measures the partition, which short-circuits them to
// no work at all, so it says nothing about the matcher. The dimension that used
// to cost is the number of *ordinary* patterns: that half is what an ordinary
// publish consults.
//
// Matching is a trie descent, so this asserts the property that replaced the old
// linear scan — lookup independent of how many patterns are registered. A result
// that grows with the pattern count means the index has regressed to a scan.
func BenchmarkTopicIndexFindMatchingOrdinaryPatternScale(b *testing.B) {
	// A handful of queue-addressed queues alongside them, so the partition is
	// present but is not the variable under test.
	const queueAddressedQueues = 8

	newIndex := func(ordinaryPatterns int) *TopicIndex {
		index := NewTopicIndex()
		for i := range queueAddressedQueues {
			index.AddQueue(fmt.Sprintf("queue-%d", i), []string{fmt.Sprintf("$queue/queue-%d/#", i)})
		}
		for i := range ordinaryPatterns {
			index.AddQueue(fmt.Sprintf("bound-%d", i), []string{fmt.Sprintf("t%d/+/events/#", i)})
		}
		return index
	}

	for _, patterns := range []int{8, 64, 512, 2048, 8192} {
		index := newIndex(patterns)
		for _, tc := range []struct {
			name  string
			topic string
		}{
			{name: "no_match", topic: benchOrdinaryTopic},
			// Both cases descend the same trie; they differ only by the
			// allocation the result slice makes when something matches.
			{name: "one_match", topic: "t3/acme/events/temp"},
		} {
			b.Run(fmt.Sprintf("%s/%d_ordinary_patterns", tc.name, patterns), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					index.FindMatching(tc.topic)
				}
			})
		}
	}
}

// Publishes addressed to a queue once cost the same scan, growing with the number
// of configured queues rather than with traffic. They are indexed by the same
// trie, and this holds that: it is the dimension a broker with many queues
// actually grows along.
func BenchmarkTopicIndexFindMatchingQueueAddressScale(b *testing.B) {
	newIndex := func(queues int) *TopicIndex {
		index := NewTopicIndex()
		for i := range queues {
			name := fmt.Sprintf("queue-%d", i)
			index.AddQueue(name, []string{"$queue/" + name + "/#"})
		}
		return index
	}

	for _, queues := range []int{8, 512, 8192} {
		index := newIndex(queues)
		b.Run(fmt.Sprintf("one_match/%d_queues", queues), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				index.FindMatching("$queue/queue-7/items")
			}
		})
	}
}
