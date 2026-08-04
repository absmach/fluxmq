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
