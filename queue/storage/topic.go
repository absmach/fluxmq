// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"slices"
	"sync"
)

// TopicIndex provides efficient topic-to-queue matching.
// It maintains an index of queue topic patterns for O(n) topic matching
// where n is the number of unique topic patterns that can match the topic.
type TopicIndex struct {
	mu sync.RWMutex

	// queues maps queue name to its topic patterns
	queues map[string][]string

	// Patterns are partitioned by whether they begin with '$', because MQTT
	// wildcard rules make the two halves mutually exclusive: a '$'-prefixed
	// topic matches only a '$'-prefixed filter, and a filter whose first level
	// is a literal '$...' matches no ordinary topic. A publish therefore only
	// ever consults one half.
	//
	// This matters because queues are addressed under "$queue/" while their
	// patterns are also matched against ordinary pub/sub topics on the publish
	// path of every protocol. Without the split, a broker whose queues are all
	// addressed through "$queue/" would test every one of them on every
	// ordinary publish and never match.
	//
	// Each half is a trie keyed on topic levels, so matching costs the depth of
	// the topic rather than the number of patterns registered.
	dollarPatterns *patternTrie
	plainPatterns  *patternTrie
}

// NewTopicIndex creates a new topic index.
func NewTopicIndex() *TopicIndex {
	return &TopicIndex{
		queues:         make(map[string][]string),
		dollarPatterns: newPatternTrie(),
		plainPatterns:  newPatternTrie(),
	}
}

// patternsFor returns the half of the index that can match topics shaped like
// the given value. It is used for both lookup and bookkeeping, so a pattern is
// always filed where a matching topic will look for it.
func (idx *TopicIndex) patternsFor(patternOrTopic string) *patternTrie {
	if patternOrTopic != "" && patternOrTopic[0] == '$' {
		return idx.dollarPatterns
	}
	return idx.plainPatterns
}

// AddQueue registers a queue with its topic patterns.
func (idx *TopicIndex) AddQueue(queueName string, topicPatterns []string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove existing entries for this queue if any
	idx.removeQueueLocked(queueName)

	// Add new entries
	idx.queues[queueName] = topicPatterns

	for _, pattern := range topicPatterns {
		idx.patternsFor(pattern).add(pattern, queueName)
	}
}

// RemoveQueue removes a queue from the index.
func (idx *TopicIndex) RemoveQueue(queueName string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.removeQueueLocked(queueName)
}

func (idx *TopicIndex) removeQueueLocked(queueName string) {
	topicPatterns, exists := idx.queues[queueName]
	if !exists {
		return
	}

	// Remove queue from pattern index
	for _, pattern := range topicPatterns {
		idx.patternsFor(pattern).remove(pattern, queueName)
	}

	delete(idx.queues, queueName)
}

// FindMatching returns all queue names whose topic patterns match the given topic.
// Wildcard semantics follow MQTT, matching topics.TopicMatch.
//
// This runs on the publish path of every protocol, so the no-match case
// allocates nothing: only the half of the index that can match the topic is
// consulted, the trie is walked without splitting the topic, and the result
// slice is built lazily.
func (idx *TopicIndex) FindMatching(topic string) []string {
	if topic == "" {
		return nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var matched matchedQueues
	idx.patternsFor(topic).match(topic, &matched)

	return matched.names
}

// matchedQueuesMapThreshold is the result size at which deduplication switches
// from scanning the slice to a set. One topic usually matches no queue or one,
// so the map the set needs is not built unless a configuration binds enough
// queues to the same topic for the scan to cost more than the allocation.
const matchedQueuesMapThreshold = 16

// matchedQueues accumulates distinct queue names in first-match order.
type matchedQueues struct {
	names []string
	seen  map[string]struct{}
}

func (m *matchedQueues) add(queueName string) {
	if m.seen != nil {
		if _, exists := m.seen[queueName]; exists {
			return
		}
		m.seen[queueName] = struct{}{}
		m.names = append(m.names, queueName)
		return
	}

	if slices.Contains(m.names, queueName) {
		return
	}
	m.names = append(m.names, queueName)

	if len(m.names) == matchedQueuesMapThreshold {
		m.seen = make(map[string]struct{}, matchedQueuesMapThreshold*2)
		for _, name := range m.names {
			m.seen[name] = struct{}{}
		}
	}
}

// GetQueues returns a copy of all registered queues and their topic patterns.
func (idx *TopicIndex) GetQueues() map[string][]string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make(map[string][]string, len(idx.queues))
	for name, topicPatterns := range idx.queues {
		patternsCopy := make([]string, len(topicPatterns))
		copy(patternsCopy, topicPatterns)
		result[name] = patternsCopy
	}
	return result
}

// QueueCount returns the number of registered queues.
func (idx *TopicIndex) QueueCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.queues)
}

// GetQueueTopics returns the topic patterns for a specific queue.
func (idx *TopicIndex) GetQueueTopics(queueName string) ([]string, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	topicPatterns, exists := idx.queues[queueName]
	if !exists {
		return nil, false
	}

	result := make([]string, len(topicPatterns))
	copy(result, topicPatterns)
	return result, true
}
