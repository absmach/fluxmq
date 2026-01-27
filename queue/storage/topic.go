// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"sync"

	"github.com/absmach/fluxmq/topics"
)

// TopicIndex provides efficient topic-to-queue matching.
// It maintains an index of queue topic patterns for O(n) topic matching
// where n is the number of unique topic patterns.
type TopicIndex struct {
	mu sync.RWMutex

	// queues maps queue name to its topic patterns
	queues map[string][]string

	// patterns is a flat list of all patterns for iteration
	// each entry maps pattern -> queue names that use it
	patterns map[string][]string
}

// NewTopicIndex creates a new topic index.
func NewTopicIndex() *TopicIndex {
	return &TopicIndex{
		queues:   make(map[string][]string),
		patterns: make(map[string][]string),
	}
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
		idx.patterns[pattern] = append(idx.patterns[pattern], queueName)
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
		queues := idx.patterns[pattern]
		newQueues := make([]string, 0, len(queues))
		for _, q := range queues {
			if q != queueName {
				newQueues = append(newQueues, q)
			}
		}
		if len(newQueues) == 0 {
			delete(idx.patterns, pattern)
		} else {
			idx.patterns[pattern] = newQueues
		}
	}

	delete(idx.queues, queueName)
}

// FindMatching returns all queue names whose topic patterns match the given topic.
// Uses MQTT-style topic matching via topics.TopicMatch.
func (idx *TopicIndex) FindMatching(topic string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	seen := make(map[string]struct{})
	var result []string

	for pattern, queues := range idx.patterns {
		if topics.TopicMatch(pattern, topic) {
			for _, queueName := range queues {
				if _, exists := seen[queueName]; !exists {
					seen[queueName] = struct{}{}
					result = append(result, queueName)
				}
			}
		}
	}

	return result
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
