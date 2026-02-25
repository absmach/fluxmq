// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package router

import (
	"sync"

	"github.com/absmach/fluxmq/storage"
)

// Pool for topic level slices to reduce allocations in Match()/Subscribe()/Unsubscribe().
// Max depth 16 covers majority of MQTT topics without reallocation.
var topicLevelsPool = sync.Pool{
	New: func() any {
		s := make([]string, 0, 16)
		return &s
	},
}

func acquireTopicLevels() *[]string {
	return topicLevelsPool.Get().(*[]string)
}

func releaseTopicLevels(s *[]string) {
	if s == nil {
		return
	}
	*s = (*s)[:0]
	topicLevelsPool.Put(s)
}

// splitTopic splits a topic string into levels without allocating a new []string.
// It populates the provided slice in-place and returns it (may grow if cap exceeded).
func splitTopic(topic string, levels *[]string) *[]string {
	*levels = (*levels)[:0]
	start := 0
	for i := 0; i < len(topic); i++ {
		if topic[i] == '/' {
			*levels = append(*levels, topic[start:i])
			start = i + 1
		}
	}
	*levels = append(*levels, topic[start:])
	return levels
}

// Pool for subscription slices to reduce allocations in Match().
var subscriptionSlicePool = sync.Pool{
	New: func() any {
		// Pre-allocate with reasonable capacity for most use cases
		// This avoids reallocation for topics with moderate subscriber counts
		s := make([]*storage.Subscription, 0, 64)
		return &s
	},
}

// AcquireSubscriptionSlice gets a subscription slice from the pool.
// The returned slice must be returned via ReleaseSubscriptionSlice when done.
func AcquireSubscriptionSlice() *[]*storage.Subscription {
	return subscriptionSlicePool.Get().(*[]*storage.Subscription)
}

// ReleaseSubscriptionSlice returns a subscription slice to the pool after resetting it.
// The slice must not be used after calling this function.
func ReleaseSubscriptionSlice(s *[]*storage.Subscription) {
	if s == nil {
		return
	}
	// Clear the slice but keep capacity for reuse
	*s = (*s)[:0]
	subscriptionSlicePool.Put(s)
}
