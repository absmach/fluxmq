// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"strings"
	"sync"

	"github.com/absmach/fluxmq/topics"
)

// SharedSubscriptionManager manages shared subscriptions (MQTT 5.0).
// It encapsulates the logic for grouping subscribers and round-robin distribution.
type SharedSubscriptionManager struct {
	// key: "shareName/topicFilter"
	groups map[string]*topics.ShareGroup
	mu     sync.RWMutex
}

// NewSharedSubscriptionManager creates a new shared subscription manager.
func NewSharedSubscriptionManager() *SharedSubscriptionManager {
	return &SharedSubscriptionManager{
		groups: make(map[string]*topics.ShareGroup),
	}
}

// Subscribe adds a client to a shared subscription group.
// Returns true if this is a new group (first subscriber).
func (sm *SharedSubscriptionManager) Subscribe(clientID, filter string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	shareName, topicFilter, isShared := topics.ParseShared(filter)
	if !isShared {
		return false
	}

	groupKey := shareName + "/" + topicFilter
	group, exists := sm.groups[groupKey]
	isNewGroup := !exists

	if !exists {
		group = &topics.ShareGroup{
			Name:        shareName,
			TopicFilter: topicFilter,
			Subscribers: []string{},
		}
		sm.groups[groupKey] = group
	}

	group.AddSubscriber(clientID)
	return isNewGroup
}

// Unsubscribe removes a client from a shared subscription group.
// Returns true if the group becomes empty and should be removed.
func (sm *SharedSubscriptionManager) Unsubscribe(clientID, filter string) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	shareName, topicFilter, isShared := topics.ParseShared(filter)
	if !isShared {
		return false
	}

	groupKey := shareName + "/" + topicFilter
	group, exists := sm.groups[groupKey]
	if !exists {
		return false
	}

	group.RemoveSubscriber(clientID)

	if group.IsEmpty() {
		delete(sm.groups, groupKey)
		return true
	}

	return false
}

// SelectSubscriber selects the next subscriber in the group (round-robin) and
// reports the rotation it came from, so a caller that cannot deliver to the
// selected member can walk the rest of the group with SubscriberAt.
func (sm *SharedSubscriptionManager) SelectSubscriber(filter string) (clientID string, rotation int, ok bool) {
	// Pre-compute group key outside the lock
	groupKey := shareGroupKey(filter)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	group, exists := sm.groups[groupKey]
	if !exists {
		return "", 0, false
	}

	return group.Select()
}

// SubscriberAt returns the group member offset positions after rotation, for a
// caller retrying a delivery the selected member could not take. ok is false
// once offset reaches the group size, which is what bounds the retry walk.
func (sm *SharedSubscriptionManager) SubscriberAt(filter string, rotation, offset int) (string, bool) {
	// Pre-compute group key outside the lock
	groupKey := shareGroupKey(filter)

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	group, exists := sm.groups[groupKey]
	if !exists {
		return "", false
	}

	return group.SubscriberAt(rotation, offset)
}

// shareGroupKey normalizes a filter to the key groups are stored under. Callers hold
// either form: the router carries "$share/<name>/<filter>", the manager keys on
// "<name>/<filter>".
func shareGroupKey(filter string) string {
	if !strings.HasPrefix(filter, "$share/") {
		return filter
	}

	shareName, topicFilter, _ := topics.ParseShared(filter)
	return shareName + "/" + topicFilter
}

// RemoveClient removes a client from all shared groups it is a member of.
// Returns a list of topic filters for groups that became empty and were removed.
func (sm *SharedSubscriptionManager) RemoveClient(clientID string) []string {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var emptyGroups []string

	for key, group := range sm.groups {
		if group.RemoveSubscriber(clientID) {
			if group.IsEmpty() {
				delete(sm.groups, key)
				emptyGroups = append(emptyGroups, group.TopicFilter)
			}
		}
	}

	return emptyGroups
}

// GetGroup returns a share group by key (for testing).
func (sm *SharedSubscriptionManager) GetGroup(key string) *topics.ShareGroup {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.groups[key]
}
