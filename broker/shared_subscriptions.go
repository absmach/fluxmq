// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"strings"
	"sync"

	"github.com/absmach/mqtt/topics"
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

// GetNextSubscriber selects the next subscriber in the group (round-robin).
// Returns the client ID and true if a subscriber was found.
func (sm *SharedSubscriptionManager) GetNextSubscriber(filter string) (string, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock() // Use Lock because NextSubscriber updates internal state (round-robin index)

	// In the broker distribute logic, the filter passed here is often the groupKey derived from the special client ID
	// The broker sees matched subscription clientID as "$share/groupName/topicFilter"
	// So we need to handle that format or just raw groupKey

	// If input is full $share format, parse it
	if strings.HasPrefix(filter, "$share/") {
		shareName, topicFilter, _ := topics.ParseShared(filter)
		filter = shareName + "/" + topicFilter
	}
	// If input is raw groupKey format (e.g. from tests "group/topic"), use as is

	group, exists := sm.groups[filter]
	if !exists || group.IsEmpty() {
		return "", false
	}

	return group.NextSubscriber(), true
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
