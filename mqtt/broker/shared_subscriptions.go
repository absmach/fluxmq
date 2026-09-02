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

	// cursors is the round-robin position per group, kept apart from the group
	// itself because a group can have members on other nodes and none here: a
	// cursor that lived on the local membership would have nowhere to live for
	// such a group, and this node still has to take its turn choosing one of
	// its remote members.
	//
	// The cursor is deliberately node-local. Nodes do not agree on whose turn
	// it is; each spreads its own publishes across the whole group, which
	// balances the group without a round trip per message.
	cursors map[string]uint64

	mu sync.RWMutex
}

// NewSharedSubscriptionManager creates a new shared subscription manager.
func NewSharedSubscriptionManager() *SharedSubscriptionManager {
	return &SharedSubscriptionManager{
		groups:  make(map[string]*topics.ShareGroup),
		cursors: make(map[string]uint64),
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
		delete(sm.cursors, groupKey)
		return true
	}

	return false
}

// NextRotation reserves this node's next position in a group of size members
// and reports it. Callers walk the group from there, so consecutive publishes
// from this node start at consecutive members.
func (sm *SharedSubscriptionManager) NextRotation(groupKey string, size int) int {
	if size <= 0 {
		return 0
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	cursor := sm.cursors[groupKey]
	sm.cursors[groupKey] = cursor + 1

	return int(cursor % uint64(size))
}

// LocalMemberCount reports how many of a group's members are connected to this
// node.
func (sm *SharedSubscriptionManager) LocalMemberCount(groupKey string) int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	group, exists := sm.groups[shareGroupKey(groupKey)]
	if !exists {
		return 0
	}

	return len(group.Subscribers)
}

// LocalMemberAt returns the group's i-th local member. ok is false once i
// reaches the count, and for an i whose member left since the count was taken.
func (sm *SharedSubscriptionManager) LocalMemberAt(groupKey string, i int) (string, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	group, exists := sm.groups[shareGroupKey(groupKey)]
	if !exists {
		return "", false
	}

	return group.SubscriberAt(0, i)
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
				delete(sm.cursors, key)
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
