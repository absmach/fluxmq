// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sync"

	"github.com/absmach/fluxmq/topics"
)

// SharedSubscriptionManager manages shared subscriptions (MQTT 5.0).
// It encapsulates the logic for grouping subscribers and round-robin distribution.
// shareGroupID names one share group. A group is identified by its name and
// the filter it is bound to together: two groups sharing a name but bound to
// different filters are different groups, so neither half names one on its own.
//
// It is a struct rather than a joined string because the publish path derives
// it on every message, and joining would allocate once per share group member
// the topic matched.
type shareGroupID struct {
	Name   string
	Filter string
}

type SharedSubscriptionManager struct {
	groups map[shareGroupID]*topics.ShareGroup

	// cursors is the round-robin position per group, kept apart from the group
	// itself because a group can have members on other nodes and none here: a
	// cursor that lived on the local membership would have nowhere to live for
	// such a group, and this node still has to take its turn choosing one of
	// its remote members.
	//
	// The cursor is deliberately node-local. Nodes do not agree on whose turn
	// it is; each spreads its own publishes across the whole group, which
	// balances the group without a round trip per message.
	cursors map[shareGroupID]uint64

	mu sync.RWMutex
}

// NewSharedSubscriptionManager creates a new shared subscription manager.
func NewSharedSubscriptionManager() *SharedSubscriptionManager {
	return &SharedSubscriptionManager{
		groups:  make(map[shareGroupID]*topics.ShareGroup),
		cursors: make(map[shareGroupID]uint64),
	}
}

// Subscribe adds a client to a shared subscription group.
// Returns true if this is a new group (first subscriber).
func (sm *SharedSubscriptionManager) Subscribe(clientID, filter string, qos byte) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	shareName, topicFilter, isShared := topics.ParseShared(filter)
	if !isShared {
		return false
	}

	id := shareGroupID{Name: shareName, Filter: topicFilter}
	group, exists := sm.groups[id]
	isNewGroup := !exists

	if !exists {
		group = &topics.ShareGroup{
			Name:        shareName,
			TopicFilter: topicFilter,
			Subscribers: []topics.ShareSubscriber{},
		}
		sm.groups[id] = group
	}

	group.AddSubscriber(clientID, qos)

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

	id := shareGroupID{Name: shareName, Filter: topicFilter}
	group, exists := sm.groups[id]
	if !exists {
		return false
	}

	group.RemoveSubscriber(clientID)

	if group.IsEmpty() {
		delete(sm.groups, id)
		delete(sm.cursors, id)
		return true
	}

	return false
}

// SelectMember reserves this node's turn in a group of localCount + remoteCount
// members and reports the position it chose, how many of the members are local,
// and — when the chosen position is a local member — which one. Everything the
// choice depends on is read under one lock, because a group is chosen from on
// every message published to its topic.
//
// ok is false for a group with no members at all.
func (sm *SharedSubscriptionManager) SelectMember(id shareGroupID, remoteCount int) (rotation, localCount int, chosen topics.ShareSubscriber, ok bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	group := sm.groups[id]
	if group != nil {
		localCount = len(group.Subscribers)
	}

	total := localCount + remoteCount
	if total == 0 {
		return 0, 0, topics.ShareSubscriber{}, false
	}

	cursor := sm.cursors[id]
	sm.cursors[id] = cursor + 1
	rotation = int(cursor % uint64(total))

	if rotation < localCount {
		chosen, _ = group.SubscriberAt(0, rotation)
	}

	return rotation, localCount, chosen, true
}

// LocalMemberAt returns the group's i-th local member, for a caller walking the
// group after the member it selected could not take the message. ok is false
// once i reaches the count, and for an i whose member left in the meantime.
func (sm *SharedSubscriptionManager) LocalMemberAt(id shareGroupID, i int) (topics.ShareSubscriber, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	group, exists := sm.groups[id]
	if !exists {
		return topics.ShareSubscriber{}, false
	}

	return group.SubscriberAt(0, i)
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

// GetGroup returns a share group by name and filter (for testing).
func (sm *SharedSubscriptionManager) GetGroup(name, filter string) *topics.ShareGroup {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.groups[shareGroupID{Name: name, Filter: filter}]
}
