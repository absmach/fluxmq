// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import "time"

type subscriptionRef struct {
	queueName string
	groupID   string
	refCount  int
	lastSeen  time.Time
}

type subscriptionTarget struct {
	key       string
	queueName string
	groupID   string
}

func (m *Manager) subscriptionRefKey(queueName, groupID string) string {
	return queueName + "\x00" + groupID
}

func (m *Manager) trackSubscription(clientID, queueName, groupID string) {
	if clientID == "" || queueName == "" || groupID == "" {
		return
	}

	key := m.subscriptionRefKey(queueName, groupID)
	now := time.Now()

	m.subscriptionsMu.Lock()
	defer m.subscriptionsMu.Unlock()

	refs, ok := m.subscriptions[clientID]
	if !ok {
		refs = make(map[string]*subscriptionRef)
		m.subscriptions[clientID] = refs
	}

	if ref, ok := refs[key]; ok {
		ref.refCount++
		ref.lastSeen = now
		return
	}

	refs[key] = &subscriptionRef{
		queueName: queueName,
		groupID:   groupID,
		refCount:  1,
		lastSeen:  now,
	}
}

func (m *Manager) untrackSubscription(clientID, queueName, groupID string) {
	if clientID == "" || queueName == "" || groupID == "" {
		return
	}

	key := m.subscriptionRefKey(queueName, groupID)

	m.subscriptionsMu.Lock()
	defer m.subscriptionsMu.Unlock()

	refs, ok := m.subscriptions[clientID]
	if !ok {
		return
	}

	ref, ok := refs[key]
	if !ok {
		return
	}

	ref.refCount--
	if ref.refCount <= 0 {
		delete(refs, key)
	}

	if len(refs) == 0 {
		delete(m.subscriptions, clientID)
	}
}

func (m *Manager) getSubscriptionTargets(clientID string) []subscriptionTarget {
	m.subscriptionsMu.RLock()
	defer m.subscriptionsMu.RUnlock()

	refs, ok := m.subscriptions[clientID]
	if !ok {
		return nil
	}

	targets := make([]subscriptionTarget, 0, len(refs))
	for key, ref := range refs {
		targets = append(targets, subscriptionTarget{
			key:       key,
			queueName: ref.queueName,
			groupID:   ref.groupID,
		})
	}

	return targets
}

func (m *Manager) touchSubscription(clientID, key string, ts time.Time) {
	m.subscriptionsMu.Lock()
	defer m.subscriptionsMu.Unlock()

	refs, ok := m.subscriptions[clientID]
	if !ok {
		return
	}

	ref, ok := refs[key]
	if !ok {
		return
	}

	ref.lastSeen = ts
}

func (m *Manager) removeSubscriptionKeys(clientID string, keys []string) {
	if len(keys) == 0 {
		return
	}

	m.subscriptionsMu.Lock()
	defer m.subscriptionsMu.Unlock()

	refs, ok := m.subscriptions[clientID]
	if !ok {
		return
	}

	for _, key := range keys {
		delete(refs, key)
	}

	if len(refs) == 0 {
		delete(m.subscriptions, clientID)
	}
}

func (m *Manager) pruneStaleSubscriptions() {
	maxIdle := m.config.ConsumerTimeout * 2
	if maxIdle <= 0 {
		maxIdle = 5 * time.Minute
	}
	cutoff := time.Now().Add(-maxIdle)

	m.subscriptionsMu.Lock()
	defer m.subscriptionsMu.Unlock()

	for clientID, refs := range m.subscriptions {
		for key, ref := range refs {
			if ref.lastSeen.Before(cutoff) {
				delete(refs, key)
			}
		}
		if len(refs) == 0 {
			delete(m.subscriptions, clientID)
		}
	}
}
