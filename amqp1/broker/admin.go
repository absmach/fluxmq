// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"cmp"
	"slices"
	"time"
)

// SubscriptionSnapshot describes an established receiver link.
type SubscriptionSnapshot struct {
	Filter        string
	ConsumerGroup string
}

// ConnectionSnapshot is the client inventory view, one entry per connection.
// AMQP channels and links remain internal to that client connection.
type ConnectionSnapshot struct {
	ClientID      string
	ContainerID   string
	ConnectedAt   time.Time
	Subscriptions []SubscriptionSnapshot
}

func (b *Broker) ConnectionSnapshot(containerID string) (ConnectionSnapshot, bool) {
	value, ok := b.connections.Load(containerID)
	if !ok {
		return ConnectionSnapshot{}, false
	}
	c := value.(*Connection)
	if c.closed.Load() {
		return ConnectionSnapshot{}, false
	}
	result := ConnectionSnapshot{ClientID: PrefixedClientID(containerID), ContainerID: containerID, ConnectedAt: c.openedAt}
	seen := make(map[SubscriptionSnapshot]bool)
	c.sessionsMu.RLock()
	defer c.sessionsMu.RUnlock()
	for _, s := range c.sessions {
		s.linksMu.RLock()
		for _, l := range s.links {
			if sub := l.adminSubscription.Load(); sub != nil && !seen[*sub] {
				seen[*sub] = true
				result.Subscriptions = append(result.Subscriptions, *sub)
			}
		}
		s.linksMu.RUnlock()
	}
	slices.SortFunc(result.Subscriptions, func(a, b SubscriptionSnapshot) int {
		if order := cmp.Compare(a.Filter, b.Filter); order != 0 {
			return order
		}
		return cmp.Compare(a.ConsumerGroup, b.ConsumerGroup)
	})
	return result, true
}

func (b *Broker) ConnectionSnapshots() []ConnectionSnapshot {
	var result []ConnectionSnapshot
	b.connections.Range(func(key, _ any) bool {
		if s, ok := b.ConnectionSnapshot(key.(string)); ok {
			result = append(result, s)
		}
		return true
	})
	slices.SortFunc(result, func(a, b ConnectionSnapshot) int {
		return cmp.Compare(a.ClientID, b.ClientID)
	})
	return result
}
