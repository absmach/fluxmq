// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"cmp"
	"slices"
)

// SubscriptionSnapshot is an active AMQP 0.9.1 consumer view for admin APIs.
type SubscriptionSnapshot struct {
	ClientID string
	Filter   string
	QoS      byte
}

// ConnectionSubscriptions returns the active subscriptions for a connection.
func (b *Broker) ConnectionSubscriptions(connID string) []SubscriptionSnapshot {
	v, ok := b.connections.Load(connID)
	if !ok {
		return nil
	}

	return v.(*Connection).subscriptionSnapshots()
}

// ListSubscriptionSnapshots returns active subscriptions across all connections.
func (b *Broker) ListSubscriptionSnapshots() []SubscriptionSnapshot {
	connIDs := b.ConnectionIDs()
	if len(connIDs) == 0 {
		return nil
	}

	all := make([]SubscriptionSnapshot, 0)
	for _, connID := range connIDs {
		all = append(all, b.ConnectionSubscriptions(connID)...)
	}

	slices.SortFunc(all, func(a, b SubscriptionSnapshot) int {
		if c := cmp.Compare(a.Filter, b.Filter); c != 0 {
			return c
		}
		return cmp.Compare(a.ClientID, b.ClientID)
	})

	return all
}

func (c *Connection) subscriptionSnapshots() []SubscriptionSnapshot {
	clientID := PrefixedClientID(c.connID)

	c.channelsMu.RLock()
	channels := make([]*Channel, 0, len(c.channels))
	for _, ch := range c.channels {
		channels = append(channels, ch)
	}
	c.channelsMu.RUnlock()

	if len(channels) == 0 {
		return nil
	}

	byFilter := make(map[string]byte)
	for _, ch := range channels {
		for _, sub := range ch.subscriptionSnapshots(clientID) {
			if existing, ok := byFilter[sub.Filter]; !ok || sub.QoS > existing {
				byFilter[sub.Filter] = sub.QoS
			}
		}
	}

	if len(byFilter) == 0 {
		return nil
	}

	snapshots := make([]SubscriptionSnapshot, 0, len(byFilter))
	for filter, qos := range byFilter {
		snapshots = append(snapshots, SubscriptionSnapshot{
			ClientID: clientID,
			Filter:   filter,
			QoS:      qos,
		})
	}

	slices.SortFunc(snapshots, func(a, b SubscriptionSnapshot) int {
		return cmp.Compare(a.Filter, b.Filter)
	})

	return snapshots
}

func (ch *Channel) subscriptionSnapshots(clientID string) []SubscriptionSnapshot {
	ch.consumersMu.RLock()
	defer ch.consumersMu.RUnlock()

	if len(ch.consumers) == 0 {
		return nil
	}

	// AMQP 0.9.1 consumers do not expose MQTT QoS semantics. We report
	// a fixed QoS=1 matching the shared router integration level.
	const amqpQoS byte = 1

	filters := make(map[string]struct{})
	for _, cons := range ch.consumers {
		filter := cons.queue
		if filter == "" {
			filter = cons.mqttFilter
		}
		if filter == "" {
			continue
		}
		filters[filter] = struct{}{}
	}

	if len(filters) == 0 {
		return nil
	}

	snapshots := make([]SubscriptionSnapshot, 0, len(filters))
	for filter := range filters {
		snapshots = append(snapshots, SubscriptionSnapshot{
			ClientID: clientID,
			Filter:   filter,
			QoS:      amqpQoS,
		})
	}

	// No sort here — the caller (Connection.subscriptionSnapshots) deduplicates
	// into a map and sorts the final result.
	return snapshots
}
