// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "sort"

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

	sort.Slice(all, func(i, j int) bool {
		if all[i].Filter == all[j].Filter {
			return all[i].ClientID < all[j].ClientID
		}
		return all[i].Filter < all[j].Filter
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

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Filter < snapshots[j].Filter
	})

	return snapshots
}

func (ch *Channel) subscriptionSnapshots(clientID string) []SubscriptionSnapshot {
	ch.consumersMu.RLock()
	defer ch.consumersMu.RUnlock()

	if len(ch.consumers) == 0 {
		return nil
	}

	byFilter := make(map[string]byte)
	for _, cons := range ch.consumers {
		filter := cons.queue
		if filter == "" {
			filter = cons.mqttFilter
		}
		if filter == "" {
			continue
		}

		// AMQP 0.9.1 consumers do not expose MQTT QoS semantics. We report
		// the same QoS=1 level used for the shared router integration.
		const qos byte = 1
		if existing, ok := byFilter[filter]; !ok || qos > existing {
			byFilter[filter] = qos
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

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Filter < snapshots[j].Filter
	})

	return snapshots
}
