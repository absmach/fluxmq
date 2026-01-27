// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/absmach/fluxmq/broker/events"
	"github.com/absmach/fluxmq/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/topics"
)

// subscribe adds a subscription for a session (internal domain method).
func (b *Broker) subscribe(s *session.Session, filter string, qos byte, opts storage.SubscribeOptions) error {
	b.logOp("subscribe", slog.String("client_id", s.ID), slog.String("filter", filter), slog.Int("qos", int(qos)))

	// Check if this is a queue subscription
	if b.queueManager != nil && isQueueTopic(filter) {
		ctx := context.Background()
		groupID := opts.ConsumerGroup // ConsumerGroup from SubscribeOptions (set by handler)
		proxyNodeID := ""
		if b.cluster != nil {
			proxyNodeID = b.cluster.NodeID()
		}

		// Parse filter into stream name and pattern
		streamName, pattern := parseQueueFilter(filter)

		b.logOp("queue_subscribe",
			slog.String("client_id", s.ID),
			slog.String("stream", streamName),
			slog.String("pattern", pattern),
			slog.String("group", groupID))

		return b.queueManager.Subscribe(ctx, streamName, pattern, s.ID, groupID, proxyNodeID)
	}

	// Check if this is a shared subscription
	if b.sharedSubs.Subscribe(s.ID, filter) {
		// Only subscribe to router when creating a new share group
		// The manager handles the grouping logic.
		// We still need to register the group with the router strictly for routing purposes.
		// The client ID used for routing is the "$share/group/filter" string.
		shareName, topicFilter, _ := topics.ParseShared(filter)
		shareClientID := "$share/" + shareName + "/" + topicFilter
		b.router.Subscribe(shareClientID, topicFilter, qos, opts)

		b.logOp("shared_subscribe",
			slog.String("client_id", s.ID),
			slog.String("share_name", shareName),
			slog.String("topic_filter", topicFilter))
	} else if !topics.IsShared(filter) {
		// Normal subscription
		b.router.Subscribe(s.ID, filter, qos, opts)
	}

	b.stats.IncrementSubscriptions()

	// Record metrics
	if b.metrics != nil {
		b.metrics.RecordSubscriptionAdded()
	}

	sub := &storage.Subscription{
		ClientID: s.ID,
		Filter:   filter,
		QoS:      qos,
		Options:  opts,
	}
	if err := b.subscriptions.Add(sub); err != nil {
		return fmt.Errorf("failed to persist subscription: %w", err)
	}

	// Add subscription to cluster
	if b.cluster != nil {
		ctx := context.Background()
		if err := b.cluster.AddSubscription(ctx, s.ID, filter, qos, opts); err != nil {
			b.logError("cluster_add_subscription", err, slog.String("client_id", s.ID), slog.String("filter", filter))
		}
	}

	s.AddSubscription(filter, opts)

	// Webhook: subscription created
	if b.webhooks != nil {
		b.webhooks.Notify(context.Background(), events.SubscriptionCreated{
			ClientID:       s.ID,
			TopicFilter:    filter,
			QoS:            qos,
			SubscriptionID: 0, // MQTT 5.0 subscription ID not available at broker level
		})
	}

	return nil
}

// unsubscribeInternal removes a subscription for a session (internal domain method).
func (b *Broker) unsubscribeInternal(s *session.Session, filter string) error {
	b.logOp("unsubscribe", slog.String("client_id", s.ID), slog.String("filter", filter))

	// Check if this is a queue unsubscription
	if b.queueManager != nil && isQueueTopic(filter) {
		ctx := context.Background()
		groupID := "" // Will be derived from clientID in queue manager

		// Parse filter into stream name and pattern
		streamName, pattern := parseQueueFilter(filter)

		b.logOp("queue_unsubscribe",
			slog.String("client_id", s.ID),
			slog.String("stream", streamName),
			slog.String("pattern", pattern))

		return b.queueManager.Unsubscribe(ctx, streamName, pattern, s.ID, groupID)
	}

	// Check if this is a shared subscription
	if b.sharedSubs.Unsubscribe(s.ID, filter) {
		// If group became empty, unsubscribe from router
		shareName, topicFilter, _ := topics.ParseShared(filter)
		shareClientID := "$share/" + shareName + "/" + topicFilter
		b.router.Unsubscribe(shareClientID, topicFilter)

		b.logOp("shared_unsubscribe",
			slog.String("client_id", s.ID),
			slog.String("share_name", shareName),
			slog.String("topic_filter", topicFilter))
	} else if !topics.IsShared(filter) {
		// Normal unsubscribe
		b.router.Unsubscribe(s.ID, filter)
	}

	b.stats.DecrementSubscriptions()

	// Record metrics
	if b.metrics != nil {
		b.metrics.RecordSubscriptionRemoved()
	}

	if err := b.subscriptions.Remove(s.ID, filter); err != nil {
		return fmt.Errorf("failed to remove subscription: %w", err)
	}

	// Remove subscription from cluster
	if b.cluster != nil {
		ctx := context.Background()
		if err := b.cluster.RemoveSubscription(ctx, s.ID, filter); err != nil {
			b.logError("cluster_remove_subscription", err, slog.String("client_id", s.ID), slog.String("filter", filter))
		}
	}

	s.RemoveSubscription(filter)

	// Webhook: subscription removed
	if b.webhooks != nil {
		b.webhooks.Notify(context.Background(), events.SubscriptionRemoved{
			ClientID:    s.ID,
			TopicFilter: filter,
		})
	}

	return nil
}

// Subscribe adds a subscription (implements Service interface).
func (b *Broker) Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error {
	s := b.Get(clientID)
	if s == nil {
		return ErrSessionNotFound
	}

	return b.subscribe(s, filter, qos, opts)
}

// Unsubscribe removes a subscription (implements Service interface).
func (b *Broker) Unsubscribe(clientID string, filter string) error {
	s := b.Get(clientID)
	if s == nil {
		return ErrSessionNotFound
	}

	return b.unsubscribeInternal(s, filter)
}

// Match returns all subscriptions matching a topic (implements Service interface).
func (b *Broker) Match(topic string) ([]*storage.Subscription, error) {
	return b.router.Match(topic)
}
