// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/events"
	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

// Publish publishes a message, handling retained storage and distribution to subscribers.
// Publish takes ownership of msg on every path; callers must not use it after
// the call.
func (b *Broker) Publish(ctx context.Context, msg *message.Envelope) error {
	defer message.Release(msg)
	if b.telemetry.logger.Enabled(ctx, slog.LevelDebug) {
		b.logOp("publish", slog.String("topic", msg.Topic), slog.Int("qos", int(msg.Broker.Delivery.QoS)), slog.Bool("retain", msg.Broker.Delivery.Retain))
	}
	b.telemetry.stats.IncrementPublishReceived()

	payloadLen := len(msg.PayloadBytes())
	b.telemetry.stats.AddBytesReceived(uint64(payloadLen))

	// Record metrics
	if b.telemetry.metrics != nil {
		b.telemetry.metrics.RecordMessageReceived(msg.Broker.Delivery.QoS, int64(payloadLen)) //nolint:contextcheck // metrics recording uses background context internally
	}

	route := b.routeResolver.Resolve(msg.Topic)

	// Handle retained messages before routing — ensures queue topics also
	// store retained state so new subscribers receive the last known value.
	if msg.Broker.Delivery.Retain {
		if err := b.handleRetained(ctx, msg, payloadLen); err != nil {
			if route.Kind == broker.RouteQueue {
				b.logError("retained_store_failed", err, slog.String("topic", msg.Topic))
			} else {
				return err
			}
		}
		// For queue topics, retained messages are stored only in the retained
		// store (for last-known-value delivery on subscribe). They are NOT
		// enqueued — the queue handles the ordered stream of non-retained
		// messages separately, avoiding duplicates on subscribe.
		if route.Kind == broker.RouteQueue {
			return nil
		}
	}

	// Route queue topics and ack topics to queue manager
	if b.queueManager != nil {
		switch route.Kind {
		case broker.RouteQueueMalformed:
			b.logError("queue_control_verb_misplaced",
				fmt.Errorf("%q must be the last level of a queue address", route.ControlVerb),
				slog.String("topic", msg.Topic))
			return fmt.Errorf("queue address %q: %s must be the last level", msg.Topic, route.ControlVerb)
		case broker.RouteQueueAck:
			return b.handleQueueAck(ctx, msg, route)
		case broker.RouteQueue:
			// PublishRequest is a byte-slice ingress contract. The queue creates
			// its own immutable buffer before this envelope is released.
			return b.queueManager.Publish(ctx, queuePublishRequest(msg))
		}
	}

	// A configured stream may bind an ordinary topic pattern (for example
	// m/#). Capture it only on the ingress node; cluster-forwarded pub/sub
	// deliveries use ForwardPublish and therefore cannot append duplicates.
	//
	// A capture failure never fails the publish: see broker.TopicQueuePublisher.
	if publisher, ok := b.queueManager.(broker.TopicQueuePublisher); ok {
		if err := publisher.PublishToMatchingQueues(ctx, queuePublishRequest(msg)); err != nil {
			b.logError("queue_topic_capture", err, slog.String("topic", msg.Topic))
		}
	}

	// Webhook: message published
	if b.telemetry.webhooks != nil {
		payload := ""
		// Note: Payload encoding should be done by caller if needed
		// ClientID not available at broker level, will be set by handler
		b.telemetry.webhooks.Notify(ctx, events.MessagePublished{ //nolint:errcheck // fire-and-forget webhook notification
			ClientID:     "", // Set by handler
			MessageTopic: msg.Topic,
			QoS:          msg.Broker.Delivery.QoS,
			Retained:     msg.Broker.Delivery.Retain,
			PayloadSize:  payloadLen,
			Payload:      payload, // Will be set if includePayload is true
		})
	}

	// Event hook: message published
	if b.eventHook != nil {
		if err := b.eventHook.OnPublish(ctx, msg.Broker.Source.ClientID, msg.Topic, msg.Broker.Delivery.QoS, msg.PayloadBytes()); err != nil {
			b.logError("event_hook_publish", err, slog.String("topic", msg.Topic))
		}
	}

	// Distribute message to subscribers (this will retain the buffer as needed)
	err := b.distribute(ctx, msg)

	return err
}

// PublishWill publishes a session's will message if it exists.
func (b *Broker) PublishWill(ctx context.Context, clientID string) error {
	if b.stores.wills == nil {
		return nil
	}

	will, err := b.stores.wills.Get(ctx, clientID)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil
		}
		return err
	}

	if err := b.publishWillMessage(ctx, will); err != nil {
		return err
	}

	return b.stores.wills.Delete(ctx, clientID)
}

// publishWillMessage distributes a Will message. Used both for stored Wills
// (PublishWill) and for the Will of a connection displaced by a takeover.
func (b *Broker) publishWillMessage(ctx context.Context, will *storage.WillMessage) error {
	// Persisted Will payloads are byte slices; ingress copies them into an
	// immutable broker buffer.
	msg := message.New(will.Topic, will.Payload)
	msg.Broker.Source.ClientID = will.ClientID
	msg.Broker.Delivery.QoS = will.QoS
	msg.Broker.Delivery.Retain = will.Retain
	msg.User.Properties = will.Properties

	err := b.distribute(ctx, msg)
	message.Release(msg)
	return err
}

// handleRetained stores or clears a retained message.
func (b *Broker) handleRetained(ctx context.Context, msg *message.Envelope, payloadLen int) error {
	if payloadLen == 0 {
		if err := b.stores.retained.Delete(ctx, msg.Topic); err != nil {
			return err
		}
		if b.cluster != nil {
			if err := b.cluster.Retained().Delete(ctx, msg.Topic); err != nil {
				b.logError("cluster_delete_retained", err, slog.String("topic", msg.Topic))
			}
		}
		if b.telemetry.webhooks != nil {
			b.telemetry.webhooks.Notify(ctx, events.RetainedMessageSet{ //nolint:errcheck // fire-and-forget webhook notification
				MessageTopic: msg.Topic,
				PayloadSize:  0,
				Cleared:      true,
			})
		}
		return nil
	}

	retainedMsg := msg.Clone()
	defer message.Release(retainedMsg)
	retainedMsg.Broker.Delivery.Retain = true
	if err := b.stores.retained.Set(ctx, msg.Topic, retainedMsg); err != nil {
		return err
	}
	if b.cluster != nil {
		if err := b.cluster.Retained().Set(ctx, msg.Topic, retainedMsg); err != nil {
			b.logError("cluster_set_retained", err, slog.String("topic", msg.Topic))
		}
	}
	if b.telemetry.webhooks != nil {
		b.telemetry.webhooks.Notify(ctx, events.RetainedMessageSet{ //nolint:errcheck // fire-and-forget webhook notification
			MessageTopic: msg.Topic,
			PayloadSize:  payloadLen,
			Cleared:      false,
		})
	}
	return nil
}

// Distribute distributes a message to all matching subscribers (implements Service interface).
func (b *Broker) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	msg := message.New(topic, payload)
	msg.Broker.Delivery.QoS = qos
	msg.Broker.Delivery.Retain = retain
	msg.User.Properties = props

	err := b.distribute(context.Background(), msg)

	message.Release(msg)

	return err
}

// distribute distributes a message to all matching subscribers (local and remote).
func (b *Broker) distribute(ctx context.Context, msg *message.Envelope) error {
	if _, err := b.distributeLocal(ctx, msg, true); err != nil {
		return err
	}

	// Route to remote subscribers in cluster
	if b.cluster != nil {
		timeout := b.cfg.routePublishTimeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Zero-copy: Pass the payload directly (cluster routing will handle serialization)
		payload := msg.PayloadBytes()
		properties := message.ProjectProperties(msg, message.TrustedServiceProjection)
		if err := b.cluster.RoutePublish(ctx, msg.Topic, payload, msg.Broker.Delivery.QoS, false, properties); err != nil {
			b.logError("cluster_route_publish", err, slog.String("topic", msg.Topic))
			if msg.Broker.Delivery.QoS > 0 {
				return fmt.Errorf("cluster route publish: %w", err)
			}
		}
	}

	return nil
}

// distributeLocal delivers a message to local subscribers and returns the
// number of matched subscriptions.
// allowCross controls whether cross-protocol delivery callbacks may run.
func (b *Broker) distributeLocal(ctx context.Context, msg *message.Envelope, allowCross bool) (int, error) {
	matched := router.AcquireSubscriptionSlice()
	defer router.ReleaseSubscriptionSlice(matched)

	if err := b.router.MatchInto(msg.Topic, matched); err != nil {
		return 0, err
	}

	// Track which share groups have already received the message (lazy init)
	var deliveredGroups map[string]bool

	for _, sub := range *matched {
		clientID := sub.ClientID

		// Check if this is a shared subscription
		if strings.HasPrefix(clientID, "$share/") {
			// Extract group key from the special client ID
			groupKey := clientID[7:] // Remove "$share/" prefix

			// Lazy init the map only when we have shared subscriptions
			if deliveredGroups == nil {
				deliveredGroups = make(map[string]bool)
			}

			// Skip if we already delivered to this group
			if deliveredGroups[groupKey] {
				continue
			}

			// Select next subscriber in the group
			// groupKey here typically looks like "groupName/topicFilter"
			// GetNextSubscriber handles matching
			selectedClientID, ok := b.sharedSubs.GetNextSubscriber(groupKey)
			if !ok {
				continue
			}

			deliveredGroups[groupKey] = true

			s := b.sessionsMap.Get(selectedClientID)
			if s == nil {
				continue
			}

			deliverQoS := msg.Broker.Delivery.QoS
			if sub.QoS < deliverQoS {
				deliverQoS = sub.QoS
			}
			if deliverQoS == 0 {
				_ = b.deliverSharedQoS0(ctx, s, msg, false)
				continue
			}

			deliverMsg := msg.Clone()
			deliverMsg.Broker.Delivery.QoS = deliverQoS
			deliverMsg.Broker.Delivery.Retain = false // MQTT spec: shared subscriptions don't receive retained flag

			// DeliverToSession takes full ownership of the message
			if _, err := b.DeliverToSession(ctx, s, deliverMsg); err != nil {
				if deliverQoS > 0 {
					b.telemetry.logger.Warn("failed to deliver QoS message",
						slog.String("client_id", selectedClientID),
						slog.String("topic", msg.Topic),
						slog.Uint64("qos", uint64(deliverQoS)),
						slog.String("error", err.Error()))
				}
				continue
			}
		} else {
			if sub.Options.NoLocal && clientID == msg.Broker.Source.ClientID {
				continue
			}
			if broker.IsAMQP091Client(clientID) || broker.IsAMQP1Client(clientID) {
				if allowCross && b.crossDeliver != nil {
					b.crossDeliver(ctx, clientID, msg.Topic, msg.PayloadBytes(), sub.QoS, message.ProjectProperties(msg, message.TrustedServiceProjection))
				}
				continue
			}
			// Normal subscription
			s := b.sessionsMap.Get(clientID)
			if s == nil {
				continue
			}

			deliverQoS := msg.Broker.Delivery.QoS
			if sub.QoS < deliverQoS {
				deliverQoS = sub.QoS
			}
			retain := msg.Broker.Delivery.Retain && sub.Options.RetainAsPublished
			if deliverQoS == 0 {
				_ = b.deliverSharedQoS0(ctx, s, msg, retain)
				continue
			}

			deliverMsg := msg.Clone()
			deliverMsg.Broker.Delivery.QoS = deliverQoS
			deliverMsg.Broker.Delivery.Retain = retain

			// DeliverToSession takes full ownership of the message
			if _, err := b.DeliverToSession(ctx, s, deliverMsg); err != nil {
				if deliverQoS > 0 {
					b.telemetry.logger.Warn("failed to deliver QoS message",
						slog.String("client_id", clientID),
						slog.String("topic", msg.Topic),
						slog.Uint64("qos", uint64(deliverQoS)),
						slog.String("error", err.Error()))
				}
				continue
			}
		}
	}

	return len(*matched), nil
}

// ForwardPublish handles a forwarded publish from a remote cluster node.
// It converts the cluster message to a storage message and delivers locally.
func (b *Broker) ForwardPublish(ctx context.Context, msg *message.Envelope) error {
	storeMsg := msg.Clone()

	matched, err := b.distributeLocal(ctx, storeMsg, false)
	message.Release(storeMsg)
	if err == nil && matched == 0 {
		// The sending node believed a subscriber lives here (stale owner
		// route); without this the message vanishes with no trace.
		b.warnUnroutableForward(msg.Topic)
	}
	return err
}

// warnUnroutableForward logs (at most once per 10s) that a forwarded publish
// matched no local subscription.
func (b *Broker) warnUnroutableForward(topic string) {
	const throttle = int64(10 * time.Second)
	now := time.Now().UnixNano()
	last := b.lastUnroutableWarn.Load()
	if now-last < throttle || !b.lastUnroutableWarn.CompareAndSwap(last, now) {
		return
	}
	b.telemetry.logger.Warn("forwarded publish matched no local subscription",
		slog.String("topic", topic))
}

// handleQueueAck handles queue acknowledgment messages ($ack, $nack, $reject).
func (b *Broker) handleQueueAck(ctx context.Context, msg *message.Envelope, route broker.RouteResult) error {
	queueName := route.QueueName

	if queueName == "" {
		b.logError("queue_ack_invalid_queue_topic", fmt.Errorf("invalid queue topic %q", route.PublishTopic),
			slog.String("topic", msg.Topic))
		return fmt.Errorf("invalid queue topic: %s", route.PublishTopic)
	}

	// Extract message ID and group ID from properties
	groupID := msg.Broker.Queue.GroupID
	offset := msg.Broker.Queue.Offset

	if groupID == "" {
		b.logError("queue_ack_missing_group_id", fmt.Errorf("group-id not found in properties"),
			slog.String("topic", msg.Topic))
		return fmt.Errorf("group-id required for queue acknowledgment")
	}

	switch route.AckKind {
	case broker.AckAccept:
		b.logOp("queue_ack", slog.String("queue", queueName), slog.Uint64("offset", offset), slog.String("group_id", groupID))
		return b.queueManager.Ack(ctx, queueName, groupID, offset)
	case broker.AckNack:
		b.logOp("queue_nack", slog.String("queue", queueName), slog.Uint64("offset", offset), slog.String("group_id", groupID))
		return b.queueManager.Nack(ctx, queueName, groupID, offset)
	case broker.AckReject:
		reason := "rejected by consumer"
		if msg.User.Properties != nil && msg.User.Properties[types.PropRejectReason] != "" {
			reason = msg.User.Properties[types.PropRejectReason]
		}
		b.logOp("queue_reject", slog.String("queue", queueName), slog.Uint64("offset", offset), slog.String("group_id", groupID), slog.String("reason", reason))
		return b.queueManager.Reject(ctx, queueName, groupID, offset, reason)
	default:
		return fmt.Errorf("invalid queue ack topic: %s", msg.Topic)
	}
}

// GetRetainedMatching returns all retained messages matching a topic filter.
// In clustered mode, queries the cluster; otherwise uses local storage.
func (b *Broker) GetRetainedMatching(filter string) ([]*message.Envelope, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if b.cluster != nil {
		return b.cluster.Retained().Match(ctx, filter)
	}
	return b.stores.retained.Match(ctx, filter)
}

// triggerWills processes pending will messages.
func (b *Broker) triggerWills() {
	if b.stores.wills == nil {
		return
	}

	ctx := context.Background()
	pending, err := b.stores.wills.GetPending(ctx, time.Now())
	if err != nil {
		return
	}

	for _, will := range pending {
		s := b.Get(will.ClientID)
		if s != nil && s.IsConnected() {
			b.stores.wills.Delete(ctx, will.ClientID) //nolint:errcheck // best-effort will cleanup for connected client
			continue
		}

		// Create a broker-owned envelope from the persisted Will payload.
		msg := message.New(will.Topic, will.Payload)
		msg.Broker.Source.ClientID = will.ClientID
		msg.Broker.Delivery.QoS = will.QoS
		msg.Broker.Delivery.Retain = will.Retain
		msg.User.Properties = will.Properties

		b.distribute(ctx, msg) //nolint:errcheck // fire-and-forget will message distribution

		message.Release(msg)

		b.stores.wills.Delete(ctx, will.ClientID) //nolint:errcheck // best-effort will cleanup after distribution
	}
}

// GetRetainedMessage implements cluster.MessageHandler.GetRetainedMessage.
// Fetches a retained message from the local storage for remote node requests.
func (b *Broker) GetRetainedMessage(ctx context.Context, topic string) (*message.Envelope, error) {
	if b.stores.retained == nil {
		return nil, fmt.Errorf("retained store not configured")
	}
	return b.stores.retained.Get(ctx, topic)
}

// GetWillMessage implements cluster.MessageHandler.GetWillMessage.
// Fetches a will message from the local storage for remote node requests.
func (b *Broker) GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	if b.stores.wills == nil {
		return nil, fmt.Errorf("will store not configured")
	}
	return b.stores.wills.Get(ctx, clientID)
}
