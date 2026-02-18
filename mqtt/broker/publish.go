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
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

// Publish publishes a message, handling retained storage and distribution to subscribers.
func (b *Broker) Publish(msg *storage.Message) error {
	b.logOp("publish", slog.String("topic", msg.Topic), slog.Int("qos", int(msg.QoS)), slog.Bool("retain", msg.Retain))
	b.stats.IncrementPublishReceived()

	payloadLen := len(msg.GetPayload())
	b.stats.AddBytesReceived(uint64(payloadLen))

	// Record metrics
	if b.metrics != nil {
		b.metrics.RecordMessageReceived(msg.QoS, int64(payloadLen))
	}

	route := b.routeResolver.Resolve(msg.Topic)

	// Handle retained messages before routing — ensures queue topics also
	// store retained state so new subscribers receive the last known value.
	if msg.Retain {
		if err := b.handleRetained(msg, payloadLen); err != nil {
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
		case broker.RouteQueueAck:
			return b.handleQueueAck(msg, route)
		case broker.RouteQueue:
			return b.queueManager.Publish(context.Background(), types.PublishRequest{
				Topic:      msg.Topic,
				Payload:    msg.GetPayload(),
				Properties: msg.Properties,
			})
		}
	}

	// Webhook: message published
	if b.webhooks != nil {
		payload := ""
		// Note: Payload encoding should be done by caller if needed
		// ClientID not available at broker level, will be set by handler
		b.webhooks.Notify(context.Background(), events.MessagePublished{
			ClientID:     "", // Set by handler
			MessageTopic: msg.Topic,
			QoS:          msg.QoS,
			Retained:     msg.Retain,
			PayloadSize:  payloadLen,
			Payload:      payload, // Will be set if includePayload is true
		})
	}

	// Distribute message to subscribers (this will retain the buffer as needed)
	err := b.distribute(msg)

	// Release the original message's buffer - Publish "consumes" the message
	msg.ReleasePayload()

	return err
}

// PublishWill publishes a session's will message if it exists.
func (b *Broker) PublishWill(clientID string) error {
	if b.wills == nil {
		return nil
	}

	ctx := context.Background()
	will, err := b.wills.Get(ctx, clientID)
	if err != nil {
		if err == storage.ErrNotFound {
			return nil
		}
		return err
	}

	// Create message from will (will payload is []byte, not RefCountedBuffer)
	msg := &storage.Message{
		Topic:      will.Topic,
		QoS:        will.QoS,
		Retain:     will.Retain,
		Properties: will.Properties,
	}
	msg.SetPayloadFromBytes(will.Payload)

	if err := b.distribute(msg); err != nil {
		msg.ReleasePayload()
		return err
	}

	// Release the message buffer after distribution
	msg.ReleasePayload()

	return b.wills.Delete(ctx, clientID)
}

// handleRetained stores or clears a retained message.
func (b *Broker) handleRetained(msg *storage.Message, payloadLen int) error {
	ctx := context.Background()
	if payloadLen == 0 {
		if err := b.retained.Delete(ctx, msg.Topic); err != nil {
			return err
		}
		if b.cluster != nil {
			if err := b.cluster.Retained().Delete(ctx, msg.Topic); err != nil {
				b.logError("cluster_delete_retained", err, slog.String("topic", msg.Topic))
			}
		}
		if b.webhooks != nil {
			b.webhooks.Notify(ctx, events.RetainedMessageSet{
				MessageTopic: msg.Topic,
				PayloadSize:  0,
				Cleared:      true,
			})
		}
		return nil
	}

	retainedMsg := storage.CopyMessage(msg)
	retainedMsg.Retain = true
	if err := b.retained.Set(ctx, msg.Topic, retainedMsg); err != nil {
		retainedMsg.ReleasePayload()
		return err
	}
	if b.cluster != nil {
		retainedMsg.RetainPayload()
		if err := b.cluster.Retained().Set(ctx, msg.Topic, retainedMsg); err != nil {
			retainedMsg.ReleasePayload()
			b.logError("cluster_set_retained", err, slog.String("topic", msg.Topic))
		}
	}
	if b.webhooks != nil {
		b.webhooks.Notify(ctx, events.RetainedMessageSet{
			MessageTopic: msg.Topic,
			PayloadSize:  payloadLen,
			Cleared:      false,
		})
	}
	return nil
}

// Distribute distributes a message to all matching subscribers (implements Service interface).
func (b *Broker) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	msg := &storage.Message{
		Topic:      topic,
		QoS:        qos,
		Retain:     retain,
		Properties: props,
	}
	msg.SetPayloadFromBytes(payload)

	err := b.distribute(msg)

	// Release the message buffer after distribution
	msg.ReleasePayload()

	return err
}

// distribute distributes a message to all matching subscribers (local and remote).
func (b *Broker) distribute(msg *storage.Message) error {
	if err := b.distributeLocal(msg); err != nil {
		return err
	}

	// Route to remote subscribers in cluster
	if b.cluster != nil {
		timeout := b.routePublishTimeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Zero-copy: Pass the payload directly (cluster routing will handle serialization)
		payload := msg.GetPayload()
		if err := b.cluster.RoutePublish(ctx, msg.Topic, payload, msg.QoS, false, msg.Properties); err != nil {
			b.logError("cluster_route_publish", err, slog.String("topic", msg.Topic))
			if msg.QoS > 0 {
				return fmt.Errorf("cluster route publish: %w", err)
			}
		}
	}

	return nil
}

// distributeLocal delivers a message to all matching local subscribers without cluster routing.
func (b *Broker) distributeLocal(msg *storage.Message) error {
	return b.distributeLocalScoped(msg, true)
}

// distributeLocalScoped delivers a message to local subscribers.
// allowCross controls whether cross-protocol delivery callbacks may run.
func (b *Broker) distributeLocalScoped(msg *storage.Message, allowCross bool) error {
	matched, err := b.router.Match(msg.Topic)
	if err != nil {
		return err
	}

	// Track which share groups have already received the message (lazy init)
	var deliveredGroups map[string]bool

	for _, sub := range matched {
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

			deliverQoS := msg.QoS
			if sub.QoS < deliverQoS {
				deliverQoS = sub.QoS
			}

			// Zero-copy: Retain buffer for this subscriber's message
			msg.RetainPayload()
			deliverMsg := storage.AcquireMessage()
			deliverMsg.Topic = msg.Topic
			deliverMsg.QoS = deliverQoS
			deliverMsg.Retain = false // MQTT spec: shared subscriptions don't receive retained flag
			deliverMsg.Properties = msg.Properties
			deliverMsg.SetPayloadFromBuffer(msg.PayloadBuf)

			// DeliverToSession takes full ownership of the message
			if _, err := b.DeliverToSession(s, deliverMsg); err != nil {
				if deliverQoS > 0 {
					b.logger.Warn("failed to deliver QoS message",
						slog.String("client_id", selectedClientID),
						slog.String("topic", msg.Topic),
						slog.Uint64("qos", uint64(deliverQoS)),
						slog.String("error", err.Error()))
				}
				continue
			}
		} else {
			if broker.IsAMQP091Client(clientID) || broker.IsAMQP1Client(clientID) {
				if allowCross && b.crossDeliver != nil {
					b.crossDeliver(clientID, msg.Topic, msg.GetPayload(), sub.QoS, msg.Properties)
				}
				continue
			}
			// Normal subscription
			s := b.sessionsMap.Get(clientID)
			if s == nil {
				continue
			}

			deliverQoS := msg.QoS
			if sub.QoS < deliverQoS {
				deliverQoS = sub.QoS
			}

			// Zero-copy: Retain buffer for this subscriber's message
			msg.RetainPayload()
			deliverMsg := storage.AcquireMessage()
			deliverMsg.Topic = msg.Topic
			deliverMsg.QoS = deliverQoS
			deliverMsg.Retain = false // Don't retain during distribution
			deliverMsg.Properties = msg.Properties
			deliverMsg.SetPayloadFromBuffer(msg.PayloadBuf)

			// DeliverToSession takes full ownership of the message
			if _, err := b.DeliverToSession(s, deliverMsg); err != nil {
				if deliverQoS > 0 {
					b.logger.Warn("failed to deliver QoS message",
						slog.String("client_id", clientID),
						slog.String("topic", msg.Topic),
						slog.Uint64("qos", uint64(deliverQoS)),
						slog.String("error", err.Error()))
				}
				continue
			}
		}
	}

	return nil
}

// ForwardPublish handles a forwarded publish from a remote cluster node.
// It converts the cluster message to a storage message and delivers locally.
func (b *Broker) ForwardPublish(ctx context.Context, msg *cluster.Message) error {
	storeMsg := storage.AcquireMessage()
	storeMsg.Topic = msg.Topic
	storeMsg.QoS = msg.QoS
	storeMsg.Retain = msg.Retain
	storeMsg.Properties = msg.Properties
	storeMsg.SetPayloadFromBytes(msg.Payload)

	err := b.distributeLocalScoped(storeMsg, false)
	storeMsg.ReleasePayload()
	return err
}

// handleQueueAck handles queue acknowledgment messages ($ack, $nack, $reject).
func (b *Broker) handleQueueAck(msg *storage.Message, route broker.RouteResult) error {
	ctx := context.Background()
	queueName := route.QueueName

	if queueName == "" {
		b.logError("queue_ack_invalid_queue_topic", fmt.Errorf("invalid queue topic %q", route.PublishTopic),
			slog.String("topic", msg.Topic))
		return fmt.Errorf("invalid queue topic: %s", route.PublishTopic)
	}

	// Extract message ID and group ID from properties
	var messageID, groupID string
	if msg.Properties != nil {
		messageID = msg.Properties[types.PropMessageID]
		groupID = msg.Properties[types.PropGroupID]
	}

	if messageID == "" {
		b.logError("queue_ack_missing_message_id", fmt.Errorf("message-id not found in properties"),
			slog.String("topic", msg.Topic))
		return fmt.Errorf("message-id required for queue acknowledgment")
	}

	if groupID == "" {
		b.logError("queue_ack_missing_group_id", fmt.Errorf("group-id not found in properties"),
			slog.String("topic", msg.Topic))
		return fmt.Errorf("group-id required for queue acknowledgment")
	}

	switch route.AckKind {
	case broker.AckAccept:
		b.logOp("queue_ack", slog.String("queue", queueName), slog.String("message_id", messageID), slog.String("group_id", groupID))
		return b.queueManager.Ack(ctx, queueName, messageID, groupID)
	case broker.AckNack:
		b.logOp("queue_nack", slog.String("queue", queueName), slog.String("message_id", messageID), slog.String("group_id", groupID))
		return b.queueManager.Nack(ctx, queueName, messageID, groupID)
	case broker.AckReject:
		reason := "rejected by consumer"
		if msg.Properties != nil && msg.Properties[types.PropRejectReason] != "" {
			reason = msg.Properties[types.PropRejectReason]
		}
		b.logOp("queue_reject", slog.String("queue", queueName), slog.String("message_id", messageID), slog.String("group_id", groupID), slog.String("reason", reason))
		return b.queueManager.Reject(ctx, queueName, messageID, groupID, reason)
	default:
		return fmt.Errorf("invalid queue ack topic: %s", msg.Topic)
	}
}

// GetRetainedMatching returns all retained messages matching a topic filter.
// In clustered mode, queries the cluster; otherwise uses local storage.
func (b *Broker) GetRetainedMatching(filter string) ([]*storage.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if b.cluster != nil {
		return b.cluster.Retained().Match(ctx, filter)
	}
	return b.retained.Match(ctx, filter)
}

// triggerWills processes pending will messages.
func (b *Broker) triggerWills() {
	if b.wills == nil {
		return
	}

	ctx := context.Background()
	pending, err := b.wills.GetPending(ctx, time.Now())
	if err != nil {
		return
	}

	for _, will := range pending {
		s := b.Get(will.ClientID)
		if s != nil && s.IsConnected() {
			b.wills.Delete(ctx, will.ClientID)
			continue
		}

		// Create message from will (will payload is []byte, not RefCountedBuffer)
		msg := &storage.Message{
			Topic:      will.Topic,
			QoS:        will.QoS,
			Retain:     will.Retain,
			Properties: will.Properties,
		}
		msg.SetPayloadFromBytes(will.Payload)

		b.distribute(msg)

		// Release the message buffer after distribution
		msg.ReleasePayload()

		b.wills.Delete(ctx, will.ClientID)
	}
}

// GetRetainedMessage implements cluster.MessageHandler.GetRetainedMessage.
// Fetches a retained message from the local storage for remote node requests.
func (b *Broker) GetRetainedMessage(ctx context.Context, topic string) (*storage.Message, error) {
	if b.retained == nil {
		return nil, fmt.Errorf("retained store not configured")
	}
	return b.retained.Get(ctx, topic)
}

// GetWillMessage implements cluster.MessageHandler.GetWillMessage.
// Fetches a will message from the local storage for remote node requests.
func (b *Broker) GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	if b.wills == nil {
		return nil, fmt.Errorf("will store not configured")
	}
	return b.wills.Get(ctx, clientID)
}
