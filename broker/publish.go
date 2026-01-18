// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/absmach/fluxmq/broker/events"
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

	// Route queue topics and ack topics to queue manager
	if b.queueManager != nil {
		if isQueueAckTopic(msg.Topic) {
			// Handle ack/nack/reject
			return b.handleQueueAck(msg)
		}

		if isQueueTopic(msg.Topic) {
			// Route to queue manager - use existing properties or nil (avoid allocation)
			return b.queueManager.Enqueue(context.Background(), msg.Topic, msg.GetPayload(), msg.Properties)
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

	if msg.Retain {
		ctx := context.Background()
		if payloadLen == 0 {
			// Clear retained message
			if err := b.retained.Delete(ctx, msg.Topic); err != nil {
				return err
			}
			// Also delete from cluster
			if b.cluster != nil {
				if err := b.cluster.Retained().Delete(ctx, msg.Topic); err != nil {
					b.logError("cluster_delete_retained", err, slog.String("topic", msg.Topic))
				}
			}
			// Webhook: retained message cleared
			if b.webhooks != nil {
				b.webhooks.Notify(context.Background(), events.RetainedMessageSet{
					MessageTopic: msg.Topic,
					PayloadSize:  0,
					Cleared:      true,
				})
			}
		} else {
			// Set retained message - need to retain buffer for storage
			msg.RetainPayload()
			retainedMsg := storage.CopyMessage(msg)
			retainedMsg.Retain = true
			if err := b.retained.Set(ctx, msg.Topic, retainedMsg); err != nil {
				retainedMsg.ReleasePayload()
				return err
			}
			// Also store in cluster
			if b.cluster != nil {
				retainedMsg.RetainPayload()
				if err := b.cluster.Retained().Set(ctx, msg.Topic, retainedMsg); err != nil {
					retainedMsg.ReleasePayload()
					b.logError("cluster_set_retained", err, slog.String("topic", msg.Topic))
				}
			}
			// Webhook: retained message set
			if b.webhooks != nil {
				b.webhooks.Notify(context.Background(), events.RetainedMessageSet{
					MessageTopic: msg.Topic,
					PayloadSize:  payloadLen,
					Cleared:      false,
				})
			}
		}
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
	// Deliver to local subscribers
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

			// DeliverToSession takes ownership of the buffer and message
			if _, err := b.DeliverToSession(s, deliverMsg); err != nil {
				// Failed to deliver - release message back to pool
				storage.ReleaseMessage(deliverMsg)
				continue
			}
		} else {
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

			// DeliverToSession takes ownership of the buffer and message
			if _, err := b.DeliverToSession(s, deliverMsg); err != nil {
				// Failed to deliver - release message back to pool
				storage.ReleaseMessage(deliverMsg)
				continue
			}
		}
	}

	// Route to remote subscribers in cluster
	if b.cluster != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// Zero-copy: Pass the payload directly (cluster routing will handle serialization)
		payload := msg.GetPayload()
		if err := b.cluster.RoutePublish(ctx, msg.Topic, payload, msg.QoS, false, msg.Properties); err != nil {
			b.logError("cluster_route_publish", err, slog.String("topic", msg.Topic))
		}
	}

	return nil
}

// handleQueueAck handles queue acknowledgment messages ($ack, $nack, $reject).
func (b *Broker) handleQueueAck(msg *storage.Message) error {
	ctx := context.Background()

	// Extract queue topic and message ID
	queueTopic := extractQueueTopicFromAck(msg.Topic)
	messageID := ""
	groupID := ""

	// Extract message ID and group ID from properties
	if msg.Properties != nil {
		messageID = msg.Properties["message-id"]
		groupID = msg.Properties["group-id"]
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

	// Route to appropriate ack method
	if strings.HasSuffix(msg.Topic, "/$ack") {
		b.logOp("queue_ack", slog.String("queue", queueTopic), slog.String("message_id", messageID), slog.String("group_id", groupID))
		return b.queueManager.Ack(ctx, queueTopic, messageID, groupID)
	} else if strings.HasSuffix(msg.Topic, "/$nack") {
		b.logOp("queue_nack", slog.String("queue", queueTopic), slog.String("message_id", messageID), slog.String("group_id", groupID))
		return b.queueManager.Nack(ctx, queueTopic, messageID, groupID)
	} else if strings.HasSuffix(msg.Topic, "/$reject") {
		reason := "rejected by consumer"
		if msg.Properties != nil && msg.Properties["reason"] != "" {
			reason = msg.Properties["reason"]
		}
		b.logOp("queue_reject", slog.String("queue", queueTopic), slog.String("message_id", messageID), slog.String("group_id", groupID), slog.String("reason", reason))
		return b.queueManager.Reject(ctx, queueTopic, messageID, groupID, reason)
	}

	return fmt.Errorf("invalid queue ack topic: %s", msg.Topic)
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
