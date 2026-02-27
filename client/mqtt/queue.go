// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

// QueuePublishOptions configures queue message publishing.
type QueuePublishOptions struct {
	QueueName  string            // Queue topic name (without $queue/ prefix)
	Payload    []byte            // Message payload
	Properties map[string]string // Additional user properties (optional)
	QoS        byte              // Quality of Service (default 1)
}

// QueueMessageHandler is called when a queue message is received.
type QueueMessageHandler func(msg *QueueMessage)

// QueueMessage represents a message received from a durable queue.
type QueueMessage struct {
	*Message // Embedded standard MQTT message

	MessageID string // Unique message ID for acknowledgment
	GroupID   string // Consumer group ID for acknowledgment
	Offset    uint64 // Queue offset
	Sequence  uint64 // Legacy alias for Offset

	// Internal fields for acknowledgment
	client    *Client
	queueName string
}

// Ack acknowledges successful message processing.
// The message will be removed from the queue.
func (qm *QueueMessage) Ack(ctx context.Context) error {
	if qm.client == nil {
		return fmt.Errorf("cannot acknowledge: client not set")
	}
	return qm.client.AckWithGroup(ctx, qm.queueName, qm.MessageID, qm.GroupID)
}

// Nack negatively acknowledges the message, triggering a retry.
// The message will be redelivered according to the retry policy.
func (qm *QueueMessage) Nack(ctx context.Context) error {
	if qm.client == nil {
		return fmt.Errorf("cannot nack: client not set")
	}
	return qm.client.NackWithGroup(ctx, qm.queueName, qm.MessageID, qm.GroupID)
}

// Reject rejects the message, sending it to the dead-letter queue.
// The message will not be retried.
func (qm *QueueMessage) Reject(ctx context.Context) error {
	if qm.client == nil {
		return fmt.Errorf("cannot reject: client not set")
	}
	return qm.client.RejectWithGroup(ctx, qm.queueName, qm.MessageID, qm.GroupID)
}

// RejectWithReason rejects the message and provides a broker-visible reason.
func (qm *QueueMessage) RejectWithReason(ctx context.Context, reason string) error {
	if qm.client == nil {
		return fmt.Errorf("cannot reject: client not set")
	}
	return qm.client.RejectWithGroupReason(ctx, qm.queueName, qm.MessageID, qm.GroupID, reason)
}

// queueSubscription tracks a queue subscription and its handler.
type queueSubscription struct {
	queueName     string
	consumerGroup string
	handler       QueueMessageHandler
}

// queueSubscriptions manages queue subscriptions.
type queueSubscriptions struct {
	mu   sync.RWMutex
	subs map[string]*queueSubscription // key: queue topic (with $queue/ prefix)
}

func newQueueSubscriptions() *queueSubscriptions {
	return &queueSubscriptions{
		subs: make(map[string]*queueSubscription),
	}
}

func (qs *queueSubscriptions) add(queueTopic string, sub *queueSubscription) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	qs.subs[queueTopic] = sub
}

func (qs *queueSubscriptions) get(queueTopic string) (*queueSubscription, bool) {
	qs.mu.RLock()
	defer qs.mu.RUnlock()
	sub, ok := qs.subs[queueTopic]
	return sub, ok
}

func (qs *queueSubscriptions) remove(queueTopic string) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	delete(qs.subs, queueTopic)
}

type queueAckInfo struct {
	groupID string
	expires time.Time
}

const maxAckCacheEntries = 10000

type queueAckCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]queueAckInfo
}

func newQueueAckCache(ttl time.Duration) *queueAckCache {
	return &queueAckCache{
		ttl:     ttl,
		entries: make(map[string]queueAckInfo),
	}
}

func (c *queueAckCache) set(messageID, groupID string) {
	if messageID == "" || groupID == "" {
		return
	}

	now := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict expired entries when cache reaches size limit
	if len(c.entries) >= maxAckCacheEntries {
		for id, entry := range c.entries {
			if entry.expires.Before(now) {
				delete(c.entries, id)
			}
		}
		// If still at limit after eviction, drop an arbitrary half
		if len(c.entries) >= maxAckCacheEntries {
			count := 0
			for id := range c.entries {
				delete(c.entries, id)
				count++
				if count >= len(c.entries)/2 {
					break
				}
			}
		}
	}

	c.entries[messageID] = queueAckInfo{
		groupID: groupID,
		expires: now.Add(c.ttl),
	}
}

func (c *queueAckCache) get(messageID string) (string, bool) {
	if messageID == "" {
		return "", false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[messageID]
	if !ok {
		return "", false
	}
	if !entry.expires.IsZero() && entry.expires.Before(time.Now()) {
		delete(c.entries, messageID)
		return "", false
	}
	return entry.groupID, true
}

// PublishToQueue publishes a message to a durable queue.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) PublishToQueue(ctx context.Context, queueName string, payload []byte) error {
	return c.PublishToQueueWithOptions(ctx, &QueuePublishOptions{
		QueueName: queueName,
		Payload:   payload,
		QoS:       1, // Default QoS 1 for queues
	})
}

// PublishToQueueWithOptions publishes a message to a durable queue with full control.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) PublishToQueueWithOptions(ctx context.Context, opts *QueuePublishOptions) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if opts == nil {
		return ErrInvalidMessage
	}
	if opts.QueueName == "" {
		return ErrInvalidTopic
	}

	topic := "$queue/" + opts.QueueName

	var userProps map[string]string
	if len(opts.Properties) > 0 {
		userProps = make(map[string]string)
		for k, v := range opts.Properties {
			userProps[k] = v
		}
	}

	qos := opts.QoS
	if qos == 0 {
		qos = 1
	}

	if c.opts.ProtocolVersion == 5 && len(userProps) > 0 {
		return c.publishWithUserProperties(ctx, topic, opts.Payload, qos, false, userProps)
	}

	return c.Publish(ctx, topic, opts.Payload, qos, false)
}

// publishWithUserProperties publishes a message with MQTT v5 user properties.
func (c *Client) publishWithUserProperties(ctx context.Context, topic string, payload []byte, qos byte, retain bool, userProps map[string]string) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	msg := NewMessage(topic, payload, qos, retain)
	msg.UserProperties = userProps
	if !c.state.isConnected() {
		return c.handleDisconnectedPublish(msg)
	}

	if qos == 0 {
		return c.sendPublishV5(ctx, msg, 0)
	}

	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}
	msg.PacketID = packetID

	if err := c.store.StoreOutbound(packetID, msg); err != nil {
		return err
	}

	op, err := c.pending.add(packetID, pendingPublish, msg)
	if err != nil {
		c.store.DeleteOutbound(packetID)
		return err
	}

	if err := c.sendPublishV5(ctx, msg, packetID); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return err
	}

	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return err
	}
	return nil
}

// sendPublishV5 sends a PUBLISH packet with user properties (MQTT v5 only).
func (c *Client) sendPublishV5(ctx context.Context, msg *Message, packetID uint16) error {
	deadline := c.writeDeadline(ctx)

	pkt := &v5.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
			Dup:        msg.Dup,
		},
		TopicName: msg.Topic,
		Payload:   msg.Payload,
		ID:        packetID,
	}

	if len(msg.UserProperties) > 0 {
		if pkt.Properties == nil {
			pkt.Properties = &v5.PublishProperties{}
		}
		pkt.Properties.User = make([]v5.User, 0, len(msg.UserProperties))
		for k, v := range msg.UserProperties {
			pkt.Properties.User = append(pkt.Properties.User, v5.User{Key: k, Value: v})
		}
	}

	if c.topicAliases != nil {
		if alias, isNew, ok := c.topicAliases.getOrAssignOutbound(msg.Topic); ok {
			if pkt.Properties == nil {
				pkt.Properties = &v5.PublishProperties{}
			}
			pkt.Properties.TopicAlias = &alias
			if !isNew {
				pkt.TopicName = ""
			}
		}
	}

	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
}

// SubscribeToQueue subscribes to a durable queue with a consumer group.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
// The handler will be called for each message received from the queue.
func (c *Client) SubscribeToQueue(ctx context.Context, queueName, consumerGroup string, handler QueueMessageHandler) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if queueName == "" {
		return ErrInvalidTopic
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	// Add $queue/ prefix
	queueTopic := "$queue/" + queueName

	// Store subscription handler
	c.queueSubs.add(queueTopic, &queueSubscription{
		queueName:     queueName,
		consumerGroup: consumerGroup,
		handler:       handler,
	})

	// Build subscription with consumer group user property (MQTT v5)
	if c.opts.ProtocolVersion == 5 {
		userProps := make(map[string]string)
		if consumerGroup != "" {
			userProps["consumer-group"] = consumerGroup
		}

		return c.subscribeWithUserProperties(ctx, queueTopic, 1, userProps)
	}

	// For MQTT v3, subscribe without consumer group (will use client ID as group)
	return c.SubscribeSingle(ctx, queueTopic, 1)
}

// subscribeWithUserProperties subscribes to a topic with MQTT v5 user properties.
func (c *Client) subscribeWithUserProperties(ctx context.Context, topic string, qos byte, userProps map[string]string) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}

	op, err := c.pending.add(packetID, pendingSubscribe, nil)
	if err != nil {
		return err
	}

	if err := c.sendSubscribeWithUserProps(ctx, packetID, topic, qos, userProps); err != nil {
		c.pending.remove(packetID)
		return err
	}

	if err := op.waitWithContext(ctx, c.opts.AckTimeout); err != nil {
		c.pending.remove(packetID)
		return err
	}
	return nil
}

// sendSubscribeWithUserProps sends a SUBSCRIBE packet with user properties (MQTT v5).
func (c *Client) sendSubscribeWithUserProps(ctx context.Context, packetID uint16, topic string, qos byte, userProps map[string]string) error {
	deadline := c.writeDeadline(ctx)

	pkt := &v5.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          packetID,
		Opts: []v5.SubOption{
			{Topic: topic, MaxQoS: qos},
		},
	}

	if len(userProps) > 0 {
		if pkt.Properties == nil {
			pkt.Properties = &v5.SubscribeProperties{}
		}
		pkt.Properties.User = make([]v5.User, 0, len(userProps))
		for k, v := range userProps {
			pkt.Properties.User = append(pkt.Properties.User, v5.User{Key: k, Value: v})
		}
	}

	c.updateActivity()
	return c.queueWrite(pkt.Encode(), deadline)
}

// UnsubscribeFromQueue unsubscribes from a durable queue.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) UnsubscribeFromQueue(ctx context.Context, queueName string) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	queueTopic := "$queue/" + queueName

	// Remove handler
	c.queueSubs.remove(queueTopic)

	// Unsubscribe from topic
	return c.Unsubscribe(ctx, queueTopic)
}

// Ack acknowledges successful processing of a queue message.
func (c *Client) Ack(ctx context.Context, queueName, messageID string) error {
	return c.sendQueueAck(ctx, queueName, messageID, "", "$ack", "")
}

// Nack negatively acknowledges a queue message, triggering retry.
func (c *Client) Nack(ctx context.Context, queueName, messageID string) error {
	return c.sendQueueAck(ctx, queueName, messageID, "", "$nack", "")
}

// Reject rejects a queue message, sending it to the dead-letter queue.
func (c *Client) Reject(ctx context.Context, queueName, messageID string) error {
	return c.sendQueueAck(ctx, queueName, messageID, "", "$reject", "")
}

// RejectWithReason rejects a queue message with a broker-visible reason.
func (c *Client) RejectWithReason(ctx context.Context, queueName, messageID, reason string) error {
	return c.sendQueueAck(ctx, queueName, messageID, "", "$reject", reason)
}

// AckWithGroup acknowledges a queue message with an explicit consumer group.
func (c *Client) AckWithGroup(ctx context.Context, queueName, messageID, groupID string) error {
	return c.sendQueueAck(ctx, queueName, messageID, groupID, "$ack", "")
}

// NackWithGroup negatively acknowledges a queue message with an explicit consumer group.
func (c *Client) NackWithGroup(ctx context.Context, queueName, messageID, groupID string) error {
	return c.sendQueueAck(ctx, queueName, messageID, groupID, "$nack", "")
}

// RejectWithGroup rejects a queue message with an explicit consumer group.
func (c *Client) RejectWithGroup(ctx context.Context, queueName, messageID, groupID string) error {
	return c.sendQueueAck(ctx, queueName, messageID, groupID, "$reject", "")
}

// RejectWithGroupReason rejects a queue message with explicit group and reason.
func (c *Client) RejectWithGroupReason(ctx context.Context, queueName, messageID, groupID, reason string) error {
	return c.sendQueueAck(ctx, queueName, messageID, groupID, "$reject", reason)
}

// sendQueueAck sends an acknowledgment for a queue message.
func (c *Client) sendQueueAck(ctx context.Context, queueName, messageID, groupID, ackType, reason string) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if !c.state.isConnected() {
		return ErrNotConnected
	}

	if c.opts.ProtocolVersion != 5 {
		return ErrQueueAckRequiresV5
	}

	if groupID == "" && c.queueAckCache != nil {
		if cachedGroup, ok := c.queueAckCache.get(messageID); ok {
			groupID = cachedGroup
		}
	}

	if groupID == "" {
		queueTopic := "$queue/" + queueName
		if sub, ok := c.queueSubs.get(queueTopic); ok {
			if sub.consumerGroup != "" {
				groupID = sub.consumerGroup
			} else if c.opts.ClientID != "" {
				groupID = defaultGroupID(c.opts.ClientID)
			}
		}
	}

	if groupID == "" {
		return ErrQueueAckMissingGroup
	}

	// Build ack topic: $queue/{queueName}/{$ack|$nack|$reject}
	ackTopic := "$queue/" + queueName + "/" + ackType

	// Send with message-id user property (MQTT v5)
	userProps := map[string]string{
		"message-id": messageID,
		"group-id":   groupID,
	}
	if reason != "" && ackType == "$reject" {
		userProps["reason"] = reason
	}
	return c.publishWithUserProperties(ctx, ackTopic, nil, 1, false, userProps)
}

func defaultGroupID(clientID string) string {
	for i, c := range clientID {
		if c == '-' {
			return clientID[:i]
		}
	}
	return clientID
}

// isQueueTopic returns true if the topic is a queue topic.
func isQueueTopic(topic string) bool {
	return len(topic) >= 7 && topic[:7] == "$queue/"
}
