// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
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
func (qm *QueueMessage) Ack() error {
	if qm.client == nil {
		return fmt.Errorf("cannot acknowledge: client not set")
	}
	return qm.client.AckWithGroup(qm.queueName, qm.MessageID, qm.GroupID)
}

// Nack negatively acknowledges the message, triggering a retry.
// The message will be redelivered according to the retry policy.
func (qm *QueueMessage) Nack() error {
	if qm.client == nil {
		return fmt.Errorf("cannot nack: client not set")
	}
	return qm.client.NackWithGroup(qm.queueName, qm.MessageID, qm.GroupID)
}

// Reject rejects the message, sending it to the dead-letter queue.
// The message will not be retried.
func (qm *QueueMessage) Reject() error {
	if qm.client == nil {
		return fmt.Errorf("cannot reject: client not set")
	}
	return qm.client.RejectWithGroup(qm.queueName, qm.MessageID, qm.GroupID)
}

// RejectWithReason rejects the message and provides a broker-visible reason.
func (qm *QueueMessage) RejectWithReason(reason string) error {
	if qm.client == nil {
		return fmt.Errorf("cannot reject: client not set")
	}
	return qm.client.RejectWithGroupReason(qm.queueName, qm.MessageID, qm.GroupID, reason)
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

	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[messageID] = queueAckInfo{
		groupID: groupID,
		expires: time.Now().Add(c.ttl),
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
func (c *Client) PublishToQueue(queueName string, payload []byte) error {
	return c.PublishToQueueWithOptions(&QueuePublishOptions{
		QueueName: queueName,
		Payload:   payload,
		QoS:       1, // Default QoS 1 for queues
	})
}

// PublishToQueueWithOptions publishes a message to a durable queue with full control.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) PublishToQueueWithOptions(opts *QueuePublishOptions) error {
	if !c.state.isConnected() {
		return ErrNotConnected
	}
	if opts == nil {
		return ErrInvalidMessage
	}
	if opts.QueueName == "" {
		return ErrInvalidTopic
	}

	// Add $queue/ prefix
	topic := "$queue/" + opts.QueueName

	// Build user properties
	var userProps map[string]string
	if len(opts.Properties) > 0 {
		userProps = make(map[string]string)
		for k, v := range opts.Properties {
			userProps[k] = v
		}
	}

	qos := opts.QoS
	if qos == 0 {
		qos = 1 // Default to QoS 1 for reliability
	}

	// For MQTT v5, use user properties
	if c.opts.ProtocolVersion == 5 && len(userProps) > 0 {
		return c.publishWithUserProperties(topic, opts.Payload, qos, false, userProps)
	}

	// For MQTT v3, just publish (partition key will be random)
	return c.Publish(topic, opts.Payload, qos, false)
}

// publishWithUserProperties publishes a message with MQTT v5 user properties.
func (c *Client) publishWithUserProperties(topic string, payload []byte, qos byte, retain bool, userProps map[string]string) error {
	msg := NewMessage(topic, payload, qos, retain)
	msg.UserProperties = userProps

	if qos == 0 {
		return c.sendPublishV5(msg, 0)
	}

	// QoS 1 or 2: need packet ID and wait for ack
	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}
	msg.PacketID = packetID

	// Store message for potential retransmission
	if err := c.store.StoreOutbound(packetID, msg); err != nil {
		return err
	}

	op, err := c.pending.add(packetID, pendingPublish, msg)
	if err != nil {
		c.store.DeleteOutbound(packetID)
		return err
	}

	if err := c.sendPublishV5(msg, packetID); err != nil {
		c.pending.remove(packetID)
		c.store.DeleteOutbound(packetID)
		return err
	}

	return op.wait(c.opts.AckTimeout)
}

// sendPublishV5 sends a PUBLISH packet with user properties (MQTT v5 only).
func (c *Client) sendPublishV5(msg *Message, packetID uint16) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(timeZero)

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

	// Add user properties if present
	if len(msg.UserProperties) > 0 {
		if pkt.Properties == nil {
			pkt.Properties = &v5.PublishProperties{}
		}
		pkt.Properties.User = make([]v5.User, 0, len(msg.UserProperties))
		for k, v := range msg.UserProperties {
			pkt.Properties.User = append(pkt.Properties.User, v5.User{Key: k, Value: v})
		}
	}

	// Apply topic alias if available
	if c.topicAliases != nil {
		if alias, isNew, ok := c.topicAliases.getOrAssignOutbound(msg.Topic); ok {
			if pkt.Properties == nil {
				pkt.Properties = &v5.PublishProperties{}
			}
			pkt.Properties.TopicAlias = &alias

			// If using existing alias, clear topic name to save bandwidth
			if !isNew {
				pkt.TopicName = ""
			}
		}
	}

	c.updateActivity()
	return pkt.Pack(conn)
}

// SubscribeToQueue subscribes to a durable queue with a consumer group.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
// The handler will be called for each message received from the queue.
func (c *Client) SubscribeToQueue(queueName, consumerGroup string, handler QueueMessageHandler) error {
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

		return c.subscribeWithUserProperties(queueTopic, 1, userProps)
	}

	// For MQTT v3, subscribe without consumer group (will use client ID as group)
	return c.SubscribeSingle(queueTopic, 1)
}

// subscribeWithUserProperties subscribes to a topic with MQTT v5 user properties.
func (c *Client) subscribeWithUserProperties(topic string, qos byte, userProps map[string]string) error {
	packetID := c.pending.nextPacketID()
	if packetID == 0 {
		return ErrMaxInflight
	}

	op, err := c.pending.add(packetID, pendingSubscribe, nil)
	if err != nil {
		return err
	}

	if err := c.sendSubscribeWithUserProps(packetID, topic, qos, userProps); err != nil {
		c.pending.remove(packetID)
		return err
	}

	return op.wait(c.opts.AckTimeout)
}

// sendSubscribeWithUserProps sends a SUBSCRIBE packet with user properties (MQTT v5).
func (c *Client) sendSubscribeWithUserProps(packetID uint16, topic string, qos byte, userProps map[string]string) error {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return ErrNotConnected
	}

	conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
	defer conn.SetWriteDeadline(timeZero)

	pkt := &v5.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: qos},
		ID:          packetID,
		Opts: []v5.SubOption{
			{Topic: topic, MaxQoS: qos},
		},
	}

	// Add user properties if present
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
	return pkt.Pack(conn)
}

// UnsubscribeFromQueue unsubscribes from a durable queue.
// The queueName should NOT include the "$queue/" prefix - it will be added automatically.
func (c *Client) UnsubscribeFromQueue(queueName string) error {
	queueTopic := "$queue/" + queueName

	// Remove handler
	c.queueSubs.remove(queueTopic)

	// Unsubscribe from topic
	return c.Unsubscribe(queueTopic)
}

// Ack acknowledges successful processing of a queue message.
func (c *Client) Ack(queueName, messageID string) error {
	return c.sendQueueAck(queueName, messageID, "", "$ack", "")
}

// Nack negatively acknowledges a queue message, triggering retry.
func (c *Client) Nack(queueName, messageID string) error {
	return c.sendQueueAck(queueName, messageID, "", "$nack", "")
}

// Reject rejects a queue message, sending it to the dead-letter queue.
func (c *Client) Reject(queueName, messageID string) error {
	return c.sendQueueAck(queueName, messageID, "", "$reject", "")
}

// RejectWithReason rejects a queue message with a broker-visible reason.
func (c *Client) RejectWithReason(queueName, messageID, reason string) error {
	return c.sendQueueAck(queueName, messageID, "", "$reject", reason)
}

// AckWithGroup acknowledges a queue message with an explicit consumer group.
func (c *Client) AckWithGroup(queueName, messageID, groupID string) error {
	return c.sendQueueAck(queueName, messageID, groupID, "$ack", "")
}

// NackWithGroup negatively acknowledges a queue message with an explicit consumer group.
func (c *Client) NackWithGroup(queueName, messageID, groupID string) error {
	return c.sendQueueAck(queueName, messageID, groupID, "$nack", "")
}

// RejectWithGroup rejects a queue message with an explicit consumer group.
func (c *Client) RejectWithGroup(queueName, messageID, groupID string) error {
	return c.sendQueueAck(queueName, messageID, groupID, "$reject", "")
}

// RejectWithGroupReason rejects a queue message with explicit group and reason.
func (c *Client) RejectWithGroupReason(queueName, messageID, groupID, reason string) error {
	return c.sendQueueAck(queueName, messageID, groupID, "$reject", reason)
}

// sendQueueAck sends an acknowledgment for a queue message.
func (c *Client) sendQueueAck(queueName, messageID, groupID, ackType, reason string) error {
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
	return c.publishWithUserProperties(ackTopic, nil, 1, false, userProps)
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
