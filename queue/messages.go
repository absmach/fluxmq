// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"fmt"
	"strconv"
	"time"

	"github.com/absmach/fluxmq/queue/types"
	brokerstorage "github.com/absmach/fluxmq/storage"
)

func (m *Manager) createDeliveryMessage(msg *types.Message, groupID string, queueName string) *brokerstorage.Message {
	props := m.createRouteProperties(msg, groupID, queueName)

	deliveryMsg := &brokerstorage.Message{
		Topic:      msg.Topic,
		QoS:        1, // queue messages use QoS 1 by default
		Properties: props,
	}
	deliveryMsg.SetPayloadFromBytes(msg.GetPayload())

	return deliveryMsg
}

func (m *Manager) decorateStreamDelivery(delivery *brokerstorage.Message, msg *types.Message, _ *types.ConsumerGroup, workCommitted uint64, hasWorkCommitted bool, primaryGroup string) {
	if delivery == nil || msg == nil {
		return
	}
	if delivery.Properties == nil {
		delivery.Properties = make(map[string]string)
	}

	m.decorateStreamProperties(delivery.Properties, msg, workCommitted, hasWorkCommitted, primaryGroup)
}

func (m *Manager) createRouteProperties(msg *types.Message, groupID, queueName string) map[string]string {
	props := make(map[string]string, len(msg.Properties)+4)
	for k, v := range msg.Properties {
		props[k] = v
	}
	props["message-id"] = fmt.Sprintf("%s:%d", queueName, msg.Sequence)
	props["group-id"] = groupID
	props["queue"] = queueName
	props["offset"] = fmt.Sprintf("%d", msg.Sequence)

	return props
}

func (m *Manager) decorateStreamProperties(properties map[string]string, msg *types.Message, workCommitted uint64, hasWorkCommitted bool, primaryGroup string) {
	if properties == nil || msg == nil {
		return
	}

	properties["x-stream-offset"] = fmt.Sprintf("%d", msg.Sequence)
	if !msg.CreatedAt.IsZero() {
		properties["x-stream-timestamp"] = fmt.Sprintf("%d", msg.CreatedAt.UnixMilli())
	}

	if hasWorkCommitted {
		properties["x-work-committed-offset"] = fmt.Sprintf("%d", workCommitted)
		properties["x-work-acked"] = strconv.FormatBool(msg.Sequence < workCommitted)
		if primaryGroup != "" {
			properties["x-work-group"] = primaryGroup
		}
	}
}

// DeliveryMessage is the internal message format for queue delivery tracking.
type DeliveryMessage struct {
	ID          string
	Payload     []byte
	Topic       string
	Properties  map[string]string
	GroupID     string
	Offset      uint64
	DeliveredAt time.Time
	AckTopic    string
	NackTopic   string
	RejectTopic string
}

func extractGroupFromClientID(clientID string) string {
	for i, c := range clientID {
		if c == '-' {
			return clientID[:i]
		}
	}
	return clientID
}

func generateMessageID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func parseMessageID(messageID string) (uint64, error) {
	var offset uint64
	// Format: queueName:offset (we only need the offset)
	for i := len(messageID) - 1; i >= 0; i-- {
		if messageID[i] == ':' {
			_, err := fmt.Sscanf(messageID[i+1:], "%d", &offset)
			return offset, err
		}
	}
	// Try parsing as just an offset
	_, err := fmt.Sscanf(messageID, "%d", &offset)
	return offset, err
}
