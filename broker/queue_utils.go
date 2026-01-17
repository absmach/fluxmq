// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"strings"

	v5 "github.com/absmach/fluxmq/core/packets/v5"
)

// isQueueTopic returns true if the topic is a queue topic.
func isQueueTopic(topic string) bool {
	return strings.HasPrefix(topic, "$queue/")
}

// isQueueAckTopic returns true if the topic is a queue acknowledgment topic.
func isQueueAckTopic(topic string) bool {
	return strings.HasSuffix(topic, "/$ack") ||
		strings.HasSuffix(topic, "/$nack") ||
		strings.HasSuffix(topic, "/$reject")
}

// extractQueueTopicFromAck extracts the queue topic from an ack topic.
// Example: "$queue/tasks/image/$ack" -> "$queue/tasks/image".
func extractQueueTopicFromAck(ackTopic string) string {
	if strings.HasSuffix(ackTopic, "/$ack") {
		return strings.TrimSuffix(ackTopic, "/$ack")
	}
	if strings.HasSuffix(ackTopic, "/$nack") {
		return strings.TrimSuffix(ackTopic, "/$nack")
	}
	if strings.HasSuffix(ackTopic, "/$reject") {
		return strings.TrimSuffix(ackTopic, "/$reject")
	}
	return ackTopic
}

// extractConsumerGroup extracts the consumer group from SUBSCRIBE properties.
func extractConsumerGroup(props *v5.SubscribeProperties) string {
	if props == nil || props.User == nil {
		return "" // Use clientID prefix as fallback
	}

	for _, prop := range props.User {
		if prop.Key == "consumer-group" {
			return prop.Value
		}
	}
	return ""
}

// extractAllProperties converts PUBLISH properties to a map.
func extractAllProperties(props *v5.PublishProperties) map[string]string {
	result := make(map[string]string)

	if props == nil {
		return result
	}

	if props.User != nil {
		for _, prop := range props.User {
			result[prop.Key] = prop.Value
		}
	}

	// Add other MQTT v5 properties if present
	if props.ResponseTopic != "" {
		result["response-topic"] = props.ResponseTopic
	}

	if props.CorrelationData != nil {
		result["correlation-id"] = string(props.CorrelationData)
	}

	return result
}
