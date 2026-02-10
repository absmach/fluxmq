// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"encoding/base64"
	"strconv"

	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

// extractConsumerGroup extracts the consumer group from SUBSCRIBE properties.
func extractConsumerGroup(id string, props *v5.SubscribeProperties) string {
	if props == nil || props.User == nil {
		return id // Use clientID prefix as fallback
	}

	for _, prop := range props.User {
		if prop.Key == "consumer-group" {
			return prop.Value
		}
	}
	return id
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
	if props.ContentType != "" {
		result["content-type"] = props.ContentType
	}

	if props.ResponseTopic != "" {
		result["response-topic"] = props.ResponseTopic
	}

	if props.CorrelationData != nil {
		result["correlation-id"] = base64.StdEncoding.EncodeToString(props.CorrelationData)
	}

	if props.PayloadFormat != nil {
		result["payload-format"] = strconv.FormatUint(uint64(*props.PayloadFormat), 10)
	}

	return result
}
