// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"github.com/absmach/fluxmq/message"
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

// extractUserProperties copies publisher-owned MQTT user properties. Typed
// MQTT properties are mapped directly to UserMetadata by the v5 adapter.
func extractUserProperties(props *v5.PublishProperties) map[string]string {
	result := make(map[string]string)

	if props == nil {
		return result
	}

	if props.User != nil {
		for _, prop := range props.User {
			// A device may not set broker-internal properties. They authenticate
			// nothing, so a service reading one must be able to rely on it having
			// come from another service rather than from a publishing client.
			if message.IsReservedProperty(prop.Key) {
				continue
			}
			result[prop.Key] = prop.Value
		}
	}

	return result
}
