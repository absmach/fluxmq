// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "github.com/absmach/fluxmq/message"

// setOriginProperties stamps the MQTT origin protocol and (when available)
// external identity onto the shared properties map carried with the message.
func setOriginProperties(props map[string]string, externalID string) map[string]string {
	if props == nil {
		props = make(map[string]string, 2)
	}
	props[message.PropertyProtocol] = string(message.ProtocolMQTT)
	if externalID != "" {
		props[message.PropertyExternalID] = externalID
	}
	return props
}
