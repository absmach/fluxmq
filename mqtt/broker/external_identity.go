// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import corebroker "github.com/absmach/fluxmq/broker"

// setOriginProperties stamps the MQTT origin protocol and (when available)
// external identity onto the shared properties map carried with the message.
func setOriginProperties(props map[string]string, externalID string) map[string]string {
	if props == nil {
		props = make(map[string]string, 2)
	}
	props[corebroker.ProtocolProperty] = corebroker.ProtocolMQTT
	if externalID != "" {
		props[corebroker.ExternalIDProperty] = externalID
	}
	return props
}
