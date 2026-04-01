// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import corebroker "github.com/absmach/fluxmq/broker"

func setExternalIDProperty(props map[string]string, externalID string) map[string]string {
	if externalID == "" {
		return props
	}
	if props == nil {
		props = make(map[string]string, 1)
	}
	props[corebroker.ExternalIDProperty] = externalID
	return props
}
