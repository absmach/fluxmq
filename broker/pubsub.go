// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import "strings"

const (
	AMQP091ClientPrefix = "amqp091-"
	AMQP1ClientPrefix   = "amqp:"
)

// CrossDeliverFunc delivers a pub/sub message to a client in another protocol broker.
type CrossDeliverFunc func(clientID string, topic string, payload []byte, qos byte, props map[string]string)

// IsAMQP091Client returns true when the client ID belongs to an AMQP 0.9.1 connection.
func IsAMQP091Client(clientID string) bool {
	return strings.HasPrefix(clientID, AMQP091ClientPrefix)
}

// PrefixedAMQP091ClientID returns the canonical AMQP 0.9.1 client ID.
func PrefixedAMQP091ClientID(connID string) string {
	return AMQP091ClientPrefix + connID
}

// IsAMQP1Client returns true when the client ID belongs to an AMQP 1.0 connection.
func IsAMQP1Client(clientID string) bool {
	return strings.HasPrefix(clientID, AMQP1ClientPrefix)
}

// PrefixedAMQP1ClientID returns the canonical AMQP 1.0 client ID.
func PrefixedAMQP1ClientID(containerID string) string {
	return AMQP1ClientPrefix + containerID
}
