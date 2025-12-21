// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package core

// Message represents an MQTT PUBLISH message for delivery and routing.
// This is a lightweight representation used across broker, cluster, and storage layers.
// It contains the essential fields needed for message delivery, routing, and will messages.
type Message struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Dup        bool // Duplicate delivery flag
	Properties map[string]string

	// Optional fields for specific use cases
	ClientID string // Used for will messages to identify the client
	Delay    uint32 // Used for will messages (delay before publishing)
	Expiry   uint32 // Used for will messages (message expiry interval)
}
