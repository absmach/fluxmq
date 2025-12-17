// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

// Message represents a protocol-agnostic MQTT message.
type Message struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	PacketID   uint16
	Properties map[string]string
}

// SessionOptions contains protocol-agnostic session configuration.
type SessionOptions struct {
	CleanStart     bool
	KeepAlive      uint16
	SessionExpiry  uint32
	ReceiveMaximum uint16
	WillMessage    *Message
}

// SubscriptionOptions contains protocol-agnostic subscription configuration.
type SubscriptionOptions struct {
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
}
