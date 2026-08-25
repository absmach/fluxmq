// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import "github.com/absmach/fluxmq/message"

func envelopeFromWire(topic string, data []byte, qos byte, retain, duplicate bool, properties map[string]string) *message.Envelope {
	envelope := message.New(topic, data)
	message.ApplyTrustedProperties(envelope, properties)
	envelope.Broker.Delivery.QoS = qos
	envelope.Broker.Delivery.Retain = retain
	envelope.Broker.Delivery.Duplicate = duplicate
	return envelope
}

// QueueDelivery pairs a queue message envelope with its target local client.
// Used for batched cross-node queue delivery.
type QueueDelivery struct {
	ClientID string
	Message  *message.Envelope
}
