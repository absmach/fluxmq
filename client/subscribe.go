// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

// SubscribeOption represents subscription options for MQTT 5.0.
type SubscribeOption struct {
	// Topic is the topic filter to subscribe to.
	Topic string

	// QoS is the maximum QoS level the client wishes to receive.
	QoS byte

	// NoLocal prevents the client from receiving messages it published itself.
	// Only applies to MQTT 5.0. Default is false.
	NoLocal bool

	// RetainAsPublished preserves the RETAIN flag as it was set by the publisher.
	// If false, the server clears the RETAIN flag before forwarding.
	// Only applies to MQTT 5.0. Default is false.
	RetainAsPublished bool

	// RetainHandling controls when retained messages are sent:
	//   0 = Send retained messages at subscription time (default)
	//   1 = Send retained messages only if this is a new subscription
	//   2 = Don't send retained messages at all
	// Only applies to MQTT 5.0. Default is 0.
	RetainHandling byte

	// SubscriptionID is a numeric identifier for this subscription.
	// When the server sends a PUBLISH for this subscription, it will include this ID.
	// This helps the client determine which subscription(s) triggered the message.
	// 0 means no subscription identifier.
	// Only applies to MQTT 5.0. Default is 0.
	SubscriptionID uint32
}

// NewSubscribeOption creates a basic subscribe option with just topic and QoS.
func NewSubscribeOption(topic string, qos byte) *SubscribeOption {
	return &SubscribeOption{
		Topic: topic,
		QoS:   qos,
	}
}

// SetNoLocal sets the NoLocal flag.
func (o *SubscribeOption) SetNoLocal(noLocal bool) *SubscribeOption {
	o.NoLocal = noLocal
	return o
}

// SetRetainAsPublished sets the RetainAsPublished flag.
func (o *SubscribeOption) SetRetainAsPublished(retain bool) *SubscribeOption {
	o.RetainAsPublished = retain
	return o
}

// SetRetainHandling sets the retain handling option (0, 1, or 2).
func (o *SubscribeOption) SetRetainHandling(handling byte) *SubscribeOption {
	o.RetainHandling = handling
	return o
}

// SetSubscriptionID sets the subscription identifier.
// The ID will be included in PUBLISH packets for messages matching this subscription.
func (o *SubscribeOption) SetSubscriptionID(id uint32) *SubscribeOption {
	o.SubscriptionID = id
	return o
}
