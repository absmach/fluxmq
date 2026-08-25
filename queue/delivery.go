// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"

	"github.com/absmach/fluxmq/message"
)

// Deliverer is the output boundary for queue message delivery.
// Protocol brokers implement this interface so the delivery engine
// can push messages to connected clients without knowing protocol details.
// Deliver takes ownership of msg on every return path.
type Deliverer interface {
	Deliver(ctx context.Context, clientID string, msg *message.Envelope) error
}

// ClientDeliveryTargetChecker is optionally implemented by a Deliverer that can
// tell whether queue delivery has a valid target before queue cursors move. For
// MQTT, a disconnected persistent session is still a valid delivery target
// because the broker can enqueue QoS>0 messages for later delivery.
type ClientDeliveryTargetChecker interface {
	HasDeliveryTarget(clientID string) bool
}

// ClientConnectionChecker is kept for deliverers where only live connections
// are valid queue delivery targets.
type ClientConnectionChecker interface {
	IsClientConnected(clientID string) bool
}

// DeliveryTargetFunc adapts a plain function to the Deliverer interface.
type DeliveryTargetFunc func(ctx context.Context, clientID string, msg *message.Envelope) error

func (f DeliveryTargetFunc) Deliver(ctx context.Context, clientID string, msg *message.Envelope) error {
	return f(ctx, clientID, msg)
}
