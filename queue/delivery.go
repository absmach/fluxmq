// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"

	"github.com/absmach/fluxmq/storage"
)

// Deliverer is the output boundary for queue message delivery.
// Protocol brokers implement this interface so the delivery engine
// can push messages to connected clients without knowing protocol details.
type Deliverer interface {
	Deliver(ctx context.Context, clientID string, msg *storage.Message) error
}

// ClientConnectionChecker is optionally implemented by a Deliverer that can
// tell whether a protocol client is still connected before queue cursors move.
type ClientConnectionChecker interface {
	IsClientConnected(clientID string) bool
}

// DeliveryTargetFunc adapts a plain function to the DeliveryTarget interface.
type DeliveryTargetFunc func(ctx context.Context, clientID string, msg *storage.Message) error

func (f DeliveryTargetFunc) Deliver(ctx context.Context, clientID string, msg *storage.Message) error {
	return f(ctx, clientID, msg)
}
