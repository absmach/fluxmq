// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp091

import "errors"

// Client errors.
var (
	ErrNoAddress         = errors.New("no broker address configured")
	ErrNotConnected      = errors.New("client not connected")
	ErrAlreadyConnected  = errors.New("client already connected")
	ErrAlreadySubscribed = errors.New("already subscribed to queue")
	ErrInvalidQueueName  = errors.New("queue name cannot be empty")
	ErrNilHandler        = errors.New("handler cannot be nil")
)
