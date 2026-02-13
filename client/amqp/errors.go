// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package amqp

import "errors"

// Client errors.
var (
	ErrNoAddress         = errors.New("no broker address configured")
	ErrNotConnected      = errors.New("client not connected")
	ErrAlreadyConnected  = errors.New("client already connected")
	ErrAlreadySubscribed = errors.New("already subscribed")
	ErrPublisherConfirm  = errors.New("publisher confirm not acknowledged")
	ErrTimeout           = errors.New("operation timed out")
	ErrNoReturnHandler   = errors.New("mandatory publish requires a return handler")
	ErrInvalidQueueName  = errors.New("queue name cannot be empty")
	ErrInvalidTopic      = errors.New("topic cannot be empty")
	ErrNilHandler        = errors.New("handler cannot be nil")
	ErrNilOptions        = errors.New("options cannot be nil")
)
