// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import "errors"

var (
	ErrNoTransport     = errors.New("client has no transport configured")
	ErrInvalidProtocol = errors.New("invalid protocol")
	ErrNoRouteProtocol = errors.New("requested protocol transport not configured")
	ErrConnectFailed   = errors.New("connect failed")
	ErrCloseFailed     = errors.New("close failed")
	ErrPublishFailed   = errors.New("publish failed")
	ErrSubscribeFailed = errors.New("subscribe failed")
	ErrUnsubFailed     = errors.New("unsubscribe failed")

	ErrQueuePublishFailed   = errors.New("queue publish failed")
	ErrQueueSubscribeFailed = errors.New("queue subscribe failed")
	ErrQueueUnsubFailed     = errors.New("queue unsubscribe failed")
)
