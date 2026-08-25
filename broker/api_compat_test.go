// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"

	"github.com/absmach/fluxmq/queue/types"
)

// These interfaces are the Go v1 compatibility baseline. Reciprocal
// assignments deliberately make the check exact: removing, changing, or adding
// a method to a frozen interface fails this package at compile time.
type v1Authenticator interface {
	Authenticate(ctx context.Context, clientID, username, secret string) (*AuthnResult, error)
}

type v1Authorizer interface {
	CanPublish(ctx context.Context, clientID string, topic string) bool
	CanSubscribe(ctx context.Context, clientID string, filter string) bool
}

type v1QueueManager interface {
	Publish(ctx context.Context, publish types.PublishRequest) error
	Subscribe(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string) error
	SubscribeWithCursor(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string, cursor *types.CursorOption) error
	Unsubscribe(ctx context.Context, queueName, pattern, clientID, groupID string) error
	Ack(ctx context.Context, queueName, groupID string, offset uint64) error
	Nack(ctx context.Context, queueName, groupID string, offset uint64) error
	Reject(ctx context.Context, queueName, groupID string, offset uint64, reason string) error
	Start(ctx context.Context) error
	Stop() error
	UpdateHeartbeat(ctx context.Context, clientID string) error
	GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error)
	ListQueues(ctx context.Context) ([]types.QueueConfig, error)
	CreateQueue(ctx context.Context, config types.QueueConfig) error
	DeleteQueue(ctx context.Context, queueName string) error
}

// StreamQueueManager embeds QueueManager, so the baseline embeds its own
// QueueManager rather than restating those methods. Duplicating them would let
// the two copies drift apart without failing anything.
type v1StreamQueueManager interface {
	v1QueueManager
	UpdateQueue(ctx context.Context, config types.QueueConfig) error
	CommitOffset(ctx context.Context, queueName, groupID string, offset uint64) error
}

var (
	_ v1Authenticator = Authenticator(nil)
	_ Authenticator   = v1Authenticator(nil)

	_ v1Authorizer = Authorizer(nil)
	_ Authorizer   = v1Authorizer(nil)

	_ v1QueueManager = QueueManager(nil)
	_ QueueManager   = v1QueueManager(nil)

	_ v1StreamQueueManager = StreamQueueManager(nil)
	_ StreamQueueManager   = v1StreamQueueManager(nil)
)
