// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"

	"github.com/absmach/mqtt/queue/consumer"
	queueStorage "github.com/absmach/mqtt/queue/storage"
)

// DeliverFn defines the function signature for delivering messages to clients.
type DeliverFn func(ctx context.Context, clientID string, msg any) error

// ConsumerRoutingMode defines how messages are delivered to consumers.
type ConsumerRoutingMode string

const (
	// ProxyMode routes messages through the consumer's proxy node.
	ProxyMode ConsumerRoutingMode = "proxy"
	// DirectMode routes messages directly to the consumer node.
	DirectMode ConsumerRoutingMode = "direct"
)

// QueueSource abstract the queue source to avoid circular dependency.
type QueueSource interface {
	Name() string
	Config() queueStorage.QueueConfig
	OrderingEnforcer() OrderingEnforcer
	ConsumerGroups() *consumer.GroupManager
}

// OrderingEnforcer abstracts the ordering enforcement logic.
// We can define methods if needed, or if it's a specific struct in queue, we might need to extract it or use interface.
// queue/ordering.go defines OrderingEnforcer struct.
// To avoid importing queue, we need an interface.
type OrderingEnforcer interface {
	CanDeliver(msg *queueStorage.Message) (bool, error)
	MarkDelivered(msg *queueStorage.Message)
}

// RaftManager abstracts the Raft consensus manager.
type RaftManager interface {
	IsLeader(partitionID int) bool
}

// RetentionManager abstracts the retention manager.
type RetentionManager interface {
	Start(ctx context.Context, partitionID int)
	Stop()
}
