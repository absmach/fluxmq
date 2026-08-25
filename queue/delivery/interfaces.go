// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package delivery

import (
	"context"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/types"
)

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
	Config() types.QueueConfig
	OrderingEnforcer() OrderingEnforcer
	ConsumerGroups() *consumer.GroupManager
}

// OrderingEnforcer abstracts the ordering enforcement logic.
// Group-aware: each consumer group tracks its own ordering independently.
type OrderingEnforcer interface {
	CanDeliver(msg *message.Envelope, groupID string) (bool, error)
	MarkDelivered(msg *message.Envelope, groupID string)
}

// RaftManager abstracts the Raft consensus manager.
type RaftManager interface {
	IsLeader(ctx context.Context) bool
}

// RetentionManager abstracts the retention manager.
type RetentionManager interface {
	Start(ctx context.Context)
	Stop()
}
