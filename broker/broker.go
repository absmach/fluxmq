// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"

	"github.com/absmach/fluxmq/queue/types"
)

// Notifier defines the interface for webhook notifications.
type Notifier interface {
	Notify(ctx context.Context, event interface{}) error
	Close() error
}

// QueueManager defines the interface for durable queue-based queue management.
type QueueManager interface {
	Start(ctx context.Context) error
	Stop() error
	// Publish adds a message to all queues whose topic patterns match the topic.
	Publish(ctx context.Context, publish types.PublishRequest) error
	// Subscribe adds a consumer to a queue with optional pattern matching.
	Subscribe(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string) error
	// SubscribeWithCursor adds a consumer with explicit cursor positioning.
	SubscribeWithCursor(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string, cursor *types.CursorOption) error
	// Unsubscribe removes a consumer from a queue.
	Unsubscribe(ctx context.Context, queueName, pattern, clientID, groupID string) error
	// Ack acknowledges successful processing of a message by a consumer group.
	// groupID is required for fan-out support - each group acknowledges independently.
	Ack(ctx context.Context, queueName, messageID, groupID string) error
	// Nack negatively acknowledges a message for a consumer group (triggers retry).
	Nack(ctx context.Context, queueName, messageID, groupID string) error
	// Reject permanently rejects a message by a consumer group (move to DLQ).
	Reject(ctx context.Context, queueName, messageID, groupID, reason string) error
	// UpdateHeartbeat updates the heartbeat timestamp for a consumer across all queues/groups.
	// This should be called when a PINGREQ is received from a client.
	UpdateHeartbeat(ctx context.Context, clientID string) error
	// CreateQueue creates a new queue with the given configuration.
	CreateQueue(ctx context.Context, config types.QueueConfig) error
	// DeleteQueue deletes a queue by name.
	DeleteQueue(ctx context.Context, queueName string) error
	// GetQueue returns the configuration for a queue.
	GetQueue(ctx context.Context, queueName string) (*types.QueueConfig, error)
	// ListQueues returns all queue configurations.
	ListQueues(ctx context.Context) ([]types.QueueConfig, error)
}

// StreamQueueManager extends QueueManager with stream-specific controls.
// Used by protocol implementations that support stream retention updates and manual commits.
type StreamQueueManager interface {
	QueueManager
	// UpdateQueue updates queue settings such as retention policy and queue type.
	UpdateQueue(ctx context.Context, config types.QueueConfig) error
	// CommitOffset commits a stream group offset when auto-commit is disabled.
	CommitOffset(ctx context.Context, queueName, groupID string, offset uint64) error
}

// ClientRateLimiter defines the interface for per-client rate limiting.
type ClientRateLimiter interface {
	// AllowPublish checks if a publish from the given client is allowed.
	AllowPublish(clientID string) bool
	// AllowSubscribe checks if a subscription from the given client is allowed.
	AllowSubscribe(clientID string) bool
	// OnClientDisconnect cleans up rate limiters for a disconnected client.
	OnClientDisconnect(clientID string)
}
