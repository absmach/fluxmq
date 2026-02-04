// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"time"

	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
)

// QueueConsumerInfo represents a queue consumer registration visible across the cluster.
type QueueConsumerInfo struct {
	QueueName    string // Queue name (e.g., "test" for $queue/test)
	GroupID      string // Consumer group ID
	ConsumerID   string // Consumer identifier (usually client ID)
	ClientID     string // MQTT client ID
	Pattern      string // Subscription pattern within the queue
	Mode         string // Consumer group mode (queue or stream)
	ProxyNodeID  string // Node where the consumer is connected
	RegisteredAt time.Time
}

// OwnershipChange represents a session ownership change event.
type OwnershipChange struct {
	ClientID string
	OldNode  string // Empty if session is new
	NewNode  string // Empty if session was released
	Time     time.Time
}

// NodeInfo contains information about a cluster node.
type NodeInfo struct {
	ID      string
	Address string // Inter-broker transport address
	Healthy bool
	Leader  bool
	Uptime  time.Duration
}

// SessionOwnership manages distributed session ownership across cluster nodes.
type SessionOwnership interface {
	// AcquireSession registers this node as the owner of a session.
	// Returns error if another node owns the session.
	AcquireSession(ctx context.Context, clientID, nodeID string) error

	// ReleaseSession releases ownership of a session.
	ReleaseSession(ctx context.Context, clientID string) error

	// GetSessionOwner returns the node ID that owns the session.
	// Returns (nodeID, true, nil) if found, ("", false, nil) if not found.
	GetSessionOwner(ctx context.Context, clientID string) (nodeID string, exists bool, err error)

	// WatchSessionOwner watches for ownership changes of a specific session.
	// Useful for detecting when another node takes over a session.
	WatchSessionOwner(ctx context.Context, clientID string) <-chan OwnershipChange
}

// SubscriptionRouter manages cluster-wide subscription routing.
type SubscriptionRouter interface {
	// AddSubscription adds a subscription for a client.
	// This is visible to all nodes in the cluster for routing.
	AddSubscription(ctx context.Context, clientID, filter string, qos byte, opts storage.SubscribeOptions) error

	// RemoveSubscription removes a subscription for a client.
	RemoveSubscription(ctx context.Context, clientID, filter string) error

	// GetSubscriptionsForClient returns all subscriptions for a specific client.
	GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*storage.Subscription, error)

	// GetSubscribersForTopic returns all subscriptions matching a topic.
	// Used for routing publishes to interested nodes.
	GetSubscribersForTopic(ctx context.Context, topic string) ([]*storage.Subscription, error)
}

// QueueConsumerRegistry manages cluster-wide queue consumer registrations.
// This enables cross-node queue message routing.
type QueueConsumerRegistry interface {
	// RegisterQueueConsumer registers a queue consumer visible to all nodes.
	RegisterQueueConsumer(ctx context.Context, info *QueueConsumerInfo) error

	// UnregisterQueueConsumer removes a queue consumer registration.
	UnregisterQueueConsumer(ctx context.Context, queueName, groupID, consumerID string) error

	// ListQueueConsumers returns all consumers for a queue across all nodes.
	ListQueueConsumers(ctx context.Context, queueName string) ([]*QueueConsumerInfo, error)

	// ListQueueConsumersByGroup returns all consumers for a specific group.
	ListQueueConsumersByGroup(ctx context.Context, queueName, groupID string) ([]*QueueConsumerInfo, error)

	// ListAllQueueConsumers returns all queue consumers across all queues.
	// Used to find which nodes have consumers for a topic.
	ListAllQueueConsumers(ctx context.Context) ([]*QueueConsumerInfo, error)

	// ForwardQueuePublish forwards a queue publish to a remote node.
	// The remote node will store the message in its local matching queues.
	ForwardQueuePublish(ctx context.Context, nodeID, topic string, payload []byte, properties map[string]string) error
}

type Lifecycle interface {
	// Leadership - for coordinating background tasks

	// IsLeader returns true if this node is the cluster leader.
	// Only the leader should execute background tasks like session expiry
	// and will message processing to avoid duplication.
	IsLeader() bool

	// WaitForLeader blocks until this node becomes the leader or context is cancelled.
	WaitForLeader(ctx context.Context) error

	// Lifecycle

	// Start initializes the cluster and begins participating.
	Start() error

	// Stop gracefully shuts down the cluster.
	Stop() error

	// NodeID returns this node's unique identifier.
	NodeID() string

	// Nodes returns information about all nodes in the cluster.
	Nodes() []NodeInfo
}

// Cluster provides distributed coordination for the broker.
// This interface abstracts the clustering implementation, allowing
// different backends (etcd, raft, or noop for single-node).
type Cluster interface {
	SessionOwnership
	SubscriptionRouter
	QueueConsumerRegistry
	Lifecycle

	// Retained returns the cluster-wide retained message store.
	Retained() storage.RetainedStore

	// Wills returns the cluster-wide will message store.
	Wills() storage.WillStore

	// RoutePublish routes a publish message to all nodes with interested subscribers.
	// The cluster implementation finds which nodes have matching subscriptions
	// and forwards the message to them.
	RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error

	// TakeoverSession initiates session takeover from one node to another.
	// This is called when a client reconnects to a different node.
	// The old node disconnects the client and returns its full state.
	// Returns the session state to be restored, or nil if no state exists.
	TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*clusterv1.SessionState, error)

	// RouteQueueMessage sends a queue message to a remote consumer.
	// This is called in proxy mode when the worker needs to deliver a message
	// to a consumer connected to a different node.
	RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64) error
}

// MessageHandler handles message delivery and session management for the cluster.
// This interface is implemented by the broker to handle cluster operations:
// - Delivering messages routed from other nodes
// - Providing session state during takeover
// - Fetching retained messages from local storage
// - Fetching will messages from local storage.
type MessageHandler interface {
	// DeliverToClient delivers a message to a local MQTT client.
	// This is called when a message is routed from another broker node.
	DeliverToClient(ctx context.Context, clientID string, msg *Message) error

	// GetSessionStateAndClose captures the full state of a session and closes it.
	// This is called when another node is taking over the session.
	// Returns nil if the session doesn't exist on this node.
	GetSessionStateAndClose(ctx context.Context, clientID string) (*clusterv1.SessionState, error)

	// GetRetainedMessage fetches a retained message from the local store.
	// This is called when another node requests a large retained message payload.
	// Returns (nil, nil) if the message doesn't exist.
	GetRetainedMessage(ctx context.Context, topic string) (*storage.Message, error)

	// GetWillMessage fetches a will message from the local store.
	// This is called when another node requests a large will message payload.
	// Returns (nil, nil) if the message doesn't exist.
	GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error)
}
