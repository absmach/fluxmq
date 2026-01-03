// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"time"

	"github.com/absmach/mqtt/cluster/grpc"
	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/storage"
)

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

// PartitionOwnership manages distributed partition ownership across cluster nodes.
// Used for distributing queue partitions across the cluster.
type PartitionOwnership interface {
	// AcquirePartition registers this node as the owner of a queue partition.
	// Uses leased keys that auto-expire if the node dies (30s TTL).
	AcquirePartition(ctx context.Context, queueName string, partitionID int, nodeID string) error

	// ReleasePartition releases ownership of a queue partition.
	ReleasePartition(ctx context.Context, queueName string, partitionID int) error

	// GetPartitionOwner returns the node ID that owns the partition.
	// Returns (nodeID, true, nil) if found, ("", false, nil) if not found.
	GetPartitionOwner(ctx context.Context, queueName string, partitionID int) (nodeID string, exists bool, err error)

	// WatchPartitionOwnership watches for partition ownership changes for a queue.
	// Useful for detecting partition migrations and rebalancing.
	WatchPartitionOwnership(ctx context.Context, queueName string) <-chan PartitionOwnershipChange
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
	PartitionOwnership
	SubscriptionRouter
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
	TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*grpc.SessionState, error)

	// EnqueueRemote sends an enqueue request to a remote partition owner.
	// This is called when a message needs to be enqueued on a partition owned by another node.
	EnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string) (messageID string, err error)

	// RouteQueueMessage sends a queue message to a remote consumer.
	// This is called in proxy mode when the partition worker needs to deliver a message
	// to a consumer connected to a different node.
	RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64, partitionID int) error
}

// OwnershipChange represents a session ownership change event.
type OwnershipChange struct {
	ClientID string
	OldNode  string // Empty if session is new
	NewNode  string // Empty if session was released
	Time     time.Time
}

// PartitionOwnershipChange represents a partition ownership change event.
type PartitionOwnershipChange struct {
	QueueName   string
	PartitionID int
	OldNode     string // Empty if partition is newly created
	NewNode     string // Empty if partition was released
	Time        time.Time
}

// NodeInfo contains information about a cluster node.
type NodeInfo struct {
	ID      string
	Address string // Inter-broker transport address
	Healthy bool
	Leader  bool
	Uptime  time.Duration
}

// MessageHandler handles message delivery and session management for the cluster.
// This interface is implemented by the broker to handle cluster operations:
// - Delivering messages routed from other nodes
// - Providing session state during takeover
// - Fetching retained messages from local storage
// - Fetching will messages from local storage
type MessageHandler interface {
	// DeliverToClient delivers a message to a local MQTT client.
	// This is called when a message is routed from another broker node.
	DeliverToClient(ctx context.Context, clientID string, msg *core.Message) error

	// GetSessionStateAndClose captures the full state of a session and closes it.
	// This is called when another node is taking over the session.
	// Returns nil if the session doesn't exist on this node.
	GetSessionStateAndClose(ctx context.Context, clientID string) (*grpc.SessionState, error)

	// GetRetainedMessage fetches a retained message from the local store.
	// This is called when another node requests a large retained message payload.
	// Returns (nil, nil) if the message doesn't exist.
	GetRetainedMessage(ctx context.Context, topic string) (*storage.Message, error)

	// GetWillMessage fetches a will message from the local store.
	// This is called when another node requests a large will message payload.
	// Returns (nil, nil) if the message doesn't exist.
	GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error)
}
