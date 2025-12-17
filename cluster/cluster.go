// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"time"

	"github.com/absmach/mqtt/storage"
)

// Cluster provides distributed coordination for the broker.
// This interface abstracts the clustering implementation, allowing
// different backends (etcd, raft, or noop for single-node).
type Cluster interface {
	// Session ownership - strongly consistent

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

	// Subscriptions - cluster-wide visibility

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

	// Retained messages - cluster-wide replication

	// SetRetained stores a retained message visible to all nodes.
	// Empty payload deletes the retained message.
	SetRetained(ctx context.Context, topic string, msg *storage.Message) error

	// GetRetained retrieves a retained message by exact topic.
	GetRetained(ctx context.Context, topic string) (*storage.Message, error)

	// DeleteRetained removes a retained message.
	DeleteRetained(ctx context.Context, topic string) error

	// GetRetainedMatching returns all retained messages matching a filter.
	// Used when a client subscribes to deliver matching retained messages.
	GetRetainedMatching(ctx context.Context, filter string) ([]*storage.Message, error)

	// Will messages - cluster-wide, strongly consistent

	// SetWill stores a will message for a client.
	SetWill(ctx context.Context, clientID string, will *storage.WillMessage) error

	// GetWill retrieves the will message for a client.
	GetWill(ctx context.Context, clientID string) (*storage.WillMessage, error)

	// DeleteWill removes the will message for a client.
	DeleteWill(ctx context.Context, clientID string) error

	// GetPendingWills returns will messages that should be triggered.
	// Used by leader node to process pending wills.
	GetPendingWills(ctx context.Context) ([]*storage.WillMessage, error)

	// Message routing - inter-broker communication

	// RoutePublish routes a publish message to all nodes with interested subscribers.
	// The cluster implementation finds which nodes have matching subscriptions
	// and forwards the message to them.
	RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error

	// Session takeover - coordinated session migration

	// TakeoverSession initiates session takeover from one node to another.
	// This is called when a client reconnects to a different node.
	// The old node should save state and disconnect the client.
	TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) error

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

// Message represents a routed message between brokers.
type Message struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	Properties map[string]string
}
