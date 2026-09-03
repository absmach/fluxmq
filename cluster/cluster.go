// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"time"

	"github.com/absmach/fluxmq/message"
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
	// Version is the peer's build version, as reported by that node itself.
	// It is empty for a node that has not registered its metadata yet, and is
	// meant for operators watching a rolling upgrade.
	//
	// It carries whatever `git describe` produced for that build - "dev", a
	// bare commit for an untagged tree, a tag with a -dirty suffix - so it is
	// a label rather than an ordering, and it says nothing about which
	// features config left enabled on that peer. Code that has to adapt to an
	// older peer needs a comparable signal published next to this one: a
	// capability set, or an explicit wire version the way queue/raft versions
	// its own formats.
	Version string
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

	// RemoveAllSubscriptions removes all subscriptions for a client in a single operation.
	RemoveAllSubscriptions(ctx context.Context, clientID string) error

	// GetSubscriptionsForClient returns all subscriptions for a specific client.
	GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*storage.Subscription, error)

	// GetSubscribersForTopic returns all subscriptions matching a topic.
	// Used for routing publishes to interested nodes.
	GetSubscribersForTopic(ctx context.Context, topic string) ([]*storage.Subscription, error)
}

// QueueConsumerDirectory manages cluster-wide queue consumer registrations.
type QueueConsumerDirectory interface {
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
}

// QueueForwarder forwards queue-related operations to peer nodes.
type QueueForwarder interface {
	// ForwardQueuePublish forwards a queue publish to a remote node. It borrows
	// msg for the duration of the call. targetQueues names the queues the
	// publish must land in, or is empty to route by topic.
	// The remote node will store the message in its local matching queues.
	ForwardQueuePublish(ctx context.Context, nodeID string, msg *message.Envelope, targetQueues []string, forwardToLeader bool) error

	// ForwardGroupOp forwards a consumer group mutation to the Raft leader
	// node for the given queue.
	ForwardGroupOp(ctx context.Context, nodeID, queueName string, op *clusterv1.GroupOperation) error
}

// QueueConsumerRegistry keeps the existing composite for compatibility.
// This enables cross-node queue message routing.
type QueueConsumerRegistry interface {
	QueueConsumerDirectory
	QueueForwarder
}

type Lifecycle interface {
	// Leadership - for coordinating background tasks

	// IsLeader returns true if this node is the cluster leader.
	// Only the leader should execute background tasks like session expiry
	// and will message processing to avoid duplication.
	IsLeader(ctx context.Context) bool

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
	// and forwards the message to them. It borrows msg for the duration of the
	// call.
	RoutePublish(ctx context.Context, msg *message.Envelope) error

	// TakeoverSession initiates session takeover from one node to another.
	// This is called when a client reconnects to a different node.
	// The old node disconnects the client and returns its full state.
	// Returns the session state to be restored, or nil if no state exists.
	TakeoverSession(ctx context.Context, clientID, fromNode, toNode string, identity *SessionIdentityGuard) (*clusterv1.SessionState, error)

	// ShareGroupMembers appends the members of shared subscriptions matching
	// topic that live on other nodes to dst, and returns the extended slice.
	// Only members whose owning node is known are reported: one whose owner
	// cannot be resolved is left out rather than guessed at, because a guess
	// would send the message to a node that cannot deliver it while the group
	// counted it as delivered.
	//
	// Local members are the caller's own business — it holds them directly, and
	// without the round trip this would otherwise take on every publish.
	ShareGroupMembers(ctx context.Context, topic string, dst []ShareMember) ([]ShareMember, error)

	// RoutePublishToClient delivers msg to one named client on a named node,
	// bypassing the receiving node's own subscription matching. It is how a
	// share group whose selected member lives elsewhere reaches that member
	// without every node holding a member delivering its own copy. It borrows
	// msg for the duration of the call.
	RoutePublishToClient(ctx context.Context, nodeID, clientID string, msg *message.Envelope) error

	// RouteQueueMessage sends a queue message to a remote consumer.
	// This is called in proxy mode when the worker needs to deliver a message
	// to a consumer connected to a different node. It borrows msg for the
	// duration of the call.
	RouteQueueMessage(ctx context.Context, nodeID, clientID string, msg *message.Envelope) error
}

// ShareMember is one member of a shared subscription group, together with the
// node its session lives on.
type ShareMember struct {
	ClientID string
	NodeID   string

	// ShareName and Filter together name the group. Two groups sharing a name
	// but bound to different topic filters are different groups, so neither
	// half identifies one on its own.
	ShareName string
	Filter    string

	QoS byte
}

// ForwardPublishHandler handles topic-based message forwarding from remote nodes.
// The receiving node matches its own local subscriptions and delivers to them.
type ForwardPublishHandler interface {
	// ForwardPublish takes ownership of msg on every return path.
	ForwardPublish(ctx context.Context, msg *message.Envelope) error
}

// MessageHandler handles message delivery and session management for the cluster.
// This interface is implemented by the broker to handle cluster operations:
// - Delivering messages routed from other nodes
// - Providing session state during takeover
// - Fetching retained messages from local storage
// - Fetching will messages from local storage.
type MessageHandler interface {
	// DeliverToClient delivers a message to a local MQTT client and takes
	// ownership of msg on every return path.
	// This is called when a message is routed from another broker node.
	DeliverToClient(ctx context.Context, clientID string, msg *message.Envelope) error

	// GetSessionStateAndClose captures the full state of a session and closes it.
	// This is called when another node is taking over the session.
	// Returns nil if the session doesn't exist on this node.
	GetSessionStateAndClose(ctx context.Context, clientID string, identity *SessionIdentityGuard) (*clusterv1.SessionState, error)

	// HandleSessionLeaseLost fences local sessions whose ownership lease was
	// lost. The implementation must stop serving those connections before this
	// node begins acquiring ownership under a replacement lease.
	HandleSessionLeaseLost(ctx context.Context, clientIDs []string)

	// GetRetainedMessage fetches an owned retained message from the local store.
	// The caller must release it.
	// This is called when another node requests a large retained message payload.
	// Returns (nil, nil) if the message doesn't exist.
	GetRetainedMessage(ctx context.Context, topic string) (*message.Envelope, error)

	// GetWillMessage fetches a will message from the local store.
	// This is called when another node requests a large will message payload.
	// Returns (nil, nil) if the message doesn't exist.
	GetWillMessage(ctx context.Context, clientID string) (*storage.WillMessage, error)
}
