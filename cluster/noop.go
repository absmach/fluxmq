// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"errors"
	"time"

	clusterv1 "github.com/absmach/fluxmq/pkg/proto/cluster/v1"
	"github.com/absmach/fluxmq/storage"
)

// ErrClusterNotEnabled is returned when cluster operations are called on a non-clustered broker.
var ErrClusterNotEnabled = errors.New("clustering is not enabled")

// NoopCluster is a no-op cluster implementation for single-node mode.
// It satisfies the Cluster interface but returns appropriate errors
// indicating that clustering is not enabled.
type NoopCluster struct {
	nodeID string
}

// NewNoopCluster creates a new no-op cluster for single-node operation.
func NewNoopCluster(nodeID string) *NoopCluster {
	return &NoopCluster{
		nodeID: nodeID,
	}
}

// Session ownership - not applicable in single-node

func (n *NoopCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	// In single-node mode, sessions are always owned locally
	return nil
}

func (n *NoopCluster) ReleaseSession(ctx context.Context, clientID string) error {
	return nil
}

func (n *NoopCluster) GetSessionOwner(ctx context.Context, clientID string) (string, bool, error) {
	// Single-node: this node always owns the session if it exists
	return n.nodeID, true, nil
}

func (n *NoopCluster) WatchSessionOwner(ctx context.Context, clientID string) <-chan OwnershipChange {
	// No ownership changes in single-node mode
	ch := make(chan OwnershipChange)
	close(ch)
	return ch
}

// Subscriptions - not replicated in single-node

func (n *NoopCluster) AddSubscription(ctx context.Context, clientID, filter string, qos byte, opts storage.SubscribeOptions) error {
	// Single-node: subscriptions handled locally by broker
	return nil
}

func (n *NoopCluster) RemoveSubscription(ctx context.Context, clientID, filter string) error {
	return nil
}

func (n *NoopCluster) GetSubscriptionsForClient(ctx context.Context, clientID string) ([]*storage.Subscription, error) {
	// Single-node: use local storage
	return nil, ErrClusterNotEnabled
}

func (n *NoopCluster) GetSubscribersForTopic(ctx context.Context, topic string) ([]*storage.Subscription, error) {
	// Single-node: use local router
	return nil, ErrClusterNotEnabled
}

// Retained returns a noop retained store for single-node mode.
func (n *NoopCluster) Retained() storage.RetainedStore {
	return &noopRetainedStore{}
}

// Wills returns a noop will store for single-node mode.
func (n *NoopCluster) Wills() storage.WillStore {
	return &noopWillStore{}
}

// noopRetainedStore is a no-op implementation of storage.RetainedStore.
type noopRetainedStore struct{}

func (s *noopRetainedStore) Set(ctx context.Context, _ string, _ *storage.Message) error {
	return nil
}

func (s *noopRetainedStore) Get(ctx context.Context, _ string) (*storage.Message, error) {
	return nil, ErrClusterNotEnabled
}

func (s *noopRetainedStore) Delete(ctx context.Context, _ string) error {
	return nil
}

func (s *noopRetainedStore) Match(ctx context.Context, _ string) ([]*storage.Message, error) {
	return nil, ErrClusterNotEnabled
}

// noopWillStore is a no-op implementation of storage.WillStore.
type noopWillStore struct{}

func (s *noopWillStore) Set(ctx context.Context, _ string, _ *storage.WillMessage) error {
	return nil
}

func (s *noopWillStore) Get(ctx context.Context, _ string) (*storage.WillMessage, error) {
	return nil, ErrClusterNotEnabled
}

func (s *noopWillStore) Delete(ctx context.Context, _ string) error {
	return nil
}

func (s *noopWillStore) GetPending(ctx context.Context, _ time.Time) ([]*storage.WillMessage, error) {
	return nil, ErrClusterNotEnabled
}

// Message routing - no routing in single-node

func (n *NoopCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	// Single-node: no remote nodes to route to
	return nil
}

// Session takeover - not applicable in single-node

func (n *NoopCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) (*clusterv1.SessionState, error) {
	// Single-node: no remote nodes to take over from
	return nil, nil
}

// Queue routing - not applicable in single-node

func (n *NoopCluster) EnqueueRemote(ctx context.Context, nodeID, queueName string, payload []byte, properties map[string]string) (string, error) {
	// Single-node: no remote nodes to enqueue to
	return "", ErrClusterNotEnabled
}

func (n *NoopCluster) RouteQueueMessage(ctx context.Context, nodeID, clientID, queueName, messageID string, payload []byte, properties map[string]string, sequence int64) error {
	// Single-node: no remote nodes to route to
	return ErrClusterNotEnabled
}

// Queue consumer registry - local only in single-node

func (n *NoopCluster) RegisterQueueConsumer(ctx context.Context, info *QueueConsumerInfo) error {
	// Single-node: consumer registration is handled locally by queue manager
	return nil
}

func (n *NoopCluster) UnregisterQueueConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	// Single-node: consumer unregistration is handled locally by queue manager
	return nil
}

func (n *NoopCluster) ListQueueConsumers(ctx context.Context, queueName string) ([]*QueueConsumerInfo, error) {
	// Single-node: return empty list, queue manager uses local storage
	return nil, nil
}

func (n *NoopCluster) ListQueueConsumersByGroup(ctx context.Context, queueName, groupID string) ([]*QueueConsumerInfo, error) {
	// Single-node: return empty list, queue manager uses local storage
	return nil, nil
}

func (n *NoopCluster) ListAllQueueConsumers(ctx context.Context) ([]*QueueConsumerInfo, error) {
	// Single-node: return empty list, queue manager uses local storage
	return nil, nil
}

func (n *NoopCluster) ForwardQueuePublish(ctx context.Context, nodeID, topic string, payload []byte, properties map[string]string, forwardToLeader bool) error {
	// Single-node: no remote nodes to forward to
	return ErrClusterNotEnabled
}

// Leadership - always leader in single-node

func (n *NoopCluster) IsLeader() bool {
	// Single-node is always the leader
	return true
}

func (n *NoopCluster) WaitForLeader(ctx context.Context) error {
	// Single-node is immediately the leader
	return nil
}

// Lifecycle

func (n *NoopCluster) Start() error {
	// Nothing to start in single-node mode
	return nil
}

func (n *NoopCluster) Stop() error {
	// Nothing to stop
	return nil
}

func (n *NoopCluster) NodeID() string {
	return n.nodeID
}

func (n *NoopCluster) Nodes() []NodeInfo {
	// Single-node: only this node exists
	return []NodeInfo{
		{
			ID:      n.nodeID,
			Address: "local",
			Healthy: true,
			Leader:  true,
			Uptime:  0,
		},
	}
}
