package cluster

import (
	"context"
	"errors"

	"github.com/absmach/mqtt/storage"
)

var (
	// ErrClusterNotEnabled is returned when cluster operations are called on a non-clustered broker.
	ErrClusterNotEnabled = errors.New("clustering is not enabled")
)

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

// Retained messages - not replicated in single-node

func (n *NoopCluster) SetRetained(ctx context.Context, topic string, msg *storage.Message) error {
	// Single-node: retained messages handled by local storage
	return nil
}

func (n *NoopCluster) GetRetained(ctx context.Context, topic string) (*storage.Message, error) {
	return nil, ErrClusterNotEnabled
}

func (n *NoopCluster) DeleteRetained(ctx context.Context, topic string) error {
	return nil
}

func (n *NoopCluster) GetRetainedMatching(ctx context.Context, filter string) ([]*storage.Message, error) {
	return nil, ErrClusterNotEnabled
}

// Will messages - not replicated in single-node

func (n *NoopCluster) SetWill(ctx context.Context, clientID string, will *storage.WillMessage) error {
	// Single-node: wills handled by local storage
	return nil
}

func (n *NoopCluster) GetWill(ctx context.Context, clientID string) (*storage.WillMessage, error) {
	return nil, ErrClusterNotEnabled
}

func (n *NoopCluster) DeleteWill(ctx context.Context, clientID string) error {
	return nil
}

func (n *NoopCluster) GetPendingWills(ctx context.Context) ([]*storage.WillMessage, error) {
	return nil, ErrClusterNotEnabled
}

// Message routing - no routing in single-node

func (n *NoopCluster) RoutePublish(ctx context.Context, topic string, payload []byte, qos byte, retain bool, properties map[string]string) error {
	// Single-node: no remote nodes to route to
	return nil
}

// Session takeover - not applicable in single-node

func (n *NoopCluster) TakeoverSession(ctx context.Context, clientID, fromNode, toNode string) error {
	// Single-node: no remote nodes to take over from
	return nil
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
