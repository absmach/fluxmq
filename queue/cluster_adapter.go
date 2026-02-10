// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"log/slog"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/queue/types"
)

// ClusterAdapter abstracts all cluster interactions for the queue Manager.
// Implementations are nil-safe: a noopClusterAdapter is used in single-node mode.
type ClusterAdapter interface {
	RegisterConsumer(ctx context.Context, info *cluster.QueueConsumerInfo) error
	UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error
	ForwardPublish(ctx context.Context, nodeID, topic string, payload []byte, properties map[string]string, forwardToLeader bool) error
	RouteMessage(ctx context.Context, nodeID, clientID, queueName string, msg *cluster.QueueMessage) error
	ListConsumers(ctx context.Context, queueName string) ([]*cluster.QueueConsumerInfo, error)
	ListAllConsumers(ctx context.Context) ([]*cluster.QueueConsumerInfo, error)
	ForwardToRemoteNodes(ctx context.Context, publish types.PublishRequest, queueExists func(string) bool)
	LocalNodeID() string
	IsRemote(nodeID string) bool
	DistributionMode() DistributionMode
}

// clusterAdapter wraps a cluster.Cluster with distribution-mode awareness.
type clusterAdapter struct {
	cluster          cluster.Cluster
	localNodeID      string
	distributionMode DistributionMode
	logger           *slog.Logger
}

func (a *clusterAdapter) RegisterConsumer(ctx context.Context, info *cluster.QueueConsumerInfo) error {
	return a.cluster.RegisterQueueConsumer(ctx, info)
}

func (a *clusterAdapter) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	return a.cluster.UnregisterQueueConsumer(ctx, queueName, groupID, consumerID)
}

func (a *clusterAdapter) ForwardPublish(ctx context.Context, nodeID, topic string, payload []byte, properties map[string]string, forwardToLeader bool) error {
	return a.cluster.ForwardQueuePublish(ctx, nodeID, topic, payload, properties, forwardToLeader)
}

func (a *clusterAdapter) RouteMessage(ctx context.Context, nodeID, clientID, queueName string, msg *cluster.QueueMessage) error {
	return a.cluster.RouteQueueMessage(ctx, nodeID, clientID, queueName, msg)
}

func (a *clusterAdapter) ListConsumers(ctx context.Context, queueName string) ([]*cluster.QueueConsumerInfo, error) {
	return a.cluster.ListQueueConsumers(ctx, queueName)
}

func (a *clusterAdapter) ListAllConsumers(ctx context.Context) ([]*cluster.QueueConsumerInfo, error) {
	return a.cluster.ListAllQueueConsumers(ctx)
}

func (a *clusterAdapter) ForwardToRemoteNodes(ctx context.Context, publish types.PublishRequest, queueExists func(string) bool) {
	switch a.distributionMode {
	case DistributionForward:
		a.forwardToRemoteNodes(ctx, publish, false, queueExists)
	case DistributionReplicate:
		a.forwardToRemoteNodes(ctx, publish, true, queueExists)
	}
}

func (a *clusterAdapter) forwardToRemoteNodes(ctx context.Context, publish types.PublishRequest, unknownOnly bool, queueExists func(string) bool) {
	consumers, err := a.cluster.ListAllQueueConsumers(ctx)
	if err != nil {
		a.logger.Debug("failed to list cluster consumers for forwarding",
			slog.String("error", err.Error()))
		return
	}

	remoteNodes := make(map[string]bool)
	for _, c := range consumers {
		if c.ProxyNodeID == a.localNodeID {
			continue
		}

		if unknownOnly && queueExists(c.QueueName) {
			continue
		}

		queuePattern := "$queue/" + c.QueueName + "/#"
		if matchesTopic(queuePattern, publish.Topic) {
			remoteNodes[c.ProxyNodeID] = true
		}
	}

	for nodeID := range remoteNodes {
		if err := a.cluster.ForwardQueuePublish(ctx, nodeID, publish.Topic, publish.Payload, publish.Properties, false); err != nil {
			a.logger.Warn("failed to forward publish to remote node",
				slog.String("node", nodeID),
				slog.String("topic", publish.Topic),
				slog.String("error", err.Error()))
		} else {
			a.logger.Debug("forwarded publish to remote node",
				slog.String("node", nodeID),
				slog.String("topic", publish.Topic))
		}
	}
}

func (a *clusterAdapter) LocalNodeID() string         { return a.localNodeID }
func (a *clusterAdapter) DistributionMode() DistributionMode { return a.distributionMode }

func (a *clusterAdapter) IsRemote(nodeID string) bool {
	return nodeID != "" && nodeID != a.localNodeID
}

// noopClusterAdapter is used in single-node mode. All methods are no-ops.
type noopClusterAdapter struct{}

func (noopClusterAdapter) RegisterConsumer(context.Context, *cluster.QueueConsumerInfo) error {
	return nil
}
func (noopClusterAdapter) UnregisterConsumer(context.Context, string, string, string) error {
	return nil
}
func (noopClusterAdapter) ForwardPublish(context.Context, string, string, []byte, map[string]string, bool) error {
	return nil
}
func (noopClusterAdapter) RouteMessage(context.Context, string, string, string, *cluster.QueueMessage) error {
	return nil
}
func (noopClusterAdapter) ListConsumers(context.Context, string) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}
func (noopClusterAdapter) ListAllConsumers(context.Context) ([]*cluster.QueueConsumerInfo, error) {
	return nil, nil
}
func (noopClusterAdapter) ForwardToRemoteNodes(context.Context, types.PublishRequest, func(string) bool) {
}
func (noopClusterAdapter) LocalNodeID() string            { return "" }
func (noopClusterAdapter) IsRemote(string) bool           { return false }
func (noopClusterAdapter) DistributionMode() DistributionMode { return "" }
