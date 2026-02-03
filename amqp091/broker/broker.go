// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"strings"

	qtypes "github.com/absmach/fluxmq/queue/types"
)

const amqp091Prefix = "amqp091-"

// IsAMQP091Client checks if a client ID belongs to an AMQP 0.9.1 client.
func IsAMQP091Client(clientID string) bool {
	return strings.HasPrefix(clientID, amqp091Prefix)
}

// QueueManager defines the interface for durable queue-based queue management.
type QueueManager interface {
	Start(ctx context.Context) error
	Stop() error
	Publish(ctx context.Context, topic string, payload []byte, properties map[string]string) error
	Subscribe(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string) error
	SubscribeWithCursor(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string, cursor *qtypes.CursorOption) error
	Unsubscribe(ctx context.Context, queueName, pattern, clientID, groupID string) error
	Ack(ctx context.Context, queueName, messageID, groupID string) error
	Nack(ctx context.Context, queueName, messageID, groupID string) error
	Reject(ctx context.Context, queueName, messageID, groupID, reason string) error
}

// Broker is the core AMQP 0.9.1 broker.
type Broker struct {
	queueManager QueueManager
	logger       *slog.Logger
}

// New creates a new AMQP 0.9.1 broker.
func New(qm QueueManager, logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Broker{
		queueManager: qm,
		logger:       logger,
	}
}

// SetQueueManager sets the queue manager for the broker.
func (b *Broker) SetQueueManager(qm QueueManager) {
	b.queueManager = qm
}

// DeliverToClient delivers a message from the queue to an AMQP 0.9.1 client.
// This will be called by the central dispatcher.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg any) error {
	// TODO: Implement the logic to find the client's session/channel
	// and send the message using AMQP 0.9.1's basic.deliver.
	b.logger.Info("Delivering message to AMQP 0.9.1 client", "client_id", clientID)
	return nil
}

// Close gracefully shuts down the broker.
func (b *Broker) Close() error {
	b.logger.Info("AMQP 0.9.1 broker is shutting down")
	return nil
}
