// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"

	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/storage"

	qtypes "github.com/absmach/fluxmq/queue/types"
)

const amqp091Prefix = "amqp091-"

// IsAMQP091Client checks if a client ID belongs to an AMQP 0.9.1 client.
func IsAMQP091Client(clientID string) bool {
	return strings.HasPrefix(clientID, amqp091Prefix)
}

// PrefixedClientID returns a client ID with the AMQP 0.9.1 prefix.
func PrefixedClientID(connID string) string {
	return amqp091Prefix + connID
}

// QueueManager defines the interface for durable queue-based queue management.
type QueueManager interface {
	Start(ctx context.Context) error
	Stop() error
	CreateQueue(ctx context.Context, config qtypes.QueueConfig) error
	UpdateQueue(ctx context.Context, config qtypes.QueueConfig) error
	GetQueue(ctx context.Context, queueName string) (*qtypes.QueueConfig, error)
	Publish(ctx context.Context, publish qtypes.PublishRequest) error
	Subscribe(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string) error
	SubscribeWithCursor(ctx context.Context, queueName, pattern, clientID, groupID, proxyNodeID string, cursor *qtypes.CursorOption) error
	Unsubscribe(ctx context.Context, queueName, pattern, clientID, groupID string) error
	Ack(ctx context.Context, queueName, messageID, groupID string) error
	Nack(ctx context.Context, queueName, messageID, groupID string) error
	Reject(ctx context.Context, queueName, messageID, groupID, reason string) error
	CommitOffset(ctx context.Context, queueName, groupID string, offset uint64) error
}

// Broker is the core AMQP 0.9.1 broker.
type Broker struct {
	connections  sync.Map // connID -> *Connection
	router       *router.TrieRouter
	queueManager QueueManager
	stats        *Stats
	logger       *slog.Logger
	mu           sync.RWMutex
}

// New creates a new AMQP 0.9.1 broker.
func New(qm QueueManager, logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Broker{
		router:       router.NewRouter(),
		queueManager: qm,
		stats:        NewStats(),
		logger:       logger,
	}
}

// GetStats returns the broker's stats.
func (b *Broker) GetStats() *Stats { return b.stats }

// SetQueueManager sets the queue manager for the broker.
func (b *Broker) SetQueueManager(qm QueueManager) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queueManager = qm
}

func (b *Broker) getQueueManager() QueueManager {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.queueManager
}

// HandleConnection handles a new raw TCP connection through the full AMQP 0.9.1 lifecycle.
func (b *Broker) HandleConnection(netConn net.Conn) {
	c := newConnection(b, netConn)
	if err := c.run(); err != nil {
		b.logger.Debug("AMQP 0.9.1 connection ended", "remote", netConn.RemoteAddr(), "error", err)
	}
}

func (b *Broker) registerConnection(connID string, c *Connection) {
	b.connections.Store(connID, c)
}

func (b *Broker) unregisterConnection(connID string) {
	b.connections.Delete(connID)
}

// Publish routes a message to local AMQP 0.9.1 subscribers via the router.
func (b *Broker) Publish(topic string, payload []byte, props map[string]string) {
	subs, err := b.router.Match(topic)
	if err != nil {
		b.logger.Error("router match failed", "topic", topic, "error", err)
		return
	}

	for _, sub := range subs {
		val, ok := b.connections.Load(sub.ClientID)
		if !ok {
			continue
		}
		c := val.(*Connection)
		c.deliverMessage(topic, payload, props)
	}
}

// DeliverToClient delivers a queue message to a specific AMQP 0.9.1 client.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg any) error {
	connID := strings.TrimPrefix(clientID, amqp091Prefix)

	val, ok := b.connections.Load(connID)
	if !ok {
		return fmt.Errorf("AMQP 0.9.1 client not found: %s", connID)
	}

	c := val.(*Connection)

	switch m := msg.(type) {
	case *storage.Message:
		payload := m.GetPayload()
		c.deliverMessage(m.Topic, payload, m.Properties)
		return nil
	default:
		return fmt.Errorf("unsupported message type: %T", msg)
	}
}

// DeliverToClusterMessage delivers a message routed from another cluster node to a local AMQP 0.9.1 client.
func (b *Broker) DeliverToClusterMessage(ctx context.Context, clientID string, msg *cluster.Message) error {
	connID := strings.TrimPrefix(clientID, amqp091Prefix)

	val, ok := b.connections.Load(connID)
	if !ok {
		return fmt.Errorf("AMQP 0.9.1 client not found: %s", connID)
	}

	c := val.(*Connection)
	c.deliverMessage(msg.Topic, msg.Payload, msg.Properties)
	return nil
}

// Close gracefully shuts down the broker.
func (b *Broker) Close() error {
	b.connections.Range(func(key, val any) bool {
		c := val.(*Connection)
		c.close()
		return true
	})
	b.logger.Info("AMQP 0.9.1 broker shut down")
	return nil
}
