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
	"time"

	"github.com/absmach/fluxmq/amqp/message"
	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/storage"
)

const clientIDPrefix = "amqp:"

// Broker manages AMQP 1.0 connections and message routing.
type Broker struct {
	connections  sync.Map // containerID -> *Connection
	router       *router.TrieRouter
	queueManager broker.QueueManager
	cluster      cluster.Cluster
	auth         *broker.AuthEngine
	stats        *Stats
	metrics      *Metrics // nil if OTel disabled
	logger       *slog.Logger
	mu           sync.RWMutex
}

// New creates a new AMQP broker.
func New(qm broker.QueueManager, stats *Stats, logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}
	if stats == nil {
		stats = NewStats()
	}
	return &Broker{
		router:       router.NewRouter(),
		queueManager: qm,
		stats:        stats,
		logger:       logger,
	}
}

// SetMetrics sets the OTel metrics instance.
func (b *Broker) SetMetrics(m *Metrics) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.metrics = m
}

func (b *Broker) getMetrics() *Metrics {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.metrics
}

// GetStats returns the broker's stats.
func (b *Broker) GetStats() *Stats {
	return b.stats
}

// HandleConnection handles a new raw TCP connection through the full AMQP lifecycle.
func (b *Broker) HandleConnection(conn net.Conn) {
	c := newConnection(b, conn)
	if err := c.run(); err != nil {
		b.logger.Debug("AMQP connection ended", "remote", conn.RemoteAddr(), "error", err)
	}
}

// registerConnection stores a connection by container ID.
func (b *Broker) registerConnection(containerID string, c *Connection) {
	b.connections.Store(containerID, c)
}

// unregisterConnection removes a connection by container ID.
func (b *Broker) unregisterConnection(containerID string) {
	b.connections.Delete(containerID)
}

// Publish routes a message to AMQP subscribers via the router and cluster.
func (b *Broker) Publish(topic string, payload []byte, props map[string]string) {
	subs, err := b.router.Match(topic)
	if err != nil {
		b.logger.Error("AMQP router match failed", "topic", topic, "error", err)
		return
	}

	for _, sub := range subs {
		containerID := sub.ClientID
		val, ok := b.connections.Load(containerID)
		if !ok {
			continue
		}
		c := val.(*Connection)
		c.deliverMessage(topic, payload, props, sub.QoS)
	}

	if cl := b.getCluster(); cl != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := cl.RoutePublish(ctx, topic, payload, 1, false, props); err != nil {
			b.logger.Error("AMQP cluster route publish failed", "topic", topic, "error", err)
		}
	}
}

// DeliverToClient delivers a queue message to a specific AMQP client.
// clientID must have the "amqp:" prefix already stripped.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg any) error {
	// Strip the amqp: prefix to get the container ID
	containerID := strings.TrimPrefix(clientID, clientIDPrefix)

	val, ok := b.connections.Load(containerID)
	if !ok {
		return fmt.Errorf("AMQP client not found: %s", containerID)
	}

	c := val.(*Connection)

	// Convert storage.Message to delivery
	switch m := msg.(type) {
	case *storage.Message:
		payload := m.GetPayload()
		topic := m.Topic
		props := m.Properties

		// Build AMQP message
		amqpMsg := &message.Message{
			Properties: &message.Properties{
				To: topic,
			},
			Data: [][]byte{payload},
		}
		if props != nil {
			amqpMsg.ApplicationProperties = make(map[string]any, len(props))
			for k, v := range props {
				amqpMsg.ApplicationProperties[k] = v
			}
		}
		if msgID, ok := props["message-id"]; ok {
			amqpMsg.Properties.MessageID = msgID
		}

		c.deliverAMQPMessage(topic, amqpMsg, m.QoS)
		return nil
	default:
		return fmt.Errorf("unsupported message type: %T", msg)
	}
}

// SetCluster sets the cluster reference for cross-node pub/sub routing.
func (b *Broker) SetCluster(cl cluster.Cluster) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cluster = cl
}

// DeliverToClusterMessage delivers a message routed from another cluster node to a local AMQP client.
func (b *Broker) DeliverToClusterMessage(ctx context.Context, clientID string, msg *cluster.Message) error {
	containerID := strings.TrimPrefix(clientID, clientIDPrefix)
	val, ok := b.connections.Load(containerID)
	if !ok {
		return fmt.Errorf("AMQP client not found: %s", containerID)
	}
	c := val.(*Connection)
	c.deliverMessage(msg.Topic, msg.Payload, msg.Properties, msg.QoS)
	return nil
}

// SetQueueManager sets the queue manager for the AMQP broker.
func (b *Broker) SetQueueManager(qm broker.QueueManager) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queueManager = qm
}

// SetAuthEngine sets the authentication and authorization engine.
func (b *Broker) SetAuthEngine(auth *broker.AuthEngine) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.auth = auth
}

func (b *Broker) getCluster() cluster.Cluster {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.cluster
}

func (b *Broker) getAuth() *broker.AuthEngine {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.auth
}

func (b *Broker) getQueueManager() broker.QueueManager {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.queueManager
}

// Close shuts down the broker and all connections.
func (b *Broker) Close() {
	b.connections.Range(func(key, val any) bool {
		c := val.(*Connection)
		c.close(nil)
		return true
	})
}

// PrefixedClientID returns a client ID with the AMQP prefix for use in the QueueManager.
func PrefixedClientID(containerID string) string {
	return clientIDPrefix + containerID
}

// IsAMQPClient checks if a client ID belongs to an AMQP client.
func IsAMQPClient(clientID string) bool {
	return strings.HasPrefix(clientID, clientIDPrefix)
}
