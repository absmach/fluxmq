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

	"github.com/absmach/fluxmq/amqp1/message"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/cluster"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/storage"
)

// Broker manages AMQP 1.0 connections and message routing.
type Broker struct {
	connections         sync.Map // containerID -> *Connection
	router              *router.TrieRouter
	routeResolver       *corebroker.RoutingResolver
	queueManager        corebroker.QueueManager
	cluster             cluster.Cluster
	auth                *corebroker.AuthEngine
	crossDeliver        corebroker.CrossDeliverFunc
	stats               *Stats
	metrics             *Metrics // nil if OTel disabled
	logger              *slog.Logger
	routePublishTimeout time.Duration
}

// New creates a new AMQP broker.
func New(qm corebroker.QueueManager, stats *Stats, logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}
	if stats == nil {
		stats = NewStats()
	}
	return &Broker{
		router:        router.NewRouter(),
		routeResolver: corebroker.NewRoutingResolver(),
		queueManager:  qm,
		stats:         stats,
		logger:        logger,
	}
}

// SetMetrics sets the OTel metrics instance.
func (b *Broker) SetMetrics(m *Metrics) {
	b.metrics = m
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

// Publish routes a message to all subscribers via the shared router.
// AMQP 1.0 subscribers are delivered locally; others via the cross-deliver callback.
func (b *Broker) Publish(topic string, payload []byte, props map[string]string) {
	subs, err := b.router.Match(topic)
	if err != nil {
		b.logger.Error("AMQP router match failed", "topic", topic, "error", err)
		return
	}

	for _, sub := range subs {
		if corebroker.IsAMQP1Client(sub.ClientID) {
			containerID := strings.TrimPrefix(sub.ClientID, corebroker.AMQP1ClientPrefix)
			val, ok := b.connections.Load(containerID)
			if !ok {
				continue
			}
			c := val.(*Connection)
			c.deliverMessage(topic, payload, props, sub.QoS)
		} else {
			if b.crossDeliver != nil {
				b.crossDeliver(sub.ClientID, topic, payload, sub.QoS, props)
			}
		}
	}

	if cl := b.cluster; cl != nil {
		timeout := b.routePublishTimeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := cl.RoutePublish(ctx, topic, payload, 1, false, props); err != nil {
			b.logger.Error("AMQP cluster route publish failed", "topic", topic, "error", err)
		}
	}
}

// ForwardPublish handles a forwarded publish from a remote cluster node.
// It delivers only to local AMQP 1.0 subscribers without re-routing to the cluster.
func (b *Broker) ForwardPublish(ctx context.Context, msg *cluster.Message) error {
	subs, err := b.router.Match(msg.Topic)
	if err != nil {
		return err
	}

	for _, sub := range subs {
		if !corebroker.IsAMQP1Client(sub.ClientID) {
			continue
		}
		containerID := strings.TrimPrefix(sub.ClientID, corebroker.AMQP1ClientPrefix)
		val, ok := b.connections.Load(containerID)
		if !ok {
			continue
		}
		c := val.(*Connection)
		c.deliverMessage(msg.Topic, msg.Payload, msg.Properties, sub.QoS)
	}

	return nil
}

// LocalDeliverPubSub delivers a pub/sub message to a specific local AMQP 1.0 connection.
// Called by the cross-deliver callback from other protocol brokers.
func (b *Broker) LocalDeliverPubSub(clientID string, topic string, payload []byte, qos byte, props map[string]string) {
	containerID := strings.TrimPrefix(clientID, corebroker.AMQP1ClientPrefix)
	val, ok := b.connections.Load(containerID)
	if !ok {
		return
	}
	c := val.(*Connection)
	c.deliverMessage(topic, payload, props, qos)
}

// DeliverToClient delivers a queue message to a specific AMQP client.
// clientID must have the "amqp:" prefix already stripped.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg any) error {
	// Strip the amqp: prefix to get the container ID
	containerID := strings.TrimPrefix(clientID, corebroker.AMQP1ClientPrefix)

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
		if msgID, ok := props[qtypes.PropMessageID]; ok {
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
	b.cluster = cl
}

// SetRoutePublishTimeout sets the timeout for cross-cluster publish routing.
func (b *Broker) SetRoutePublishTimeout(d time.Duration) {
	b.routePublishTimeout = d
}

// DeliverToClusterMessage delivers a message routed from another cluster node to a local AMQP client.
func (b *Broker) DeliverToClusterMessage(ctx context.Context, clientID string, msg *cluster.Message) error {
	containerID := strings.TrimPrefix(clientID, corebroker.AMQP1ClientPrefix)
	val, ok := b.connections.Load(containerID)
	if !ok {
		return fmt.Errorf("AMQP client not found: %s", containerID)
	}
	c := val.(*Connection)
	c.deliverMessage(msg.Topic, msg.Payload, msg.Properties, msg.QoS)
	return nil
}

// SetQueueManager sets the queue manager for the AMQP broker.
func (b *Broker) SetQueueManager(qm corebroker.QueueManager) {
	b.queueManager = qm
}

// SetAuthEngine sets the authentication and authorization engine.
func (b *Broker) SetAuthEngine(auth *corebroker.AuthEngine) {
	b.auth = auth
}

// SetRouter replaces the router used for local pub/sub matching.
// Must be called before the broker starts accepting connections.
func (b *Broker) SetRouter(r *router.TrieRouter) {
	b.router = r
}

// SetCrossDeliver sets the callback invoked to deliver pub/sub messages to
// other-protocol clients that share the same router.
// Must be called before the broker starts accepting connections.
func (b *Broker) SetCrossDeliver(fn corebroker.CrossDeliverFunc) {
	b.crossDeliver = fn
}

// Close shuts down the broker and all connections.
func (b *Broker) Close() {
	b.connections.Range(func(key, val any) bool {
		c := val.(*Connection)
		c.close(nil)
		return true
	})
}

// PrefixedClientID returns a client ID with the AMQP 1.0 prefix.
func PrefixedClientID(containerID string) string {
	return corebroker.PrefixedAMQP1ClientID(containerID)
}

// IsAMQPClient checks if a client ID belongs to an AMQP 1.0 client.
func IsAMQPClient(clientID string) bool {
	return corebroker.IsAMQP1Client(clientID)
}
