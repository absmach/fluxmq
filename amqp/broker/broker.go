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

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/storage"
)

// IsAMQP091Client checks if a client ID belongs to an AMQP 0.9.1 client.
func IsAMQP091Client(clientID string) bool {
	return corebroker.IsAMQP091Client(clientID)
}

// PrefixedClientID returns a client ID with the AMQP 0.9.1 prefix.
func PrefixedClientID(connID string) string {
	return corebroker.PrefixedAMQP091ClientID(connID)
}

// Broker is the core AMQP 0.9.1 broker.
type Broker struct {
	connections         sync.Map // connID -> *Connection
	router              *router.TrieRouter
	routeResolver       *corebroker.RoutingResolver
	queueManager        corebroker.StreamQueueManager
	cluster             cluster.Cluster
	crossDeliver        corebroker.CrossDeliverFunc
	routePublishTimeout time.Duration
	stats               *Stats
	logger              *slog.Logger
}

// New creates a new AMQP 0.9.1 broker.
func New(qm corebroker.StreamQueueManager, logger *slog.Logger) *Broker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Broker{
		router:        router.NewRouter(),
		routeResolver: corebroker.NewRoutingResolver(),
		queueManager:  qm,
		stats:         NewStats(),
		logger:        logger,
	}
}

// GetStats returns the broker's stats.
func (b *Broker) GetStats() *Stats { return b.stats }

// SetQueueManager sets the queue manager for the broker.
func (b *Broker) SetQueueManager(qm corebroker.StreamQueueManager) {
	b.queueManager = qm
}

// SetCluster sets the cluster reference for cross-node pub/sub routing.
func (b *Broker) SetCluster(cl cluster.Cluster) {
	b.cluster = cl
}

// SetCrossDeliver sets the local cross-protocol pub/sub delivery callback.
func (b *Broker) SetCrossDeliver(fn corebroker.CrossDeliverFunc) {
	b.crossDeliver = fn
}

// SetRouter swaps the broker router. Must be called before accepting connections.
func (b *Broker) SetRouter(r *router.TrieRouter) {
	if r == nil {
		return
	}
	b.router = r
}

// SetRoutePublishTimeout sets the timeout for cross-cluster publish routing.
func (b *Broker) SetRoutePublishTimeout(d time.Duration) {
	b.routePublishTimeout = d
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

// Publish routes a message to local AMQP 0.9.1 subscribers and remote cluster nodes.
// It returns an error if cluster routing fails, so callers in confirm mode can NACK.
func (b *Broker) Publish(topic string, payload []byte, props map[string]string) error {
	subs, err := b.router.Match(topic)
	if err != nil {
		b.logger.Error("router match failed", "topic", topic, "error", err)
		return err
	}

	for _, sub := range subs {
		if IsAMQP091Client(sub.ClientID) {
			connID := strings.TrimPrefix(sub.ClientID, corebroker.AMQP091ClientPrefix)
			val, ok := b.connections.Load(connID)
			if !ok {
				continue
			}
			c := val.(*Connection)
			c.deliverMessage(topic, payload, props)
			continue
		}
		if b.crossDeliver != nil {
			b.crossDeliver(sub.ClientID, topic, payload, sub.QoS, props)
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
			b.logger.Error("AMQP 0.9.1 cluster route publish failed", "topic", topic, "error", err)
			return fmt.Errorf("cluster route publish: %w", err)
		}
	}

	return nil
}

// ForwardPublish handles a forwarded publish from a remote cluster node.
// It matches local AMQP 0.9.1 subscriptions and delivers without re-routing to the cluster.
func (b *Broker) ForwardPublish(ctx context.Context, msg *cluster.Message) error {
	subs, err := b.router.Match(msg.Topic)
	if err != nil {
		return err
	}

	for _, sub := range subs {
		if !IsAMQP091Client(sub.ClientID) {
			continue
		}
		connID := strings.TrimPrefix(sub.ClientID, corebroker.AMQP091ClientPrefix)
		val, ok := b.connections.Load(connID)
		if !ok {
			continue
		}
		c := val.(*Connection)
		c.deliverMessage(msg.Topic, msg.Payload, msg.Properties)
	}

	return nil
}

// DeliverToClient delivers a queue message to a specific AMQP 0.9.1 client.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg any) error {
	connID := strings.TrimPrefix(clientID, corebroker.AMQP091ClientPrefix)

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
	connID := strings.TrimPrefix(clientID, corebroker.AMQP091ClientPrefix)

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

// LocalDeliverPubSub delivers a local pub/sub message to an AMQP 0.9.1 client.
func (b *Broker) LocalDeliverPubSub(clientID string, topic string, payload []byte, _ byte, props map[string]string) {
	connID := strings.TrimPrefix(clientID, corebroker.AMQP091ClientPrefix)
	val, ok := b.connections.Load(connID)
	if !ok {
		return
	}
	val.(*Connection).deliverMessage(topic, payload, props)
}
