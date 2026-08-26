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
	coremessage "github.com/absmach/fluxmq/message"
)

type queueLinkManager interface {
	corebroker.QueuePublisher
	corebroker.QueueSubscriber
	corebroker.QueueAcknowledger
}

type queueAdminManager interface {
	corebroker.QueueAdmin
}

// Broker manages AMQP 1.0 connections and message routing.
type Broker struct {
	connections         sync.Map // containerID -> *Connection
	router              *router.TrieRouter
	routeResolver       *corebroker.RoutingResolver
	queueLinkManager    queueLinkManager
	queueAdminManager   queueAdminManager
	cluster             cluster.Cluster
	auth                *corebroker.AuthEngine
	hooks               *corebroker.BlockingHookEngine
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
		router:            router.NewRouter(),
		routeResolver:     corebroker.NewRoutingResolver(),
		queueLinkManager:  qm,
		queueAdminManager: qm,
		stats:             stats,
		logger:            logger,
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
func (b *Broker) HandleConnection(ctx context.Context, conn net.Conn) {
	c := newConnection(ctx, b, conn)
	if err := c.run(); err != nil { //nolint:contextcheck // connection lifecycle ctx is stored in c.ctx and used downstream
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

// IsClientConnected reports whether the AMQP 1.0 client has a live
// connection in this broker instance.
func (b *Broker) IsClientConnected(clientID string) bool {
	containerID := strings.TrimPrefix(clientID, corebroker.AMQP1ClientPrefix)
	_, ok := b.connections.Load(containerID)
	return ok
}

// Publish routes a message to all subscribers via the shared router.
// AMQP 1.0 subscribers are delivered locally; others via the cross-deliver callback.
// The ctx is forwarded to cross-protocol delivery so that connection or broker
// shutdown unblocks downstream operations.
func (b *Broker) Publish(ctx context.Context, topic string, payload []byte, props map[string]string) { //nolint:contextcheck // ctx is propagated to cross-deliver and cluster route
	if ctx == nil {
		ctx = context.Background()
	}
	// A capture failure never fails the publish: see
	// corebroker.TopicQueuePublisher.
	if publisher, ok := b.queueLinkManager.(corebroker.TopicQueuePublisher); ok {
		captured := queuePublishEnvelope(topic, payload, props, coremessage.SourceMetadata{
			ClientID:   props[coremessage.PropertyClientID],
			ExternalID: props[coremessage.PropertyExternalID],
			Protocol:   coremessage.Protocol(props[coremessage.PropertyProtocol]),
		})
		err := publisher.PublishToMatchingQueues(ctx, captured)
		coremessage.Release(captured)
		if err != nil {
			b.logger.Error("queue topic capture failed", "topic", topic, "error", err)
		}
	}
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
				b.crossDeliver(ctx, sub.ClientID, topic, payload, sub.QoS, props)
			}
		}
	}

	if cl := b.cluster; cl != nil {
		timeout := b.routePublishTimeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		// Derive from the caller's ctx so broker shutdown cancels in-flight
		// cluster routes, but cap with a timeout.
		routeCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		routed := coremessage.New(topic, payload)
		if err := coremessage.ApplyTrustedProperties(routed, props); err != nil {
			b.logger.Warn("AMQP cluster route publish dropped malformed properties",
				"topic", topic, "error", err)
		}
		routed.Broker.Delivery.QoS = 1
		err := cl.RoutePublish(routeCtx, routed)
		coremessage.Release(routed)
		if err != nil {
			b.logger.Error("AMQP cluster route publish failed", "topic", topic, "error", err)
		}
	}
}

// ForwardPublish handles a forwarded publish from a remote cluster node.
// It delivers only to local AMQP 1.0 subscribers without re-routing to the cluster.
func (b *Broker) ForwardPublish(ctx context.Context, msg *coremessage.Envelope) error {
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
		c.deliverMessage(msg.Topic, msg.PayloadBytes(), coremessage.ProjectProperties(msg, coremessage.PublicProjection), sub.QoS) //nolint:contextcheck // fire-and-forget delivery, metrics use background context
	}

	return nil
}

// LocalDeliverPubSub delivers a pub/sub message to a specific local AMQP 1.0 connection.
// Called by the cross-deliver callback from other protocol brokers.
func (b *Broker) LocalDeliverPubSub(ctx context.Context, clientID string, topic string, payload []byte, qos byte, props map[string]string) {
	containerID := strings.TrimPrefix(clientID, corebroker.AMQP1ClientPrefix)
	val, ok := b.connections.Load(containerID)
	if !ok {
		return
	}
	c := val.(*Connection)
	c.deliverMessage(topic, payload, props, qos) //nolint:contextcheck // fire-and-forget delivery, metrics use background context
}

// DeliverToClient delivers a queue message to a specific AMQP client.
// clientID must have the "amqp:" prefix already stripped.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg *coremessage.Envelope) error {
	defer coremessage.Release(msg)
	// Strip the amqp: prefix to get the container ID
	containerID := strings.TrimPrefix(clientID, corebroker.AMQP1ClientPrefix)

	val, ok := b.connections.Load(containerID)
	if !ok {
		return fmt.Errorf("%w: AMQP client not found: %s", corebroker.ErrClientNotConnected, containerID)
	}

	c := val.(*Connection)

	props := coremessage.ProjectProperties(msg, coremessage.PublicProjection)
	amqpMsg := &message.Message{
		Properties: &message.Properties{To: msg.Topic},
		Data:       [][]byte{msg.PayloadBytes()},
	}
	if props != nil {
		amqpMsg.ApplicationProperties = make(map[string]any, len(props))
		for key, value := range props {
			amqpMsg.ApplicationProperties[key] = value
		}
	}
	// A durable delivery is named by the broker's handle; anything else carries
	// whatever identifier the publisher set.
	if handle := msg.Broker.Queue.DeliveryID(); handle != "" {
		amqpMsg.Properties.MessageID = handle
	} else if msg.User.MessageID != "" {
		amqpMsg.Properties.MessageID = msg.User.MessageID
	}

	c.deliverAMQPMessage(msg.Topic, amqpMsg, msg.Broker.Delivery.QoS) //nolint:contextcheck // fire-and-forget delivery, metrics use background context
	return nil
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
func (b *Broker) DeliverToClusterMessage(ctx context.Context, clientID string, msg *coremessage.Envelope) error {
	containerID := strings.TrimPrefix(clientID, corebroker.AMQP1ClientPrefix)
	val, ok := b.connections.Load(containerID)
	if !ok {
		return fmt.Errorf("%w: AMQP client not found: %s", corebroker.ErrClientNotConnected, containerID)
	}
	c := val.(*Connection)
	c.deliverMessage(msg.Topic, msg.PayloadBytes(), coremessage.ProjectProperties(msg, coremessage.PublicProjection), msg.Broker.Delivery.QoS) //nolint:contextcheck // fire-and-forget delivery, metrics use background context
	return nil
}

// SetQueueManager sets the queue manager for the AMQP broker.
func (b *Broker) SetQueueManager(qm corebroker.QueueManager) {
	b.queueLinkManager = qm
	b.queueAdminManager = qm
}

// SetAuthEngine sets the authentication and authorization engine.
func (b *Broker) SetAuthEngine(auth *corebroker.AuthEngine) {
	b.auth = auth
}

// SetBlockingHooks sets the optional blocking hook engine.
func (b *Broker) SetBlockingHooks(h *corebroker.BlockingHookEngine) {
	b.hooks = h
}

// ApplyHook runs the optional blocking hook.
func (b *Broker) ApplyHook(ctx context.Context, req corebroker.BlockingHookRequest) (corebroker.BlockingHookRequest, bool) {
	if b.hooks == nil {
		return req, true
	}
	req.Protocol = corebroker.HookProtocolAMQP10
	return b.hooks.Handle(ctx, req)
}

// ApplyPublishHooks runs the optional auth_on_publish hook.
func (b *Broker) ApplyPublishHooks(ctx context.Context, clientID, externalID, topic string, payload []byte, props map[string]string) (corebroker.BlockingHookRequest, bool) {
	return b.ApplyHook(ctx, corebroker.BlockingHookRequest{
		Hook:       corebroker.HookAuthOnPublish,
		ClientID:   clientID,
		ExternalID: externalID,
		Topic:      topic,
		Payload:    payload,
		Properties: props,
	})
}

// ApplySubscribeHooks runs the optional auth_on_subscribe hook.
func (b *Broker) ApplySubscribeHooks(ctx context.Context, clientID, externalID, filter string) (string, bool) {
	req, ok := b.ApplyHook(ctx, corebroker.BlockingHookRequest{
		Hook:       corebroker.HookAuthOnSubscribe,
		ClientID:   clientID,
		ExternalID: externalID,
		Topic:      filter,
	})
	return req.Topic, ok
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

// queuePublishEnvelope builds the envelope an AMQP 1.0 publish hands to the
// queue manager. The manager borrows it, so the caller releases it once the
// publish returns.
func queuePublishEnvelope(
	topic string, payload []byte, props map[string]string, source coremessage.SourceMetadata,
) *coremessage.Envelope {
	envelope := coremessage.New(topic, payload)
	envelope.Broker.Source = source
	envelope.Broker.Trace = coremessage.TraceFromProperties(props)
	envelope.User.Properties = coremessage.FilterUserProperties(props)
	return envelope
}
