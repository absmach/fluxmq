// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/amqp/codec"
	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/message"
	qtypes "github.com/absmach/fluxmq/queue/types"
)

// channelQueueManager composes the shared broker interfaces rather than
// restating their methods, so a change to the queue contract cannot leave this
// adapter's copy silently behind.
type channelQueueManager interface {
	corebroker.QueuePublisher
	corebroker.QueueSubscriber
	corebroker.QueueAcknowledger
	corebroker.QueueStreamOps
	CreateQueue(ctx context.Context, config qtypes.QueueConfig) error
	GetQueue(ctx context.Context, queueName string) (*qtypes.QueueConfig, error)
}

// durableStreamQueuePublisher is intentionally separate from the general
// queue-manager interface. The local-principal listener fails closed unless
// the concrete manager can target and durably sync one exact stream.
type durableStreamQueuePublisher interface {
	PublishToDurableStream(ctx context.Context, queueName string, publish qtypes.PublishRequest) error
}

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
	connectionSeq       atomic.Uint64
	router              *router.TrieRouter
	routeResolver       *corebroker.RoutingResolver
	queueManager        channelQueueManager
	auth                *corebroker.AuthEngine
	hooks               *corebroker.BlockingHookEngine
	cluster             cluster.Cluster
	crossDeliver        corebroker.CrossDeliverFunc
	routePublishTimeout time.Duration
	durableAppends      durableAppendLimiter
	stats               *Stats
	logger              *slog.Logger
}

func (b *Broker) nextConnectionID(remote net.Addr) string {
	remoteAddress := "unknown"
	if remote != nil {
		remoteAddress = remote.String()
	}
	return fmt.Sprintf("%s@%d", remoteAddress, b.connectionSeq.Add(1))
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

// SetAuthEngine sets the authentication and authorization engine.
func (b *Broker) SetAuthEngine(auth *corebroker.AuthEngine) {
	b.auth = auth
}

// SetBlockingHooks sets the optional blocking hook engine.
func (b *Broker) SetBlockingHooks(h *corebroker.BlockingHookEngine) {
	b.hooks = h
}

// ExternalID returns the cached external identity for a protocol-level client ID.
func (b *Broker) ExternalID(clientID string) string {
	if b.auth == nil {
		return ""
	}
	return b.auth.ExternalID(clientID)
}

// ApplyHook runs the optional blocking hook.
func (b *Broker) ApplyHook(ctx context.Context, req corebroker.BlockingHookRequest) (corebroker.BlockingHookRequest, bool) {
	if b.hooks == nil {
		return req, true
	}
	req.Protocol = corebroker.HookProtocolAMQP091
	return b.hooks.Handle(ctx, req)
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
func (b *Broker) HandleConnection(ctx context.Context, netConn net.Conn) {
	b.handleConnection(ctx, netConn, nil)
}

// HandleConnectionWithPolicy handles a connection using an immutable,
// listener-scoped security policy. This is the entry point for servers that
// expose multiple AMQP listeners with different trust boundaries.
func (b *Broker) HandleConnectionWithPolicy(ctx context.Context, netConn net.Conn, policy *ConnectionPolicy) {
	b.handleConnection(ctx, netConn, policy)
}

func (b *Broker) handleConnection(ctx context.Context, netConn net.Conn, policy *ConnectionPolicy) {
	c := newConnection(ctx, b, netConn, policy)
	if err := c.run(); err != nil { //nolint:contextcheck // connection lifecycle manages its own context for cleanup
		b.logger.Debug("AMQP 0.9.1 connection ended", "remote", netConn.RemoteAddr(), "error", err)
	}
}

func (b *Broker) registerConnection(connID string, c *Connection) {
	b.connections.Store(connID, c)
}

func (b *Broker) unregisterConnection(connID string) {
	b.connections.Delete(connID)
}

// IsClientConnected reports whether the AMQP 0.9.1 client has a live
// connection in this broker instance.
func (b *Broker) IsClientConnected(clientID string) bool {
	connID := strings.TrimPrefix(clientID, corebroker.AMQP091ClientPrefix)
	_, ok := b.connections.Load(connID)
	return ok
}

// ConnectionIDs returns active AMQP 0.9.1 connection IDs sorted ascending.
func (b *Broker) ConnectionIDs() []string {
	ids := make([]string, 0)
	b.connections.Range(func(key, _ any) bool {
		if connID, ok := key.(string); ok {
			ids = append(ids, connID)
		}
		return true
	})
	sort.Strings(ids)
	return ids
}

// HasConnection reports whether an AMQP 0.9.1 connection is currently active.
func (b *Broker) HasConnection(connID string) bool {
	_, ok := b.connections.Load(connID)
	return ok
}

// ConnectionName returns the human-readable name for the given connection, if set.
func (b *Broker) ConnectionName(connID string) string {
	v, ok := b.connections.Load(connID)
	if !ok {
		return ""
	}
	return v.(*Connection).connectionName
}

// DisconnectInvalidLocalSessions disconnects every local-principal session for
// which isValid returns false. It is intended for atomic credential/ACL reloads
// and never evaluates or mutates external-auth sessions.
//
// The returned value is the number of connections selected for disconnection.
func (b *Broker) DisconnectInvalidLocalSessions(isValid func(LocalSessionIdentity) bool) int {
	if isValid == nil {
		return 0
	}

	// Each disconnect writes a Connection.Close under a write deadline, so an
	// unresponsive peer costs up to that deadline. Revocation runs while the
	// reload holds its lock, so disconnect concurrently: the cost stays one
	// deadline in total rather than one per stalled peer.
	var revoked sync.WaitGroup
	disconnected := 0
	b.connections.Range(func(_, value any) bool {
		conn, ok := value.(*Connection)
		if !ok {
			return true
		}
		identity, ok := conn.localSessionIdentity()
		if !ok || isValid(identity) {
			return true
		}
		disconnected++
		revoked.Add(1)
		go func() {
			defer revoked.Done()
			conn.disconnect(codec.AccessRefused, "local principal credentials revoked")
		}()
		return true
	})
	revoked.Wait()
	if disconnected > 0 {
		b.stats.AddLocalForcedDisconnects(uint64(disconnected))
		b.logger.Warn("amqp091_local_sessions_disconnected",
			"auth_mode", "local",
			"outcome", "disconnected",
			"reason", "credentials_or_policy_revoked",
			"count", disconnected)
	}
	return disconnected
}

// RecordLocalPrincipalReload records one completed local-principal reload
// attempt. Successful no-op reloads count as successes because mounted secret
// files are intentionally revalidated on every attempt.
func (b *Broker) RecordLocalPrincipalReload(success bool) {
	if success {
		b.stats.IncrementLocalReloadSuccess()
		b.logger.Info("amqp091_local_principal_reload",
			"auth_mode", "local",
			"outcome", "success")
		return
	}
	b.stats.IncrementLocalReloadFailures()
	b.logger.Warn("amqp091_local_principal_reload",
		"auth_mode", "local",
		"outcome", "failure",
		"reason", "validation_or_load_failed")
}

// Publish routes a message to local AMQP 0.9.1 subscribers and remote cluster nodes.
// It returns an error if cluster routing fails, so callers in confirm mode can NACK.
//
// ctx bounds queue capture, cross-protocol delivery, and cluster routing so
// none of them outlive the broker. It is the listener context the connection
// was accepted under, so it is cancelled at server shutdown rather than when
// the publishing peer disconnects.
//
// That is deliberate. The same context is handed to crossDeliver, which
// delivers to other subscribers, so cancelling it when the publisher goes away
// would drop a message the publisher already handed over into clients that are
// still connected. A publication outlives its publisher by design; only broker
// shutdown ends it.
func (b *Broker) Publish(ctx context.Context, topic string, payload []byte, props map[string]string) error { //nolint:contextcheck // ctx is propagated to capture, cross-deliver and cluster route
	if ctx == nil {
		ctx = context.Background()
	}
	// A capture failure never fails the publish: see
	// corebroker.TopicQueuePublisher.
	if publisher, ok := b.queueManager.(corebroker.TopicQueuePublisher); ok {
		if err := publisher.PublishToMatchingQueues(ctx, qtypes.PublishRequest{
			Source: message.SourceMetadata{
				ClientID:   props[message.PropertyClientID],
				ExternalID: props[message.PropertyExternalID],
				Protocol:   message.Protocol(props[message.PropertyProtocol]),
			},
			Trace:      message.TraceFromProperties(props),
			Topic:      topic,
			Payload:    payload,
			Properties: message.FilterUserProperties(props),
		}); err != nil {
			b.logger.Error("queue topic capture failed", "topic", topic, "error", err)
		}
	}

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
			b.crossDeliver(ctx, sub.ClientID, topic, payload, sub.QoS, props)
		}
	}

	if cl := b.cluster; cl != nil {
		timeout := b.routePublishTimeout
		if timeout <= 0 {
			timeout = 15 * time.Second
		}
		// Derive from the caller's ctx so a closed connection or broker
		// shutdown cancels in-flight cluster routes, but cap with a timeout.
		routeCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		routed := message.New(topic, payload)
		if err := message.ApplyTrustedProperties(routed, props); err != nil {
			b.logger.Warn("AMQP 0.9.1 cluster route publish dropped malformed properties",
				"topic", topic, "error", err)
		}
		routed.Broker.Delivery.QoS = 1
		err := cl.RoutePublish(routeCtx, routed)
		message.Release(routed)
		if err != nil {
			b.logger.Error("AMQP 0.9.1 cluster route publish failed", "topic", topic, "error", err)
			return fmt.Errorf("cluster route publish: %w", err)
		}
	}

	return nil
}

// ForwardPublish handles a forwarded publish from a remote cluster node.
// It matches local AMQP 0.9.1 subscriptions and delivers without re-routing to the cluster.
func (b *Broker) ForwardPublish(ctx context.Context, msg *message.Envelope) error {
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
		projection := message.PublicProjection
		if c.connectionPolicy().carriesReservedProperties() {
			projection = message.TrustedServiceProjection
		}
		c.deliverMessage(msg.Topic, msg.PayloadBytes(), message.ProjectProperties(msg, projection))
	}

	return nil
}

// DeliverToClient delivers a queue message to a specific AMQP 0.9.1 client.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg *message.Envelope) error {
	defer message.Release(msg)
	connID := strings.TrimPrefix(clientID, corebroker.AMQP091ClientPrefix)

	val, ok := b.connections.Load(connID)
	if !ok {
		return fmt.Errorf("%w: AMQP 0.9.1 client not found: %s", corebroker.ErrClientNotConnected, connID)
	}

	c := val.(*Connection)

	projection := message.PublicProjection
	if c.connectionPolicy().carriesReservedProperties() {
		projection = message.TrustedServiceProjection
	}
	c.deliverMessage(msg.Topic, msg.PayloadBytes(), message.ProjectProperties(msg, projection))
	return nil
}

// DeliverToClusterMessage delivers a message routed from another cluster node to a local AMQP 0.9.1 client.
func (b *Broker) DeliverToClusterMessage(ctx context.Context, clientID string, msg *message.Envelope) error {
	connID := strings.TrimPrefix(clientID, corebroker.AMQP091ClientPrefix)

	val, ok := b.connections.Load(connID)
	if !ok {
		return fmt.Errorf("%w: AMQP 0.9.1 client not found: %s", corebroker.ErrClientNotConnected, connID)
	}

	c := val.(*Connection)
	projection := message.PublicProjection
	if c.connectionPolicy().carriesReservedProperties() {
		projection = message.TrustedServiceProjection
	}
	c.deliverMessage(msg.Topic, msg.PayloadBytes(), message.ProjectProperties(msg, projection))
	return nil
}

// CancelConsumers sends server-initiated basic.cancel frames for consumers
// removed from the given queue/group. Only AMQP 0.9.1 clients are affected;
// other protocol prefixes are skipped.
func (b *Broker) CancelConsumers(queueName, groupID string, clientIDs []string) {
	for _, clientID := range clientIDs {
		if !corebroker.IsAMQP091Client(clientID) {
			continue
		}
		connID := strings.TrimPrefix(clientID, corebroker.AMQP091ClientPrefix)
		val, ok := b.connections.Load(connID)
		if !ok {
			continue
		}
		val.(*Connection).cancelConsumerByQueue(queueName, groupID)
	}
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
func (b *Broker) LocalDeliverPubSub(ctx context.Context, clientID string, topic string, payload []byte, _ byte, props map[string]string) {
	connID := strings.TrimPrefix(clientID, corebroker.AMQP091ClientPrefix)
	val, ok := b.connections.Load(connID)
	if !ok {
		return
	}
	val.(*Connection).deliverMessage(topic, payload, props)
}
