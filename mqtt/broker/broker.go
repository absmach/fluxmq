// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/router"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/server/otel"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"go.opentelemetry.io/otel/trace"
)

const (
	inflightPrefix = "/inflight/"
	queuePrefix    = "/queue/"
)

type queueManager interface {
	broker.QueueLifecycle
	broker.QueuePublisher
	broker.QueueSubscriber
	broker.QueueAcknowledger
	broker.QueueAdminRead
}

// brokerStores groups all storage backends.
type brokerStores struct {
	messages      storage.MessageStore
	sessions      storage.SessionStore
	subscriptions storage.SubscriptionStore
	retained      storage.RetainedStore
	wills         storage.WillStore
}

// brokerTelemetry groups observability dependencies.
type brokerTelemetry struct {
	logger   *slog.Logger
	stats    *Stats
	webhooks broker.Notifier // nil if webhooks disabled
	metrics  *otel.Metrics   // nil if metrics disabled
	tracer   trace.Tracer    // nil if tracing disabled
}

// brokerConfig groups tunable broker settings.
type brokerConfig struct {
	maxQoS              atomic.Uint32 // accessed atomically for hot-reload safety
	maxOfflineQueueSize int
	offlineQueueEvict   bool
	maxInflightMessages int
	routePublishTimeout time.Duration
	asyncFanOut         bool
	sessionCfg          config.SessionConfig
}

// Option configures the broker.
type Option func(*Broker)

// WithLogger sets the logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(b *Broker) { b.telemetry.logger = l }
}

// WithStats sets the stats collector. Defaults to NewStats().
func WithStats(s *Stats) Option {
	return func(b *Broker) { b.telemetry.stats = s }
}

// WithWebhooks sets the webhook notifier.
func WithWebhooks(n broker.Notifier) Option {
	return func(b *Broker) { b.telemetry.webhooks = n }
}

// WithMetrics sets the OTel metrics instance.
func WithMetrics(m *otel.Metrics) Option {
	return func(b *Broker) { b.telemetry.metrics = m }
}

// WithTracer sets the OTel tracer.
func WithTracer(t trace.Tracer) Option {
	return func(b *Broker) { b.telemetry.tracer = t }
}

// WithSessionConfig sets session-related settings.
func WithSessionConfig(c config.SessionConfig) Option {
	return func(b *Broker) {
		b.cfg.sessionCfg = c
		b.cfg.maxOfflineQueueSize = c.MaxOfflineQueueSize
		b.cfg.offlineQueueEvict = c.OfflineQueuePolicy == "evict"
		b.cfg.maxInflightMessages = c.MaxInflightMessages
	}
}

// WithTransportConfig sets transport-related settings.
func WithTransportConfig(c config.TransportConfig) Option {
	return func(b *Broker) { b.cfg.routePublishTimeout = c.RoutePublishTimeout }
}

// WithBrokerConfig sets broker-level settings (fan-out, etc).
func WithBrokerConfig(c config.BrokerConfig) Option {
	return func(b *Broker) {
		b.cfg.asyncFanOut = c.AsyncFanOut
		if c.AsyncFanOut {
			b.fanOutPool = newFanOutPool(c.FanOutWorkers)
		}
	}
}

// Broker is the core MQTT broker with clean domain methods.
type Broker struct {
	stores    brokerStores
	telemetry brokerTelemetry
	cfg       brokerConfig

	sessionLocks  keyLock
	globalMu      sync.Mutex // protects lifecycle (Close, transferActiveSessions, expireSessions)
	wg            sync.WaitGroup
	sessionsMap   session.Cache
	router        Router
	cluster       cluster.Cluster         // nil for single-node mode
	queueManager  queueManager            // nil if queue functionality disabled
	crossDeliver  broker.CrossDeliverFunc // nil if cross-protocol local pub/sub disabled
	routeResolver *broker.RoutingResolver // shared routing policy
	auth          *broker.AuthEngine
	rateLimiter   broker.RateLimiter // nil if rate limiting disabled
	eventHook     broker.EventHook   // nil if no event hooks configured
	stopCh        chan struct{}
	shuttingDown  atomic.Bool
	closed        atomic.Bool
	sharedSubs    *SharedSubscriptionManager
	fanOutPool    *fanOutPool // non-nil only when AsyncFanOut is true
}

// NewBroker creates a new broker instance.
// store and cl are required dependencies (nil store falls back to in-memory).
// All other settings are supplied via functional options.
func NewBroker(store storage.Store, cl cluster.Cluster, opts ...Option) *Broker {
	if store == nil {
		store = memory.New()
	}

	b := &Broker{
		sessionsMap:   session.NewShardedCache(),
		router:        router.NewRouter(),
		routeResolver: broker.NewRoutingResolver(),
		stores: brokerStores{
			messages:      store.Messages(),
			sessions:      store.Sessions(),
			subscriptions: store.Subscriptions(),
			retained:      store.Retained(),
			wills:         store.Wills(),
		},
		telemetry: brokerTelemetry{
			logger: slog.Default(),
			stats:  NewStats(),
		},
		cluster:    cl,
		stopCh:     make(chan struct{}),
		sharedSubs: NewSharedSubscriptionManager(),
	}
	b.cfg.maxQoS.Store(2)

	for _, opt := range opts {
		opt(b)
	}

	b.wg.Add(2)
	go b.expiryLoop()
	go b.statsLoop()

	return b
}

// SetQueueManager sets the queue manager for the broker.
// Must be called before the broker starts accepting connections.
func (b *Broker) SetQueueManager(qm broker.QueueManager) error {
	b.queueManager = qm
	if qm != nil {
		return qm.Start(context.Background())
	}
	return nil
}

// SetRouter sets the topic router used for local pub/sub matching.
// Must be called before the broker starts accepting connections.
func (b *Broker) SetRouter(r Router) {
	if r == nil {
		return
	}
	b.router = r
}

// SetCrossDeliver sets the local cross-protocol pub/sub delivery callback.
// Must be called before the broker starts accepting connections.
func (b *Broker) SetCrossDeliver(fn broker.CrossDeliverFunc) {
	b.crossDeliver = fn
}

// GetQueueManager returns the queue manager.
func (b *Broker) GetQueueManager() broker.QueueManager {
	qm, _ := b.queueManager.(broker.QueueManager)
	return qm
}

// Get returns a session by client ID.
func (b *Broker) Get(clientID string) *session.Session {
	return b.sessionsMap.Get(clientID)
}

// Stats returns the broker statistics.
func (b *Broker) Stats() *Stats {
	return b.telemetry.stats
}

// SessionCount returns the total number of sessions (connected + disconnected).
func (b *Broker) SessionCount() int {
	return b.sessionsMap.Count()
}

// SetAuthEngine sets the authentication and authorization engine.
func (b *Broker) SetAuthEngine(auth *broker.AuthEngine) {
	b.auth = auth
}

// Authenticate validates credentials using the configured auth engine.
// Returns true when auth is not configured.
func (b *Broker) Authenticate(clientID, username, password string) (bool, error) {
	if b.auth == nil {
		return true, nil
	}
	return b.auth.Authenticate(clientID, username, password)
}

// CanPublish checks publish authorization for a client/topic pair.
// Returns true when authz is not configured.
func (b *Broker) CanPublish(clientID, topic string) bool {
	if b.auth == nil {
		return true
	}
	return b.auth.CanPublish(clientID, topic)
}

// ExternalID returns the authenticated external identity for a client.
func (b *Broker) ExternalID(clientID string) string {
	if b.auth == nil {
		return ""
	}
	return b.auth.ExternalID(clientID)
}

// SetClientRateLimiter sets the client rate limiter for publish/subscribe rate limiting.
func (b *Broker) SetClientRateLimiter(rl broker.RateLimiter) {
	b.rateLimiter = rl
}

// SetEventHook sets the event hook for lifecycle notifications.
func (b *Broker) SetEventHook(h broker.EventHook) {
	b.eventHook = h
}

// SetMaxQoS sets the maximum QoS level supported by this broker.
// Valid values are 0, 1, or 2. Default is 2. Safe to call concurrently.
func (b *Broker) SetMaxQoS(qos byte) {
	if qos > 2 {
		qos = 2
	}
	b.cfg.maxQoS.Store(uint32(qos))
}

// MaxQoS returns the maximum QoS level supported by this broker.
// Safe to call concurrently.
func (b *Broker) MaxQoS() byte {
	return byte(b.cfg.maxQoS.Load())
}

// NotifyConnect fires the event hook for a successful client connection.
// Should be called by protocol handlers after CONNACK is sent.
func (b *Broker) NotifyConnect(clientID, username, protocol string) {
	if b.eventHook != nil {
		if err := b.eventHook.OnConnect(context.Background(), clientID, username, protocol); err != nil {
			b.logError("event_hook_connect", err, slog.String("client_id", clientID))
		}
	}
}

func (b *Broker) logOp(op string, attrs ...any) {
	b.telemetry.logger.Debug(op, attrs...)
}

func (b *Broker) logError(op string, err error, attrs ...any) {
	if err != nil {
		allAttrs := append([]any{slog.String("error", err.Error())}, attrs...)
		b.telemetry.logger.Error(op, allAttrs...)
	}
}
