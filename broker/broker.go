// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"sync"

	"github.com/absmach/mqtt/broker/router"
	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/server/otel"
	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/storage/memory"
	"go.opentelemetry.io/otel/trace"
)

const (
	inflightPrefix = "/inflight/"
	queuePrefix    = "/queue/"
)

// Notifier defines the interface for webhook notifications.
type Notifier interface {
	Notify(ctx context.Context, event interface{}) error
	Close() error
}

// QueueManager defines the interface for durable queue management.
type QueueManager interface {
	Start(ctx context.Context) error
	Stop() error
	Enqueue(ctx context.Context, queueTopic string, payload []byte, properties map[string]string) error
	Subscribe(ctx context.Context, queueTopic, clientID, groupID, proxyNodeID string) error
	Unsubscribe(ctx context.Context, queueTopic, clientID, groupID string) error
	Ack(ctx context.Context, queueTopic, messageID string) error
	Nack(ctx context.Context, queueTopic, messageID string) error
	Reject(ctx context.Context, queueTopic, messageID string, reason string) error
	// UpdateHeartbeat updates the heartbeat timestamp for a consumer across all queues/groups.
	// This should be called when a PINGREQ is received from a client.
	UpdateHeartbeat(ctx context.Context, clientID string) error
}

// ClientRateLimiter defines the interface for per-client rate limiting.
type ClientRateLimiter interface {
	// AllowPublish checks if a publish from the given client is allowed.
	AllowPublish(clientID string) bool
	// AllowSubscribe checks if a subscription from the given client is allowed.
	AllowSubscribe(clientID string) bool
	// OnClientDisconnect cleans up rate limiters for a disconnected client.
	OnClientDisconnect(clientID string)
}

// Broker is the core MQTT broker with clean domain methods.
type Broker struct {
	mu            sync.RWMutex
	wg            sync.WaitGroup
	sessionsMap   session.Cache
	router        Router
	messages      storage.MessageStore
	sessions      storage.SessionStore
	subscriptions storage.SubscriptionStore
	retained      storage.RetainedStore
	wills         storage.WillStore
	cluster       cluster.Cluster // nil for single-node mode
	queueManager  QueueManager    // nil if queue functionality disabled
	auth          *AuthEngine
	rateLimiter   ClientRateLimiter // nil if rate limiting disabled
	logger        *slog.Logger
	stats         *Stats
	webhooks      Notifier      // nil if webhooks disabled
	metrics       *otel.Metrics // nil if metrics disabled
	tracer        trace.Tracer  // nil if tracing disabled
	stopCh        chan struct{}
	shuttingDown  bool
	closed        bool
	// Shared subscriptions (MQTT 5.0)
	sharedSubs *SharedSubscriptionManager
	// Maximum QoS level supported by this broker (0, 1, or 2)
	maxQoS byte
}

// NewBroker creates a new broker instance.
// Parameters:
//   - store: Storage backend for messages, sessions, subscriptions, retained, and wills (nil uses memory)
//   - cl: Cluster coordination interface (nil for single-node mode)
//   - logger: Logger instance (nil uses default)
//   - stats: Stats collector (nil creates new one)
//   - webhooks: Webhook notifier (nil if webhooks disabled)
//   - metrics: OTel metrics instance (nil if metrics disabled)
//   - tracer: OTel tracer (nil if tracing disabled)
func NewBroker(store storage.Store, cl cluster.Cluster, logger *slog.Logger, stats *Stats, webhooks Notifier, metrics *otel.Metrics, tracer trace.Tracer) *Broker {
	if store == nil {
		// Fallback to memory storage if none provided
		store = memory.New()
	}

	r := router.NewRouter()

	if logger == nil {
		logger = slog.Default()
	}
	if stats == nil {
		stats = NewStats()
	}

	b := &Broker{
		sessionsMap:   session.NewMapCache(),
		router:        r,
		messages:      store.Messages(),
		sessions:      store.Sessions(),
		subscriptions: store.Subscriptions(),
		retained:      store.Retained(),
		wills:         store.Wills(),
		cluster:       cl,
		logger:        logger,
		stats:         stats,
		webhooks:      webhooks,
		metrics:       metrics,
		tracer:        tracer,
		stopCh:        make(chan struct{}),
		sharedSubs:    NewSharedSubscriptionManager(),
		maxQoS:        2, // Default to QoS 2 (highest)
	}

	b.wg.Add(2)
	go b.expiryLoop()
	go b.statsLoop()

	return b
}

// SetQueueManager sets the queue manager for the broker.
// This should be called before the broker starts accepting connections.
func (b *Broker) SetQueueManager(qm QueueManager) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queueManager = qm

	// Start queue manager
	if qm != nil {
		return qm.Start(context.Background())
	}

	return nil
}

// Get returns a session by client ID.
func (b *Broker) Get(clientID string) *session.Session {
	return b.sessionsMap.Get(clientID)
}

// Stats returns the broker statistics.
func (b *Broker) Stats() *Stats {
	return b.stats
}

// SetAuthEngine sets the authentication and authorization engine.
func (b *Broker) SetAuthEngine(auth *AuthEngine) {
	b.auth = auth
}

// SetClientRateLimiter sets the client rate limiter for publish/subscribe rate limiting.
func (b *Broker) SetClientRateLimiter(rl ClientRateLimiter) {
	b.rateLimiter = rl
}

// SetMaxQoS sets the maximum QoS level supported by this broker.
// Valid values are 0, 1, or 2. Default is 2.
func (b *Broker) SetMaxQoS(qos byte) {
	if qos > 2 {
		qos = 2
	}
	b.maxQoS = qos
}

// MaxQoS returns the maximum QoS level supported by this broker.
func (b *Broker) MaxQoS() byte {
	return b.maxQoS
}

func (b *Broker) logOp(op string, attrs ...any) {
	b.logger.Debug(op, attrs...)
}

func (b *Broker) logError(op string, err error, attrs ...any) {
	if err != nil {
		allAttrs := append([]any{slog.String("error", err.Error())}, attrs...)
		b.logger.Error(op, allAttrs...)
	}
}
