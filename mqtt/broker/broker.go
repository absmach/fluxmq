// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/router"
	qtypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/server/otel"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
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
	UpdateHeartbeat(ctx context.Context, clientID string) error
	CreateQueue(ctx context.Context, config qtypes.QueueConfig) error
	DeleteQueue(ctx context.Context, queueName string) error
	GetQueue(ctx context.Context, queueName string) (*qtypes.QueueConfig, error)
	ListQueues(ctx context.Context) ([]qtypes.QueueConfig, error)
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
	sessionLocks  keyLock
	globalMu      sync.Mutex // protects lifecycle (Close, SetQueueManager, transferActiveSessions, expireSessions)
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
	auth          *broker.AuthEngine
	rateLimiter   ClientRateLimiter // nil if rate limiting disabled
	logger        *slog.Logger
	stats         *Stats
	webhooks      Notifier      // nil if webhooks disabled
	metrics       *otel.Metrics // nil if metrics disabled
	tracer        trace.Tracer  // nil if tracing disabled
	stopCh        chan struct{}
	shuttingDown  atomic.Bool
	closed        atomic.Bool
	// Shared subscriptions (MQTT 5.0)
	sharedSubs *SharedSubscriptionManager
	// Maximum QoS level supported by this broker (0, 1, or 2)
	maxQoS byte
	// Offline queue settings
	maxOfflineQueueSize int
	offlineQueueEvict   bool
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
func NewBroker(store storage.Store, cl cluster.Cluster, logger *slog.Logger, stats *Stats, webhooks Notifier, metrics *otel.Metrics, tracer trace.Tracer, sessionCfg config.SessionConfig) *Broker {
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
		sessionsMap:         session.NewShardedCache(),
		router:              r,
		messages:            store.Messages(),
		sessions:            store.Sessions(),
		subscriptions:       store.Subscriptions(),
		retained:            store.Retained(),
		wills:               store.Wills(),
		cluster:             cl,
		logger:              logger,
		stats:               stats,
		webhooks:            webhooks,
		metrics:             metrics,
		tracer:              tracer,
		stopCh:              make(chan struct{}),
		sharedSubs:          NewSharedSubscriptionManager(),
		maxQoS:              2, // Default to QoS 2 (highest)
		maxOfflineQueueSize: sessionCfg.MaxOfflineQueueSize,
		offlineQueueEvict:   sessionCfg.OfflineQueuePolicy == "evict",
	}

	b.wg.Add(2)
	go b.expiryLoop()
	go b.statsLoop()

	return b
}

// SetQueueManager sets the queue manager for the broker.
// This should be called before the broker starts accepting connections.
func (b *Broker) SetQueueManager(qm QueueManager) error {
	b.globalMu.Lock()
	defer b.globalMu.Unlock()

	b.queueManager = qm

	// Start queue manager
	if qm != nil {
		return qm.Start(context.Background())
	}

	return nil
}

// GetQueueManager returns the queue manager.
func (b *Broker) GetQueueManager() QueueManager {
	b.globalMu.Lock()
	defer b.globalMu.Unlock()
	return b.queueManager
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
func (b *Broker) SetAuthEngine(auth *broker.AuthEngine) {
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
