package middleware

import (
	"time"

	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/store"
)

var _ broker.Service = (*metricsMiddleware)(nil)

type metricsMiddleware struct {
	stats *broker.Stats
	svc   broker.Service
}

// NewMetrics creates metrics middleware that wraps a broker service.
func NewMetrics(svc broker.Service, stats *broker.Stats) broker.Service {
	return &metricsMiddleware{stats, svc}
}

// HandleConnection wraps the call with connection metrics.
func (mm *metricsMiddleware) HandleConnection(conn core.Connection) {
	mm.stats.IncrementConnections()
	defer func() {
		mm.stats.DecrementConnections()
	}()

	mm.svc.HandleConnection(conn)
}

// Subscribe wraps the call with subscription metrics.
func (mm *metricsMiddleware) Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error {
	err := mm.svc.Subscribe(clientID, filter, qos, opts)

	if err == nil {
		mm.stats.IncrementSubscriptions()
	}

	return err
}

// Unsubscribe wraps the call with subscription metrics.
func (mm *metricsMiddleware) Unsubscribe(clientID string, filter string) error {
	err := mm.svc.Unsubscribe(clientID, filter)

	if err == nil {
		mm.stats.DecrementSubscriptions()
	}

	return err
}

// Match returns all subscriptions matching a topic.
func (mm *metricsMiddleware) Match(topic string) ([]*store.Subscription, error) {
	return mm.svc.Match(topic)
}

// Distribute wraps the call with message distribution metrics.
func (mm *metricsMiddleware) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error {
	defer func(begin time.Time) {
		mm.stats.IncrementPublishSent()
		mm.stats.AddBytesSent(uint64(len(payload)))
	}(time.Now())

	return mm.svc.Distribute(topic, payload, qos, retain, props)
}

// Stats returns the broker statistics.
func (mm *metricsMiddleware) Stats() *broker.Stats {
	return mm.stats
}

// Close shuts down the broker.
func (mm *metricsMiddleware) Close() error {
	return mm.svc.Close()
}
