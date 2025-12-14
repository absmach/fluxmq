package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/store"
)

var _ broker.Service = (*loggingMiddleware)(nil)

type loggingMiddleware struct {
	logger *slog.Logger
	svc    broker.Service
}

// NewLogging creates logging middleware that wraps a broker service.
func NewLogging(svc broker.Service, logger *slog.Logger) broker.Service {
	return &loggingMiddleware{logger, svc}
}

// HandleConnection wraps the call with logging.
func (lm *loggingMiddleware) HandleConnection(conn core.Connection) {
	defer func(begin time.Time) {
		lm.logger.Debug("Connection handled",
			slog.String("remote_addr", conn.RemoteAddr().String()),
			slog.String("duration", time.Since(begin).String()))
	}(time.Now())

	lm.svc.HandleConnection(conn)
}

// Subscribe wraps the call with logging.
func (lm *loggingMiddleware) Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) (err error) {
	defer func(begin time.Time) {
		args := []any{
			slog.String("duration", time.Since(begin).String()),
			slog.String("client_id", clientID),
			slog.String("filter", filter),
			slog.Int("qos", int(qos)),
		}
		if err != nil {
			args = append(args, slog.String("error", err.Error()))
			lm.logger.Warn("Subscribe failed", args...)
			return
		}
		lm.logger.Info("Client subscribed", args...)
	}(time.Now())

	return lm.svc.Subscribe(clientID, filter, qos, opts)
}

// Unsubscribe wraps the call with logging.
func (lm *loggingMiddleware) Unsubscribe(clientID string, filter string) (err error) {
	defer func(begin time.Time) {
		args := []any{
			slog.String("duration", time.Since(begin).String()),
			slog.String("client_id", clientID),
			slog.String("filter", filter),
		}
		if err != nil {
			args = append(args, slog.String("error", err.Error()))
			lm.logger.Warn("Unsubscribe failed", args...)
			return
		}
		lm.logger.Info("Client unsubscribed", args...)
	}(time.Now())

	return lm.svc.Unsubscribe(clientID, filter)
}

// Match wraps the call with logging (debug level only).
func (lm *loggingMiddleware) Match(topic string) ([]*store.Subscription, error) {
	return lm.svc.Match(topic)
}

// Distribute wraps the call with logging.
func (lm *loggingMiddleware) Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) (err error) {
	defer func(begin time.Time) {
		if err != nil {
			lm.logger.Warn("Message distribution failed",
				slog.String("topic", topic),
				slog.Int("qos", int(qos)),
				slog.Int("payload_size", len(payload)),
				slog.Bool("retain", retain),
				slog.String("error", err.Error()),
				slog.String("duration", time.Since(begin).String()))
		}
	}(time.Now())

	return lm.svc.Distribute(topic, payload, qos, retain, props)
}

// Publish wraps the call with logging.
func (lm *loggingMiddleware) Publish(ctx context.Context, clientID string, topic string, payload []byte, qos byte, retain bool) (err error) {
	defer func(begin time.Time) {
		args := []any{
			slog.String("duration", time.Since(begin).String()),
			slog.String("client_id", clientID),
			slog.String("topic", topic),
			slog.Int("qos", int(qos)),
			slog.Int("payload_size", len(payload)),
			slog.Bool("retain", retain),
		}
		if err != nil {
			args = append(args, slog.String("error", err.Error()))
			lm.logger.Warn("Publish failed", args...)
			return
		}
		lm.logger.Debug("Message published", args...)
	}(time.Now())

	return lm.svc.Publish(ctx, clientID, topic, payload, qos, retain)
}

// Stats returns the broker statistics.
func (lm *loggingMiddleware) Stats() *broker.Stats {
	return lm.svc.Stats()
}

// SetAuth sets the authentication and authorization engine.
func (lm *loggingMiddleware) SetAuth(auth *broker.AuthEngine) {
	lm.logger.Info("Authentication engine configured")
	lm.svc.SetAuth(auth)
}

// Close shuts down the broker.
func (lm *loggingMiddleware) Close() error {
	lm.logger.Info("Shutting down broker")
	err := lm.svc.Close()
	if err != nil {
		lm.logger.Error("Broker shutdown failed", slog.String("error", err.Error()))
	} else {
		lm.logger.Info("Broker shutdown complete")
	}
	return err
}
