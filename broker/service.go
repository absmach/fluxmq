package broker

import (
	"context"

	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/store"
)

// Service defines the broker's core business operations.
// This interface enables middleware wrapping for cross-cutting concerns
// like logging, metrics, and tracing.
type Service interface {
	// HandleConnection processes an incoming MQTT connection.
	HandleConnection(conn core.Connection)

	// Subscribe adds a subscription for a client.
	Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error

	// Unsubscribe removes a subscription for a client.
	Unsubscribe(clientID string, filter string) error

	// Match returns all subscriptions matching a topic.
	Match(topic string) ([]*store.Subscription, error)

	// Distribute distributes a message to all matching subscribers.
	Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error

	// Publish publishes a message from a specific client.
	Publish(ctx context.Context, clientID string, topic string, payload []byte, qos byte, retain bool) error

	// Stats returns the broker statistics.
	Stats() *Stats

	// SetAuth sets the authentication and authorization engine.
	SetAuth(auth *AuthEngine)

	// Close shuts down the broker.
	Close() error
}

// Ensure Broker implements Service
var _ Service = (*Broker)(nil)
