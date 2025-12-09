package broker

import (
	"context"
	"net"

	packets "github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/store"
)

// Frontend abstracts away the underlying transport protocol.
// It is responsible for accepting connections and passing them to the broker core.
type Frontend interface {
	// Serve starts the frontend loop, accepting connections and handling them.
	// It should block until Close is called or an error occurs.
	Serve(handler ConnectionHandler) error

	// Close stops the frontend and closes any listeners.
	Close() error

	// Addr returns the listener's network address.
	Addr() net.Addr
}

// ConnectionHandler is the interface for stateful, connection-oriented protocols (TCP, WebSocket).
type ConnectionHandler interface {
	// HandleConnection handles a new connection.
	// It blocks until the connection is closed.
	HandleConnection(conn Connection)
}

// OperationHandler is the interface for stateless, request-response protocols (HTTP, basic CoAP).
type OperationHandler interface {
	// Publish injects a message directly into the broker.
	Publish(ctx context.Context, clientID string, topic string, payload []byte, qos byte, retain bool) error

	// SubscribeToTopic allows a stateless client to listen for messages.
	SubscribeToTopic(ctx context.Context, clientID string, topicFilter string) (<-chan *store.Message, error)
}

// Connection abstracts a single client connection.
// It allows reading/writing MQTT packets regardless of the transport (TCP, WS, etc).
type Connection interface {
	// ReadPacket reads the next MQTT packet from the connection.
	// It returns the packet or an error.
	ReadPacket() (packets.ControlPacket, error)

	// WritePacket writes an MQTT packet to the connection.
	WritePacket(p packets.ControlPacket) error

	// Close terminates the connection.
	Close() error

	// RemoteAddr returns the address of the connected client.
	RemoteAddr() net.Addr
}

// Session represents the state of a connected client.
type Session interface {
	// ID returns the ClientID.
	ID() string

	// Close terminates the session and the underlying connection.
	Close() error
}
