// Package handlers provides MQTT packet handlers.
package handlers

import (
	"errors"

	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
)

// Common handler errors.
var (
	ErrProtocolViolation = errors.New("protocol violation")
	ErrNotAuthorized     = errors.New("not authorized")
	ErrBadUserOrPassword = errors.New("bad username or password")
	ErrClientIDRejected  = errors.New("client identifier rejected")
	ErrServerUnavailable = errors.New("server unavailable")
	ErrTopicInvalid      = errors.New("topic name invalid")
	ErrPacketTooLarge    = errors.New("packet too large")
	ErrQuotaExceeded     = errors.New("quota exceeded")
)

// Handler is the interface for packet handlers.
type Handler interface {
	// HandleConnect handles CONNECT packets.
	HandleConnect(s *session.Session, pkt packets.ControlPacket) error

	// HandlePublish handles PUBLISH packets.
	HandlePublish(s *session.Session, pkt packets.ControlPacket) error

	// HandlePubAck handles PUBACK packets (QoS 1 acknowledgment).
	HandlePubAck(s *session.Session, pkt packets.ControlPacket) error

	// HandlePubRec handles PUBREC packets (QoS 2 step 1).
	HandlePubRec(s *session.Session, pkt packets.ControlPacket) error

	// HandlePubRel handles PUBREL packets (QoS 2 step 2).
	HandlePubRel(s *session.Session, pkt packets.ControlPacket) error

	// HandlePubComp handles PUBCOMP packets (QoS 2 step 3).
	HandlePubComp(s *session.Session, pkt packets.ControlPacket) error

	// HandleSubscribe handles SUBSCRIBE packets.
	HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error

	// HandleUnsubscribe handles UNSUBSCRIBE packets.
	HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error

	// HandlePingReq handles PINGREQ packets.
	HandlePingReq(s *session.Session) error

	// HandleDisconnect handles DISCONNECT packets.
	HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error
}

// Router is the message routing interface.
type Router interface {
	// Subscribe adds a subscription.
	Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error

	// Unsubscribe removes a subscription.
	Unsubscribe(clientID string, filter string) error

	// Match returns all subscriptions matching a topic.
	Match(topic string) ([]*store.Subscription, error)
}

// Authenticator validates client credentials.
type Authenticator interface {
	// Authenticate validates username and password.
	// Returns true if authenticated.
	Authenticate(clientID, username, password string) (bool, error)
}

// Authorizer checks topic permissions.
type Authorizer interface {
	// CanPublish checks if client can publish to topic.
	CanPublish(clientID string, topic string) bool

	// CanSubscribe checks if client can subscribe to filter.
	CanSubscribe(clientID string, filter string) bool
}

// Publisher distributes messages to subscribers.
type Publisher interface {
	// Distribute distributes a message to all matching subscribers.
	Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error
}

// RetainedStore provides access to retained messages.
type RetainedStore interface {
	// Set stores a retained message.
	Set(topic string, msg *store.Message) error

	// Get retrieves a retained message.
	Get(topic string) (*store.Message, error)

	// Match returns retained messages matching a filter.
	Match(filter string) ([]*store.Message, error)
}
