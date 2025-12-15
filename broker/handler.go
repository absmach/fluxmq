package broker

import (
	"errors"

	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
)

// Common handler errors.
var (
	ErrProtocolViolation = errors.New("protocol violation")
	ErrNotAuthorized     = errors.New("not authorized")
	ErrBadUserOrPassword = errors.New("bad username or password")
	ErrClientIDRejected  = errors.New("client identifier rejected")
	ErrClientIDRequired  = errors.New("client identifier required")
	ErrServerUnavailable = errors.New("server unavailable")
	ErrTopicInvalid      = errors.New("topic name invalid")
	ErrPacketTooLarge    = errors.New("packet too large")
	ErrQuotaExceeded     = errors.New("quota exceeded")
	ErrInvalidPacketType = errors.New("invalid packet type")
	ErrSessionNotFound   = errors.New("session not found")
)

// Handler is the interface for packet handlers (V3, V5, etc).
type Handler interface {
	// HandleConnect handles CONNECT packets.
	HandleConnect(conn core.Connection, pkt packets.ControlPacket) error

	// HandlePublish handles PUBLISH packets.
	HandlePublish(s *session.Session, pkt packets.ControlPacket) error

	// HandlePubAck handles PUBACK packets.
	HandlePubAck(s *session.Session, pkt packets.ControlPacket) error

	// HandlePubRec handles PUBREC packets.
	HandlePubRec(s *session.Session, pkt packets.ControlPacket) error

	// HandlePubRel handles PUBREL packets.
	HandlePubRel(s *session.Session, pkt packets.ControlPacket) error

	// HandlePubComp handles PUBCOMP packets.
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
	Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error
	Unsubscribe(clientID string, filter string) error
	Match(topic string) ([]*storage.Subscription, error)
}

// Authenticator validates client credentials.
type Authenticator interface {
	Authenticate(clientID, username, password string) (bool, error)
}

// Authorizer checks topic permissions.
type Authorizer interface {
	CanPublish(clientID string, topic string) bool
	CanSubscribe(clientID string, filter string) bool
}

// Publisher distributes messages to subscribers.
type Publisher interface {
	Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error
}

// RetainedStore provides access to retained messages.
type RetainedStore interface {
	Set(topic string, msg *storage.Message) error
	Get(topic string) (*storage.Message, error)
	Match(filter string) ([]*storage.Message, error)
}

// Service defines the broker service interface for middleware wrapping.
type Service interface {
	HandleConnection(conn core.Connection)
	Subscribe(clientID string, filter string, qos byte, opts storage.SubscribeOptions) error
	Unsubscribe(clientID string, filter string) error
	Match(topic string) ([]*storage.Subscription, error)
	Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error
	Close() error
}
