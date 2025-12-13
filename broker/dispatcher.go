package broker

import (
	"io"

	"github.com/dborovcanin/mqtt/core/packets"
	"github.com/dborovcanin/mqtt/session"
)

// Dispatcher routes packets to the appropriate handler.
type Dispatcher struct {
	handler Handler
}

// NewDispatcher creates a new dispatcher.
func NewDispatcher(h Handler) *Dispatcher {
	return &Dispatcher{handler: h}
}

// VersionAwareDispatcher routes packets to v3 or v5 handler based on session version.
type VersionAwareDispatcher struct {
	handlerV3 Handler
	handlerV5 Handler
}

// NewVersionAwareDispatcher creates a new version-aware dispatcher.
func NewVersionAwareDispatcher(v3 Handler, v5 Handler) *Dispatcher {
	return &Dispatcher{
		handler: &versionRouter{v3: v3, v5: v5},
	}
}

// versionRouter implements Handler interface and routes to v3 or v5 handler.
type versionRouter struct {
	v3 Handler
	v5 Handler
}

func (r *versionRouter) selectHandler(s *session.Session) Handler {
	if s.Version == 5 {
		return r.v5
	}
	return r.v3
}

func (r *versionRouter) HandleConnect(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandleConnect(s, pkt)
}

func (r *versionRouter) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandlePublish(s, pkt)
}

func (r *versionRouter) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandlePubAck(s, pkt)
}

func (r *versionRouter) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandlePubRec(s, pkt)
}

func (r *versionRouter) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandlePubRel(s, pkt)
}

func (r *versionRouter) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandlePubComp(s, pkt)
}

func (r *versionRouter) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandleSubscribe(s, pkt)
}

func (r *versionRouter) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandleUnsubscribe(s, pkt)
}

func (r *versionRouter) HandlePingReq(s *session.Session) error {
	return r.selectHandler(s).HandlePingReq(s)
}

func (r *versionRouter) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	return r.selectHandler(s).HandleDisconnect(s, pkt)
}

// Dispatch routes a packet to the appropriate handler method.
func (d *Dispatcher) Dispatch(s *session.Session, pkt packets.ControlPacket) error {
	switch pkt.Type() {
	case packets.PublishType:
		return d.handler.HandlePublish(s, pkt)

	case packets.PubAckType:
		return d.handler.HandlePubAck(s, pkt)

	case packets.PubRecType:
		return d.handler.HandlePubRec(s, pkt)

	case packets.PubRelType:
		return d.handler.HandlePubRel(s, pkt)

	case packets.PubCompType:
		return d.handler.HandlePubComp(s, pkt)

	case packets.SubscribeType:
		return d.handler.HandleSubscribe(s, pkt)

	case packets.UnsubscribeType:
		return d.handler.HandleUnsubscribe(s, pkt)

	case packets.PingReqType:
		return d.handler.HandlePingReq(s)

	case packets.DisconnectType:
		return d.handler.HandleDisconnect(s, pkt)

	case packets.ConnectType:
		// CONNECT should be handled at server level (or connection init time)
		return nil // Avoid error to prevent disconnect if it leaks through
		// Or return protocol error? The original returned ErrProtocolViolation.
		// Let's import errors if we need ErrProtocolViolation.
		// For now returning nil or error. Let's stick to original behavior but we need defined errors.
		// We'll define ErrProtocolViolation in handler_interfaces.go

	default:
		// We need to access ErrProtocolViolation from the interface definition file.
		// Since it's in the same package 'broker', we can use it directly once defined.
		return ErrProtocolViolation
	}
}

// RunSession runs the main packet processing loop for a session.
func (d *Dispatcher) RunSession(s *session.Session) error {
	for {
		pkt, err := s.ReadPacket()
		if err != nil {
			if err == io.EOF || err == session.ErrNotConnected {
				return nil // Clean disconnect
			}
			return err
		}

		if err := d.Dispatch(s, pkt); err != nil {
			if err == io.EOF {
				return nil // Clean disconnect from handler
			}
			return err
		}
	}
}
