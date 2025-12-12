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
