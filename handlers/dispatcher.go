package handlers

import (
	"io"

	"github.com/dborovcanin/mqtt/packets"
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
func (d *Dispatcher) Dispatch(sess *session.Session, pkt packets.ControlPacket) error {
	switch pkt.Type() {
	case packets.PublishType:
		return d.handler.HandlePublish(sess, pkt)

	case packets.PubAckType:
		return d.handler.HandlePubAck(sess, pkt)

	case packets.PubRecType:
		return d.handler.HandlePubRec(sess, pkt)

	case packets.PubRelType:
		return d.handler.HandlePubRel(sess, pkt)

	case packets.PubCompType:
		return d.handler.HandlePubComp(sess, pkt)

	case packets.SubscribeType:
		return d.handler.HandleSubscribe(sess, pkt)

	case packets.UnsubscribeType:
		return d.handler.HandleUnsubscribe(sess, pkt)

	case packets.PingReqType:
		return d.handler.HandlePingReq(sess)

	case packets.DisconnectType:
		return d.handler.HandleDisconnect(sess, pkt)

	case packets.ConnectType:
		// CONNECT should be handled at server level
		return ErrProtocolViolation

	default:
		return ErrProtocolViolation
	}
}

// RunSession runs the main packet processing loop for a session.
func (d *Dispatcher) RunSession(sess *session.Session) error {
	for {
		pkt, err := sess.ReadPacket()
		if err != nil {
			if err == io.EOF || err == session.ErrNotConnected {
				return nil // Clean disconnect
			}
			return err
		}

		if err := d.Dispatch(sess, pkt); err != nil {
			if err == io.EOF {
				return nil // Clean disconnect from handler
			}
			return err
		}
	}
}
