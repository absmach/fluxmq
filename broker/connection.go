package broker

import (
	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/core/packets"
	v3 "github.com/dborovcanin/mqtt/core/packets/v3"
	v5 "github.com/dborovcanin/mqtt/core/packets/v5"
)

// ConnectionHandler handles incoming connections and dispatches to protocol handlers.
type ConnectionHandler struct {
	broker *Broker
}

// NewConnectionHandler creates a new connection handler.
func NewConnectionHandler(broker *Broker, auth *AuthEngine) *ConnectionHandler {
	if auth != nil {
		broker.SetAuth(auth)
	}
	return &ConnectionHandler{
		broker: broker,
	}
}

// HandleConnection handles a new incoming connection from the TCP server.
func (ch *ConnectionHandler) HandleConnection(conn core.Connection) {
	pkt, err := conn.ReadPacket()
	if err != nil {
		ch.broker.stats.IncrementPacketErrors()
		conn.Close()
		return
	}

	if pkt.Type() != packets.ConnectType {
		ch.broker.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	p3, ok := pkt.(*v3.Connect)
	if ok {
		if p3.ProtocolVersion != 3 && p3.ProtocolVersion != 4 {
			ch.broker.stats.IncrementProtocolErrors()
			ch.sendV3ConnAck(conn, false, 0x01)
			conn.Close()
			return
		}
		ch.broker.HandleConnect(conn, p3)
		return
	}

	p5, ok := pkt.(*v5.Connect)
	if ok {
		if p5.ProtocolVersion != 5 {
			ch.broker.stats.IncrementProtocolErrors()
			ch.sendV5ConnAck(conn, false, 0x84)
			conn.Close()
			return
		}
		ch.broker.HandleConnect(conn, p5)
		return
	}

	ch.broker.stats.IncrementProtocolErrors()
	conn.Close()
}

func (ch *ConnectionHandler) sendV3ConnAck(conn core.Connection, sessionPresent bool, returnCode byte) error {
	ack := &v3.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReturnCode:     returnCode,
	}
	return conn.WritePacket(ack)
}

func (ch *ConnectionHandler) sendV5ConnAck(conn core.Connection, sessionPresent bool, reasonCode byte) error {
	ack := &v5.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReasonCode:     reasonCode,
		Properties:     &v5.ConnAckProperties{},
	}
	return conn.WritePacket(ack)
}
