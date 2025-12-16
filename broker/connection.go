package broker

import (
	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	v3 "github.com/absmach/mqtt/core/packets/v3"
	v5 "github.com/absmach/mqtt/core/packets/v5"
)

// HandleConnection handles a new incoming connection from the TCP server.
// It detects the MQTT protocol version and creates the appropriate handler.
func HandleConnection(broker *Broker, conn core.Connection) {
	pkt, err := conn.ReadPacket()
	if err != nil {
		broker.stats.IncrementPacketErrors()
		conn.Close()
		return
	}

	if pkt.Type() != packets.ConnectType {
		broker.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	p3, ok := pkt.(*v3.Connect)
	if ok {
		if p3.ProtocolVersion != 3 && p3.ProtocolVersion != 4 {
			broker.stats.IncrementProtocolErrors()
			sendV3ConnAck(conn, false, 0x01)
			conn.Close()
			return
		}
		handler := NewV3Handler(broker)
		handler.HandleConnect(conn, p3)
		return
	}

	p5, ok := pkt.(*v5.Connect)
	if ok {
		if p5.ProtocolVersion != 5 {
			broker.stats.IncrementProtocolErrors()
			sendV5ConnAck(conn, false, 0x84, nil)
			conn.Close()
			return
		}
		handler := NewV5Handler(broker)
		handler.HandleConnect(conn, p5)
		return
	}

	broker.stats.IncrementProtocolErrors()
	conn.Close()
}

func sendV3ConnAck(conn core.Connection, sessionPresent bool, returnCode byte) error {
	ack := &v3.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReturnCode:     returnCode,
	}
	return conn.WritePacket(ack)
}

func sendV5ConnAck(conn core.Connection, sessionPresent bool, reasonCode byte, props *v5.ConnAckProperties) error {
	if props == nil {
		props = &v5.ConnAckProperties{}
	}
	ack := &v5.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReasonCode:     reasonCode,
		Properties:     props,
	}
	return conn.WritePacket(ack)
}
