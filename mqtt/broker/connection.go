// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

// HandleConnection handles a new incoming connection from the TCP server.
// It detects the MQTT protocol version and creates the appropriate handler.
func HandleConnection(broker *Broker, conn core.Connection) {
	pkt, err := conn.ReadPacket()
	if err != nil {
		broker.telemetry.stats.IncrementPacketErrors()
		conn.Close()
		return
	}

	if pkt.Type() != packets.ConnectType {
		broker.telemetry.stats.IncrementProtocolErrors()
		conn.Close()
		return
	}

	p3, ok := pkt.(*v3.Connect)
	if ok {
		if p3.ProtocolVersion != 3 && p3.ProtocolVersion != 4 {
			broker.telemetry.stats.IncrementProtocolErrors()
			sendV3ConnAck(conn, false, v3.ConnAckUnacceptableProtocol) //nolint:errcheck // best-effort rejection reply before closing
			conn.Close()
			return
		}
		handler := NewV3Handler(broker)
		handler.HandleConnect(conn, p3) //nolint:errcheck // handler manages connection lifecycle
		return
	}

	p5, ok := pkt.(*v5.Connect)
	if ok {
		if p5.ProtocolVersion != 5 {
			broker.telemetry.stats.IncrementProtocolErrors()
			sendV5ConnAck(conn, false, v5.ConnAckUnsupportedProtocolVersion, nil) //nolint:errcheck // best-effort rejection reply before closing
			conn.Close()
			return
		}
		handler := NewV5Handler(broker)
		handler.HandleConnect(conn, p5) //nolint:errcheck // handler manages connection lifecycle
		return
	}

	broker.telemetry.stats.IncrementProtocolErrors()
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
