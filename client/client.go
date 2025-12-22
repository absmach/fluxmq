// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"fmt"
	"log/slog"
	"net"

	packets "github.com/absmach/mqtt/core/packets"
	v3 "github.com/absmach/mqtt/core/packets/v3"
	v5 "github.com/absmach/mqtt/core/packets/v5"
	"github.com/absmach/mqtt/storage"
)

// Options defines client configuration.
type Options struct {
	ClientID   string
	BrokerAddr string
	Version    int // 3 or 5, defaults to 5
	Will       *storage.WillMessage
}

// Client implements an MQTT client.
type Client struct {
	opts       Options
	conn       net.Conn
	logger     *slog.Logger
	onMessage  func(topic string, payload []byte)
	nextPacketID uint16
}

// NewClient creates a new client.
func NewClient(opts Options) *Client {
	if opts.Version == 0 {
		opts.Version = 5
	}
	return &Client{
		opts:         opts,
		logger:       slog.Default(),
		nextPacketID: 1,
	}
}

// getNextPacketID returns the next packet ID and increments the counter.
func (c *Client) getNextPacketID() uint16 {
	id := c.nextPacketID
	c.nextPacketID++
	if c.nextPacketID == 0 {
		c.nextPacketID = 1
	}
	return id
}

// SetMessageHandler sets the callback for received messages.
func (c *Client) SetMessageHandler(handler func(topic string, payload []byte)) {
	c.onMessage = handler
}

// Close closes the connection (ungraceful disconnect).
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Connect establishes connection to the broker.
func (c *Client) Connect() error {
	conn, err := net.Dial("tcp", c.opts.BrokerAddr)
	if err != nil {
		return err
	}
	c.conn = conn

	// Send CONNECT
	if c.opts.Version == 5 {
		pkt := &v5.Connect{
			FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
			ClientID:        c.opts.ClientID,
			KeepAlive:       60,
			ProtocolName:    "MQTT",
			ProtocolVersion: 5,
		}
		if c.opts.Will != nil {
			pkt.WillFlag = true
			pkt.WillQoS = c.opts.Will.QoS
			pkt.WillRetain = c.opts.Will.Retain
			pkt.WillTopic = c.opts.Will.Topic
			pkt.WillPayload = c.opts.Will.Payload
			// V5 properties not fully supported in helper yet
		}
		if err := pkt.Pack(conn); err != nil {
			conn.Close()
			return err
		}
	} else {
		pkt := &v3.Connect{
			FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
			ClientID:        c.opts.ClientID,
			KeepAlive:       60,
			ProtocolName:    "MQTT",
			ProtocolVersion: 4,
		}
		if c.opts.Will != nil {
			pkt.WillFlag = true
			pkt.WillQoS = c.opts.Will.QoS
			pkt.WillRetain = c.opts.Will.Retain
			pkt.WillTopic = c.opts.Will.Topic
			pkt.WillMessage = c.opts.Will.Payload
		}
		if err := pkt.Pack(conn); err != nil {
			conn.Close()
			return err
		}
	}

	// Read CONNACK
	if c.opts.Version == 5 {
		pkt, _, _, err := v5.ReadPacket(conn)
		if err != nil {
			conn.Close()
			return err
		}
		if pkt.Type() != packets.ConnAckType {
			conn.Close()
			return fmt.Errorf("expected CONNACK, got %d", pkt.Type())
		}
		// Check ReasonCode...
	} else {
		pkt, err := v3.ReadPacket(conn)
		if err != nil {
			conn.Close()
			return err
		}
		if pkt.Type() != packets.ConnAckType {
			conn.Close()
			return fmt.Errorf("expected CONNACK, got %d", pkt.Type())
		}
	}

	// Start reading loop
	go c.readLoop()
	return nil
}

func (c *Client) readLoop() {
	for {
		var pkt packets.ControlPacket
		var err error

		if c.opts.Version == 5 {
			pkt, _, _, err = v5.ReadPacket(c.conn)
		} else {
			pkt, err = v3.ReadPacket(c.conn)
		}

		if err != nil {
			// Log and exit (reconnect logic omitted for brevity)
			c.logger.Debug("Client read error",
				slog.String("client_id", c.opts.ClientID),
				slog.String("error", err.Error()))
			return
		}

		switch pkt.Type() {
		case packets.PublishType:
			c.handlePublish(pkt)

		case packets.PubAckType:
			// QoS 1 flow complete
			c.logger.Debug("Received PUBACK", slog.String("client_id", c.opts.ClientID))

		case packets.PubRecType:
			// QoS 2: Received PUBREC, send PUBREL
			c.handlePubRec(pkt)

		case packets.PubRelType:
			// QoS 2: Received PUBREL, send PUBCOMP
			c.handlePubRel(pkt)

		case packets.PubCompType:
			// QoS 2 flow complete
			c.logger.Debug("Received PUBCOMP", slog.String("client_id", c.opts.ClientID))

		case packets.SubAckType:
			// Subscription acknowledged
			c.logger.Debug("Received SUBACK", slog.String("client_id", c.opts.ClientID))
		}
	}
}

func (c *Client) handlePublish(pkt packets.ControlPacket) {
	var topic string
	var payload []byte
	var qos byte
	var packetID uint16

	if c.opts.Version == 5 {
		p := pkt.(*v5.Publish)
		topic = p.TopicName
		payload = p.Payload
		qos = p.QoS
		packetID = p.ID
	} else {
		p := pkt.(*v3.Publish)
		topic = p.TopicName
		payload = p.Payload
		qos = p.QoS
		packetID = p.ID
	}

	// Deliver message to handler
	if c.onMessage != nil {
		c.onMessage(topic, payload)
	}

	// Send acknowledgment based on QoS
	if qos == 1 {
		// Send PUBACK
		if c.opts.Version == 5 {
			ack := &v5.PubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
				ID:          packetID,
			}
			ack.Pack(c.conn)
		} else {
			ack := &v3.PubAck{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
				ID:          packetID,
			}
			ack.Pack(c.conn)
		}
	} else if qos == 2 {
		// Send PUBREC
		if c.opts.Version == 5 {
			rec := &v5.PubRec{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
				ID:          packetID,
			}
			rec.Pack(c.conn)
		} else {
			rec := &v3.PubRec{
				FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
				ID:          packetID,
			}
			rec.Pack(c.conn)
		}
	}
}

func (c *Client) handlePubRec(pkt packets.ControlPacket) {
	var packetID uint16

	if c.opts.Version == 5 {
		p := pkt.(*v5.PubRec)
		packetID = p.ID
	} else {
		p := pkt.(*v3.PubRec)
		packetID = p.ID
	}

	// Send PUBREL
	if c.opts.Version == 5 {
		rel := &v5.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType},
			ID:          packetID,
		}
		rel.Pack(c.conn)
	} else {
		rel := &v3.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType},
			ID:          packetID,
		}
		rel.Pack(c.conn)
	}
}

func (c *Client) handlePubRel(pkt packets.ControlPacket) {
	var packetID uint16

	if c.opts.Version == 5 {
		p := pkt.(*v5.PubRel)
		packetID = p.ID
	} else {
		p := pkt.(*v3.PubRel)
		packetID = p.ID
	}

	// Send PUBCOMP
	if c.opts.Version == 5 {
		comp := &v5.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
			ID:          packetID,
		}
		comp.Pack(c.conn)
	} else {
		comp := &v3.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
			ID:          packetID,
		}
		comp.Pack(c.conn)
	}
}

// Publish sends a text message.
func (c *Client) Publish(topic string, payload []byte, qos byte, retain bool) error {
	var packetID uint16
	if qos > 0 {
		packetID = c.getNextPacketID()
	}

	if c.opts.Version == 5 {
		pkt := &v5.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        qos,
				Retain:     retain,
			},
			TopicName: topic,
			Payload:   payload,
			ID:        packetID,
		}
		return pkt.Pack(c.conn)
	} else {
		pkt := &v3.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        qos,
				Retain:     retain,
			},
			TopicName: topic,
			Payload:   payload,
			ID:        packetID,
		}
		return pkt.Pack(c.conn)
	}
}

// Subscribe subscribes to a topic.
func (c *Client) Subscribe(topic string) error {
	packetID := c.getNextPacketID()

	if c.opts.Version == 5 {
		pkt := &v5.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType},
			ID:          packetID,
			Opts: []v5.SubOption{
				{Topic: topic, MaxQoS: 2},
			},
		}
		return pkt.Pack(c.conn)
	} else {
		pkt := &v3.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType},
			ID:          packetID,
			Topics: []v3.Topic{
				{Name: topic, QoS: 2},
			},
		}
		return pkt.Pack(c.conn)
	}
}
