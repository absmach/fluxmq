package client

import (
	"fmt"
	"log/slog"
	"net"
	"sync"

	packets "github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
	"github.com/dborovcanin/mqtt/store"
)

// Options defines client configuration.
type Options struct {
	ClientID   string
	BrokerAddr string
	Version    int // 3 or 5, defaults to 5
	Will       *store.WillMessage
}

// Client implements an MQTT client.
type Client struct {
	opts   Options
	conn   net.Conn
	logger *slog.Logger

	// Message handling
	// msgCh chan *packets.Publish // Removed
	// Let's use callback for now for simplicity or expose a Consume method.
	// For this demonstration, we'll just log or use a callback interface.
	// Actually, user requested "implement Client". Standard is callbacks or channel.
	onMessage func(topic string, payload []byte)

	// Internal
	mu sync.Mutex
}

// NewClient creates a new client.
func NewClient(opts Options) *Client {
	if opts.Version == 0 {
		opts.Version = 5
	}
	return &Client{
		opts:   opts,
		logger: slog.Default(),
	}
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
	// We should probably use our packets.ReadPacket helper, but it's in broker/tcp loop logic.
	// We can use v5.ReadPacket or v3.ReadPacket directly.
	// Wait, the client needs to listen for packets in a loop AFTER connect.
	// CONNECT/CONNACK is synchronous.

	// Basic synchronous Read
	// We need to use version specific reader?
	// Let's assume server replies with correct version.

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
		pkt, _, _, err := v3.ReadPacket(conn)
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
			pkt, _, _, err = v3.ReadPacket(c.conn)
		}

		if err != nil {
			// Log and exit (reconnect logic omitted for brevity)
			c.logger.Debug("Client read error",
				slog.String("client_id", c.opts.ClientID),
				slog.String("error", err.Error()))
			return
		}

		// Handle PUBLISH
		if pkt.Type() == packets.PublishType {
			var topic string
			var payload []byte

			if c.opts.Version == 5 {
				p := pkt.(*v5.Publish)
				topic = p.TopicName
				payload = p.Payload
			} else {
				p := pkt.(*v3.Publish)
				topic = p.TopicName
				payload = p.Payload
			}

			if c.onMessage != nil {
				c.onMessage(topic, payload)
			}
		}

		// Handle SUBACK (log)
	}
}

// Publish sends a text message.
func (c *Client) Publish(topic string, payload []byte, qos byte, retain bool) error {
	// TODO: Qos 1/2 need packet ID tracking
	if c.opts.Version == 5 {
		pkt := &v5.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        qos,
				Retain:     retain,
			},
			TopicName: topic,
			Payload:   payload,
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
		}
		return pkt.Pack(c.conn)
	}
}

// Subscribe subscribes to a topic.
func (c *Client) Subscribe(topic string) error {
	// TODO: Packet ID
	if c.opts.Version == 5 {
		pkt := &v5.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType},
			ID:          10, // Random ID
			Opts: []v5.SubOption{
				{Topic: topic, MaxQoS: 0},
			},
		}
		return pkt.Pack(c.conn)
	} else {
		pkt := &v3.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType},
			ID:          10,
			Topics: []v3.Topic{
				{Name: topic, QoS: 0},
			},
		}
		return pkt.Pack(c.conn)
	}
}
