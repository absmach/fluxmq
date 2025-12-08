package session

import (
	"fmt"
	"io"
	"net"
	"sync"

	packets "github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
)

// BrokerInterface defines the methods Session needs from the Broker
type BrokerInterface interface {
	Distribute(topic string, payload []byte)
	Subscribe(topic string, sessionID string, qos byte)
}

// Connection interface duplicated here or imported?
// Ideally we import duplicate interface or extract it to a `common` package.
// Or we just accept io.ReadWriteCloser + PacketReader capability.
// Let's redefine minimal interface or use `broker.Connection` if we extract transport?
// User said "separate session package".
// Let's assume for now we use a simple interface local here and map it.

type Connection interface {
	ReadPacket() (packets.ControlPacket, error)
	WritePacket(p packets.ControlPacket) error
	Close() error
	RemoteAddr() net.Addr
}

// Session represents an active client session.
type Session struct {
	Broker  BrokerInterface
	Conn    Connection
	ID      string
	Version int

	// Outbox?
	mu sync.Mutex
}

func New(broker BrokerInterface, conn Connection, id string, version int) *Session {
	return &Session{
		Broker:  broker,
		Conn:    conn,
		ID:      id,
		Version: version,
	}
}

func (s *Session) Close() error {
	return s.Conn.Close()
}

func (s *Session) Start() {
	defer s.Close()

	for {
		pkt, err := s.Conn.ReadPacket()
		if err != nil {
			if err != io.EOF {
				// Log error?
			}
			return
		}

		if err := s.handlePacket(pkt); err != nil {
			return
		}
	}
}

func (s *Session) handlePacket(pkt packets.ControlPacket) error {
	switch pkt.Type() {
	case packets.PublishType:
		return s.handlePublish(pkt)
	case packets.SubscribeType:
		return s.handleSubscribe(pkt)
	case packets.PingReqType:
		return s.handlePingReq()
	case packets.DisconnectType:
		return io.EOF // Clean exit
	default:
		return fmt.Errorf("unhandled packet type: %d", pkt.Type())
	}
}

func (s *Session) handlePublish(pkt packets.ControlPacket) error {
	var topic string
	var payload []byte

	if s.Version == 5 {
		p := pkt.(*v5.Publish)
		topic = p.TopicName
		payload = p.Payload
	} else {
		p := pkt.(*v3.Publish)
		topic = p.TopicName
		payload = p.Payload
	}

	s.Broker.Distribute(topic, payload)
	return nil
}

func (s *Session) handleSubscribe(pkt packets.ControlPacket) error {
	var subs []struct {
		Topic string
		QoS   byte
	}
	var packetID uint16

	if s.Version == 5 {
		p := pkt.(*v5.Subscribe)
		packetID = p.ID
		for _, opt := range p.Opts {
			subs = append(subs, struct {
				Topic string
				QoS   byte
			}{opt.Topic, opt.MaxQoS})
			s.Broker.Subscribe(opt.Topic, s.ID, opt.MaxQoS)
		}
	} else {
		p := pkt.(*v3.Subscribe)
		packetID = p.ID
		for _, t := range p.Topics {
			subs = append(subs, struct {
				Topic string
				QoS   byte
			}{t.Name, t.QoS})
			s.Broker.Subscribe(t.Name, s.ID, t.QoS)
		}
	}

	return s.sendSubAck(packetID, len(subs))
}

func (s *Session) sendSubAck(packetID uint16, count int) error {
	var ack packets.ControlPacket
	if s.Version == 5 {
		sa := &v5.SubAck{
			ID: packetID,
		}
		codes := make([]byte, count)
		for i := 0; i < count; i++ {
			codes[i] = 0
		}
		sa.ReasonCodes = &codes
		sa.FixedHeader.PacketType = packets.SubAckType
		ack = sa
	} else {
		sa := &v3.SubAck{
			ID:          packetID,
			ReturnCodes: make([]byte, count),
		}
		for i := 0; i < count; i++ {
			sa.ReturnCodes[i] = 0 // Success
		}
		sa.FixedHeader.PacketType = packets.SubAckType
		ack = sa
	}
	return s.Conn.WritePacket(ack)
}

func (s *Session) handlePingReq() error {
	var resp packets.ControlPacket
	if s.Version == 5 {
		resp = &v5.PingResp{FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType}}
	} else {
		resp = &v3.PingResp{FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType}}
	}
	return s.Conn.WritePacket(resp)
}

// Deliver sends a message to this session.
func (s *Session) Deliver(topic string, payload []byte) {
	var pkt packets.ControlPacket
	if s.Version == 5 {
		pkt = &v5.Publish{
			FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
			TopicName:   topic,
			Payload:     payload,
		}
	} else {
		pkt = &v3.Publish{
			FixedHeader: packets.FixedHeader{PacketType: packets.PublishType},
			TopicName:   topic,
			Payload:     payload,
		}
	}
	_ = s.Conn.WritePacket(pkt)
}
