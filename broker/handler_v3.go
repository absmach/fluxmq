package broker

import (
	"io"
	"time"

	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/core/packets"
	v3 "github.com/dborovcanin/mqtt/core/packets/v3"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/messages"
)

// HandleV3Connect handles CONNECT packets and sets up the session.
func (b *Broker) HandleV3Connect(conn core.Connection, p *v3.Connect) error {
	clientID := p.ClientID
	cleanStart := p.CleanSession

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				b.stats.IncrementProtocolErrors()
				sendV3ConnAck(conn, false, 0x02)
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			b.stats.IncrementProtocolErrors()
			sendV3ConnAck(conn, false, 0x02)
			conn.Close()
			return ErrClientIDRequired
		}
	}

	if b.auth != nil {
		username := p.Username
		password := string(p.Password)

		authenticated, err := b.auth.Authenticate(clientID, username, password)
		if err != nil || !authenticated {
			b.stats.IncrementAuthErrors()
			sendV3ConnAck(conn, false, 0x04)
			conn.Close()
			return ErrNotAuthorized
		}
	}

	var willMsg *Message
	if p.WillFlag {
		willMsg = &Message{
			Topic:   p.WillTopic,
			Payload: p.WillMessage,
			QoS:     p.WillQoS,
			Retain:  p.WillRetain,
		}
	}

	opts := SessionOptions{
		CleanStart:     cleanStart,
		KeepAlive:      p.KeepAlive,
		ReceiveMaximum: 65535,
		WillMessage:    willMsg,
	}

	s, isNew, err := b.CreateSession(clientID, opts)
	if err != nil {
		b.stats.IncrementProtocolErrors()
		sendV3ConnAck(conn, false, 0x03)
		conn.Close()
		return err
	}

	s.Version = p.ProtocolVersion
	s.KeepAlive = time.Duration(p.KeepAlive) * time.Second

	if err := s.Connect(conn); err != nil {
		b.stats.IncrementProtocolErrors()
		conn.Close()
		return err
	}

	sessionPresent := !isNew && !cleanStart
	if err := sendV3ConnAck(conn, sessionPresent, 0x00); err != nil {
		s.Disconnect(false)
		return err
	}

	b.stats.IncrementConnections()

	b.deliverOfflineV3Messages(s)

	return b.runV3Session(s)
}

// runSession runs the main packet loop for a session.
func (b *Broker) runV3Session(s *session.Session) error {
	conn := s.Conn()
	if conn == nil {
		return nil
	}

	if s.KeepAlive > 0 {
		deadline := time.Now().Add(s.KeepAlive + s.KeepAlive/2)
		conn.SetReadDeadline(deadline)
	}

	for {
		pkt, err := s.ReadPacket()
		if err != nil {
			if err != io.EOF && err != session.ErrNotConnected {
				b.stats.IncrementPacketErrors()
			}
			b.stats.DecrementConnections()
			s.Disconnect(false)
			return err
		}

		if s.KeepAlive > 0 {
			deadline := time.Now().Add(s.KeepAlive + s.KeepAlive/2)
			conn.SetReadDeadline(deadline)
		}

		b.stats.IncrementMessagesReceived()

		if err := b.handleV3Packet(s, pkt); err != nil {
			if err == io.EOF {
				b.stats.DecrementConnections()
				return nil
			}
			b.stats.IncrementProtocolErrors()
			b.stats.DecrementConnections()
			s.Disconnect(false)
			return err
		}
	}
}

// handlePacket dispatches a packet to the appropriate handler.
func (b *Broker) handleV3Packet(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	switch p := pkt.(type) {
	case *v3.Publish:
		return b.handleV3Publish(s, p)
	case *v3.PubAck:
		return b.handleV3PubAck(s, p)
	case *v3.PubRec:
		return b.handleV3PubRec(s, p)
	case *v3.PubRel:
		return b.handleV3PubRel(s, p)
	case *v3.PubComp:
		return b.handleV3PubComp(s, p)
	case *v3.Subscribe:
		return b.handleV3Subscribe(s, p)
	case *v3.Unsubscribe:
		return b.handleV3Unsubscribe(s, p)
	case *v3.PingReq:
		return b.handleV3PingReq(s)
	case *v3.Disconnect:
		return b.handleV3Disconnect(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// handleV3Publish handles PUBLISH packets.
func (b *Broker) handleV3Publish(s *session.Session, p *v3.Publish) error {
	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID
	dup := p.FixedHeader.Dup

	if b.auth != nil && !b.auth.CanPublish(s.ID, topic) {
		b.stats.IncrementAuthzErrors()
		return ErrNotAuthorized
	}

	b.stats.IncrementPublishReceived()
	b.stats.AddBytesReceived(uint64(len(payload)))

	switch qos {
	case 0:
		msg := Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		return b.Publish(s, msg)

	case 1:
		msg := Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		if err := b.Publish(s, msg); err != nil {
			return err
		}
		return sendV3PubAck(s, packetID)

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return sendV3PubRec(s, packetID)
		}

		s.Inflight().MarkReceived(packetID)

		storeMsg := &store.Message{
			Topic:    topic,
			Payload:  payload,
			QoS:      2,
			Retain:   retain,
			PacketID: packetID,
		}
		if err := s.Inflight().Add(packetID, storeMsg, messages.Inbound); err != nil {
			return err
		}

		return sendV3PubRec(s, packetID)
	}

	return nil
}

// handleV3PubAck handles PUBACK packets (QoS 1 acknowledgment from client).
func (b *Broker) handleV3PubAck(s *session.Session, p *v3.PubAck) error {
	return b.AckMessage(s, p.ID)
}

// handleV3PubRec handles PUBREC packets (QoS 2 step 1 from client).
func (b *Broker) handleV3PubRec(s *session.Session, p *v3.PubRec) error {
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived)
	return sendV3PubRel(s, p.ID)
}

// handleV3PubRel handles PUBREL packets (QoS 2 step 2 from client).
func (b *Broker) handleV3PubRel(s *session.Session, p *v3.PubRel) error {
	packetID := p.ID

	inf, ok := s.Inflight().Get(packetID)
	if ok && inf.Message != nil {
		msg := Message{
			Topic:   inf.Message.Topic,
			Payload: inf.Message.Payload,
			QoS:     inf.Message.QoS,
			Retain:  inf.Message.Retain,
		}
		b.Publish(s, msg)
	}

	b.AckMessage(s, packetID)
	s.Inflight().ClearReceived(packetID)

	return sendV3PubComp(s, packetID)
}

// handleV3PubComp handles PUBCOMP packets (QoS 2 step 3 from client).
func (b *Broker) handleV3PubComp(s *session.Session, p *v3.PubComp) error {
	return b.AckMessage(s, p.ID)
}

// handleV3Subscribe handles SUBSCRIBE packets.
func (b *Broker) handleV3Subscribe(s *session.Session, p *v3.Subscribe) error {
	packetID := p.ID

	reasonCodes := make([]byte, len(p.Topics))
	for i, t := range p.Topics {
		if b.auth != nil && !b.auth.CanSubscribe(s.ID, t.Name) {
			b.stats.IncrementAuthzErrors()
			reasonCodes[i] = 0x80
			continue
		}

		opts := SubscriptionOptions{
			QoS: t.QoS,
		}

		if err := b.subscribeInternal(s, t.Name, opts); err != nil {
			reasonCodes[i] = 0x80
			continue
		}

		reasonCodes[i] = t.QoS
		b.stats.IncrementSubscriptions()

		retained, err := b.retained.Match(t.Name)
		if err == nil {
			for _, msg := range retained {
				deliverQoS := msg.QoS
				if t.QoS < deliverQoS {
					deliverQoS = t.QoS
				}
				deliverMsg := Message{
					Topic:   msg.Topic,
					Payload: msg.Payload,
					QoS:     deliverQoS,
					Retain:  true,
				}
				b.DeliverToSession(s, deliverMsg)
			}
		}
	}

	return sendV3SubAck(s, packetID, reasonCodes)
}

// handleV3Unsubscribe handles UNSUBSCRIBE packets.
func (b *Broker) handleV3Unsubscribe(s *session.Session, p *v3.Unsubscribe) error {
	for _, filter := range p.Topics {
		b.unsubscribeInternal(s, filter)
		b.stats.DecrementSubscriptions()
	}

	return sendV3UnsubAck(s, p.ID)
}

// handleV3PingReq handles PINGREQ packets.
func (b *Broker) handleV3PingReq(s *session.Session) error {
	return sendV3PingResp(s)
}

// handleV3Disconnect handles DISCONNECT packets.
func (b *Broker) handleV3Disconnect(s *session.Session, p *v3.Disconnect) error {
	s.Disconnect(true)
	return io.EOF
}

// DeliverV3Message implements MessageDeliverer for V3 protocol.
func (b *Broker) DeliverV3Message(s *session.Session, msg Message) error {
	pub := &v3.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
		},
		TopicName: msg.Topic,
		Payload:   msg.Payload,
		ID:        msg.PacketID,
	}

	if err := s.WritePacket(pub); err != nil {
		return err
	}

	b.stats.IncrementPublishSent()
	b.stats.AddBytesSent(uint64(len(msg.Payload)))

	return nil
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (b *Broker) deliverOfflineV3Messages(s *session.Session) {
	msgs := s.OfflineQueue().Drain()
	for _, msg := range msgs {
		deliverMsg := Message{
			Topic:   msg.Topic,
			Payload: msg.Payload,
			QoS:     msg.QoS,
			Retain:  msg.Retain,
		}
		b.DeliverToSession(s, deliverMsg)
	}
}

// --- Response packet senders ---

func sendV3ConnAck(conn core.Connection, sessionPresent bool, returnCode byte) error {
	ack := &v3.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReturnCode:     returnCode,
	}
	return conn.WritePacket(ack)
}

func sendV3PubAck(s *session.Session, packetID uint16) error {
	ack := &v3.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
	}
	return s.WritePacket(ack)
}

func sendV3PubRec(s *session.Session, packetID uint16) error {
	rec := &v3.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
	}
	return s.WritePacket(rec)
}

func sendV3PubRel(s *session.Session, packetID uint16) error {
	rel := &v3.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          packetID,
	}
	return s.WritePacket(rel)
}

func sendV3PubComp(s *session.Session, packetID uint16) error {
	comp := &v3.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
	}
	return s.WritePacket(comp)
}

func sendV3SubAck(s *session.Session, packetID uint16, returnCodes []byte) error {
	ack := &v3.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReturnCodes: returnCodes,
	}
	return s.WritePacket(ack)
}

func sendV3UnsubAck(s *session.Session, packetID uint16) error {
	ack := &v3.UnSubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          packetID,
	}
	return s.WritePacket(ack)
}

func sendV3PingResp(s *session.Session) error {
	resp := &v3.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
}
