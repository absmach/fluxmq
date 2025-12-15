package broker

import (
	"io"
	"time"

	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	v5 "github.com/absmach/mqtt/core/packets/v5"
	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/storage/messages"
)

// handleV5Connect handles CONNECT packets and sets up the session.
func (b *Broker) handleV5Connect(conn core.Connection, p *v5.Connect) error {
	clientID := p.ClientID
	cleanStart := p.CleanStart

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				b.stats.IncrementProtocolErrors()
				b.sendV5ConnAck(conn, false, 0x85, nil)
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			b.stats.IncrementProtocolErrors()
			b.sendV5ConnAck(conn, false, 0x85, nil)
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
			b.sendV5ConnAck(conn, false, 0x86, nil)
			conn.Close()
			return ErrNotAuthorized
		}
	}

	var willMsg *Message
	if p.WillFlag {
		willMsg = &Message{
			Topic:   p.WillTopic,
			Payload: p.WillPayload,
			QoS:     p.WillQoS,
			Retain:  p.WillRetain,
		}
	}

	receiveMax := uint16(65535)
	topicAliasMax := uint16(10)
	sessionExpiry := uint32(0)

	if p.Properties != nil {
		if p.Properties.ReceiveMaximum != nil {
			receiveMax = *p.Properties.ReceiveMaximum
		}
		if p.Properties.TopicAliasMaximum != nil {
			topicAliasMax = *p.Properties.TopicAliasMaximum
		}
		if p.Properties.SessionExpiryInterval != nil {
			sessionExpiry = *p.Properties.SessionExpiryInterval
		}
	}

	opts := SessionOptions{
		CleanStart:     cleanStart,
		KeepAlive:      p.KeepAlive,
		ReceiveMaximum: receiveMax,
		SessionExpiry:  sessionExpiry,
		WillMessage:    willMsg,
	}

	s, isNew, err := b.CreateSession(clientID, opts)
	if err != nil {
		b.stats.IncrementProtocolErrors()
		b.sendV5ConnAck(conn, false, 0x80, nil)
		conn.Close()
		return err
	}

	s.Version = p.ProtocolVersion
	s.KeepAlive = time.Duration(p.KeepAlive) * time.Second
	s.TopicAliasMax = topicAliasMax

	if err := s.Connect(conn); err != nil {
		b.stats.IncrementProtocolErrors()
		conn.Close()
		return err
	}

	sessionPresent := !isNew && !cleanStart
	if err := b.sendV5ConnAckWithProperties(conn, s, sessionPresent, 0x00); err != nil {
		s.Disconnect(false)
		return err
	}

	b.stats.IncrementConnections()

	b.deliverV5OfflineMessages(s)

	return b.runSession(s)
}

// handleV5Packet dispatches a packet to the appropriate handler.
// func (b *Broker) handleV5Packet(s *session.Session, pkt packets.ControlPacket) error {
// 	s.Touch()

// 	switch p := pkt.(type) {
// 	case *v5.Publish:
// 		return b.handleV5Publish(s, p)
// 	case *v5.PubAck:
// 		return b.handleV5PubAck(s, p)
// 	case *v5.PubRec:
// 		return b.handleV5PubRec(s, p)
// 	case *v5.PubRel:
// 		return b.handleV5PubRel(s, p)
// 	case *v5.PubComp:
// 		return b.handleV5PubComp(s, p)
// 	case *v5.Subscribe:
// 		return b.handleV5Subscribe(s, p)
// 	case *v5.Unsubscribe:
// 		return b.handleV5Unsubscribe(s, p)
// 	case *v5.PingReq:
// 		return b.handleV5PingReq(s)
// 	case *v5.Disconnect:
// 		return b.handleV5Disconnect(s, p)
// 	case *v5.Auth:
// 		return b.handleV5Auth(s, p)
// 	default:
// 		return ErrInvalidPacketType
// 	}
// }

// handleV5Publish handles PUBLISH packets.
func (b *Broker) handleV5Publish(s *session.Session, p *v5.Publish) error {
	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID
	dup := p.FixedHeader.Dup

	if p.Properties != nil && p.Properties.TopicAlias != nil {
		alias := *p.Properties.TopicAlias
		if alias > s.TopicAliasMax {
			return b.sendV5PubAck(s, packetID, 0x94, "Topic alias invalid")
		}
		if topic == "" {
			resolvedTopic, ok := s.ResolveInboundAlias(alias)
			if !ok {
				return b.sendV5PubAck(s, packetID, 0x82, "Topic alias not established")
			}
			topic = resolvedTopic
		} else {
			s.SetInboundAlias(alias, topic)
		}
	}

	if b.auth != nil && !b.auth.CanPublish(s.ID, topic) {
		b.stats.IncrementAuthzErrors()
		return b.sendV5PubAck(s, packetID, 0x87, "Not authorized")
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
		return b.Publish(msg)

	case 1:
		msg := Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		if err := b.Publish(msg); err != nil {
			return b.sendV5PubAck(s, packetID, 0x80, "Unspecified error")
		}
		return b.sendV5PubAck(s, packetID, 0x00, "")

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return b.sendV5PubRec(s, packetID, 0x00, "")
		}

		s.Inflight().MarkReceived(packetID)

		storeMsg := &storage.Message{
			Topic:    topic,
			Payload:  payload,
			QoS:      2,
			Retain:   retain,
			PacketID: packetID,
		}
		if err := s.Inflight().Add(packetID, storeMsg, messages.Inbound); err != nil {
			return err
		}

		return b.sendV5PubRec(s, packetID, 0x00, "")
	}

	return nil
}

// handleV5PubAck handles PUBACK packets.
func (b *Broker) handleV5PubAck(s *session.Session, p *v5.PubAck) error {
	return b.AckMessage(s, p.ID)
}

// handleV5PubRec handles PUBREC packets.
func (b *Broker) handleV5PubRec(s *session.Session, p *v5.PubRec) error {
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived)
	return b.sendV5PubRel(s, p.ID, 0x00, "")
}

// handleV5PubRel handles PUBREL packets.
func (b *Broker) handleV5PubRel(s *session.Session, p *v5.PubRel) error {
	packetID := p.ID

	inf, ok := s.Inflight().Get(packetID)
	if ok && inf.Message != nil {
		msg := Message{
			Topic:   inf.Message.Topic,
			Payload: inf.Message.Payload,
			QoS:     inf.Message.QoS,
			Retain:  inf.Message.Retain,
		}
		b.Publish(msg)
	}

	b.AckMessage(s, packetID)
	s.Inflight().ClearReceived(packetID)

	return b.sendV5PubComp(s, packetID, 0x00, "")
}

// handleV5PubComp handles PUBCOMP packets.
func (b *Broker) handleV5PubComp(s *session.Session, p *v5.PubComp) error {
	return b.AckMessage(s, p.ID)
}

// handleV5Subscribe handles SUBSCRIBE packets.
func (b *Broker) handleV5Subscribe(s *session.Session, p *v5.Subscribe) error {
	packetID := p.ID

	reasonCodes := make([]byte, len(p.Opts))
	for i, t := range p.Opts {
		if b.auth != nil && !b.auth.CanSubscribe(s.ID, t.Topic) {
			b.stats.IncrementAuthzErrors()
			reasonCodes[i] = 0x87
			continue
		}

		noLocal := false
		retainAsPublished := false
		retainHandling := byte(0)

		if t.NoLocal != nil {
			noLocal = *t.NoLocal
		}
		if t.RetainAsPublished != nil {
			retainAsPublished = *t.RetainAsPublished
		}
		if t.RetainHandling != nil {
			retainHandling = *t.RetainHandling
		}

		opts := SubscriptionOptions{
			QoS:               t.MaxQoS,
			NoLocal:           noLocal,
			RetainAsPublished: retainAsPublished,
			RetainHandling:    retainHandling,
		}

		if err := b.subscribeInternal(s, t.Topic, opts); err != nil {
			reasonCodes[i] = 0x80
			continue
		}

		reasonCodes[i] = t.MaxQoS
		b.stats.IncrementSubscriptions()

		retained, err := b.retained.Match(t.Topic)
		if err == nil {
			for _, msg := range retained {
				deliverQoS := msg.QoS
				if t.MaxQoS < deliverQoS {
					deliverQoS = t.MaxQoS
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

	return b.sendV5SubAck(s, packetID, reasonCodes)
}

// handleV5Unsubscribe handles UNSUBSCRIBE packets.
func (b *Broker) handleV5Unsubscribe(s *session.Session, p *v5.Unsubscribe) error {
	reasonCodes := make([]byte, len(p.Topics))
	for i, filter := range p.Topics {
		if err := b.unsubscribeInternal(s, filter); err != nil {
			reasonCodes[i] = 0x80
		} else {
			reasonCodes[i] = 0x00
			b.stats.DecrementSubscriptions()
		}
	}

	return b.sendV5UnsubAck(s, p.ID, reasonCodes)
}

// handleV5PingReq handles PINGREQ packets.
func (b *Broker) handleV5PingReq(s *session.Session) error {
	return b.sendV5PingResp(s)
}

// handleV5Disconnect handles DISCONNECT packets.
func (b *Broker) handleV5Disconnect(s *session.Session, p *v5.Disconnect) error {
	s.Disconnect(true)
	return io.EOF
}

// handleV5Auth handles AUTH packets.
func (b *Broker) handleV5Auth(s *session.Session, p *v5.Auth) error {
	return nil
}

// DeliverV5Message implements MessageDeliverer for V5 protocol.
func (b *Broker) DeliverV5Message(s *session.Session, msg Message) error {
	pub := &v5.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
		},
		TopicName:  msg.Topic,
		Payload:    msg.Payload,
		ID:         msg.PacketID,
		Properties: &v5.PublishProperties{},
	}

	if err := s.WritePacket(pub); err != nil {
		return err
	}

	b.stats.IncrementPublishSent()
	b.stats.AddBytesSent(uint64(len(msg.Payload)))

	return nil
}

// deliverV5OfflineMessages sends queued messages to reconnected client.
func (b *Broker) deliverV5OfflineMessages(s *session.Session) {
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

func (b *Broker) sendV5ConnAck(conn core.Connection, sessionPresent bool, reasonCode byte, props *v5.ConnAckProperties) error {
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

func (b *Broker) sendV5ConnAckWithProperties(conn core.Connection, s *session.Session, sessionPresent bool, reasonCode byte) error {
	receiveMax := uint16(65535)
	topicAliasMax := s.TopicAliasMax
	retainAvailable := byte(1)
	wildcardSubAvailable := byte(1)
	subIDAvailable := byte(1)

	props := &v5.ConnAckProperties{
		ReceiveMax:           &receiveMax,
		TopicAliasMax:        &topicAliasMax,
		RetainAvailable:      &retainAvailable,
		WildcardSubAvailable: &wildcardSubAvailable,
		SubIDAvailable:       &subIDAvailable,
	}

	ack := &v5.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReasonCode:     reasonCode,
		Properties:     props,
	}

	return conn.WritePacket(ack)
}

func (b *Broker) sendV5PubAck(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	ack := &v5.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func (b *Broker) sendV5PubRec(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	rec := &v5.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(rec)
}

func (b *Broker) sendV5PubRel(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	rel := &v5.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(rel)
}

func (b *Broker) sendV5PubComp(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	comp := &v5.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(comp)
}

func (b *Broker) sendV5SubAck(s *session.Session, packetID uint16, reasonCodes []byte) error {
	ack := &v5.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReasonCodes: &reasonCodes,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func (b *Broker) sendV5UnsubAck(s *session.Session, packetID uint16, reasonCodes []byte) error {
	ack := &v5.UnSubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          packetID,
		ReasonCodes: &reasonCodes,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func (b *Broker) sendV5PingResp(s *session.Session) error {
	resp := &v5.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
}
