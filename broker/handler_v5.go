package broker

import (
	"io"
	"time"

	"github.com/dborovcanin/mqtt/core"
	"github.com/dborovcanin/mqtt/core/packets"
	v5 "github.com/dborovcanin/mqtt/core/packets/v5"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/messages"
)

// V5Handler implements the Handler interface for MQTT v5.0.
type V5Handler struct {
	broker *Broker
	auth   *AuthEngine
}

// NewV5Handler creates a new v5 handler.
func NewV5Handler(broker *Broker, auth *AuthEngine) *V5Handler {
	return &V5Handler{
		broker: broker,
		auth:   auth,
	}
}

// HandleConnect handles CONNECT packets and sets up the session.
func (h *V5Handler) HandleConnect(conn core.Connection, p *v5.Connect) error {
	clientID := p.ClientID
	cleanStart := p.CleanStart

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				h.broker.stats.IncrementProtocolErrors()
				h.sendConnAck(conn, false, 0x85, nil)
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			h.broker.stats.IncrementProtocolErrors()
			h.sendConnAck(conn, false, 0x85, nil)
			conn.Close()
			return ErrClientIDRequired
		}
	}

	if h.auth != nil {
		username := p.Username
		password := string(p.Password)

		authenticated, err := h.auth.Authenticate(clientID, username, password)
		if err != nil || !authenticated {
			h.broker.stats.IncrementAuthErrors()
			h.sendConnAck(conn, false, 0x86, nil)
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

	sess, isNew, err := h.broker.CreateSession(clientID, opts)
	if err != nil {
		h.broker.stats.IncrementProtocolErrors()
		h.sendConnAck(conn, false, 0x80, nil)
		conn.Close()
		return err
	}

	sess.Version = p.ProtocolVersion
	sess.KeepAlive = time.Duration(p.KeepAlive) * time.Second
	sess.TopicAliasMax = topicAliasMax

	if err := sess.Connect(conn); err != nil {
		h.broker.stats.IncrementProtocolErrors()
		conn.Close()
		return err
	}

	sessionPresent := !isNew && !cleanStart
	if err := h.sendConnAckWithProperties(conn, sess, sessionPresent, 0x00); err != nil {
		sess.Disconnect(false)
		return err
	}

	h.broker.stats.IncrementConnections()

	h.deliverOfflineMessages(sess)

	return h.runSession(sess)
}

// runSession runs the main packet loop for a session.
func (h *V5Handler) runSession(s *session.Session) error {
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
				h.broker.stats.IncrementPacketErrors()
			}
			h.broker.stats.DecrementConnections()
			s.Disconnect(false)
			return err
		}

		if s.KeepAlive > 0 {
			deadline := time.Now().Add(s.KeepAlive + s.KeepAlive/2)
			conn.SetReadDeadline(deadline)
		}

		h.broker.stats.IncrementMessagesReceived()

		if err := h.handlePacket(s, pkt); err != nil {
			if err == io.EOF {
				h.broker.stats.DecrementConnections()
				return nil
			}
			h.broker.stats.IncrementProtocolErrors()
			h.broker.stats.DecrementConnections()
			s.Disconnect(false)
			return err
		}
	}
}

// handlePacket dispatches a packet to the appropriate handler.
func (h *V5Handler) handlePacket(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	switch p := pkt.(type) {
	case *v5.Publish:
		return h.handlePublish(s, p)
	case *v5.PubAck:
		return h.handlePubAck(s, p)
	case *v5.PubRec:
		return h.handlePubRec(s, p)
	case *v5.PubRel:
		return h.handlePubRel(s, p)
	case *v5.PubComp:
		return h.handlePubComp(s, p)
	case *v5.Subscribe:
		return h.handleSubscribe(s, p)
	case *v5.Unsubscribe:
		return h.handleUnsubscribe(s, p)
	case *v5.PingReq:
		return h.handlePingReq(s)
	case *v5.Disconnect:
		return h.handleDisconnect(s, p)
	case *v5.Auth:
		return h.handleAuth(s, p)
	default:
		return ErrInvalidPacketType
	}
}

// handlePublish handles PUBLISH packets.
func (h *V5Handler) handlePublish(s *session.Session, p *v5.Publish) error {
	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID
	dup := p.FixedHeader.Dup

	if p.Properties != nil && p.Properties.TopicAlias != nil {
		alias := *p.Properties.TopicAlias
		if alias > s.TopicAliasMax {
			return h.sendPubAck(s, packetID, 0x94, "Topic alias invalid")
		}
		if topic == "" {
			resolvedTopic, ok := s.ResolveInboundAlias(alias)
			if !ok {
				return h.sendPubAck(s, packetID, 0x82, "Topic alias not established")
			}
			topic = resolvedTopic
		} else {
			s.SetInboundAlias(alias, topic)
		}
	}

	if h.auth != nil && !h.auth.CanPublish(s.ID, topic) {
		h.broker.stats.IncrementAuthzErrors()
		return h.sendPubAck(s, packetID, 0x87, "Not authorized")
	}

	h.broker.stats.IncrementPublishReceived()
	h.broker.stats.AddBytesReceived(uint64(len(payload)))

	switch qos {
	case 0:
		msg := Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		return h.broker.Publish(s, msg)

	case 1:
		msg := Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		if err := h.broker.Publish(s, msg); err != nil {
			return h.sendPubAck(s, packetID, 0x80, "Unspecified error")
		}
		return h.sendPubAck(s, packetID, 0x00, "")

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return h.sendPubRec(s, packetID, 0x00, "")
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

		return h.sendPubRec(s, packetID, 0x00, "")
	}

	return nil
}

// handlePubAck handles PUBACK packets.
func (h *V5Handler) handlePubAck(s *session.Session, p *v5.PubAck) error {
	return h.broker.AckMessage(s, p.ID)
}

// handlePubRec handles PUBREC packets.
func (h *V5Handler) handlePubRec(s *session.Session, p *v5.PubRec) error {
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived)
	return h.sendPubRel(s, p.ID, 0x00, "")
}

// handlePubRel handles PUBREL packets.
func (h *V5Handler) handlePubRel(s *session.Session, p *v5.PubRel) error {
	packetID := p.ID

	inf, ok := s.Inflight().Get(packetID)
	if ok && inf.Message != nil {
		msg := Message{
			Topic:   inf.Message.Topic,
			Payload: inf.Message.Payload,
			QoS:     inf.Message.QoS,
			Retain:  inf.Message.Retain,
		}
		h.broker.Publish(s, msg)
	}

	h.broker.AckMessage(s, packetID)
	s.Inflight().ClearReceived(packetID)

	return h.sendPubComp(s, packetID, 0x00, "")
}

// handlePubComp handles PUBCOMP packets.
func (h *V5Handler) handlePubComp(s *session.Session, p *v5.PubComp) error {
	return h.broker.AckMessage(s, p.ID)
}

// handleSubscribe handles SUBSCRIBE packets.
func (h *V5Handler) handleSubscribe(s *session.Session, p *v5.Subscribe) error {
	packetID := p.ID

	reasonCodes := make([]byte, len(p.Opts))
	for i, t := range p.Opts {
		if h.auth != nil && !h.auth.CanSubscribe(s.ID, t.Topic) {
			h.broker.stats.IncrementAuthzErrors()
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

		if err := h.broker.subscribeInternal(s, t.Topic, opts); err != nil {
			reasonCodes[i] = 0x80
			continue
		}

		reasonCodes[i] = t.MaxQoS
		h.broker.stats.IncrementSubscriptions()

		retained, err := h.broker.retained.Match(t.Topic)
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
				h.broker.DeliverToSession(s, deliverMsg)
			}
		}
	}

	return h.sendSubAck(s, packetID, reasonCodes)
}

// handleUnsubscribe handles UNSUBSCRIBE packets.
func (h *V5Handler) handleUnsubscribe(s *session.Session, p *v5.Unsubscribe) error {
	reasonCodes := make([]byte, len(p.Topics))
	for i, filter := range p.Topics {
		if err := h.broker.unsubscribeInternal(s, filter); err != nil {
			reasonCodes[i] = 0x80
		} else {
			reasonCodes[i] = 0x00
			h.broker.stats.DecrementSubscriptions()
		}
	}

	return h.sendUnsubAck(s, p.ID, reasonCodes)
}

// handlePingReq handles PINGREQ packets.
func (h *V5Handler) handlePingReq(s *session.Session) error {
	return h.sendPingResp(s)
}

// handleDisconnect handles DISCONNECT packets.
func (h *V5Handler) handleDisconnect(s *session.Session, p *v5.Disconnect) error {
	s.Disconnect(true)
	return io.EOF
}

// handleAuth handles AUTH packets.
func (h *V5Handler) handleAuth(s *session.Session, p *v5.Auth) error {
	return nil
}

// DeliverMessage implements MessageDeliverer for V5 protocol.
func (h *V5Handler) DeliverMessage(sess *session.Session, msg Message) error {
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

	if err := sess.WritePacket(pub); err != nil {
		return err
	}

	h.broker.stats.IncrementPublishSent()
	h.broker.stats.AddBytesSent(uint64(len(msg.Payload)))

	return nil
}

// --- Handler Interface Implementations ---

// HandlePublish implements Handler.HandlePublish.
func (h *V5Handler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.Publish)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePublish(s, p)
}

// HandlePubAck implements Handler.HandlePubAck.
func (h *V5Handler) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubAck)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePubAck(s, p)
}

// HandlePubRec implements Handler.HandlePubRec.
func (h *V5Handler) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubRec)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePubRec(s, p)
}

// HandlePubRel implements Handler.HandlePubRel.
func (h *V5Handler) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubRel)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePubRel(s, p)
}

// HandlePubComp implements Handler.HandlePubComp.
func (h *V5Handler) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubComp)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePubComp(s, p)
}

// HandleSubscribe implements Handler.HandleSubscribe.
func (h *V5Handler) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.Subscribe)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handleSubscribe(s, p)
}

// HandleUnsubscribe implements Handler.HandleUnsubscribe.
func (h *V5Handler) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.Unsubscribe)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handleUnsubscribe(s, p)
}

// HandlePingReq implements Handler.HandlePingReq.
func (h *V5Handler) HandlePingReq(s *session.Session) error {
	return h.handlePingReq(s)
}

// HandleDisconnect implements Handler.HandleDisconnect.
func (h *V5Handler) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.Disconnect)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handleDisconnect(s, p)
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (h *V5Handler) deliverOfflineMessages(s *session.Session) {
	msgs := s.OfflineQueue().Drain()
	for _, msg := range msgs {
		deliverMsg := Message{
			Topic:   msg.Topic,
			Payload: msg.Payload,
			QoS:     msg.QoS,
			Retain:  msg.Retain,
		}
		h.broker.DeliverToSession(s, deliverMsg)
	}
}

// --- Response packet senders ---

func (h *V5Handler) sendConnAck(conn core.Connection, sessionPresent bool, reasonCode byte, props *v5.ConnAckProperties) error {
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

func (h *V5Handler) sendConnAckWithProperties(conn core.Connection, s *session.Session, sessionPresent bool, reasonCode byte) error {
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

func (h *V5Handler) sendPubAck(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	ack := &v5.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func (h *V5Handler) sendPubRec(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	rec := &v5.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(rec)
}

func (h *V5Handler) sendPubRel(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	rel := &v5.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(rel)
}

func (h *V5Handler) sendPubComp(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	comp := &v5.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(comp)
}

func (h *V5Handler) sendSubAck(s *session.Session, packetID uint16, reasonCodes []byte) error {
	ack := &v5.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReasonCodes: &reasonCodes,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func (h *V5Handler) sendUnsubAck(s *session.Session, packetID uint16, reasonCodes []byte) error {
	ack := &v5.UnSubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          packetID,
		ReasonCodes: &reasonCodes,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func (h *V5Handler) sendPingResp(s *session.Session) error {
	resp := &v5.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
}
