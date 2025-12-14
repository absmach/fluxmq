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

// V3Handler implements the Handler interface for MQTT v3.1.1.
type V3Handler struct {
	broker *Broker
	auth   *AuthEngine
}

// NewV3Handler creates a new v3 handler.
func NewV3Handler(broker *Broker, auth *AuthEngine) *V3Handler {
	return &V3Handler{
		broker: broker,
		auth:   auth,
	}
}

// HandleConnect handles CONNECT packets and sets up the session.
func (h *V3Handler) HandleConnect(conn core.Connection, p *v3.Connect) error {
	clientID := p.ClientID
	cleanStart := p.CleanSession

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				h.broker.stats.IncrementProtocolErrors()
				h.sendConnAck(conn, false, 0x02)
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			h.broker.stats.IncrementProtocolErrors()
			h.sendConnAck(conn, false, 0x02)
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
			h.sendConnAck(conn, false, 0x04)
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

	sess, isNew, err := h.broker.CreateSession(clientID, opts)
	if err != nil {
		h.broker.stats.IncrementProtocolErrors()
		h.sendConnAck(conn, false, 0x03)
		conn.Close()
		return err
	}

	sess.Version = p.ProtocolVersion
	sess.KeepAlive = time.Duration(p.KeepAlive) * time.Second

	if err := sess.Connect(conn); err != nil {
		h.broker.stats.IncrementProtocolErrors()
		conn.Close()
		return err
	}

	sessionPresent := !isNew && !cleanStart
	if err := h.sendConnAck(conn, sessionPresent, 0x00); err != nil {
		sess.Disconnect(false)
		return err
	}

	h.broker.stats.IncrementConnections()

	h.deliverOfflineMessages(sess)

	return h.runSession(sess)
}

// runSession runs the main packet loop for a session.
func (h *V3Handler) runSession(s *session.Session) error {
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
func (h *V3Handler) handlePacket(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	switch p := pkt.(type) {
	case *v3.Publish:
		return h.handlePublish(s, p)
	case *v3.PubAck:
		return h.handlePubAck(s, p)
	case *v3.PubRec:
		return h.handlePubRec(s, p)
	case *v3.PubRel:
		return h.handlePubRel(s, p)
	case *v3.PubComp:
		return h.handlePubComp(s, p)
	case *v3.Subscribe:
		return h.handleSubscribe(s, p)
	case *v3.Unsubscribe:
		return h.handleUnsubscribe(s, p)
	case *v3.PingReq:
		return h.handlePingReq(s)
	case *v3.Disconnect:
		return h.handleDisconnect(s)
	default:
		return ErrInvalidPacketType
	}
}

// handlePublish handles PUBLISH packets.
func (h *V3Handler) handlePublish(s *session.Session, p *v3.Publish) error {
	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID
	dup := p.FixedHeader.Dup

	if h.auth != nil && !h.auth.CanPublish(s.ID, topic) {
		h.broker.stats.IncrementAuthzErrors()
		return ErrNotAuthorized
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
			return err
		}
		return h.sendPubAck(s, packetID)

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return h.sendPubRec(s, packetID)
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

		return h.sendPubRec(s, packetID)
	}

	return nil
}

// handlePubAck handles PUBACK packets (QoS 1 acknowledgment from client).
func (h *V3Handler) handlePubAck(s *session.Session, p *v3.PubAck) error {
	return h.broker.AckMessage(s, p.ID)
}

// handlePubRec handles PUBREC packets (QoS 2 step 1 from client).
func (h *V3Handler) handlePubRec(s *session.Session, p *v3.PubRec) error {
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived)
	return h.sendPubRel(s, p.ID)
}

// handlePubRel handles PUBREL packets (QoS 2 step 2 from client).
func (h *V3Handler) handlePubRel(s *session.Session, p *v3.PubRel) error {
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

	return h.sendPubComp(s, packetID)
}

// handlePubComp handles PUBCOMP packets (QoS 2 step 3 from client).
func (h *V3Handler) handlePubComp(s *session.Session, p *v3.PubComp) error {
	return h.broker.AckMessage(s, p.ID)
}

// handleSubscribe handles SUBSCRIBE packets.
func (h *V3Handler) handleSubscribe(s *session.Session, p *v3.Subscribe) error {
	packetID := p.ID

	reasonCodes := make([]byte, len(p.Topics))
	for i, t := range p.Topics {
		if h.auth != nil && !h.auth.CanSubscribe(s.ID, t.Name) {
			h.broker.stats.IncrementAuthzErrors()
			reasonCodes[i] = 0x80
			continue
		}

		opts := SubscriptionOptions{
			QoS: t.QoS,
		}

		if err := h.broker.subscribeInternal(s, t.Name, opts); err != nil {
			reasonCodes[i] = 0x80
			continue
		}

		reasonCodes[i] = t.QoS
		h.broker.stats.IncrementSubscriptions()

		retained, err := h.broker.retained.Match(t.Name)
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
				h.broker.DeliverToSession(s, deliverMsg)
			}
		}
	}

	return h.sendSubAck(s, packetID, reasonCodes)
}

// handleUnsubscribe handles UNSUBSCRIBE packets.
func (h *V3Handler) handleUnsubscribe(s *session.Session, p *v3.Unsubscribe) error {
	for _, filter := range p.Topics {
		h.broker.unsubscribeInternal(s, filter)
		h.broker.stats.DecrementSubscriptions()
	}

	return h.sendUnsubAck(s, p.ID)
}

// handlePingReq handles PINGREQ packets.
func (h *V3Handler) handlePingReq(s *session.Session) error {
	return h.sendPingResp(s)
}

// handleDisconnect handles DISCONNECT packets.
func (h *V3Handler) handleDisconnect(s *session.Session) error {
	s.Disconnect(true)
	return io.EOF
}

// DeliverMessage implements MessageDeliverer for V3 protocol.
func (h *V3Handler) DeliverMessage(sess *session.Session, msg Message) error {
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

	if err := sess.WritePacket(pub); err != nil {
		return err
	}

	h.broker.stats.IncrementPublishSent()
	h.broker.stats.AddBytesSent(uint64(len(msg.Payload)))

	return nil
}

// --- Handler Interface Implementations ---

// HandlePublish implements Handler.HandlePublish.
func (h *V3Handler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.Publish)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePublish(s, p)
}

// HandlePubAck implements Handler.HandlePubAck.
func (h *V3Handler) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubAck)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePubAck(s, p)
}

// HandlePubRec implements Handler.HandlePubRec.
func (h *V3Handler) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubRec)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePubRec(s, p)
}

// HandlePubRel implements Handler.HandlePubRel.
func (h *V3Handler) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubRel)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePubRel(s, p)
}

// HandlePubComp implements Handler.HandlePubComp.
func (h *V3Handler) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubComp)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handlePubComp(s, p)
}

// HandleSubscribe implements Handler.HandleSubscribe.
func (h *V3Handler) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.Subscribe)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handleSubscribe(s, p)
}

// HandleUnsubscribe implements Handler.HandleUnsubscribe.
func (h *V3Handler) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.Unsubscribe)
	if !ok {
		return ErrInvalidPacketType
	}
	return h.handleUnsubscribe(s, p)
}

// HandlePingReq implements Handler.HandlePingReq.
func (h *V3Handler) HandlePingReq(s *session.Session) error {
	return h.handlePingReq(s)
}

// HandleDisconnect implements Handler.HandleDisconnect.
func (h *V3Handler) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	return h.handleDisconnect(s)
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (h *V3Handler) deliverOfflineMessages(s *session.Session) {
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

func (h *V3Handler) sendConnAck(conn core.Connection, sessionPresent bool, returnCode byte) error {
	ack := &v3.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReturnCode:     returnCode,
	}
	return conn.WritePacket(ack)
}

func (h *V3Handler) sendPubAck(s *session.Session, packetID uint16) error {
	ack := &v3.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
	}
	return s.WritePacket(ack)
}

func (h *V3Handler) sendPubRec(s *session.Session, packetID uint16) error {
	rec := &v3.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
	}
	return s.WritePacket(rec)
}

func (h *V3Handler) sendPubRel(s *session.Session, packetID uint16) error {
	rel := &v3.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          packetID,
	}
	return s.WritePacket(rel)
}

func (h *V3Handler) sendPubComp(s *session.Session, packetID uint16) error {
	comp := &v3.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
	}
	return s.WritePacket(comp)
}

func (h *V3Handler) sendSubAck(s *session.Session, packetID uint16, returnCodes []byte) error {
	ack := &v3.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReturnCodes: returnCodes,
	}
	return s.WritePacket(ack)
}

func (h *V3Handler) sendUnsubAck(s *session.Session, packetID uint16) error {
	ack := &v3.UnSubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          packetID,
	}
	return s.WritePacket(ack)
}

func (h *V3Handler) sendPingResp(s *session.Session) error {
	resp := &v3.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
}
