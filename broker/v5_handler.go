package broker

import (
	"io"
	"log/slog"
	"time"

	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	v5 "github.com/absmach/mqtt/core/packets/v5"
	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/storage/messages"
)

var _ Handler = (*V5Handler)(nil)

// V5Handler is a stateless adapter that translates MQTT v5 packets to broker domain operations.
type V5Handler struct {
	broker *Broker
}

// NewV5Handler creates a new V5 protocol handler.
func NewV5Handler(broker *Broker) *V5Handler {
	return &V5Handler{broker: broker}
}

// HandleConnect handles CONNECT packets.
func (h *V5Handler) HandleConnect(conn core.Connection, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v5.Connect)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Info("v5_connect",
		slog.String("remote_addr", conn.RemoteAddr().String()),
		slog.String("client_id", p.ClientID),
	)

	clientID := p.ClientID
	cleanStart := p.CleanStart

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				h.broker.stats.IncrementProtocolErrors()
				sendV5ConnAck(conn, false, 0x85, nil)
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			h.broker.stats.IncrementProtocolErrors()
			sendV5ConnAck(conn, false, 0x85, nil)
			conn.Close()
			return ErrClientIDRequired
		}
	}

	if h.broker.auth != nil {
		username := p.Username
		password := string(p.Password)

		authenticated, err := h.broker.auth.Authenticate(clientID, username, password)
		if err != nil || !authenticated {
			h.broker.stats.IncrementAuthErrors()
			sendV5ConnAck(conn, false, 0x86, nil)
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

	s, isNew, err := h.broker.CreateSession(clientID, opts)
	if err != nil {
		h.broker.stats.IncrementProtocolErrors()
		sendV5ConnAck(conn, false, 0x80, nil)
		conn.Close()
		return err
	}

	s.Version = p.ProtocolVersion
	s.KeepAlive = time.Duration(p.KeepAlive) * time.Second
	s.TopicAliasMax = topicAliasMax

	if err := s.Connect(conn); err != nil {
		h.broker.stats.IncrementProtocolErrors()
		conn.Close()
		return err
	}

	sessionPresent := !isNew && !cleanStart
	if err := sendV5ConnAckWithProperties(conn, s, sessionPresent, 0x00); err != nil {
		s.Disconnect(false)
		return err
	}

	h.broker.stats.IncrementConnections()
	h.broker.logger.Info("v5_connect_success",
		slog.String("client_id", clientID),
		slog.Bool("session_present", sessionPresent),
		slog.Duration("duration", time.Since(start)),
	)

	h.deliverOfflineMessages(s)

	return h.broker.runSession(h, s)
}

// HandlePublish handles PUBLISH packets.
func (h *V5Handler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v5.Publish)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v5_publish",
		slog.String("client_id", s.ID),
		slog.String("topic", p.TopicName),
		slog.Int("qos", int(p.FixedHeader.QoS)),
	)

	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID
	dup := p.FixedHeader.Dup

	if p.Properties != nil && p.Properties.TopicAlias != nil {
		alias := *p.Properties.TopicAlias
		if alias > s.TopicAliasMax {
			return sendV5PubAck(s, packetID, 0x94, "Topic alias invalid")
		}
		if topic == "" {
			resolvedTopic, ok := s.ResolveInboundAlias(alias)
			if !ok {
				return sendV5PubAck(s, packetID, 0x82, "Topic alias not established")
			}
			topic = resolvedTopic
		} else {
			s.SetInboundAlias(alias, topic)
		}
	}

	if h.broker.auth != nil && !h.broker.auth.CanPublish(s.ID, topic) {
		h.broker.stats.IncrementAuthzErrors()
		return sendV5PubAck(s, packetID, 0x87, "Not authorized")
	}

	switch qos {
	case 0:
		msg := Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		err := h.broker.Publish(msg)
		h.broker.logger.Debug("v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err),
		)
		return err

	case 1:
		msg := Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		if err := h.broker.Publish(msg); err != nil {
			return sendV5PubAck(s, packetID, 0x80, "Unspecified error")
		}
		h.broker.logger.Debug("v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)
		return sendV5PubAck(s, packetID, 0x00, "")

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return sendV5PubRec(s, packetID, 0x00, "")
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

		return sendV5PubRec(s, packetID, 0x00, "")
	}

	return nil
}

// HandlePubAck handles PUBACK packets.
func (h *V5Handler) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubAck)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v5_puback", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s, p.ID)
}

// HandlePubRec handles PUBREC packets.
func (h *V5Handler) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubRec)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v5_pubrec", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived)
	return sendV5PubRel(s, p.ID, 0x00, "")
}

// HandlePubRel handles PUBREL packets.
func (h *V5Handler) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubRel)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v5_pubrel", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))

	packetID := p.ID

	inf, ok := s.Inflight().Get(packetID)
	if ok && inf.Message != nil {
		msg := Message{
			Topic:   inf.Message.Topic,
			Payload: inf.Message.Payload,
			QoS:     inf.Message.QoS,
			Retain:  inf.Message.Retain,
		}
		h.broker.Publish(msg)
	}

	h.broker.AckMessage(s, packetID)
	s.Inflight().ClearReceived(packetID)

	return sendV5PubComp(s, packetID, 0x00, "")
}

// HandlePubComp handles PUBCOMP packets.
func (h *V5Handler) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubComp)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v5_pubcomp", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s, p.ID)
}

// HandleSubscribe handles SUBSCRIBE packets.
func (h *V5Handler) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v5.Subscribe)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Info("v5_subscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Opts)))

	packetID := p.ID

	reasonCodes := make([]byte, len(p.Opts))
	for i, t := range p.Opts {
		if h.broker.auth != nil && !h.broker.auth.CanSubscribe(s.ID, t.Topic) {
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

	h.broker.logger.Info("v5_subscribe_complete",
		slog.String("client_id", s.ID),
		slog.Duration("duration", time.Since(start)),
	)

	return sendV5SubAck(s, packetID, reasonCodes)
}

// HandleUnsubscribe handles UNSUBSCRIBE packets.
func (h *V5Handler) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v5.Unsubscribe)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Info("v5_unsubscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Topics)))

	reasonCodes := make([]byte, len(p.Topics))
	for i, filter := range p.Topics {
		if err := h.broker.unsubscribeInternal(s, filter); err != nil {
			reasonCodes[i] = 0x80
		} else {
			reasonCodes[i] = 0x00
		}
	}

	h.broker.logger.Info("v5_unsubscribe_complete",
		slog.String("client_id", s.ID),
		slog.Duration("duration", time.Since(start)),
	)

	return sendV5UnsubAck(s, p.ID, reasonCodes)
}

// HandlePingReq handles PINGREQ packets.
func (h *V5Handler) HandlePingReq(s *session.Session) error {
	h.broker.logger.Debug("v5_pingreq", slog.String("client_id", s.ID))
	return sendV5PingResp(s)
}

// HandleDisconnect handles DISCONNECT packets.
func (h *V5Handler) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	_, ok := pkt.(*v5.Disconnect)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Info("v5_disconnect", slog.String("client_id", s.ID))
	s.Disconnect(true)
	return io.EOF
}

// HandleAuth handles AUTH packets.
func (h *V5Handler) HandleAuth(s *session.Session, pkt packets.ControlPacket) error {
	_, ok := pkt.(*v5.Auth)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v5_auth", slog.String("client_id", s.ID))
	return nil
}

// HandleConnAck - not used by broker.
func (h *V5Handler) HandleConnAck(s *session.Session, pkt packets.ControlPacket) error {
	return ErrProtocolViolation
}

// HandleUnsubAck - not used by broker.
func (h *V5Handler) HandleUnsubAck(s *session.Session, pkt packets.ControlPacket) error {
	return ErrProtocolViolation
}

// HandlePingResp - not used by broker.
func (h *V5Handler) HandlePingResp(s *session.Session) error {
	return ErrProtocolViolation
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

func sendV5ConnAckWithProperties(conn core.Connection, s *session.Session, sessionPresent bool, reasonCode byte) error {
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

func sendV5PubAck(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	ack := &v5.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func sendV5PubRec(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	rec := &v5.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(rec)
}

func sendV5PubRel(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	rel := &v5.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(rel)
}

func sendV5PubComp(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	comp := &v5.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(comp)
}

func sendV5SubAck(s *session.Session, packetID uint16, reasonCodes []byte) error {
	ack := &v5.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReasonCodes: &reasonCodes,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func sendV5UnsubAck(s *session.Session, packetID uint16, reasonCodes []byte) error {
	ack := &v5.UnSubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          packetID,
		ReasonCodes: &reasonCodes,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func sendV5PingResp(s *session.Session) error {
	resp := &v5.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
}
