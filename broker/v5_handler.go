// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

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

const maxReceived = 65535

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
				sendV5ConnAck(conn, false, v5.ConnAckInvalidClientID, nil)
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			h.broker.stats.IncrementProtocolErrors()
			sendV5ConnAck(conn, false, v5.ConnAckInvalidClientID, nil)
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
			sendV5ConnAck(conn, false, v5.ConnAckBadUsernameOrPassword, nil)
			conn.Close()
			return ErrNotAuthorized
		}
	}

	var will *storage.WillMessage
	if p.WillFlag {
		// Note: Will payload is stored as []byte in storage.WillMessage
		// TODO: Consider zero-copy for will messages in future
		will = &storage.WillMessage{
			ClientID: clientID,
			Topic:    p.WillTopic,
			Payload:  p.WillPayload,
			QoS:      p.WillQoS,
			Retain:   p.WillRetain,
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

	opts := session.Options{
		CleanStart:     cleanStart,
		KeepAlive:      time.Duration(p.KeepAlive) * time.Second,
		ReceiveMaximum: receiveMax,
		ExpiryInterval: sessionExpiry,
		Will:           will,
	}

	s, isNew, err := h.broker.CreateSession(clientID, p.ProtocolVersion, opts)
	if err != nil {
		h.broker.stats.IncrementProtocolErrors()
		sendV5ConnAck(conn, false, v5.ConnAckUnspecifiedError, nil)
		conn.Close()
		return err
	}

	s.TopicAliasMax = topicAliasMax

	if err := s.Connect(conn); err != nil {
		h.broker.stats.IncrementProtocolErrors()
		conn.Close()
		return err
	}

	sessionPresent := !isNew && !cleanStart
	if err := sendV5ConnAckWithProperties(conn, s, sessionPresent, v5.ConnAckSuccess); err != nil {
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
			return sendV5PubAck(s, packetID, v5.PubAckTopicNameInvalid, "Topic alias invalid")
		}
		if topic == "" {
			resolvedTopic, ok := s.ResolveInboundAlias(alias)
			if !ok {
				return sendV5PubAck(s, packetID, v5.PubAckTopicNameInvalid, "Topic alias not established")
			}
			topic = resolvedTopic
		} else {
			s.SetInboundAlias(alias, topic)
		}
	}

	if h.broker.auth != nil && !h.broker.auth.CanPublish(s.ID, topic) {
		h.broker.stats.IncrementAuthzErrors()
		return sendV5PubAck(s, packetID, v5.PubAckNotAuthorized, "Not authorized")
	}

	// Extract message expiry interval if present
	var messageExpiry *uint32
	var expiryTime time.Time
	publishTime := time.Now()

	if p.Properties != nil && p.Properties.MessageExpiry != nil {
		messageExpiry = p.Properties.MessageExpiry
		expiryTime = publishTime.Add(time.Duration(*messageExpiry) * time.Second)
	}

	switch qos {
	case 0:
		// Zero-copy: Create ref-counted buffer from payload
		buf := core.GetBufferWithData(payload)
		msg := &storage.Message{
			Topic:         topic,
			QoS:           qos,
			Retain:        retain,
			MessageExpiry: messageExpiry,
			Expiry:        expiryTime,
			PublishTime:   publishTime,
		}
		msg.SetPayloadFromBuffer(buf)
		err := h.broker.Publish(msg)
		h.broker.logger.Debug("v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err),
		)
		return err

	case 1:
		// Zero-copy: Create ref-counted buffer from payload
		buf := core.GetBufferWithData(payload)
		msg := &storage.Message{
			Topic:         topic,
			QoS:           qos,
			Retain:        retain,
			MessageExpiry: messageExpiry,
			Expiry:        expiryTime,
			PublishTime:   publishTime,
		}
		msg.SetPayloadFromBuffer(buf)
		if err := h.broker.Publish(msg); err != nil {
			return sendV5PubAck(s, packetID, v5.PubAckUnspecifiedError, "Unspecified error")
		}
		h.broker.logger.Debug("v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)
		return sendV5PubAck(s, packetID, v5.PubAckSuccess, "")

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return sendV5PubRec(s, packetID, v5.PubRecSuccess, "")
		}

		s.Inflight().MarkReceived(packetID)

		// Zero-copy: Create ref-counted buffer from payload
		buf := core.GetBufferWithData(payload)

		// Publish message immediately (distribution to subscribers)
		msg := &storage.Message{
			Topic:         topic,
			QoS:           qos,
			Retain:        retain,
			MessageExpiry: messageExpiry,
			Expiry:        expiryTime,
			PublishTime:   publishTime,
		}
		msg.SetPayloadFromBuffer(buf)
		if err := h.broker.Publish(msg); err != nil {
			return sendV5PubRec(s, packetID, v5.PubRecUnspecifiedError, "Publish failed")
		}

		// Store for QoS 2 flow tracking - retain buffer for second message
		msg.PayloadBuf.Retain()
		storeMsg := &storage.Message{
			Topic:         topic,
			QoS:           2,
			Retain:        retain,
			PacketID:      packetID,
			MessageExpiry: messageExpiry,
			Expiry:        expiryTime,
			PublishTime:   publishTime,
		}
		storeMsg.SetPayloadFromBuffer(msg.PayloadBuf)
		if err := s.Inflight().Add(packetID, storeMsg, messages.Inbound); err != nil {
			return err
		}

		h.broker.logger.Debug("v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)

		return sendV5PubRec(s, packetID, v5.PubRecSuccess, "")
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
	rc := byte(0x00)
	rel := &v5.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          p.ID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(rel)
}

// HandlePubRel handles PUBREL packets.
func (h *V5Handler) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubRel)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v5_pubrel", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))

	packetID := p.ID

	// Message was already published when PUBLISH was received
	// PUBREL just confirms the handshake for QoS 2
	h.broker.AckMessage(s, packetID)
	s.Inflight().ClearReceived(packetID)

	rc := byte(0x00)
	comp := &v5.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(comp)
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
			reasonCodes[i] = v5.SubAckNotAuthorized
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

		opts := storage.SubscribeOptions{
			NoLocal:           noLocal,
			RetainAsPublished: retainAsPublished,
			RetainHandling:    retainHandling,
		}

		if err := h.broker.subscribe(s, t.Topic, t.MaxQoS, opts); err != nil {
			reasonCodes[i] = v5.SubAckImplementationSpecificError
			continue
		}

		reasonCodes[i] = t.MaxQoS

		// Send retained messages matching the subscription filter
		retained, err := h.broker.GetRetainedMatching(t.Topic)
		if err == nil {
			for _, msg := range retained {
				deliverQoS := msg.QoS
				if t.MaxQoS < deliverQoS {
					deliverQoS = t.MaxQoS
				}
				deliverMsg := &storage.Message{
					Topic:  msg.Topic,
					QoS:    deliverQoS,
					Retain: true,
				}
				// Zero-copy: Share buffer from retained message if available
				if msg.PayloadBuf != nil {
					msg.PayloadBuf.Retain()
					deliverMsg.SetPayloadFromBuffer(msg.PayloadBuf)
				} else {
					// Legacy fallback for messages without PayloadBuf
					deliverMsg.SetPayloadFromBytes(msg.Payload)
				}
				h.broker.DeliverToSession(s, deliverMsg)
			}
		}
	}

	h.broker.logger.Info("v5_subscribe_complete",
		slog.String("client_id", s.ID),
		slog.Duration("duration", time.Since(start)),
	)
	ack := &v5.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReasonCodes: &reasonCodes,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
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
			reasonCodes[i] = v5.ConnAckUnspecifiedError
		} else {
			reasonCodes[i] = v5.ConnAckSuccess
		}
	}

	h.broker.logger.Info("v5_unsubscribe_complete",
		slog.String("client_id", s.ID),
		slog.Duration("duration", time.Since(start)),
	)
	ack := &v5.UnsubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          p.ID,
		ReasonCodes: &reasonCodes,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

// HandlePingReq handles PINGREQ packets.
func (h *V5Handler) HandlePingReq(s *session.Session) error {
	h.broker.logger.Debug("v5_pingreq", slog.String("client_id", s.ID))
	resp := &v5.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
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

// deliverOfflineMessages sends queued messages to reconnected client.
func (h *V5Handler) deliverOfflineMessages(s *session.Session) {
	msgs := s.OfflineQueue().Drain()
	for _, msg := range msgs {
		h.broker.DeliverToSession(s, msg)
	}
}

func sendV5ConnAckWithProperties(conn core.Connection, s *session.Session, sessionPresent bool, reasonCode byte) error {
	receiveMax := uint16(maxReceived)
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
