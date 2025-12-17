// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"io"
	"log/slog"
	"time"

	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	v3 "github.com/absmach/mqtt/core/packets/v3"
	"github.com/absmach/mqtt/session"
	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/storage/messages"
)

var _ Handler = (*V3Handler)(nil)

// V3Handler is a stateless adapter that translates MQTT v3/v4 packets to broker domain operations.
type V3Handler struct {
	broker *Broker
}

// NewV3Handler creates a new V3 protocol handler.
func NewV3Handler(broker *Broker) *V3Handler {
	return &V3Handler{broker: broker}
}

// HandleConnect handles CONNECT packets.
func (h *V3Handler) HandleConnect(conn core.Connection, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v3.Connect)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Info("v3_connect",
		slog.String("remote_addr", conn.RemoteAddr().String()),
		slog.String("client_id", p.ClientID),
	)

	clientID := p.ClientID
	cleanStart := p.CleanSession

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				h.broker.stats.IncrementProtocolErrors()
				sendV3ConnAck(conn, false, 0x02)
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			h.broker.stats.IncrementProtocolErrors()
			sendV3ConnAck(conn, false, 0x02)
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

	s, isNew, err := h.broker.CreateSession(clientID, opts)
	if err != nil {
		h.broker.stats.IncrementProtocolErrors()
		sendV3ConnAck(conn, false, 0x03)
		conn.Close()
		return err
	}

	s.Version = p.ProtocolVersion
	s.KeepAlive = time.Duration(p.KeepAlive) * time.Second

	if err := s.Connect(conn); err != nil {
		h.broker.stats.IncrementProtocolErrors()
		conn.Close()
		return err
	}

	sessionPresent := !isNew && !cleanStart
	if err := sendV3ConnAck(conn, sessionPresent, 0x00); err != nil {
		s.Disconnect(false)
		return err
	}

	h.broker.stats.IncrementConnections()
	h.broker.logger.Info("v3_connect_success",
		slog.String("client_id", clientID),
		slog.Bool("session_present", sessionPresent),
		slog.Duration("duration", time.Since(start)),
	)

	h.deliverOfflineMessages(s)

	return h.broker.runSession(h, s)
}

// HandlePublish handles PUBLISH packets.
func (h *V3Handler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v3.Publish)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v3_publish",
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

	if h.broker.auth != nil && !h.broker.auth.CanPublish(s.ID, topic) {
		h.broker.stats.IncrementAuthzErrors()
		return ErrNotAuthorized
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
		h.broker.logger.Debug("v3_publish_complete",
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
			return err
		}
		h.broker.logger.Debug("v3_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)
		return sendV3PubAck(s, packetID)

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return sendV3PubRec(s, packetID)
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

		return sendV3PubRec(s, packetID)
	}

	return nil
}

// HandlePubAck handles PUBACK packets.
func (h *V3Handler) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubAck)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v3_puback", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s, p.ID)
}

// HandlePubRec handles PUBREC packets.
func (h *V3Handler) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubRec)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v3_pubrec", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived)
	return sendV3PubRel(s, p.ID)
}

// HandlePubRel handles PUBREL packets.
func (h *V3Handler) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubRel)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v3_pubrel", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))

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

	return sendV3PubComp(s, packetID)
}

// HandlePubComp handles PUBCOMP packets.
func (h *V3Handler) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubComp)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Debug("v3_pubcomp", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s, p.ID)
}

// HandleSubscribe handles SUBSCRIBE packets.
func (h *V3Handler) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v3.Subscribe)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Info("v3_subscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Topics)))

	packetID := p.ID

	reasonCodes := make([]byte, len(p.Topics))
	for i, t := range p.Topics {
		if h.broker.auth != nil && !h.broker.auth.CanSubscribe(s.ID, t.Name) {
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

	h.broker.logger.Info("v3_subscribe_complete",
		slog.String("client_id", s.ID),
		slog.Duration("duration", time.Since(start)),
	)

	return sendV3SubAck(s, packetID, reasonCodes)
}

// HandleUnsubscribe handles UNSUBSCRIBE packets.
func (h *V3Handler) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v3.Unsubscribe)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Info("v3_unsubscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Topics)))

	for _, filter := range p.Topics {
		h.broker.unsubscribeInternal(s, filter)
	}

	h.broker.logger.Info("v3_unsubscribe_complete",
		slog.String("client_id", s.ID),
		slog.Duration("duration", time.Since(start)),
	)

	return sendV3UnsubAck(s, p.ID)
}

// HandlePingReq handles PINGREQ packets.
func (h *V3Handler) HandlePingReq(s *session.Session) error {
	h.broker.logger.Debug("v3_pingreq", slog.String("client_id", s.ID))
	return sendV3PingResp(s)
}

// HandleDisconnect handles DISCONNECT packets.
func (h *V3Handler) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	_, ok := pkt.(*v3.Disconnect)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.logger.Info("v3_disconnect", slog.String("client_id", s.ID))
	s.Disconnect(true)
	return io.EOF
}

// HandleAuth - not supported in V3.
func (h *V3Handler) HandleAuth(s *session.Session, pkt packets.ControlPacket) error {
	return ErrInvalidPacketType
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
