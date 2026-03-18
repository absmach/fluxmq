// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"io"
	"log/slog"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/packets"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/absmach/fluxmq/topics"
)

const (
	maxReceived     = uint16(65535)
	topicAliasMax   = uint16(10)
	noSessionExpiry = uint32(0)
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

	h.broker.telemetry.logger.Info("v5_connect",
		slog.String("remote_addr", conn.RemoteAddr().String()),
		slog.String("client_id", p.ClientID),
	)

	if rc := p.Validate(); rc != v5.Accepted {
		h.broker.telemetry.stats.IncrementProtocolErrors()
		sendV5ConnAck(conn, false, mapV5ConnectValidationReason(rc), nil)
		conn.Close()
		return ErrProtocolViolation
	}

	clientID := p.ClientID
	cleanStart := p.CleanStart

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				h.broker.telemetry.stats.IncrementProtocolErrors()
				sendV5ConnAck(conn, false, v5.ConnAckInvalidClientID, nil)
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			h.broker.telemetry.stats.IncrementProtocolErrors()
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
			h.broker.telemetry.stats.IncrementAuthErrors()
			sendV5ConnAck(conn, false, v5.ConnAckBadUsernameOrPassword, nil)
			conn.Close()
			return ErrNotAuthorized
		}
	}

	var will *storage.WillMessage
	if p.WillFlag {
		if err := topics.ValidateTopicName(p.WillTopic); err != nil {
			h.broker.telemetry.stats.IncrementProtocolErrors()
			sendV5ConnAck(conn, false, v5.ConnAckTopicNameInvalid, nil)
			conn.Close()
			return ErrTopicInvalid
		}
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

	receiveMax := maxReceived
	topicAliasMax := topicAliasMax
	sessionExpiry := noSessionExpiry

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
		h.broker.telemetry.stats.IncrementProtocolErrors()
		sendV5ConnAck(conn, false, v5.ConnAckUnspecifiedError, nil)
		conn.Close()
		return err
	}

	s.TopicAliasMax = topicAliasMax

	if err := s.Connect(conn); err != nil {
		h.broker.telemetry.stats.IncrementProtocolErrors()
		conn.Close()
		return err
	}
	h.broker.persistSessionInfo(s)

	sessionPresent := !isNew && !cleanStart
	if err := sendV5ConnAckWithProperties(conn, s, sessionPresent, v5.ConnAckSuccess, h.broker.MaxQoS()); err != nil {
		s.Disconnect(false)
		return err
	}

	h.broker.telemetry.stats.IncrementConnections()
	h.broker.telemetry.logger.Info("v5_connect_success",
		slog.String("client_id", clientID),
		slog.Bool("session_present", sessionPresent),
		slog.Duration("duration", time.Since(start)),
	)

	h.broker.NotifyConnect(clientID, p.Username, "mqtt5")

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

	// Check client rate limit
	if h.broker.rateLimiter != nil && !h.broker.rateLimiter.AllowPublish(s.ID) {
		h.broker.telemetry.logger.Warn("v5_publish_rate_limit",
			slog.String("client_id", s.ID),
			slog.String("topic", p.TopicName))
		// Return QuotaExceeded for QoS > 0, silently drop for QoS 0
		if p.FixedHeader.QoS > 0 {
			return sendV5PublishError(s, p.FixedHeader.QoS, p.ID, v5.PubAckQuotaExceeded, "Rate limit exceeded", nil)
		}
		return nil
	}

	h.broker.telemetry.logger.Debug("v5_publish",
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

	// Downgrade QoS if it exceeds server's maximum (MQTT 5.0 spec 3.3.2-4)
	if maxQoS := h.broker.MaxQoS(); qos > maxQoS {
		h.broker.telemetry.logger.Debug("v5_publish_qos_downgrade",
			slog.String("client_id", s.ID),
			slog.Int("requested_qos", int(qos)),
			slog.Int("server_max_qos", int(maxQoS)),
		)
		qos = maxQoS
	}

	if p.Properties != nil && p.Properties.TopicAlias != nil {
		alias := *p.Properties.TopicAlias
		if alias > s.TopicAliasMax {
			return sendV5PublishError(s, qos, packetID, v5.PubAckTopicNameInvalid, "Topic alias invalid", ErrTopicInvalid)
		}
		if topic == "" {
			resolvedTopic, ok := s.ResolveInboundAlias(alias)
			if !ok {
				return sendV5PublishError(s, qos, packetID, v5.PubAckTopicNameInvalid, "Topic alias not established", ErrTopicInvalid)
			}
			topic = resolvedTopic
		} else {
			s.SetInboundAlias(alias, topic)
		}
	}

	if err := topics.ValidateTopicName(topic); err != nil {
		return sendV5PublishError(s, qos, packetID, v5.PubAckTopicNameInvalid, "Topic name invalid", ErrTopicInvalid)
	}

	if h.broker.auth != nil && !h.broker.auth.CanPublish(s.ID, topic) {
		h.broker.telemetry.stats.IncrementAuthzErrors()
		return sendV5PublishError(s, qos, packetID, v5.PubAckNotAuthorized, "Not authorized", nil)
	}

	// Extract message expiry interval if present
	var messageExpiry *uint32
	var expiryTime time.Time
	publishTime := time.Now()
	var payloadFormat *byte
	var contentType string
	var responseTopic string
	var correlationData []byte

	if p.Properties != nil && p.Properties.MessageExpiry != nil {
		messageExpiry = p.Properties.MessageExpiry
		expiryTime = publishTime.Add(time.Duration(*messageExpiry) * time.Second)
	}
	if p.Properties != nil {
		payloadFormat = p.Properties.PayloadFormat
		contentType = p.Properties.ContentType
		responseTopic = p.Properties.ResponseTopic
		correlationData = p.Properties.CorrelationData
	}

	// Extract MQTT v5 properties for queue functionality
	properties := extractAllProperties(p.Properties)

	switch qos {
	case 0:
		buf := core.GetBufferWithData(payload)
		msg := storage.AcquireMessage()
		msg.Topic = topic
		msg.PublisherID = s.ID
		msg.QoS = qos
		msg.Retain = retain
		msg.MessageExpiry = messageExpiry
		msg.Expiry = expiryTime
		msg.PublishTime = publishTime
		msg.Properties = properties
		msg.PayloadFormat = payloadFormat
		msg.ContentType = contentType
		msg.ResponseTopic = responseTopic
		msg.CorrelationData = correlationData
		msg.SetPayloadFromBuffer(buf)
		err := h.broker.Publish(msg)
		storage.ReleaseMessage(msg)
		h.broker.telemetry.logger.Debug("v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err),
		)
		return err

	case 1:
		buf := core.GetBufferWithData(payload)
		msg := storage.AcquireMessage()
		msg.Topic = topic
		msg.PublisherID = s.ID
		msg.QoS = qos
		msg.Retain = retain
		msg.MessageExpiry = messageExpiry
		msg.Expiry = expiryTime
		msg.PublishTime = publishTime
		msg.Properties = properties
		msg.PayloadFormat = payloadFormat
		msg.ContentType = contentType
		msg.ResponseTopic = responseTopic
		msg.CorrelationData = correlationData
		msg.SetPayloadFromBuffer(buf)
		if err := h.broker.Publish(msg); err != nil {
			storage.ReleaseMessage(msg)
			return sendV5PubAck(s, packetID, v5.PubAckUnspecifiedError, "Unspecified error")
		}
		storage.ReleaseMessage(msg)
		h.broker.telemetry.logger.Debug("v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)
		return sendV5PubAck(s, packetID, v5.PubAckSuccess, "")

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return sendV5PubRec(s, packetID, v5.PubRecSuccess, "")
		}

		s.Inflight().MarkReceived(packetID)

		buf := core.GetBufferWithData(payload)
		storeMsg := storage.AcquireMessage()
		storeMsg.Topic = topic
		storeMsg.PublisherID = s.ID
		storeMsg.QoS = qos
		storeMsg.Retain = retain
		storeMsg.PacketID = packetID
		storeMsg.MessageExpiry = messageExpiry
		storeMsg.Expiry = expiryTime
		storeMsg.PublishTime = publishTime
		storeMsg.Properties = properties
		storeMsg.PayloadFormat = payloadFormat
		storeMsg.ContentType = contentType
		storeMsg.ResponseTopic = responseTopic
		storeMsg.CorrelationData = correlationData
		storeMsg.SetPayloadFromBuffer(buf)
		if err := s.Inflight().Add(packetID, storeMsg, messages.Inbound); err != nil {
			storeMsg.ReleasePayload()
			storage.ReleaseMessage(storeMsg)
			return err
		}

		h.broker.telemetry.logger.Debug("v5_publish_complete",
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

	h.broker.telemetry.logger.Debug("v5_puback", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s, p.ID)
}

// HandlePubRec handles PUBREC packets.
func (h *V5Handler) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubRec)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v5_pubrec", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
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

	h.broker.telemetry.logger.Debug("v5_pubrel", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))

	packetID := p.ID

	// Distribute stored message now that publisher has committed with PUBREL.
	msg, err := s.Inflight().Ack(packetID)
	if err != nil {
		h.broker.telemetry.logger.Warn("v5_pubrel_unknown_packet",
			slog.String("client_id", s.ID),
			slog.Int("packet_id", int(packetID)))
	}
	s.Inflight().ClearReceived(packetID)

	rc := byte(0x00)
	comp := &v5.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}

	if h.broker.cfg.asyncFanOut {
		if msg != nil {
			submitted := h.broker.fanOutPool != nil && h.broker.fanOutPool.Submit(func() {
				if err := h.broker.Publish(msg); err != nil {
					h.broker.logError("v5_pubrel_publish", err,
						slog.String("client_id", s.ID),
						slog.String("topic", msg.Topic))
				}
				storage.ReleaseMessage(msg)
			})
			if !submitted {
				if err := h.broker.Publish(msg); err != nil {
					h.broker.logError("v5_pubrel_publish", err,
						slog.String("client_id", s.ID),
						slog.String("topic", msg.Topic))
				}
				storage.ReleaseMessage(msg)
			}
		}
		return s.WritePacket(comp)
	}

	// Synchronous path: distribute before PUBCOMP (default).
	if msg != nil {
		if err := h.broker.Publish(msg); err != nil {
			h.broker.logError("v5_pubrel_publish", err,
				slog.String("client_id", s.ID),
				slog.String("topic", msg.Topic))
		}
		storage.ReleaseMessage(msg)
	}
	return s.WritePacket(comp)
}

// HandlePubComp handles PUBCOMP packets.
func (h *V5Handler) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubComp)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v5_pubcomp", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s, p.ID)
}

// HandleSubscribe handles SUBSCRIBE packets.
func (h *V5Handler) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v5.Subscribe)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Info("v5_subscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Opts)))

	packetID := p.ID

	reasonCodes := make([]byte, len(p.Opts))
	for i, t := range p.Opts {
		if err := topics.ValidateTopicFilter(t.Topic); err != nil {
			reasonCodes[i] = v5.SubAckTopicFilterInvalid
			continue
		}
		if t.MaxQoS > 2 {
			reasonCodes[i] = v5.SubAckTopicFilterInvalid
			continue
		}

		if h.broker.auth != nil && !h.broker.auth.CanSubscribe(s.ID, t.Topic) {
			h.broker.telemetry.stats.IncrementAuthzErrors()
			reasonCodes[i] = v5.SubAckNotAuthorized
			continue
		}

		// Check subscription rate limit
		if h.broker.rateLimiter != nil && !h.broker.rateLimiter.AllowSubscribe(s.ID) {
			h.broker.telemetry.logger.Warn("v5_subscribe_rate_limit",
				slog.String("client_id", s.ID),
				slog.String("topic", t.Topic))
			reasonCodes[i] = v5.SubAckQuotaExceeded
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

		// Extract consumer group from subscription properties
		consumerGroup := extractConsumerGroup(s.ID, p.Properties)
		wasSubscribed := s.HasSubscription(t.Topic)

		opts := storage.SubscribeOptions{
			NoLocal:           noLocal,
			RetainAsPublished: retainAsPublished,
			RetainHandling:    retainHandling,
			ConsumerGroup:     consumerGroup,
		}
		grantedQoS := t.MaxQoS
		if maxQoS := h.broker.MaxQoS(); grantedQoS > maxQoS {
			grantedQoS = maxQoS
		}

		if err := h.broker.subscribe(s, t.Topic, grantedQoS, opts); err != nil {
			reasonCodes[i] = v5.SubAckImplementationSpecificError
			continue
		}
		reasonCodes[i] = grantedQoS

		sendRetained := true
		switch retainHandling {
		case 1:
			sendRetained = !wasSubscribed
		case 2:
			sendRetained = false
		}
		if sendRetained {
			// Send retained messages matching the subscription filter
			retained, err := h.broker.GetRetainedMatching(t.Topic)
			if err == nil {
				for _, msg := range retained {
					deliverQoS := msg.QoS
					if grantedQoS < deliverQoS {
						deliverQoS = grantedQoS
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
	}

	h.broker.telemetry.logger.Info("v5_subscribe_complete",
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

	h.broker.telemetry.logger.Info("v5_unsubscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Topics)))

	reasonCodes := make([]byte, len(p.Topics))
	for i, filter := range p.Topics {
		if err := h.broker.unsubscribeInternal(s, filter); err != nil {
			reasonCodes[i] = v5.ConnAckUnspecifiedError
		} else {
			reasonCodes[i] = v5.ConnAckSuccess
		}
	}

	h.broker.telemetry.logger.Info("v5_unsubscribe_complete",
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
	h.broker.telemetry.logger.Debug("v5_pingreq", slog.String("client_id", s.ID))

	// Update heartbeat for queue consumers
	// Fire and forget - don't block PINGRESP on this.
	// Updates are interval-limited to avoid goroutine storms under ping floods.
	maybeUpdateQueueHeartbeat(h.broker, s)

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

	h.broker.telemetry.logger.Info("v5_disconnect", slog.String("client_id", s.ID))
	s.Disconnect(true)
	return io.EOF
}

// HandleAuth handles AUTH packets.
func (h *V5Handler) HandleAuth(s *session.Session, pkt packets.ControlPacket) error {
	_, ok := pkt.(*v5.Auth)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v5_auth", slog.String("client_id", s.ID))
	return nil
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (h *V5Handler) deliverOfflineMessages(s *session.Session) {
	msgs := s.OfflineQueue().Drain()
	for _, msg := range msgs {
		h.broker.DeliverToSession(s, msg)
	}
}

func sendV5ConnAckWithProperties(conn core.Connection, s *session.Session, sessionPresent bool, reasonCode byte, maxQoS byte) error {
	receiveMax := maxReceived
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
	if maxQoS < 2 {
		props.MaxQoS = &maxQoS
	}

	ack := &v5.ConnAck{
		FixedHeader:    packets.FixedHeader{PacketType: packets.ConnAckType},
		SessionPresent: sessionPresent,
		ReasonCode:     reasonCode,
		Properties:     props,
	}

	return conn.WritePacket(ack)
}

func mapV5ConnectValidationReason(code byte) byte {
	switch code {
	case v5.ErrRefusedBadProtocolVersion:
		return v5.ConnAckUnsupportedProtocolVersion
	case v5.ErrRefusedIDRejected:
		return v5.ConnAckInvalidClientID
	case v5.ErrRefusedBadUsernameOrPassword:
		return v5.ConnAckBadUsernameOrPassword
	case v5.ErrRefusedNotAuthorized:
		return v5.ConnAckNotAuthorized
	case v5.ErrProtocolViolation:
		return v5.ConnAckProtocolError
	default:
		return v5.ConnAckMalformedPacket
	}
}

func sendV5PublishError(s *session.Session, qos byte, packetID uint16, reasonCode byte, reasonString string, qos0Err error) error {
	switch qos {
	case 1:
		return sendV5PubAck(s, packetID, reasonCode, reasonString)
	case 2:
		return sendV5PubRec(s, packetID, reasonCode, reasonString)
	default:
		if qos0Err != nil {
			return qos0Err
		}
		return nil
	}
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
