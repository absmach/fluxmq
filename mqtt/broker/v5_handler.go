// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/message"
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

var _ protocolHandler = (*v5Handler)(nil)

// v5Handler is a stateless adapter that translates MQTT v5 packets to broker domain operations.
type v5Handler struct {
	broker *Broker
}

// newV5Handler creates a new V5 protocol handler.
func newV5Handler(broker *Broker) *v5Handler {
	return &v5Handler{broker: broker}
}

// HandleConnect handles CONNECT packets.
func (h *v5Handler) HandleConnect(ctx context.Context, conn core.Connection, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v5.Connect)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Info(
		"v5_connect",
		slog.String("remote_addr", conn.RemoteAddr().String()),
		slog.String("client_id", p.ClientID),
	)

	if rc := p.Validate(); rc != v5.Accepted {
		h.broker.telemetry.stats.IncrementProtocolErrors()
		sendV5ConnAck(conn, false, mapV5ConnectValidationReason(rc), nil) //nolint:errcheck // best-effort rejection reply before closing
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
				sendV5ConnAck(conn, false, v5.ConnAckInvalidClientID, nil) //nolint:errcheck // best-effort rejection reply before closing
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			h.broker.telemetry.stats.IncrementProtocolErrors()
			sendV5ConnAck(conn, false, v5.ConnAckInvalidClientID, nil) //nolint:errcheck // best-effort rejection reply before closing
			conn.Close()
			return ErrClientIDRequired
		}
	}

	externalID, boundMTLS, err := h.broker.authenticateMQTTConnect(ctx, mqttConnectCredentials{
		clientID:     clientID,
		username:     p.Username,
		password:     string(p.Password),
		usernameFlag: p.UsernameFlag,
		passwordFlag: p.PasswordFlag,
	})
	if err != nil {
		h.broker.telemetry.stats.IncrementAuthErrors()
		code := byte(v5.ConnAckNotAuthorized)
		if errors.Is(err, errMQTTCredentialsRejected) {
			code = v5.ConnAckBadUsernameOrPassword
		}
		sendV5ConnAck(conn, false, code, nil) //nolint:errcheck // best-effort rejection reply before closing
		conn.Close()
		return ErrNotAuthorized
	}

	var will *storage.WillMessage
	if p.WillFlag {
		if err := topics.ValidateTopicName(p.WillTopic); err != nil {
			h.broker.telemetry.stats.IncrementProtocolErrors()
			sendV5ConnAck(conn, false, v5.ConnAckTopicNameInvalid, nil) //nolint:errcheck // best-effort rejection reply before closing
			conn.Close()
			return ErrTopicInvalid
		}
		// Note: Will payload is stored as []byte in storage.WillMessage
		//nolint:godox // TODO: Consider zero-copy for will messages in future
		will = &storage.WillMessage{
			ClientID:   clientID,
			Topic:      p.WillTopic,
			Payload:    p.WillPayload,
			QoS:        p.WillQoS,
			Retain:     p.WillRetain,
			Properties: setOriginProperties(nil, externalID),
		}
		if p.WillProperties != nil && p.WillProperties.WillDelayInterval != nil {
			will.Delay = *p.WillProperties.WillDelayInterval
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
		ExternalID:     externalID,
		CleanStart:     cleanStart,
		KeepAlive:      time.Duration(p.KeepAlive) * time.Second,
		ReceiveMaximum: receiveMax,
		ExpiryInterval: sessionExpiry,
		Will:           will,
	}

	s, isNew, err := h.broker.CreateSession(clientID, p.ProtocolVersion, opts) //nolint:contextcheck // CreateSession has no context parameter yet; 73 call sites, tracked separately
	if err != nil {
		h.broker.telemetry.stats.IncrementProtocolErrors()
		connAckCode := byte(v5.ConnAckUnspecifiedError)
		if errors.Is(err, ErrMaxSessionsExceeded) {
			connAckCode = v5.ConnAckQuotaExceeded
		}
		sendV5ConnAck(conn, false, connAckCode, nil) //nolint:errcheck // best-effort rejection reply before closing
		conn.Close()
		return err
	}

	if !s.CanUseExternalIdentity(externalID, boundMTLS) {
		h.broker.telemetry.stats.IncrementAuthErrors()
		sendV5ConnAck(conn, false, v5.ConnAckNotAuthorized, nil) //nolint:errcheck // best-effort rejection reply before closing
		conn.Close()
		return ErrNotAuthorized
	}
	s.SetExternalIdentity(externalID)

	// Apply the negotiated options and take over any existing connection. On a
	// persistent reconnect this replaces the previous connection's version,
	// keep-alive, Will, Receive Maximum, and topic-alias maximum.
	// The advertised maximum QoS is applied with the epoch, so a takeover racing
	// a configuration reload cannot leave the connection enforcing a limit other
	// than the one its CONNACK announced.
	sessionMaxQoS := h.broker.MaxQoS()
	epoch, superseded := s.ConnectWithOptions(conn, session.ConnectOptions{
		Version:        p.ProtocolVersion,
		KeepAlive:      time.Duration(p.KeepAlive) * time.Second,
		Will:           will,
		ReceiveMaximum: receiveMax,
		TopicAliasMax:  topicAliasMax,
		MaxQoS:         sessionMaxQoS,
	})
	// Session expiry is applied verbatim on reconnect so a new value of 0
	// (expire on disconnect) replaces a previous positive one. A new session's
	// expiry, including the server default policy, was already set in
	// CreateSession.
	if !isNew {
		s.SetExpiryInterval(sessionExpiry)
	}
	if superseded != nil {
		go h.broker.drainSuperseded(context.WithoutCancel(ctx), superseded)
	}
	h.broker.BindExternalID(clientID, externalID)
	h.broker.persistSessionInfo(s)

	sessionPresent := !isNew && !cleanStart
	if err := sendV5ConnAckWithProperties(conn, s, sessionPresent, v5.ConnAckSuccess, sessionMaxQoS); err != nil {
		s.DisconnectIf(false, epoch, v5.DisconnectUnspecifiedError) //nolint:errcheck // disconnect on failed CONNACK; connection is already broken
		return err
	}

	h.broker.telemetry.stats.IncrementConnections()
	h.broker.telemetry.logger.Info(
		"v5_connect_success",
		slog.String("client_id", clientID),
		slog.Bool("session_present", sessionPresent),
		slog.Duration("duration", time.Since(start)),
	)

	h.broker.NotifyConnect(ctx, clientID, p.Username, "mqtt5")

	h.deliverOfflineMessages(ctx, s)

	return h.broker.runSession(ctx, h, s, conn, epoch, time.Duration(p.KeepAlive)*time.Second)
}

// HandlePublish handles PUBLISH packets.
func (h *v5Handler) HandlePublish(s *connCtx, pkt packets.ControlPacket) error {
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

	h.broker.telemetry.logger.Debug(
		"v5_publish",
		slog.String("client_id", s.ID),
		slog.String("topic", p.TopicName),
		slog.Int("qos", int(p.FixedHeader.QoS)),
	)

	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID

	// The inbound QoS selects the acknowledgement handshake, so it must stay as
	// the client sent it. Downgrading it here would answer a QoS 2 PUBLISH with
	// a PUBACK — or, at Maximum QoS 0, with nothing at all — leaving the
	// publisher retransmitting forever. The client was told the limit in the
	// CONNACK Maximum QoS property, so exceeding it is a protocol error:
	// [MQTT-3.2.2-11] requires DISCONNECT with 0x9B (QoS not supported).
	if maxQoS := s.MaxQoS(); qos > maxQoS {
		h.broker.telemetry.logger.Warn(
			"v5_publish_qos_not_supported",
			slog.String("client_id", s.ID),
			slog.Int("requested_qos", int(qos)),
			slog.Int("server_max_qos", int(maxQoS)),
		)
		s.Disconnect(false, v5.DisconnectQoSNotSupported) //nolint:errcheck // connection is being terminated
		return ErrQoSNotSupported
	}

	// The transports cap the whole packet, with an allowance for the topic and
	// properties. This is the limit broker.max_message_size actually documents:
	// the application payload.
	if maxSize := h.broker.MaxMessageSize(); maxSize > 0 && len(payload) > maxSize {
		h.broker.telemetry.logger.Warn(
			"v5_publish_payload_too_large",
			slog.String("client_id", s.ID),
			slog.Int("payload_size", len(payload)),
			slog.Int("max_message_size", maxSize),
		)
		s.Disconnect(false, v5.DisconnectPacketTooLarge) //nolint:errcheck // connection is being terminated
		return ErrPacketTooLarge
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
	properties := extractUserProperties(p.Properties)

	requestedTopic := topic
	hookReq, ok := h.broker.ApplyPublishHooks(context.Background(), corebroker.BlockingHookRequest{
		ClientID:   s.ID,
		ExternalID: s.ExternalIdentity(),
		Protocol:   corebroker.HookProtocolMQTT,
		Topic:      topic,
		Payload:    payload,
		QoS:        qos,
		Retain:     retain,
		Properties: properties,
	})
	if !ok {
		h.broker.telemetry.stats.IncrementAuthzErrors()
		return sendV5PublishError(s, qos, packetID, v5.PubAckNotAuthorized, "Not authorized", nil)
	}
	if hookReq.QoS != qos {
		h.broker.telemetry.logger.Warn("v5_publish_hook_qos_mutation_rejected",
			slog.String("client_id", s.ID),
			slog.String("topic", topic),
			slog.Int("requested_qos", int(qos)),
			slog.Int("hook_qos", int(hookReq.QoS)))
		h.broker.telemetry.stats.IncrementProtocolErrors()
		return sendV5PublishError(s, qos, packetID, v5.PubAckImplementationSpecificError, "QoS mutation not supported", ErrProtocolViolation)
	}
	// QoS is carried through unchanged: hooks that mutate it were rejected
	// above, and the wire QoS still owns the acknowledgement handshake.
	topic, payload, retain, properties = hookReq.Topic, hookReq.Payload, hookReq.Retain, hookReq.Properties
	sourceExternalID := hookReq.ExternalID
	// A hook can rewrite the payload, so the limit is re-checked on the result.
	// Overshooting here is the hook's doing, not the client's, so the publish is
	// refused without tearing the connection down.
	if maxSize := h.broker.MaxMessageSize(); maxSize > 0 && len(payload) > maxSize {
		h.broker.telemetry.logger.Warn("v5_publish_hook_payload_too_large",
			slog.String("client_id", s.ID),
			slog.String("topic", topic),
			slog.Int("payload_size", len(payload)),
			slog.Int("max_message_size", maxSize))
		h.broker.telemetry.stats.IncrementProtocolErrors()
		return sendV5PublishError(s, qos, packetID, v5.PubAckImplementationSpecificError, "Payload exceeds maximum size", ErrPacketTooLarge)
	}
	if topic != requestedTopic {
		if err := topics.ValidateTopicName(topic); err != nil {
			return sendV5PublishError(s, qos, packetID, v5.PubAckTopicNameInvalid, "Topic name invalid", ErrTopicInvalid)
		}
	}
	if h.broker.auth != nil && !h.broker.CanPublishIdentity(s.ctx, s.AuthorizationIdentity(), topic) {
		h.broker.telemetry.stats.IncrementAuthzErrors()
		return sendV5PublishError(s, qos, packetID, v5.PubAckNotAuthorized, "Not authorized", nil)
	}

	switch qos {
	case 0:
		msg := newMQTTEnvelope(topic, payload, s.ID, sourceExternalID, qos, retain, properties)
		setMQTT5Metadata(msg, messageExpiry, expiryTime, publishTime, payloadFormat, contentType, responseTopic, correlationData)
		err := h.broker.Publish(context.Background(), msg)
		h.broker.telemetry.logger.Debug(
			"v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err),
		)
		return err

	case 1:
		msg := newMQTTEnvelope(topic, payload, s.ID, sourceExternalID, qos, retain, properties)
		setMQTT5Metadata(msg, messageExpiry, expiryTime, publishTime, payloadFormat, contentType, responseTopic, correlationData)
		if err := h.broker.Publish(context.Background(), msg); err != nil {
			reasonCode, reason := mqtt5QueuePublishError(err)
			return sendV5PubAck(s, packetID, reasonCode, reason)
		}
		h.broker.telemetry.logger.Debug(
			"v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)
		return sendV5PubAck(s, packetID, v5.PubAckSuccess, "")

	case 2:
		storeMsg := newMQTTEnvelope(topic, payload, s.ID, sourceExternalID, qos, retain, properties)
		storeMsg.BrokerMeta.Delivery.PacketID = packetID
		setMQTT5Metadata(storeMsg, messageExpiry, expiryTime, publishTime, payloadFormat, contentType, responseTopic, correlationData)
		accepted, err := s.AddInbound(packetID, storeMsg)
		if !accepted {
			message.Release(storeMsg)
		}
		if err != nil {
			return err
		}

		h.broker.telemetry.logger.Debug(
			"v5_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)

		return sendV5PubRec(s, packetID, v5.PubRecSuccess, "")
	}

	return nil
}

// HandlePubAck handles PUBACK packets.
func (h *v5Handler) HandlePubAck(s *connCtx, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubAck)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v5_puback", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s.Session, p.ID)
}

// HandlePubRec handles PUBREC packets.
func (h *v5Handler) HandlePubRec(s *connCtx, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubRec)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v5_pubrec", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived) //nolint:errcheck // state update for in-flight QoS2; non-fatal if packet not tracked
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
func (h *v5Handler) HandlePubRel(s *connCtx, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubRel)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v5_pubrel", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))

	packetID := p.ID
	rc := byte(0x00)
	comp := &v5.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}

	found, err := h.broker.completeInboundQoS2(s, packetID, "v5_pubrel")
	if err != nil {
		return err
	}
	if !found {
		// MQTT 5.0 defines 0x92 for a PUBREL naming a packet ID this session
		// does not hold. Answering 0x00 tells the publisher a transaction it
		// never had was completed.
		rc = v5.PubCompPacketIdentifierNotFound
	}

	return s.WritePacket(comp)
}

// HandlePubComp handles PUBCOMP packets.
func (h *v5Handler) HandlePubComp(s *connCtx, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v5.PubComp)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v5_pubcomp", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s.Session, p.ID)
}

// HandleSubscribe handles SUBSCRIBE packets.
func (h *v5Handler) HandleSubscribe(s *connCtx, pkt packets.ControlPacket) error {
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

		filter := t.Topic
		subQoS := t.MaxQoS
		filter, subQoS, ok = h.broker.ApplySubscribeHooks(context.Background(), s.ID, s.ExternalIdentity(), corebroker.HookProtocolMQTT, filter, subQoS)
		if !ok {
			h.broker.telemetry.stats.IncrementAuthzErrors()
			reasonCodes[i] = v5.SubAckNotAuthorized
			continue
		}
		if subQoS > 2 {
			reasonCodes[i] = v5.SubAckTopicFilterInvalid
			continue
		}
		if filter != t.Topic {
			if err := topics.ValidateTopicFilter(filter); err != nil {
				reasonCodes[i] = v5.SubAckTopicFilterInvalid
				continue
			}
			s.AddSubscriptionAlias(t.Topic, filter)
		}
		if h.broker.auth != nil && !h.broker.CanSubscribeIdentity(s.ctx, s.AuthorizationIdentity(), filter) {
			h.broker.telemetry.stats.IncrementAuthzErrors()
			reasonCodes[i] = v5.SubAckNotAuthorized
			continue
		}

		// Check subscription rate limit
		if h.broker.rateLimiter != nil && !h.broker.rateLimiter.AllowSubscribe(s.ID) {
			h.broker.telemetry.logger.Warn("v5_subscribe_rate_limit",
				slog.String("client_id", s.ID),
				slog.String("topic", filter))
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
		wasSubscribed := s.HasSubscription(filter)

		opts := storage.SubscribeOptions{
			NoLocal:           noLocal,
			RetainAsPublished: retainAsPublished,
			RetainHandling:    retainHandling,
			ConsumerGroup:     consumerGroup,
		}
		grantedQoS := subQoS
		if maxQoS := h.broker.MaxQoS(); grantedQoS > maxQoS {
			grantedQoS = maxQoS
		}

		if err := h.broker.subscribe(s.Session, filter, grantedQoS, opts); err != nil {
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
			retained, err := h.broker.GetRetainedMatching(filter)
			if err == nil {
				for _, msg := range retained {
					deliverQoS := msg.BrokerMeta.Delivery.QoS
					if grantedQoS < deliverQoS {
						deliverQoS = grantedQoS
					}
					msg.BrokerMeta.Delivery.QoS = deliverQoS
					msg.BrokerMeta.Delivery.Retain = true
					h.broker.DeliverToSession(context.Background(), s.Session, msg) //nolint:errcheck // retained message delivery; errors are non-fatal
				}
			}
		}
	}

	h.broker.telemetry.logger.Info(
		"v5_subscribe_complete",
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
func (h *v5Handler) HandleUnsubscribe(s *connCtx, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v5.Unsubscribe)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Info("v5_unsubscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Topics)))

	reasonCodes := make([]byte, len(p.Topics))
	for i, filter := range p.Topics {
		if resolved := s.ResolveSubscriptionAlias(filter); resolved != filter {
			filter = resolved
		} else {
			filter, ok = h.broker.ApplyUnsubscribeHooks(context.Background(), s.ID, s.ExternalIdentity(), corebroker.HookProtocolMQTT, filter)
			if !ok {
				h.broker.telemetry.stats.IncrementAuthzErrors()
				reasonCodes[i] = v5.UnsubAckNotAuthorized
				continue
			}
		}
		if err := h.broker.unsubscribeInternal(s.Session, filter); err != nil {
			reasonCodes[i] = v5.UnsubAckUnspecifiedError
		} else {
			reasonCodes[i] = v5.UnsubAckSuccess
		}
	}

	h.broker.telemetry.logger.Info(
		"v5_unsubscribe_complete",
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
func (h *v5Handler) HandlePingReq(s *connCtx) error {
	h.broker.telemetry.logger.Debug("v5_pingreq", slog.String("client_id", s.ID))

	// Update heartbeat for queue consumers
	// Fire and forget - don't block PINGRESP on this.
	// Updates are interval-limited to avoid goroutine storms under ping floods.
	maybeUpdateQueueHeartbeat(h.broker, s.Session)

	resp := &v5.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
}

// HandleDisconnect handles DISCONNECT packets.
func (h *v5Handler) HandleDisconnect(s *connCtx, pkt packets.ControlPacket) error {
	_, ok := pkt.(*v5.Disconnect)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Info("v5_disconnect", slog.String("client_id", s.ID))
	s.Disconnect(true, v5.DisconnectNormalDisconnection) //nolint:errcheck // graceful disconnect initiated by client
	return io.EOF
}

// HandleAuth handles AUTH packets.
func (h *v5Handler) HandleAuth(s *connCtx, pkt packets.ControlPacket) error {
	_, ok := pkt.(*v5.Auth)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v5_auth", slog.String("client_id", s.ID))
	return nil
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (h *v5Handler) deliverOfflineMessages(ctx context.Context, s *session.Session) {
	msgs := s.OfflineQueue().Drain()
	for _, msg := range msgs {
		h.broker.DeliverToSession(ctx, s, msg) //nolint:errcheck // offline message delivery; errors are non-fatal
	}
}

func sendV5ConnAckWithProperties(conn core.Connection, s *session.Session, sessionPresent bool, reasonCode byte, maxQoS byte) error {
	// Advertise the server's actual inbound Receive Maximum (the bidirectional
	// inflight store capacity), not the protocol default, so the client does not
	// send more concurrent QoS 1/2 PUBLISH packets than the broker accepts.
	receiveMax := uint16(s.ServerMaxInflight())
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

func sendV5PublishError(s *connCtx, qos byte, packetID uint16, reasonCode byte, reasonString string, qos0Err error) error {
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

func sendV5PubAck(s *connCtx, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	ack := &v5.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(ack)
}

func sendV5PubRec(s *connCtx, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	rec := &v5.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}
	return s.WritePacket(rec)
}
