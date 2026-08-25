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
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
	"github.com/absmach/fluxmq/topics"
)

var _ protocolHandler = (*v3Handler)(nil)

// v3Handler is a stateless adapter that translates MQTT v3/v4 packets to broker domain operations.
type v3Handler struct {
	broker *Broker
}

// newV3Handler creates a new V3 protocol handler.
func newV3Handler(broker *Broker) *v3Handler {
	return &v3Handler{broker: broker}
}

// HandleConnect handles CONNECT packets.
func (h *v3Handler) HandleConnect(ctx context.Context, conn core.Connection, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v3.Connect)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Info("v3_connect",
		slog.String("remote_addr", conn.RemoteAddr().String()),
		slog.String("client_id", p.ClientID),
	)

	if err := p.Validate(); err != nil {
		h.broker.telemetry.stats.IncrementProtocolErrors()
		code := byte(v3.ConnAckIdentifierRejected)
		if errors.Is(err, v3.ErrInvalidProtocolName) {
			code = v3.ConnAckUnacceptableProtocol
		}
		sendV3ConnAck(conn, false, code) //nolint:errcheck // best-effort rejection reply before closing
		conn.Close()
		return ErrProtocolViolation
	}

	clientID := p.ClientID
	cleanStart := p.CleanSession

	if clientID == "" {
		if cleanStart {
			generated, err := GenerateClientID()
			if err != nil {
				h.broker.telemetry.stats.IncrementProtocolErrors()
				sendV3ConnAck(conn, false, v3.ConnAckIdentifierRejected) //nolint:errcheck // best-effort rejection reply before closing
				conn.Close()
				return err
			}
			clientID = generated
		} else {
			h.broker.telemetry.stats.IncrementProtocolErrors()
			sendV3ConnAck(conn, false, v3.ConnAckIdentifierRejected) //nolint:errcheck // best-effort rejection reply before closing
			conn.Close()
			return ErrClientIDRequired
		}
	}

	externalID := ""
	if h.broker.auth != nil {
		username := p.Username
		password := string(p.Password)

		authenticated, resolvedID, err := h.broker.auth.Authenticate(ctx, clientID, username, password)
		if err != nil || !authenticated {
			h.broker.telemetry.stats.IncrementAuthErrors()
			sendV3ConnAck(conn, false, v3.ConnAckBadUsernameOrPassword) //nolint:errcheck // best-effort rejection reply before closing
			conn.Close()
			return ErrNotAuthorized
		}
		externalID = resolvedID
	}
	hookExternalID, ok := h.broker.ApplyRegisterHooks(ctx, clientID, externalID, p.Username, string(p.Password), corebroker.HookProtocolMQTT)
	if !ok {
		h.broker.telemetry.stats.IncrementAuthErrors()
		sendV3ConnAck(conn, false, v3.ConnAckNotAuthorized) //nolint:errcheck // best-effort rejection reply before closing
		conn.Close()
		return ErrNotAuthorized
	}
	externalID = hookExternalID

	var will *storage.WillMessage
	if p.WillFlag {
		if err := topics.ValidateTopicName(p.WillTopic); err != nil {
			h.broker.telemetry.stats.IncrementProtocolErrors()
			sendV3ConnAck(conn, false, v3.ConnAckIdentifierRejected) //nolint:errcheck // best-effort rejection reply before closing
			conn.Close()
			return ErrTopicInvalid
		}
		// Note: Will payload is stored as []byte in storage.WillMessage
		//nolint:godox // TODO: Consider zero-copy for will messages in future
		will = &storage.WillMessage{
			ClientID:   clientID,
			Topic:      p.WillTopic,
			Payload:    p.WillMessage,
			QoS:        p.WillQoS,
			Retain:     p.WillRetain,
			Properties: setOriginProperties(nil, externalID),
		}
	}

	opts := session.Options{
		CleanStart:     cleanStart,
		KeepAlive:      time.Duration(p.KeepAlive) * time.Second,
		ReceiveMaximum: 65535,
		Will:           will,
	}

	s, isNew, err := h.broker.CreateSession(clientID, p.ProtocolVersion, opts) //nolint:contextcheck // CreateSession has no context parameter yet; 73 call sites, tracked separately
	if err != nil {
		h.broker.telemetry.stats.IncrementProtocolErrors()
		sendV3ConnAck(conn, false, v3.ConnAckServerUnavailable) //nolint:errcheck // best-effort rejection reply before closing
		conn.Close()
		return err
	}

	s.ExternalID = externalID

	// Apply the negotiated options and take over any existing connection. v3
	// has no session expiry, Receive Maximum, or topic aliases.
	// The maximum QoS in force when the connection is accepted is applied with
	// the epoch, so a takeover racing a configuration reload cannot leave the
	// connection enforcing a different limit than it was admitted under. MQTT
	// 3.1.1 cannot advertise the value, so a client has no way to learn about a
	// later change.
	epoch, superseded := s.ConnectWithOptions(conn, session.ConnectOptions{
		Version:        p.ProtocolVersion,
		KeepAlive:      time.Duration(p.KeepAlive) * time.Second,
		Will:           will,
		ReceiveMaximum: maxReceived,
		MaxQoS:         h.broker.MaxQoS(),
	})
	if superseded != nil {
		go h.broker.drainSuperseded(context.WithoutCancel(ctx), superseded)
	}
	h.broker.persistSessionInfo(s)

	sessionPresent := !isNew && !cleanStart
	if err := sendV3ConnAck(conn, sessionPresent, v3.ConnAckAccepted); err != nil {
		s.DisconnectIf(false, epoch, v5.DisconnectUnspecifiedError) //nolint:errcheck // disconnect on failed CONNACK; connection is already broken
		return err
	}

	h.broker.telemetry.stats.IncrementConnections()
	h.broker.telemetry.logger.Info("v3_connect_success",
		slog.String("client_id", clientID),
		slog.Bool("session_present", sessionPresent),
		slog.Duration("duration", time.Since(start)),
	)

	h.broker.NotifyConnect(ctx, clientID, p.Username, "mqtt3")

	h.deliverOfflineMessages(ctx, s)

	return h.broker.runSession(ctx, h, s, conn, epoch, time.Duration(p.KeepAlive)*time.Second)
}

// HandlePublish handles PUBLISH packets.
func (h *v3Handler) HandlePublish(s *connCtx, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v3.Publish)
	if !ok {
		return ErrInvalidPacketType
	}

	// Check client rate limit
	if h.broker.rateLimiter != nil && !h.broker.rateLimiter.AllowPublish(s.ID) {
		h.broker.telemetry.logger.Warn("v3_publish_rate_limit",
			slog.String("client_id", s.ID),
			slog.String("topic", p.TopicName))
		// For V3, silently drop QoS 0, return error for QoS > 0 (will disconnect)
		if p.FixedHeader.QoS > 0 {
			return ErrQuotaExceeded
		}
		return nil
	}

	h.broker.telemetry.logger.Debug("v3_publish",
		slog.String("client_id", s.ID),
		slog.String("topic", p.TopicName),
		slog.Int("qos", int(p.FixedHeader.QoS)),
	)

	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID

	if err := topics.ValidateTopicName(topic); err != nil {
		h.broker.telemetry.logger.Warn("v3_publish_invalid_topic",
			slog.String("client_id", s.ID),
			slog.String("topic", topic))
		return ErrTopicInvalid
	}

	// The inbound QoS selects the acknowledgement handshake, so it must stay as
	// the client sent it. Downgrading it here would answer a QoS 2 PUBLISH with
	// a PUBACK — or, at Maximum QoS 0, with nothing at all — leaving the
	// publisher retransmitting forever. MQTT 3.1.1 has no way to advertise a
	// maximum QoS or to report one in a response, so the connection is closed.
	if maxQoS := s.MaxQoS(); qos > maxQoS {
		h.broker.telemetry.logger.Warn("v3_publish_qos_not_supported",
			slog.String("client_id", s.ID),
			slog.Int("requested_qos", int(qos)),
			slog.Int("server_max_qos", int(maxQoS)),
		)
		return ErrQoSNotSupported
	}

	// The transports cap the whole packet, with an allowance for the topic. This
	// is the limit broker.max_message_size actually documents: the application
	// payload. MQTT 3.1.1 has no way to report it, so the connection is closed.
	if maxSize := h.broker.MaxMessageSize(); maxSize > 0 && len(payload) > maxSize {
		h.broker.telemetry.logger.Warn("v3_publish_payload_too_large",
			slog.String("client_id", s.ID),
			slog.Int("payload_size", len(payload)),
			slog.Int("max_message_size", maxSize),
		)
		return ErrPacketTooLarge
	}

	var props map[string]string
	requestedTopic := topic
	hookReq, ok := h.broker.ApplyPublishHooks(context.Background(), corebroker.BlockingHookRequest{
		ClientID:   s.ID,
		ExternalID: s.ExternalID,
		Protocol:   corebroker.HookProtocolMQTT,
		Topic:      topic,
		Payload:    payload,
		QoS:        qos,
		Retain:     retain,
		Properties: props,
	})
	if !ok {
		h.broker.telemetry.stats.IncrementAuthzErrors()
		return ErrNotAuthorized
	}
	if hookReq.QoS != qos {
		h.broker.telemetry.logger.Warn("v3_publish_hook_qos_mutation_rejected",
			slog.String("client_id", s.ID),
			slog.String("topic", topic),
			slog.Int("requested_qos", int(qos)),
			slog.Int("hook_qos", int(hookReq.QoS)))
		h.broker.telemetry.stats.IncrementProtocolErrors()
		return ErrProtocolViolation
	}
	// QoS is carried through unchanged: hooks that mutate it were rejected
	// above, and the wire QoS still owns the acknowledgement handshake.
	topic, payload, retain, props = hookReq.Topic, hookReq.Payload, hookReq.Retain, hookReq.Properties
	sourceExternalID := hookReq.ExternalID
	// A hook can rewrite the payload, so the limit is re-checked on the result.
	if maxSize := h.broker.MaxMessageSize(); maxSize > 0 && len(payload) > maxSize {
		h.broker.telemetry.logger.Warn("v3_publish_hook_payload_too_large",
			slog.String("client_id", s.ID),
			slog.String("topic", topic),
			slog.Int("payload_size", len(payload)),
			slog.Int("max_message_size", maxSize))
		h.broker.telemetry.stats.IncrementProtocolErrors()
		return ErrPacketTooLarge
	}
	if topic != requestedTopic {
		if err := topics.ValidateTopicName(topic); err != nil {
			h.broker.telemetry.logger.Warn("v3_publish_invalid_hook_topic",
				slog.String("client_id", s.ID),
				slog.String("topic", topic))
			return ErrTopicInvalid
		}
	}
	if h.broker.auth != nil && !h.broker.CanPublish(s.ctx, s.ID, topic) {
		h.broker.telemetry.stats.IncrementAuthzErrors()
		return ErrNotAuthorized
	}

	switch qos {
	case 0:
		msg := newMQTTEnvelope(topic, payload, s.ID, sourceExternalID, qos, retain, props)
		err := h.broker.Publish(context.Background(), msg)
		h.broker.telemetry.logger.Debug("v3_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err),
		)
		return err

	case 1:
		msg := newMQTTEnvelope(topic, payload, s.ID, sourceExternalID, qos, retain, props)
		if err := h.broker.Publish(context.Background(), msg); err != nil {
			return err
		}
		h.broker.telemetry.logger.Debug("v3_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)
		ack := &v3.PubAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
			ID:          packetID,
		}
		return s.WritePacket(ack)

	case 2:
		storeMsg := newMQTTEnvelope(topic, payload, s.ID, sourceExternalID, qos, retain, props)
		storeMsg.Broker.Delivery.PacketID = packetID
		accepted, err := s.AddInbound(packetID, storeMsg)
		if !accepted {
			message.Release(storeMsg)
		}
		if err != nil {
			return err
		}

		h.broker.telemetry.logger.Debug("v3_publish_complete",
			slog.String("client_id", s.ID),
			slog.Duration("duration", time.Since(start)),
		)

		return sendV3PubRec(s, packetID)
	}

	return nil
}

// HandlePubAck handles PUBACK packets.
func (h *v3Handler) HandlePubAck(s *connCtx, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubAck)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v3_puback", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s.Session, p.ID)
}

// HandlePubRec handles PUBREC packets.
func (h *v3Handler) HandlePubRec(s *connCtx, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubRec)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v3_pubrec", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived) //nolint:errcheck // state update for in-flight QoS2; non-fatal if packet not tracked
	rel := &v3.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          p.ID,
	}
	return s.WritePacket(rel)
}

// HandlePubRel handles PUBREL packets.
func (h *v3Handler) HandlePubRel(s *connCtx, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubRel)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v3_pubrel", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))

	packetID := p.ID
	comp := &v3.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
	}

	if err := h.broker.completeInboundQoS2(s, packetID, "v3_pubrel"); err != nil {
		return err
	}

	return s.WritePacket(comp)
}

// HandlePubComp handles PUBCOMP packets.
func (h *v3Handler) HandlePubComp(s *connCtx, pkt packets.ControlPacket) error {
	p, ok := pkt.(*v3.PubComp)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Debug("v3_pubcomp", slog.String("client_id", s.ID), slog.Int("packet_id", int(p.ID)))
	return h.broker.AckMessage(s.Session, p.ID)
}

// HandleSubscribe handles SUBSCRIBE packets.
func (h *v3Handler) HandleSubscribe(s *connCtx, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v3.Subscribe)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Info("v3_subscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Topics)))

	packetID := p.ID

	reasonCodes := make([]byte, len(p.Topics))
	for i, t := range p.Topics {
		if err := topics.ValidateTopicFilter(t.Name); err != nil {
			reasonCodes[i] = v3.SubAckFailure
			continue
		}
		if t.QoS > 2 {
			reasonCodes[i] = v3.SubAckFailure
			continue
		}

		filter := t.Name
		subQoS := t.QoS
		filter, subQoS, ok = h.broker.ApplySubscribeHooks(context.Background(), s.ID, s.ExternalID, corebroker.HookProtocolMQTT, filter, subQoS)
		if !ok {
			h.broker.telemetry.stats.IncrementAuthzErrors()
			reasonCodes[i] = v3.SubAckFailure
			continue
		}
		if subQoS > 2 {
			reasonCodes[i] = v3.SubAckFailure
			continue
		}
		if filter != t.Name {
			if err := topics.ValidateTopicFilter(filter); err != nil {
				reasonCodes[i] = v3.SubAckFailure
				continue
			}
			s.AddSubscriptionAlias(t.Name, filter)
		}
		if h.broker.auth != nil && !h.broker.CanSubscribe(s.ctx, s.ID, filter) {
			h.broker.telemetry.stats.IncrementAuthzErrors()
			reasonCodes[i] = v3.SubAckFailure
			continue
		}

		// Check subscription rate limit
		if h.broker.rateLimiter != nil && !h.broker.rateLimiter.AllowSubscribe(s.ID) {
			h.broker.telemetry.logger.Warn("v3_subscribe_rate_limit",
				slog.String("client_id", s.ID),
				slog.String("topic", filter))
			reasonCodes[i] = v3.SubAckFailure
			continue
		}

		opts := storage.SubscribeOptions{}
		grantedQoS := subQoS
		if maxQoS := h.broker.MaxQoS(); grantedQoS > maxQoS {
			grantedQoS = maxQoS
		}

		if err := h.broker.subscribe(s.Session, filter, grantedQoS, opts); err != nil {
			reasonCodes[i] = v3.SubAckFailure
			continue
		}

		reasonCodes[i] = grantedQoS

		// Send retained messages matching the subscription filter
		retained, err := h.broker.GetRetainedMatching(filter)
		if err == nil {
			for _, msg := range retained {
				deliverQoS := msg.Broker.Delivery.QoS
				if grantedQoS < deliverQoS {
					deliverQoS = grantedQoS
				}
				msg.Broker.Delivery.QoS = deliverQoS
				msg.Broker.Delivery.Retain = true
				h.broker.DeliverToSession(context.Background(), s.Session, msg) //nolint:errcheck // retained message delivery; errors are non-fatal
			}
		}
	}

	h.broker.telemetry.logger.Info("v3_subscribe_complete",
		slog.String("client_id", s.ID),
		slog.Duration("duration", time.Since(start)),
	)
	ack := &v3.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReturnCodes: reasonCodes,
	}
	return s.WritePacket(ack)
}

// HandleUnsubscribe handles UNSUBSCRIBE packets.
func (h *v3Handler) HandleUnsubscribe(s *connCtx, pkt packets.ControlPacket) error {
	start := time.Now()
	p, ok := pkt.(*v3.Unsubscribe)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Info("v3_unsubscribe", slog.String("client_id", s.ID), slog.Int("topics", len(p.Topics)))

	for _, filter := range p.Topics {
		if resolved := s.ResolveSubscriptionAlias(filter); resolved != filter {
			filter = resolved
		} else {
			filter, ok = h.broker.ApplyUnsubscribeHooks(context.Background(), s.ID, s.ExternalID, corebroker.HookProtocolMQTT, filter)
			if !ok {
				h.broker.telemetry.stats.IncrementAuthzErrors()
				continue
			}
		}
		h.broker.unsubscribeInternal(s.Session, filter) //nolint:errcheck // best-effort unsubscribe; errors are non-fatal
	}

	h.broker.telemetry.logger.Info("v3_unsubscribe_complete",
		slog.String("client_id", s.ID),
		slog.Duration("duration", time.Since(start)),
	)

	ack := &v3.UnSubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          p.ID,
	}
	return s.WritePacket(ack)
}

// HandlePingReq handles PINGREQ packets.
func (h *v3Handler) HandlePingReq(s *connCtx) error {
	h.broker.telemetry.logger.Debug("v3_pingreq", slog.String("client_id", s.ID))

	// Update heartbeat for queue consumers
	// Fire and forget - don't block PINGRESP on this.
	// Updates are interval-limited to avoid goroutine storms under ping floods.
	maybeUpdateQueueHeartbeat(h.broker, s.Session)

	resp := &v3.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
}

// HandleDisconnect handles DISCONNECT packets.
func (h *v3Handler) HandleDisconnect(s *connCtx, pkt packets.ControlPacket) error {
	_, ok := pkt.(*v3.Disconnect)
	if !ok {
		return ErrInvalidPacketType
	}

	h.broker.telemetry.logger.Info("v3_disconnect", slog.String("client_id", s.ID))
	s.Disconnect(true, v5.DisconnectNormalDisconnection) //nolint:errcheck // graceful disconnect initiated by client
	return io.EOF
}

// HandleAuth - not supported in V3.
func (h *v3Handler) HandleAuth(s *connCtx, pkt packets.ControlPacket) error {
	return ErrInvalidPacketType
}

// deliverOfflineMessages sends queued messages to reconnected client.
func (h *v3Handler) deliverOfflineMessages(ctx context.Context, s *session.Session) {
	msgs := s.OfflineQueue().Drain()
	for _, msg := range msgs {
		h.broker.DeliverToSession(ctx, s, msg) //nolint:errcheck // offline message delivery; errors are non-fatal
	}
}

func sendV3PubRec(s *connCtx, packetID uint16) error {
	rec := &v3.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
	}
	return s.WritePacket(rec)
}
