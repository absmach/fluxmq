package broker

import (
	"time"

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

	retryInterval time.Duration
	maxRetries    int
}

// V5HandlerConfig holds configuration for the v5 handler.
type V5HandlerConfig struct {
	Broker        *Broker
	Authenticator Authenticator
	Authorizer    Authorizer
	RetryInterval time.Duration
	MaxRetries    int
}

// NewV5Handler creates a new v5 handler.
func NewV5Handler(cfg V5HandlerConfig) *V5Handler {
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 20 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	return &V5Handler{
		broker:        cfg.Broker,
		auth:          NewAuthEngine(cfg.Authenticator, cfg.Authorizer),
		retryInterval: cfg.RetryInterval,
		maxRetries:    cfg.MaxRetries,
	}
}

// HandleConnect handles CONNECT packets for v5.
func (h *V5Handler) HandleConnect(s *session.Session, pkt packets.ControlPacket) error {
	return nil
}

// HandlePublish handles PUBLISH packets for v5.
func (h *V5Handler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v5.Publish)
	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID
	dup := p.FixedHeader.Dup
	props := p.Properties

	if props != nil && props.TopicAlias != nil {
		alias := *props.TopicAlias

		if alias > s.TopicAliasMax {
			return h.sendPubAck(s, packetID, 0x94, "Topic alias invalid")
		}

		if topic == "" {
			resolvedTopic, ok := s.ResolveInboundAlias(alias)
			if !ok {
				return h.sendPubAck(s, packetID, 0x82, "Protocol error: topic alias not established")
			}
			topic = resolvedTopic
		} else {
			s.SetInboundAlias(alias, topic)
		}
	} else if topic == "" {
		return h.sendPubAck(s, packetID, 0x82, "Protocol error: empty topic without alias")
	}

	if !h.auth.CanPublish(s.ID, topic) {
		h.broker.stats.IncrementAuthzErrors()
		return h.sendPubAck(s, packetID, 0x87, "Not authorized")
	}

	h.broker.stats.IncrementPublishReceived()
	h.broker.stats.AddBytesReceived(uint64(len(payload)))

	switch qos {
	case 0:
		return h.broker.publishMessage(topic, payload, 0, retain)

	case 1:
		if err := h.broker.publishMessage(topic, payload, 1, retain); err != nil {
			return h.sendPubAck(s, packetID, 0x80, "Unspecified error")
		}
		return h.sendPubAck(s, packetID, 0x00, "")

	case 2:
		if dup && s.Inflight().WasReceived(packetID) {
			return h.sendPubRec(s, packetID, 0x00, "")
		}

		if err := h.broker.HandleQoS2Publish(s, topic, payload, retain, packetID, dup); err != nil {
			return h.sendPubRec(s, packetID, 0x80, "Unspecified error")
		}

		return h.sendPubRec(s, packetID, 0x00, "")
	}

	return nil
}

// HandlePubAck handles PUBACK packets (QoS 1 acknowledgment from client).
func (h *V5Handler) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v5.PubAck)
	s.Inflight().Ack(p.ID)
	return nil
}

// HandlePubRec handles PUBREC packets (QoS 2 step 1 from client).
func (h *V5Handler) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v5.PubRec)
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived)
	return h.sendPubRel(s, p.ID, 0x00, "")
}

// HandlePubRel handles PUBREL packets (QoS 2 step 2 from client).
func (h *V5Handler) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v5.PubRel)
	packetID := p.ID

	inf, ok := s.Inflight().Get(packetID)
	if ok && inf.Message != nil {
		h.broker.publishMessage(inf.Message.Topic, inf.Message.Payload, inf.Message.QoS, inf.Message.Retain)
	}

	s.Inflight().Ack(packetID)
	s.Inflight().ClearReceived(packetID)

	return h.sendPubComp(s, packetID, 0x00, "")
}

// HandlePubComp handles PUBCOMP packets (QoS 2 step 3 from client).
func (h *V5Handler) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v5.PubComp)
	s.Inflight().Ack(p.ID)
	return nil
}

// HandleSubscribe handles SUBSCRIBE packets for v5.
func (h *V5Handler) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v5.Subscribe)
	packetID := p.ID

	var subscriptionID *uint32
	if p.Properties != nil && p.Properties.SubscriptionIdentifier != nil {
		id := uint32(*p.Properties.SubscriptionIdentifier)
		subscriptionID = &id
	}

	reasonCodes := make([]byte, len(p.Opts))
	for i, opt := range p.Opts {
		topic := opt.Topic
		qos := opt.MaxQoS

		if !h.auth.CanSubscribe(s.ID, topic) {
			h.broker.stats.IncrementAuthzErrors()
			reasonCodes[i] = 0x87
			continue
		}

		opts := store.SubscribeOptions{}
		if opt.NoLocal != nil {
			opts.NoLocal = *opt.NoLocal
		}
		if opt.RetainAsPublished != nil {
			opts.RetainAsPublished = *opt.RetainAsPublished
		}
		if opt.RetainHandling != nil {
			opts.RetainHandling = *opt.RetainHandling
		}

		reasonCodes[i] = h.broker.ProcessSubscription(s.ID, topic, qos)
		if reasonCodes[i] == 0x80 {
			continue
		}

		s.AddSubscription(topic, opts)

		if subscriptionID != nil {
			s.AddSubscriptionID(topic, *subscriptionID)
		}

		sendRetained := true
		if opt.RetainHandling != nil && *opt.RetainHandling == 2 {
			sendRetained = false
		}

		if sendRetained {
			h.broker.SendRetainedMessages(topic, qos, func(msg *store.Message) error {
				return h.deliverMessage(s, msg.Topic, msg.Payload, msg.QoS, true, nil, nil)
			})
		}
	}

	return h.sendSubAck(s, packetID, reasonCodes)
}

// HandleUnsubscribe handles UNSUBSCRIBE packets for v5.
func (h *V5Handler) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v5.Unsubscribe)

	reasonCodes := make([]byte, len(p.Topics))
	for i, filter := range p.Topics {
		h.broker.Unsubscribe(s.ID, filter)
		s.RemoveSubscriptionID(filter)
		reasonCodes[i] = 0x00
	}

	return h.sendUnsubAck(s, p.ID, reasonCodes)
}

// HandlePingReq handles PINGREQ packets for v5.
func (h *V5Handler) HandlePingReq(s *session.Session) error {
	s.Touch()
	return h.sendPingResp(s)
}

// HandleDisconnect handles DISCONNECT packets for v5.
func (h *V5Handler) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	p := pkt.(*v5.Disconnect)

	if p.Properties != nil && p.Properties.SessionExpiryInterval != nil {
		s.UpdateSessionExpiry(*p.Properties.SessionExpiryInterval)
	}

	graceful := p.ReasonCode == 0x00
	s.Disconnect(graceful)
	return nil
}

// deliverMessage delivers a message to a session.
func (h *V5Handler) deliverMessage(s *session.Session, topic string, payload []byte, qos byte, retain bool, props *v5.PublishProperties, subscriptionIDs []uint32) error {
	if !s.IsConnected() {
		msg := &store.Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		if props != nil {
			h.populateMessageProperties(msg, props)
		}
		return s.OfflineQueue().Enqueue(msg)
	}

	pub := &v5.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        qos,
			Retain:     retain,
		},
		TopicName:  topic,
		Payload:    payload,
		Properties: &v5.PublishProperties{},
	}

	if props != nil {
		pub.Properties = h.copyPublishProperties(props)
	}

	if len(subscriptionIDs) > 0 {
		id := int(subscriptionIDs[0])
		pub.Properties.SubscriptionID = &id
	}

	if qos > 0 {
		pub.ID = s.NextPacketID()
		msg := &store.Message{
			Topic:    topic,
			Payload:  payload,
			QoS:      qos,
			PacketID: pub.ID,
		}
		s.Inflight().Add(pub.ID, msg, messages.Outbound)
	}

	return s.WritePacket(pub)
}

// populateMessageProperties populates store.Message from v5.PublishProperties.
func (h *V5Handler) populateMessageProperties(msg *store.Message, props *v5.PublishProperties) {
	if props.MessageExpiry != nil {
		msg.MessageExpiry = props.MessageExpiry
	}
	if props.PayloadFormat != nil {
		msg.PayloadFormat = props.PayloadFormat
	}
	if props.ContentType != "" {
		msg.ContentType = props.ContentType
	}
	if props.ResponseTopic != "" {
		msg.ResponseTopic = props.ResponseTopic
	}
	if len(props.CorrelationData) > 0 {
		msg.CorrelationData = make([]byte, len(props.CorrelationData))
		copy(msg.CorrelationData, props.CorrelationData)
	}
	if len(props.User) > 0 {
		msg.UserProperties = make(map[string]string, len(props.User))
		for _, u := range props.User {
			msg.UserProperties[u.Key] = u.Value
		}
	}
}

// copyPublishProperties creates a copy of v5.PublishProperties.
func (h *V5Handler) copyPublishProperties(props *v5.PublishProperties) *v5.PublishProperties {
	cp := &v5.PublishProperties{
		ContentType:   props.ContentType,
		ResponseTopic: props.ResponseTopic,
	}

	if props.PayloadFormat != nil {
		pf := *props.PayloadFormat
		cp.PayloadFormat = &pf
	}
	if props.MessageExpiry != nil {
		me := *props.MessageExpiry
		cp.MessageExpiry = &me
	}
	if len(props.CorrelationData) > 0 {
		cp.CorrelationData = make([]byte, len(props.CorrelationData))
		copy(cp.CorrelationData, props.CorrelationData)
	}
	if len(props.User) > 0 {
		cp.User = make([]v5.User, len(props.User))
		copy(cp.User, props.User)
	}

	return cp
}

// Response packet senders

func (h *V5Handler) sendPubAck(s *session.Session, packetID uint16, reasonCode byte, reasonString string) error {
	rc := reasonCode
	ack := &v5.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
		ReasonCode:  &rc,
		Properties:  &v5.BasicProperties{},
	}

	if reasonCode != 0x00 && s.RequestsProblemInfo() && reasonString != "" {
		ack.Properties.ReasonString = reasonString
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

	if reasonCode != 0x00 && s.RequestsProblemInfo() && reasonString != "" {
		rec.Properties.ReasonString = reasonString
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

	if reasonCode != 0x00 && s.RequestsProblemInfo() && reasonString != "" {
		rel.Properties.ReasonString = reasonString
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

	if reasonCode != 0x00 && s.RequestsProblemInfo() && reasonString != "" {
		comp.Properties.ReasonString = reasonString
	}

	return s.WritePacket(comp)
}

func (h *V5Handler) sendSubAck(s *session.Session, packetID uint16, returnCodes []byte) error {
	ack := &v5.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReasonCodes: &returnCodes,
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

var _ Handler = (*V5Handler)(nil)
