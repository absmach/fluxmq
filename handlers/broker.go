package handlers

import (
	"time"

	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	v5 "github.com/dborovcanin/mqtt/packets/v5"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
)

// BrokerHandler implements the Handler interface for the broker.
type BrokerHandler struct {
	sessionMgr *session.Manager
	router     Router
	publisher  Publisher
	retained   RetainedStore
	auth       Authenticator
	authz      Authorizer

	// Retry settings for QoS 1/2
	retryInterval time.Duration
	maxRetries    int
}

// BrokerHandlerConfig holds configuration for the broker handler.
type BrokerHandlerConfig struct {
	SessionManager *session.Manager
	Router         Router
	Publisher      Publisher
	Retained       RetainedStore
	Authenticator  Authenticator
	Authorizer     Authorizer
	RetryInterval  time.Duration
	MaxRetries     int
}

// NewBrokerHandler creates a new broker handler.
func NewBrokerHandler(cfg BrokerHandlerConfig) *BrokerHandler {
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 20 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	return &BrokerHandler{
		sessionMgr:    cfg.SessionManager,
		router:        cfg.Router,
		publisher:     cfg.Publisher,
		retained:      cfg.Retained,
		auth:          cfg.Authenticator,
		authz:         cfg.Authorizer,
		retryInterval: cfg.RetryInterval,
		maxRetries:    cfg.MaxRetries,
	}
}

// HandleConnect handles CONNECT packets.
// Note: CONNECT is typically handled at the server level before creating a session.
// This method is provided for completeness.
func (h *BrokerHandler) HandleConnect(sess *session.Session, pkt packets.ControlPacket) error {
	// CONNECT is handled at server level
	return nil
}

// HandlePublish handles PUBLISH packets.
func (h *BrokerHandler) HandlePublish(sess *session.Session, pkt packets.ControlPacket) error {
	sess.TouchActivity()

	var topic string
	var payload []byte
	var qos byte
	var retain bool
	var packetID uint16
	var dup bool

	// Extract fields based on version
	if sess.Version == 5 {
		p := pkt.(*v5.Publish)
		topic = p.TopicName
		payload = p.Payload
		qos = p.FixedHeader.QoS
		retain = p.FixedHeader.Retain
		packetID = p.ID
		dup = p.FixedHeader.Dup

		// Handle topic alias
		if p.Properties != nil && p.Properties.TopicAlias != nil {
			alias := *p.Properties.TopicAlias
			if topic == "" {
				// Resolve alias
				resolved, ok := sess.ResolveInboundAlias(alias)
				if !ok {
					return ErrProtocolViolation
				}
				topic = resolved
			} else {
				// Set alias
				sess.SetInboundAlias(alias, topic)
			}
		}
	} else {
		p := pkt.(*v3.Publish)
		topic = p.TopicName
		payload = p.Payload
		qos = p.FixedHeader.QoS
		retain = p.FixedHeader.Retain
		packetID = p.ID
		dup = p.FixedHeader.Dup
	}

	// Authorization check
	if h.authz != nil && !h.authz.CanPublish(sess.ID, topic) {
		if sess.Version == 5 {
			return h.sendPubAckV5(sess, packetID, 0x87) // Not authorized
		}
		return ErrNotAuthorized
	}

	// Handle based on QoS
	switch qos {
	case 0:
		// QoS 0: Fire and forget
		return h.publishMessage(sess, topic, payload, qos, retain)

	case 1:
		// QoS 1: Publish and acknowledge
		if err := h.publishMessage(sess, topic, payload, qos, retain); err != nil {
			return err
		}
		return h.sendPubAck(sess, packetID)

	case 2:
		// QoS 2: Exactly once
		// Check if this is a duplicate
		if dup && sess.Inflight.WasReceived(packetID) {
			// Already received, just send PUBREC again
			return h.sendPubRec(sess, packetID)
		}

		// Mark as received
		sess.Inflight.MarkReceived(packetID)

		// Store for later publication (after PUBREL)
		msg := &store.Message{
			Topic:    topic,
			Payload:  payload,
			QoS:      qos,
			Retain:   retain,
			PacketID: packetID,
		}
		sess.Inflight.Add(packetID, msg, session.Inbound)

		return h.sendPubRec(sess, packetID)
	}

	return nil
}

// publishMessage publishes a message to subscribers.
func (h *BrokerHandler) publishMessage(sess *session.Session, topic string, payload []byte, qos byte, retain bool) error {
	// Handle retained message
	if retain && h.retained != nil {
		msg := &store.Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  true,
		}
		if len(payload) == 0 {
			// Empty payload clears retained message
			h.retained.Set(topic, nil)
		} else {
			h.retained.Set(topic, msg)
		}
	}

	// Publish to subscribers
	if h.publisher != nil {
		return h.publisher.Publish(topic, payload, qos, retain, nil)
	}

	return nil
}

// HandlePubAck handles PUBACK packets (QoS 1 acknowledgment from client).
func (h *BrokerHandler) HandlePubAck(sess *session.Session, pkt packets.ControlPacket) error {
	sess.TouchActivity()

	var packetID uint16
	if sess.Version == 5 {
		p := pkt.(*v5.PubAck)
		packetID = p.ID
	} else {
		p := pkt.(*v3.PubAck)
		packetID = p.ID
	}

	// Remove from inflight
	sess.Inflight.Ack(packetID)
	return nil
}

// HandlePubRec handles PUBREC packets (QoS 2 step 1 from client).
func (h *BrokerHandler) HandlePubRec(sess *session.Session, pkt packets.ControlPacket) error {
	sess.TouchActivity()

	var packetID uint16
	if sess.Version == 5 {
		p := pkt.(*v5.PubRec)
		packetID = p.ID
	} else {
		p := pkt.(*v3.PubRec)
		packetID = p.ID
	}

	// Update inflight state
	sess.Inflight.UpdateState(packetID, session.StatePubRecReceived)

	// Send PUBREL
	return h.sendPubRel(sess, packetID)
}

// HandlePubRel handles PUBREL packets (QoS 2 step 2 from client).
func (h *BrokerHandler) HandlePubRel(sess *session.Session, pkt packets.ControlPacket) error {
	sess.TouchActivity()

	var packetID uint16
	if sess.Version == 5 {
		p := pkt.(*v5.PubRel)
		packetID = p.ID
	} else {
		p := pkt.(*v3.PubRel)
		packetID = p.ID
	}

	// Get the stored message and publish it
	inf, ok := sess.Inflight.Get(packetID)
	if ok && inf.Message != nil {
		h.publishMessage(sess, inf.Message.Topic, inf.Message.Payload, inf.Message.QoS, inf.Message.Retain)
	}

	// Remove from inflight and received tracking
	sess.Inflight.Ack(packetID)
	sess.Inflight.ClearReceived(packetID)

	// Send PUBCOMP
	return h.sendPubComp(sess, packetID)
}

// HandlePubComp handles PUBCOMP packets (QoS 2 step 3 from client).
func (h *BrokerHandler) HandlePubComp(sess *session.Session, pkt packets.ControlPacket) error {
	sess.TouchActivity()

	var packetID uint16
	if sess.Version == 5 {
		p := pkt.(*v5.PubComp)
		packetID = p.ID
	} else {
		p := pkt.(*v3.PubComp)
		packetID = p.ID
	}

	// Remove from inflight - QoS 2 complete
	sess.Inflight.Ack(packetID)
	return nil
}

// HandleSubscribe handles SUBSCRIBE packets.
func (h *BrokerHandler) HandleSubscribe(sess *session.Session, pkt packets.ControlPacket) error {
	sess.TouchActivity()

	var packetID uint16
	var subscriptions []struct {
		Filter string
		QoS    byte
		Opts   store.SubscribeOptions
	}

	if sess.Version == 5 {
		p := pkt.(*v5.Subscribe)
		packetID = p.ID
		for _, opt := range p.Opts {
			subOpts := store.SubscribeOptions{}
			if opt.NoLocal != nil {
				subOpts.NoLocal = *opt.NoLocal
			}
			if opt.RetainAsPublished != nil {
				subOpts.RetainAsPublished = *opt.RetainAsPublished
			}
			if opt.RetainHandling != nil {
				subOpts.RetainHandling = *opt.RetainHandling
			}
			subscriptions = append(subscriptions, struct {
				Filter string
				QoS    byte
				Opts   store.SubscribeOptions
			}{
				Filter: opt.Topic,
				QoS:    opt.MaxQoS,
				Opts:   subOpts,
			})
		}
	} else {
		p := pkt.(*v3.Subscribe)
		packetID = p.ID
		for _, t := range p.Topics {
			subscriptions = append(subscriptions, struct {
				Filter string
				QoS    byte
				Opts   store.SubscribeOptions
			}{
				Filter: t.Name,
				QoS:    t.QoS,
				Opts:   store.SubscribeOptions{},
			})
		}
	}

	// Process subscriptions
	reasonCodes := make([]byte, len(subscriptions))
	for i, sub := range subscriptions {
		// Authorization check
		if h.authz != nil && !h.authz.CanSubscribe(sess.ID, sub.Filter) {
			if sess.Version == 5 {
				reasonCodes[i] = 0x87 // Not authorized
			} else {
				reasonCodes[i] = 0x80 // Failure
			}
			continue
		}

		// Add to router
		if h.router != nil {
			if err := h.router.Subscribe(sess.ID, sub.Filter, sub.QoS, sub.Opts); err != nil {
				reasonCodes[i] = 0x80 // Failure
				continue
			}
		}

		// Cache in session
		sess.AddSubscription(sub.Filter, sub.Opts)

		// Success - return granted QoS
		reasonCodes[i] = sub.QoS

		// Send retained messages
		if h.retained != nil && sub.Opts.RetainHandling != 2 {
			h.sendRetainedMessages(sess, sub.Filter, sub.QoS)
		}
	}

	return h.sendSubAck(sess, packetID, reasonCodes)
}

// sendRetainedMessages sends retained messages matching a filter.
func (h *BrokerHandler) sendRetainedMessages(sess *session.Session, filter string, maxQoS byte) {
	msgs, err := h.retained.Match(filter)
	if err != nil {
		return
	}

	for _, msg := range msgs {
		qos := msg.QoS
		if qos > maxQoS {
			qos = maxQoS
		}

		h.deliverMessage(sess, msg.Topic, msg.Payload, qos, true)
	}
}

// HandleUnsubscribe handles UNSUBSCRIBE packets.
func (h *BrokerHandler) HandleUnsubscribe(sess *session.Session, pkt packets.ControlPacket) error {
	sess.TouchActivity()

	var packetID uint16
	var filters []string

	if sess.Version == 5 {
		p := pkt.(*v5.Unsubscribe)
		packetID = p.ID
		filters = p.Topics
	} else {
		p := pkt.(*v3.Unsubscribe)
		packetID = p.ID
		filters = p.Topics
	}

	// Process unsubscriptions
	reasonCodes := make([]byte, len(filters))
	for i, filter := range filters {
		// Remove from router
		if h.router != nil {
			h.router.Unsubscribe(sess.ID, filter)
		}

		// Remove from session cache
		sess.RemoveSubscription(filter)

		reasonCodes[i] = 0 // Success
	}

	return h.sendUnsubAck(sess, packetID, reasonCodes)
}

// HandlePingReq handles PINGREQ packets.
func (h *BrokerHandler) HandlePingReq(sess *session.Session) error {
	sess.TouchActivity()
	return h.sendPingResp(sess)
}

// HandleDisconnect handles DISCONNECT packets.
func (h *BrokerHandler) HandleDisconnect(sess *session.Session, pkt packets.ControlPacket) error {
	// MQTT 5.0 can have reason code and session expiry in DISCONNECT
	if sess.Version == 5 && pkt != nil {
		p := pkt.(*v5.Disconnect)
		if p.Properties != nil && p.Properties.SessionExpiryInterval != nil {
			// Can't change from 0 to non-zero (protocol error)
			if sess.ExpiryInterval == 0 && *p.Properties.SessionExpiryInterval > 0 {
				return ErrProtocolViolation
			}
			sess.ExpiryInterval = *p.Properties.SessionExpiryInterval
		}
	}

	// Graceful disconnect
	sess.Disconnect(true)
	return nil
}

// deliverMessage delivers a message to a session.
func (h *BrokerHandler) deliverMessage(sess *session.Session, topic string, payload []byte, qos byte, retain bool) error {
	if !sess.IsConnected() {
		// Queue for offline delivery
		msg := &store.Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		return sess.OfflineQueue.Enqueue(msg)
	}

	// Build PUBLISH packet
	if sess.Version == 5 {
		pub := &v5.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        qos,
				Retain:     retain,
			},
			TopicName: topic,
			Payload:   payload,
		}

		if qos > 0 {
			pub.ID = sess.NextPacketID()
			// Add to inflight for acknowledgment tracking
			msg := &store.Message{
				Topic:    topic,
				Payload:  payload,
				QoS:      qos,
				PacketID: pub.ID,
			}
			sess.Inflight.Add(pub.ID, msg, session.Outbound)
		}

		return sess.WritePacket(pub)
	}

	// MQTT 3.1.1
	pub := &v3.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        qos,
			Retain:     retain,
		},
		TopicName: topic,
		Payload:   payload,
	}

	if qos > 0 {
		pub.ID = sess.NextPacketID()
		msg := &store.Message{
			Topic:    topic,
			Payload:  payload,
			QoS:      qos,
			PacketID: pub.ID,
		}
		sess.Inflight.Add(pub.ID, msg, session.Outbound)
	}

	return sess.WritePacket(pub)
}

// --- Response packet senders ---

func (h *BrokerHandler) sendPubAck(sess *session.Session, packetID uint16) error {
	if sess.Version == 5 {
		return h.sendPubAckV5(sess, packetID, 0)
	}

	ack := &v3.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
	}
	return sess.WritePacket(ack)
}

func (h *BrokerHandler) sendPubAckV5(sess *session.Session, packetID uint16, reasonCode byte) error {
	ack := &v5.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
		ReasonCode:  &reasonCode,
	}
	return sess.WritePacket(ack)
}

func (h *BrokerHandler) sendPubRec(sess *session.Session, packetID uint16) error {
	if sess.Version == 5 {
		zero := byte(0)
		rec := &v5.PubRec{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
			ID:          packetID,
			ReasonCode:  &zero,
		}
		return sess.WritePacket(rec)
	}

	rec := &v3.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
	}
	return sess.WritePacket(rec)
}

func (h *BrokerHandler) sendPubRel(sess *session.Session, packetID uint16) error {
	if sess.Version == 5 {
		zero := byte(0)
		rel := &v5.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
			ID:          packetID,
			ReasonCode:  &zero,
		}
		return sess.WritePacket(rel)
	}

	rel := &v3.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          packetID,
	}
	return sess.WritePacket(rel)
}

func (h *BrokerHandler) sendPubComp(sess *session.Session, packetID uint16) error {
	if sess.Version == 5 {
		zero := byte(0)
		comp := &v5.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
			ID:          packetID,
			ReasonCode:  &zero,
		}
		return sess.WritePacket(comp)
	}

	comp := &v3.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
	}
	return sess.WritePacket(comp)
}

func (h *BrokerHandler) sendSubAck(sess *session.Session, packetID uint16, reasonCodes []byte) error {
	if sess.Version == 5 {
		ack := &v5.SubAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
			ID:          packetID,
			ReasonCodes: &reasonCodes,
		}
		return sess.WritePacket(ack)
	}

	ack := &v3.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReturnCodes: reasonCodes,
	}
	return sess.WritePacket(ack)
}

func (h *BrokerHandler) sendUnsubAck(sess *session.Session, packetID uint16, reasonCodes []byte) error {
	if sess.Version == 5 {
		ack := &v5.UnSubAck{
			FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
			ID:          packetID,
			ReasonCodes: &reasonCodes,
		}
		return sess.WritePacket(ack)
	}

	ack := &v3.UnSubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          packetID,
	}
	return sess.WritePacket(ack)
}

func (h *BrokerHandler) sendPingResp(sess *session.Session) error {
	if sess.Version == 5 {
		resp := &v5.PingResp{
			FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
		}
		return sess.WritePacket(resp)
	}

	resp := &v3.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return sess.WritePacket(resp)
}

// Ensure BrokerHandler implements Handler.
var _ Handler = (*BrokerHandler)(nil)
