package handlers

import (
	"time"

	"github.com/dborovcanin/mqtt/handlers/core"
	"github.com/dborovcanin/mqtt/packets"
	v3 "github.com/dborovcanin/mqtt/packets/v3"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
)

// BrokerHandler implements the Handler interface for the broker.
type BrokerHandler struct {
	sessionMgr *session.Manager
	pubsub     *core.PubSubEngine
	auth       *core.AuthEngine

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
		pubsub:        core.NewPubSubEngine(cfg.Router, cfg.Publisher, cfg.Retained),
		auth:          core.NewAuthEngine(cfg.Authenticator, cfg.Authorizer),
		retryInterval: cfg.RetryInterval,
		maxRetries:    cfg.MaxRetries,
	}
}

// HandleConnect handles CONNECT packets.
// Note: CONNECT is typically handled at the server level before creating a session.
// This method is provided for completeness.
func (h *BrokerHandler) HandleConnect(s *session.Session, pkt packets.ControlPacket) error {
	return nil
}

// HandlePublish handles PUBLISH packets.
func (h *BrokerHandler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	s.TouchActivity()

	p := pkt.(*v3.Publish)
	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID
	dup := p.FixedHeader.Dup

	if !h.auth.CanPublish(s.ID, topic) {
		return ErrNotAuthorized
	}

	switch qos {
	case 0:
		// QoS 0: Fire and forget
		return h.pubsub.HandleQoS0Publish(topic, payload, retain)

	case 1:
		// QoS 1: Publish and acknowledge
		if err := h.pubsub.HandleQoS1Publish(topic, payload, retain); err != nil {
			return err
		}
		return h.sendPubAck(s, packetID)

	case 2:
		// QoS 2: Exactly once
		// Check if this is a duplicate
		if dup && s.Inflight.WasReceived(packetID) {
			// Already received, just send PUBREC again
			return h.sendPubRec(s, packetID)
		}

		if err := h.pubsub.HandleQoS2Publish(s, topic, payload, retain, packetID, dup); err != nil {
			return err
		}

		return h.sendPubRec(s, packetID)
	}

	return nil
}

// HandlePubAck handles PUBACK packets (QoS 1 acknowledgment from client).
func (h *BrokerHandler) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	s.TouchActivity()

	p := pkt.(*v3.PubAck)
	s.Inflight.Ack(p.ID)
	return nil
}

// HandlePubRec handles PUBREC packets (QoS 2 step 1 from client).
func (h *BrokerHandler) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	s.TouchActivity()

	p := pkt.(*v3.PubRec)
	s.Inflight.UpdateState(p.ID, session.StatePubRecReceived)
	return h.sendPubRel(s, p.ID)
}

// HandlePubRel handles PUBREL packets (QoS 2 step 2 from client).
func (h *BrokerHandler) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	s.TouchActivity()

	p := pkt.(*v3.PubRel)
	packetID := p.ID

	inf, ok := s.Inflight.Get(packetID)
	if ok && inf.Message != nil {
		h.pubsub.PublishQoS2Message(inf.Message.Topic, inf.Message.Payload, inf.Message.QoS, inf.Message.Retain)
	}

	s.Inflight.Ack(packetID)
	s.Inflight.ClearReceived(packetID)

	return h.sendPubComp(s, packetID)
}

// HandlePubComp handles PUBCOMP packets (QoS 2 step 3 from client).
func (h *BrokerHandler) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	s.TouchActivity()

	p := pkt.(*v3.PubComp)
	s.Inflight.Ack(p.ID)
	return nil
}

// HandleSubscribe handles SUBSCRIBE packets.
func (h *BrokerHandler) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	s.TouchActivity()

	p := pkt.(*v3.Subscribe)
	packetID := p.ID

	reasonCodes := make([]byte, len(p.Topics))
	for i, t := range p.Topics {
		if !h.auth.CanSubscribe(s.ID, t.Name) {
			reasonCodes[i] = 0x80
			continue
		}

		reasonCodes[i] = h.pubsub.ProcessSubscription(s.ID, t.Name, t.QoS)
		if reasonCodes[i] == 0x80 {
			continue
		}

		opts := store.SubscribeOptions{}
		s.AddSubscription(t.Name, opts)

		h.pubsub.SendRetainedMessages(t.Name, t.QoS, func(msg *store.Message) error {
			return h.deliverMessage(s, msg.Topic, msg.Payload, msg.QoS, true)
		})
	}

	return h.sendSubAck(s, packetID, reasonCodes)
}

// HandleUnsubscribe handles UNSUBSCRIBE packets.
func (h *BrokerHandler) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	s.TouchActivity()

	p := pkt.(*v3.Unsubscribe)

	for _, filter := range p.Topics {
		h.pubsub.ProcessUnsubscription(s.ID, filter)
		s.RemoveSubscription(filter)
	}

	return h.sendUnsubAck(s, p.ID)
}

// HandlePingReq handles PINGREQ packets.
func (h *BrokerHandler) HandlePingReq(s *session.Session) error {
	s.TouchActivity()
	return h.sendPingResp(s)
}

// HandleDisconnect handles DISCONNECT packets.
func (h *BrokerHandler) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	s.Disconnect(true)
	return nil
}

// deliverMessage delivers a message to a session.
func (h *BrokerHandler) deliverMessage(s *session.Session, topic string, payload []byte, qos byte, retain bool) error {
	if !s.IsConnected() {
		msg := &store.Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		return s.OfflineQueue.Enqueue(msg)
	}

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
		pub.ID = s.NextPacketID()
		msg := &store.Message{
			Topic:    topic,
			Payload:  payload,
			QoS:      qos,
			PacketID: pub.ID,
		}
		s.Inflight.Add(pub.ID, msg, session.Outbound)
	}

	return s.WritePacket(pub)
}

// --- Response packet senders ---

func (h *BrokerHandler) sendPubAck(s *session.Session, packetID uint16) error {
	ack := &v3.PubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubAckType},
		ID:          packetID,
	}
	return s.WritePacket(ack)
}

func (h *BrokerHandler) sendPubRec(s *session.Session, packetID uint16) error {
	rec := &v3.PubRec{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRecType},
		ID:          packetID,
	}
	return s.WritePacket(rec)
}

func (h *BrokerHandler) sendPubRel(s *session.Session, packetID uint16) error {
	rel := &v3.PubRel{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1},
		ID:          packetID,
	}
	return s.WritePacket(rel)
}

func (h *BrokerHandler) sendPubComp(s *session.Session, packetID uint16) error {
	comp := &v3.PubComp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PubCompType},
		ID:          packetID,
	}
	return s.WritePacket(comp)
}

func (h *BrokerHandler) sendSubAck(s *session.Session, packetID uint16, returnCodes []byte) error {
	ack := &v3.SubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubAckType},
		ID:          packetID,
		ReturnCodes: returnCodes,
	}
	return s.WritePacket(ack)
}

func (h *BrokerHandler) sendUnsubAck(s *session.Session, packetID uint16) error {
	ack := &v3.UnSubAck{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubAckType},
		ID:          packetID,
	}
	return s.WritePacket(ack)
}

func (h *BrokerHandler) sendPingResp(s *session.Session) error {
	resp := &v3.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}
	return s.WritePacket(resp)
}

// Ensure BrokerHandler implements Handler.
var _ Handler = (*BrokerHandler)(nil)
