package broker

import (
	"time"

	"github.com/dborovcanin/mqtt/core/packets"
	v3 "github.com/dborovcanin/mqtt/core/packets/v3"
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/messages"
)

// V3Handler implements the Handler interface for MQTT v3.1.1.
// It directly interacts with the Broker struct.
type V3Handler struct {
	broker *Broker
	auth   *AuthEngine

	// Retry settings for QoS 1/2
	retryInterval time.Duration
	maxRetries    int
}

// V3HandlerConfig holds configuration for the handler.
type V3HandlerConfig struct {
	Broker        *Broker
	Authenticator Authenticator
	Authorizer    Authorizer
	RetryInterval time.Duration
	MaxRetries    int
}

// NewV3Handler creates a new v3 handler.
func NewV3Handler(cfg V3HandlerConfig) *V3Handler {
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 20 * time.Second
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 3
	}

	return &V3Handler{
		broker:        cfg.Broker,
		auth:          NewAuthEngine(cfg.Authenticator, cfg.Authorizer),
		retryInterval: cfg.RetryInterval,
		maxRetries:    cfg.MaxRetries,
	}
}

// HandleConnect handles CONNECT packets.
func (h *V3Handler) HandleConnect(s *session.Session, pkt packets.ControlPacket) error {
	return nil
}

// HandlePublish handles PUBLISH packets.
func (h *V3Handler) HandlePublish(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v3.Publish)
	topic := p.TopicName
	payload := p.Payload
	qos := p.FixedHeader.QoS
	retain := p.FixedHeader.Retain
	packetID := p.ID
	dup := p.FixedHeader.Dup

	if !h.auth.CanPublish(s.ID, topic) {
		h.broker.stats.IncrementAuthzErrors()
		return ErrNotAuthorized
	}

	h.broker.stats.IncrementPublishReceived()
	h.broker.stats.AddBytesReceived(uint64(len(payload)))

	switch qos {
	case 0:
		// QoS 0: Fire and forget
		return h.broker.HandleQoS0Publish(topic, payload, retain)

	case 1:
		// QoS 1: Publish and acknowledge
		if err := h.broker.HandleQoS1Publish(topic, payload, retain); err != nil {
			return err
		}
		return h.sendPubAck(s, packetID)

	case 2:
		// QoS 2: Exactly once
		// Check if this is a duplicate
		if dup && s.Inflight().WasReceived(packetID) {
			// Already received, just send PUBREC again
			return h.sendPubRec(s, packetID)
		}

		if err := h.broker.HandleQoS2Publish(s, topic, payload, retain, packetID, dup); err != nil {
			return err
		}

		return h.sendPubRec(s, packetID)
	}

	return nil
}

// HandlePubAck handles PUBACK packets (QoS 1 acknowledgment from client).
func (h *V3Handler) HandlePubAck(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v3.PubAck)
	s.Inflight().Ack(p.ID)
	return nil
}

// HandlePubRec handles PUBREC packets (QoS 2 step 1 from client).
func (h *V3Handler) HandlePubRec(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v3.PubRec)
	s.Inflight().UpdateState(p.ID, messages.StatePubRecReceived)
	return h.sendPubRel(s, p.ID)
}

// HandlePubRel handles PUBREL packets (QoS 2 step 2 from client).
func (h *V3Handler) HandlePubRel(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v3.PubRel)
	packetID := p.ID

	inf, ok := s.Inflight().Get(packetID)
	if ok && inf.Message != nil {
		h.broker.PublishQoS2Message(inf.Message.Topic, inf.Message.Payload, inf.Message.QoS, inf.Message.Retain)
	}

	s.Inflight().Ack(packetID)
	s.Inflight().ClearReceived(packetID)

	return h.sendPubComp(s, packetID)
}

// HandlePubComp handles PUBCOMP packets (QoS 2 step 3 from client).
func (h *V3Handler) HandlePubComp(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v3.PubComp)
	s.Inflight().Ack(p.ID)
	return nil
}

// HandleSubscribe handles SUBSCRIBE packets.
func (h *V3Handler) HandleSubscribe(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v3.Subscribe)
	packetID := p.ID

	reasonCodes := make([]byte, len(p.Topics))
	for i, t := range p.Topics {
		if !h.auth.CanSubscribe(s.ID, t.Name) {
			h.broker.stats.IncrementAuthzErrors()
			reasonCodes[i] = 0x80
			continue
		}

		reasonCodes[i] = h.broker.ProcessSubscription(s.ID, t.Name, t.QoS)
		if reasonCodes[i] == 0x80 {
			continue
		}

		h.broker.SendRetainedMessages(t.Name, t.QoS, func(msg *store.Message) error {
			return h.deliverMessage(s, msg.Topic, msg.Payload, msg.QoS, true)
		})
	}

	return h.sendSubAck(s, packetID, reasonCodes)
}

// HandleUnsubscribe handles UNSUBSCRIBE packets.
func (h *V3Handler) HandleUnsubscribe(s *session.Session, pkt packets.ControlPacket) error {
	s.Touch()

	p := pkt.(*v3.Unsubscribe)

	for _, filter := range p.Topics {
		h.broker.ProcessUnsubscription(s.ID, filter)
	}

	return h.sendUnsubAck(s, p.ID)
}

// HandlePingReq handles PINGREQ packets.
func (h *V3Handler) HandlePingReq(s *session.Session) error {
	s.Touch()
	return h.sendPingResp(s)
}

// HandleDisconnect handles DISCONNECT packets.
func (h *V3Handler) HandleDisconnect(s *session.Session, pkt packets.ControlPacket) error {
	s.Disconnect(true)
	return nil
}

// deliverMessage delivers a message to a session.
func (h *V3Handler) deliverMessage(s *session.Session, topic string, payload []byte, qos byte, retain bool) error {
	if !s.IsConnected() {
		msg := &store.Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  retain,
		}
		return s.OfflineQueue().Enqueue(msg)
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
		s.Inflight().Add(pub.ID, msg, messages.Outbound)
	}

	return s.WritePacket(pub)
}

// --- Response packet senders ---

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

// Ensure V3Handler implements Handler (which needs to be moved/defined).
var _ Handler = (*V3Handler)(nil)
