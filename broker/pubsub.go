package broker

import (
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
	"github.com/dborovcanin/mqtt/store/messages"
)

// HandleQoS2Publish handles QoS 2 publish (exactly once).
// For QoS 2, the actual publication happens after PUBREL is received.
// This stores the message in the session's inflight tracker.
func (b *Broker) HandleQoS2Publish(s *session.Session, topic string, payload []byte, retain bool, packetID uint16, dup bool) error {
	if dup && s.Inflight().WasReceived(packetID) {
		return nil
	}

	s.Inflight().MarkReceived(packetID)

	msg := &store.Message{
		Topic:    topic,
		Payload:  payload,
		QoS:      2,
		Retain:   retain,
		PacketID: packetID,
	}
	return s.Inflight().Add(packetID, msg, messages.Inbound)
}

// publishMessage publishes a message to all matching subscribers.
func (b *Broker) publishMessage(topic string, payload []byte, qos byte, retain bool) error {
	if retain {
		msg := &store.Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  true,
		}
		if len(payload) == 0 {
			if err := b.retained.Delete(topic); err != nil {
				return err
			}
		} else {
			if err := b.retained.Set(topic, msg); err != nil {
				return err
			}
		}
	}

	return b.Distribute(topic, payload, qos, retain, nil)
}

// ProcessSubscription processes a subscription request.
// Returns granted QoS (same as requested) or 0x80 for failure.
func (b *Broker) ProcessSubscription(clientID string, filter string, qos byte) byte {
	opts := store.SubscribeOptions{}
	if err := b.Subscribe(clientID, filter, qos, opts); err != nil {
		return 0x80
	}
	return qos
}

// SendRetainedMessages sends retained messages matching a filter to a session.
// The deliver function is called for each retained message and should handle
// version-specific packet creation and sending.
func (b *Broker) SendRetainedMessages(filter string, maxQoS byte, deliver func(msg *store.Message) error) error {
	msgs, err := b.retained.Match(filter)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		qos := msg.QoS
		if qos > maxQoS {
			qos = maxQoS
		}

		deliveryMsg := &store.Message{
			Topic:   msg.Topic,
			Payload: msg.Payload,
			QoS:     qos,
			Retain:  true,
		}

		if err := deliver(deliveryMsg); err != nil {
			return err
		}
	}

	return nil
}
