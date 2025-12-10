package core

import (
	"github.com/dborovcanin/mqtt/session"
	"github.com/dborovcanin/mqtt/store"
)

// Router handles topic matching and subscriptions.
type Router interface {
	Subscribe(clientID string, filter string, qos byte, opts store.SubscribeOptions) error
	Unsubscribe(clientID string, filter string) error
	Match(topic string) ([]*store.Subscription, error)
}

// Publisher distributes messages to subscribers.
type Publisher interface {
	Distribute(topic string, payload []byte, qos byte, retain bool, props map[string]string) error
}

// RetainedStore manages retained messages.
type RetainedStore interface {
	Set(topic string, msg *store.Message) error
	Get(topic string) (*store.Message, error)
	Match(filter string) ([]*store.Message, error)
}

// PubSubEngine handles version-agnostic publish/subscribe business logic.
type PubSubEngine struct {
	router    Router
	publisher Publisher
	retained  RetainedStore
}

// NewPubSubEngine creates a new pub/sub engine with the given dependencies.
func NewPubSubEngine(router Router, publisher Publisher, retained RetainedStore) *PubSubEngine {
	return &PubSubEngine{
		router:    router,
		publisher: publisher,
		retained:  retained,
	}
}

// HandleQoS0Publish handles QoS 0 publish (fire and forget).
func (e *PubSubEngine) HandleQoS0Publish(topic string, payload []byte, retain bool) error {
	return e.publishMessage(topic, payload, 0, retain)
}

// HandleQoS1Publish handles QoS 1 publish (publish with ack).
func (e *PubSubEngine) HandleQoS1Publish(topic string, payload []byte, retain bool) error {
	return e.publishMessage(topic, payload, 1, retain)
}

// HandleQoS2Publish handles QoS 2 publish (exactly once).
// For QoS 2, the actual publication happens after PUBREL is received.
// This stores the message in the session's inflight tracker.
func (e *PubSubEngine) HandleQoS2Publish(s *session.Session, topic string, payload []byte, retain bool, packetID uint16, dup bool) error {
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
	return s.Inflight().Add(packetID, msg, session.Inbound)
}

// PublishQoS2Message publishes a QoS 2 message after PUBREL is received.
func (e *PubSubEngine) PublishQoS2Message(topic string, payload []byte, qos byte, retain bool) error {
	return e.publishMessage(topic, payload, qos, retain)
}

// publishMessage publishes a message to all matching subscribers.
func (e *PubSubEngine) publishMessage(topic string, payload []byte, qos byte, retain bool) error {
	if retain && e.retained != nil {
		msg := &store.Message{
			Topic:   topic,
			Payload: payload,
			QoS:     qos,
			Retain:  true,
		}
		if len(payload) == 0 {
			e.retained.Set(topic, nil)
		} else {
			e.retained.Set(topic, msg)
		}
	}

	if e.publisher != nil {
		return e.publisher.Distribute(topic, payload, qos, retain, nil)
	}

	return nil
}

// ProcessSubscription processes a subscription request.
// Returns granted QoS (same as requested) or 0x80 for failure.
func (e *PubSubEngine) ProcessSubscription(clientID string, filter string, qos byte) byte {
	opts := store.SubscribeOptions{}
	if e.router != nil {
		if err := e.router.Subscribe(clientID, filter, qos, opts); err != nil {
			return 0x80
		}
	}
	return qos
}

// SendRetainedMessages sends retained messages matching a filter to a session.
// The deliver function is called for each retained message and should handle
// version-specific packet creation and sending.
func (e *PubSubEngine) SendRetainedMessages(filter string, maxQoS byte, deliver func(msg *store.Message) error) error {
	if e.retained == nil {
		return nil
	}

	msgs, err := e.retained.Match(filter)
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

func (e *PubSubEngine) ProcessUnsubscription(clientID string, filter string) error {
	if e.router != nil {
		return e.router.Unsubscribe(clientID, filter)
	}
	return nil
}
