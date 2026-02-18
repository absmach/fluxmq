// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/absmach/fluxmq/broker/events"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/messages"
)

// DeliverToSession queues a message for delivery to a session.
// Returns packet ID (>0) if session is connected and QoS>0, otherwise 0.
func (b *Broker) DeliverToSession(s *session.Session, msg *storage.Message) (uint16, error) {
	// Check if message has expired
	if msg.MessageExpiry != nil && !msg.Expiry.IsZero() && time.Now().After(msg.Expiry) {
		b.logOp("message_expired",
			slog.String("client_id", s.ID),
			slog.String("topic", msg.Topic),
			slog.Time("expiry", msg.Expiry))
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
		return 0, nil
	}

	if !s.IsConnected() {
		if msg.QoS > 0 {
			// Offline queue copies the message internally, so we always release the original
			err := s.OfflineQueue().Enqueue(msg)
			msg.ReleasePayload()
			storage.ReleaseMessage(msg)
			return 0, err
		}
		msg.ReleasePayload() // Drop QoS 0 message for offline client - release buffer
		storage.ReleaseMessage(msg)
		return 0, nil
	}

	if msg.QoS == 0 {
		err := b.DeliverMessage(s, msg, nil)
		if err == nil {
			// Webhook: message delivered (QoS 0)
			if b.webhooks != nil {
				b.webhooks.Notify(context.Background(), events.MessageDelivered{
					ClientID:     s.ID,
					MessageTopic: msg.Topic,
					QoS:          msg.QoS,
					PayloadSize:  len(msg.GetPayload()),
				})
			}
		}
		// QoS 0 message delivered - release buffer and return message to pool
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
		return 0, err
	}

	packetID := s.NextPacketID()
	msg.PacketID = packetID
	// Inflight storage takes ownership - it will release when message is ACK'd or expires
	if err := s.Inflight().Add(packetID, msg, messages.Outbound); err != nil {
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
		return 0, err
	}

	onSent := func() {
		s.Inflight().MarkSent(packetID)
	}
	if err := b.DeliverMessage(s, msg, onSent); err != nil {
		// Delivery failed, but message is in inflight so buffer stays (will be retried)
		return packetID, err
	}

	// Webhook: message delivered (QoS 1/2)
	if b.webhooks != nil {
		b.webhooks.Notify(context.Background(), events.MessageDelivered{
			ClientID:     s.ID,
			MessageTopic: msg.Topic,
			QoS:          msg.QoS,
			PayloadSize:  len(msg.GetPayload()),
		})
	}

	return packetID, nil
}

// AckMessage acknowledges a message by packet ID and releases the buffer.
func (b *Broker) AckMessage(s *session.Session, packetID uint16) error {
	msg, err := s.Inflight().Ack(packetID)
	if err != nil {
		return err
	}
	if msg != nil {
		msg.ReleasePayload()
		storage.ReleaseMessage(msg)
	}
	return nil
}

// DeliverMessage sends a message packet to the session's connection.
func (b *Broker) DeliverMessage(s *session.Session, msg *storage.Message, onSent func()) error {
	b.stats.IncrementPublishSent()
	b.stats.AddBytesSent(uint64(len(msg.GetPayload())))

	var pub packets.ControlPacket
	var releasePub func()

	switch s.Version {
	case 5:
		p := v5.AcquirePublish()
		releasePub = func() { v5.ReleasePublish(p) }

		// Calculate remaining message expiry interval
		if msg.MessageExpiry != nil && !msg.Expiry.IsZero() {
			remaining := time.Until(msg.Expiry)

			if remaining > 0 {
				// Send remaining expiry in seconds
				remainingSec := uint32(remaining.Seconds())
				p.Properties.MessageExpiry = &remainingSec
			}
			// If remaining <= 0, don't include expiry (message should have been filtered already)
		}

		p.FixedHeader = packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
		}
		p.TopicName = msg.Topic
		p.Payload = msg.GetPayload()
		p.ID = msg.PacketID
		applyPublishProperties(p.Properties, msg)

		pub = p
	default:
		p := v3.AcquirePublish()
		releasePub = func() { v3.ReleasePublish(p) }

		p.FixedHeader = packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
		}
		p.TopicName = msg.Topic
		p.Payload = msg.GetPayload()
		p.ID = msg.PacketID

		pub = p
	}

	// Wrap onSent to release the pooled packet after the wire write completes.
	// In async mode, DeliverMessage returns before sendLoop writes the packet,
	// so we must not release via defer — the packet must stay alive until Pack() is done.
	var wrappedOnSent func()
	if onSent != nil {
		wrappedOnSent = func() {
			onSent()
			releasePub()
		}
	} else {
		wrappedOnSent = releasePub
	}

	err := s.WriteDataPacket(pub, wrappedOnSent)
	if err != nil {
		// WriteDataPacket failed without enqueuing; release immediately.
		releasePub()
	}
	return err
}

func applyPublishProperties(props *v5.PublishProperties, msg *storage.Message) {
	if props == nil || msg == nil {
		return
	}

	// Prefer explicit fields; fall back to mapped properties.
	if msg.ContentType != "" {
		props.ContentType = msg.ContentType
	} else if v := msg.Properties["content-type"]; v != "" {
		props.ContentType = v
	}

	if msg.ResponseTopic != "" {
		props.ResponseTopic = msg.ResponseTopic
	} else if v := msg.Properties["response-topic"]; v != "" {
		props.ResponseTopic = v
	}

	if len(msg.CorrelationData) > 0 {
		props.CorrelationData = msg.CorrelationData
	} else if v := msg.Properties["correlation-id"]; v != "" {
		if decoded, err := base64.StdEncoding.DecodeString(v); err == nil {
			props.CorrelationData = decoded
		} else {
			props.CorrelationData = []byte(v)
		}
	}

	if msg.PayloadFormat != nil {
		props.PayloadFormat = msg.PayloadFormat
	} else if v := msg.Properties["payload-format"]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 8); err == nil {
			pf := byte(n)
			props.PayloadFormat = &pf
		}
	}

	userProps := make(map[string]string)
	if msg.Properties != nil {
		for k, v := range msg.Properties {
			if isReservedUserPropertyKey(k) {
				continue
			}
			userProps[k] = v
		}
	}
	if msg.UserProperties != nil {
		for k, v := range msg.UserProperties {
			userProps[k] = v
		}
	}

	if len(userProps) > 0 {
		props.User = make([]v5.User, 0, len(userProps))
		for k, v := range userProps {
			props.User = append(props.User, v5.User{Key: k, Value: v})
		}
	}
}

func isReservedUserPropertyKey(key string) bool {
	switch key {
	case "content-type", "response-topic", "correlation-id", "payload-format":
		return true
	default:
		return false
	}
}

// DeliverToClient implements cluster.MessageHandler.DeliverToClient.
func (b *Broker) DeliverToClient(ctx context.Context, clientID string, msg *cluster.Message) error {
	s := b.Get(clientID)
	if s == nil {
		return fmt.Errorf("session not found: %s", clientID)
	}

	// cluster.Message comes from cluster - create storage.Message with zero-copy buffer
	storeMsg := storage.AcquireMessage()
	storeMsg.Topic = msg.Topic
	storeMsg.QoS = msg.QoS
	storeMsg.Retain = msg.Retain
	storeMsg.Properties = msg.Properties
	storeMsg.SetPayloadFromBytes(msg.Payload)

	_, err := b.DeliverToSession(s, storeMsg)
	// Note: DeliverToSession will release the message for QoS 0
	// For QoS 1/2, Inflight storage takes ownership
	return err
}

// DeliverToSessionByID delivers a message to a client by client ID.
// This implements the BrokerInterface required by the queue manager.
func (b *Broker) DeliverToSessionByID(ctx context.Context, clientID string, msg interface{}) error {
	s := b.Get(clientID)
	if s == nil {
		return fmt.Errorf("session not found: %s", clientID)
	}

	// Convert queue message to storage message
	queueMsg, ok := msg.(*storage.Message)
	if !ok {
		return fmt.Errorf("invalid message type")
	}

	_, err := b.DeliverToSession(s, queueMsg)
	return err
}
