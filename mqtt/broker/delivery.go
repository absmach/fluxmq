// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"log/slog"
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
		msg.ReleasePayload() // Drop expired message - release buffer
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
		err := b.DeliverMessage(s, msg)
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
		msg.ReleasePayload() // Failed to store - release buffer
		return 0, err
	}

	if err := b.DeliverMessage(s, msg); err != nil {
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

// AckMessage acknowledges a message by packet ID.
func (b *Broker) AckMessage(s *session.Session, packetID uint16) error {
	s.Inflight().Ack(packetID)
	return nil
}

// DeliverMessage sends a message packet to the session's connection.
func (b *Broker) DeliverMessage(s *session.Session, msg *storage.Message) error {
	b.stats.IncrementPublishSent()
	b.stats.AddBytesSent(uint64(len(msg.GetPayload())))

	var pub packets.ControlPacket

	switch s.Version {
	case 5:
		p := v5.AcquirePublish()
		defer v5.ReleasePublish(p)

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

		pub = p
	default:
		p := v3.AcquirePublish()
		defer v3.ReleasePublish(p)

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

	return s.WritePacket(pub)
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
