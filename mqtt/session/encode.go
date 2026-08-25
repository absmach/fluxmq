// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"time"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

// EncodePublish builds a PUBLISH control packet for the given protocol version
// from msg. The packet is taken from the version's pool and the caller must
// return it via pkt.Release() once written. dup sets the DUP flag (true for a
// retransmission). For v5 it applies the message's PUBLISH properties and the
// remaining message-expiry interval, so a retransmission carries the same
// properties as the original send.
//
// This is the single encoder shared by the first-send path (broker delivery)
// and the retransmission path (ProcessRetries), so the two cannot diverge.
//
// The payload is not copied: the packet points into the message's buffer and
// takes its own reference to it, which pkt.Release() drops. The caller may
// therefore release the message as soon as this returns, even though the packet
// may not be serialized until later on an asynchronous send queue.
func EncodePublish(msg *message.Envelope, packetID uint16, version byte, dup bool) packets.ControlPacket {
	return encodePublish(msg, packetID, version, dup, msg.Broker.Delivery.QoS, msg.Broker.Delivery.Retain)
}

// EncodePublishDelivery builds a packet from immutable publication data and
// caller-owned delivery flags. It lets QoS 0 fanout share one envelope instead
// of allocating or pooling a mutable envelope for every subscriber.
func EncodePublishDelivery(msg *message.Envelope, packetID uint16, version byte, dup bool, qos byte, retain bool) packets.ControlPacket {
	return encodePublish(msg, packetID, version, dup, qos, retain)
}

func encodePublish(msg *message.Envelope, packetID uint16, version byte, dup bool, qos byte, retain bool) packets.ControlPacket {
	if version == packets.V5 {
		p := v5.AcquirePublish()
		p.FixedHeader = packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        qos,
			Retain:     retain,
			Dup:        dup,
		}
		p.TopicName = msg.Topic
		p.Payload = msg.PayloadBytes()
		p.PayloadRef = retainPayload(msg)
		p.ID = packetID

		// Send the remaining message-expiry interval, not the original.
		if msg.User.MessageExpiry != nil && !msg.Broker.Delivery.ExpiresAt.IsZero() {
			if remaining := time.Until(msg.Broker.Delivery.ExpiresAt); remaining > 0 {
				remainingSec := uint32(remaining.Seconds())
				p.Properties.MessageExpiry = &remainingSec
			}
		}
		applyPublishProperties(p.Properties, msg)
		return p
	}

	p := v3.AcquirePublish()
	p.FixedHeader = packets.FixedHeader{
		PacketType: packets.PublishType,
		QoS:        qos,
		Retain:     retain,
		Dup:        dup,
	}
	p.TopicName = msg.Topic
	p.Payload = msg.PayloadBytes()
	p.PayloadRef = retainPayload(msg)
	p.ID = packetID
	return p
}

// retainPayload takes a reference to the message's payload buffer on behalf of
// an outbound packet.
func retainPayload(msg *message.Envelope) packets.PayloadRef {
	if msg.Payload == nil {
		return nil
	}
	msg.Payload.Retain()
	return msg.Payload
}

func applyPublishProperties(props *v5.PublishProperties, msg *message.Envelope) {
	if props == nil || msg == nil {
		return
	}

	props.ContentType = msg.User.ContentType
	props.ResponseTopic = msg.User.ResponseTopic
	props.CorrelationData = msg.User.CorrelationData
	props.PayloadFormat = msg.User.PayloadFormat

	projected := message.ProjectProperties(msg, message.PublicProjection)
	if len(projected) > 0 {
		props.User = make([]v5.User, 0, len(projected))
		for key, value := range projected {
			props.User = append(props.User, v5.User{Key: key, Value: value})
		}
	}
}
