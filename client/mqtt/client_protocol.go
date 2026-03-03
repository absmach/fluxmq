// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"bytes"
	"fmt"
	"net"

	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

func (c *Client) isMQTTv5() bool {
	return c.opts.ProtocolVersion == 5
}

func (c *Client) readPacketFromConn(conn net.Conn) (packets.ControlPacket, error) {
	if c.isMQTTv5() {
		pkt, _, _, err := v5.ReadPacket(conn)
		return pkt, err
	}
	return v3.ReadPacket(conn)
}

func packetIDFromControlPacket(pkt packets.ControlPacket) uint16 {
	switch p := pkt.(type) {
	case *v3.PubAck:
		return p.ID
	case *v5.PubAck:
		return p.ID
	case *v3.PubRec:
		return p.ID
	case *v5.PubRec:
		return p.ID
	case *v3.PubRel:
		return p.ID
	case *v5.PubRel:
		return p.ID
	case *v3.PubComp:
		return p.ID
	case *v5.PubComp:
		return p.ID
	case *v3.SubAck:
		return p.ID
	case *v5.SubAck:
		return p.ID
	case *v3.UnSubAck:
		return p.ID
	case *v5.UnsubAck:
		return p.ID
	default:
		return 0
	}
}

func subAckDetails(pkt packets.ControlPacket) (uint16, []byte, bool) {
	switch p := pkt.(type) {
	case *v5.SubAck:
		var reasonCodes []byte
		if p.ReasonCodes != nil {
			reasonCodes = *p.ReasonCodes
		}
		return p.ID, reasonCodes, true
	case *v3.SubAck:
		return p.ID, p.ReturnCodes, false
	default:
		return 0, nil, false
	}
}

func (c *Client) encodePingReqPacket() []byte {
	if c.isMQTTv5() {
		pkt := &v5.PingReq{
			FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
		}
		return pkt.Encode()
	}

	pkt := &v3.PingReq{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingReqType},
	}
	return pkt.Encode()
}

func (c *Client) encodeQoSControlPacket(packetType byte, packetID uint16, qos byte) []byte {
	if c.isMQTTv5() {
		switch packetType {
		case packets.PubAckType:
			return (&v5.PubAck{
				FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
				ID:          packetID,
			}).Encode()
		case packets.PubRecType:
			return (&v5.PubRec{
				FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
				ID:          packetID,
			}).Encode()
		case packets.PubRelType:
			return (&v5.PubRel{
				FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
				ID:          packetID,
			}).Encode()
		case packets.PubCompType:
			return (&v5.PubComp{
				FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
				ID:          packetID,
			}).Encode()
		default:
			panic(fmt.Sprintf("encodeQoSControlPacket: unexpected v5 packet type 0x%02X", packetType))
		}
	}

	switch packetType {
	case packets.PubAckType:
		return (&v3.PubAck{
			FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
			ID:          packetID,
		}).Encode()
	case packets.PubRecType:
		return (&v3.PubRec{
			FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
			ID:          packetID,
		}).Encode()
	case packets.PubRelType:
		return (&v3.PubRel{
			FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
			ID:          packetID,
		}).Encode()
	case packets.PubCompType:
		return (&v3.PubComp{
			FixedHeader: packets.FixedHeader{PacketType: packetType, QoS: qos},
			ID:          packetID,
		}).Encode()
	default:
		panic(fmt.Sprintf("encodeQoSControlPacket: unexpected v3 packet type 0x%02X", packetType))
	}
}

func (c *Client) encodePublishPacket(msg *Message, packetID uint16, useTopicAlias bool, dst []byte) []byte {
	if msg == nil {
		return dst
	}

	if c.isMQTTv5() {
		pkt := &v5.Publish{
			FixedHeader: packets.FixedHeader{
				PacketType: packets.PublishType,
				QoS:        msg.QoS,
				Retain:     msg.Retain,
				Dup:        msg.Dup,
			},
			TopicName: msg.Topic,
			Payload:   msg.Payload,
			ID:        packetID,
		}

		if msg.PayloadFormat != nil || msg.MessageExpiry != nil || msg.ContentType != "" || msg.ResponseTopic != "" || len(msg.CorrelationData) > 0 || len(msg.UserProperties) > 0 {
			pkt.Properties = &v5.PublishProperties{}
		}

		if pkt.Properties != nil {
			pkt.Properties.PayloadFormat = msg.PayloadFormat
			pkt.Properties.MessageExpiry = msg.MessageExpiry
			pkt.Properties.ContentType = msg.ContentType
			pkt.Properties.ResponseTopic = msg.ResponseTopic
			pkt.Properties.CorrelationData = msg.CorrelationData
		}

		if len(msg.UserProperties) > 0 {
			if pkt.Properties == nil {
				pkt.Properties = &v5.PublishProperties{}
			}
			pkt.Properties.User = make([]v5.User, 0, len(msg.UserProperties))
			for k, v := range msg.UserProperties {
				pkt.Properties.User = append(pkt.Properties.User, v5.User{Key: k, Value: v})
			}
		}

		if useTopicAlias && c.topicAliases != nil {
			if alias, isNew, ok := c.topicAliases.getOrAssignOutbound(msg.Topic); ok {
				if pkt.Properties == nil {
					pkt.Properties = &v5.PublishProperties{}
				}
				pkt.Properties.TopicAlias = &alias
				if !isNew {
					pkt.TopicName = ""
				}
			}
		}

		return pkt.EncodeTo(dst)
	}

	pkt := &v3.Publish{
		FixedHeader: packets.FixedHeader{
			PacketType: packets.PublishType,
			QoS:        msg.QoS,
			Retain:     msg.Retain,
			Dup:        msg.Dup,
		},
		TopicName: msg.Topic,
		Payload:   msg.Payload,
		ID:        packetID,
	}

	return pkt.EncodeTo(dst)
}

func patchPublishPacketID(wire []byte, packetID uint16) error {
	if len(wire) < 4 {
		return ErrInvalidMessage
	}

	// Skip fixed header and remaining-length VBI.
	i := 1
	for n := 0; n < 4; n++ {
		if i >= len(wire) {
			return ErrInvalidMessage
		}
		b := wire[i]
		i++
		if (b & 0x80) == 0 {
			break
		}
		if n == 3 {
			return ErrInvalidMessage
		}
	}

	// Topic name length and bytes.
	if i+2 > len(wire) {
		return ErrInvalidMessage
	}
	topicLen := int(wire[i])<<8 | int(wire[i+1])
	i += 2 + topicLen
	if i+2 > len(wire) {
		return ErrInvalidMessage
	}

	wire[i] = byte(packetID >> 8)
	wire[i+1] = byte(packetID)
	return nil
}

func (c *Client) decodeBufferedPublish(wire []byte) (*Message, error) {
	if len(wire) == 0 {
		return nil, ErrInvalidMessage
	}

	if c.isMQTTv5() {
		pkt, _, _, err := v5.ReadPacket(bytes.NewReader(wire))
		if err != nil {
			return nil, err
		}
		p, ok := pkt.(*v5.Publish)
		if !ok {
			return nil, ErrUnexpectedPacket
		}
		msg := &Message{
			Topic:    p.TopicName,
			Payload:  p.Payload,
			QoS:      p.QoS,
			Retain:   p.Retain,
			Dup:      p.Dup,
			PacketID: p.ID,
		}
		if p.Properties != nil {
			msg.PayloadFormat = p.Properties.PayloadFormat
			msg.MessageExpiry = p.Properties.MessageExpiry
			msg.ContentType = p.Properties.ContentType
			msg.ResponseTopic = p.Properties.ResponseTopic
			msg.CorrelationData = p.Properties.CorrelationData
			if len(p.Properties.User) > 0 {
				msg.UserProperties = make(map[string]string, len(p.Properties.User))
				for _, u := range p.Properties.User {
					msg.UserProperties[u.Key] = u.Value
				}
			}
		}
		return msg, nil
	}

	pkt, err := v3.ReadPacket(bytes.NewReader(wire))
	if err != nil {
		return nil, err
	}
	p, ok := pkt.(*v3.Publish)
	if !ok {
		return nil, ErrUnexpectedPacket
	}
	return &Message{
		Topic:    p.TopicName,
		Payload:  p.Payload,
		QoS:      p.QoS,
		Retain:   p.Retain,
		Dup:      p.Dup,
		PacketID: p.ID,
	}, nil
}

// Keep-alive management
