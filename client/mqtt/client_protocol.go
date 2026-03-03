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

func (c *Client) encodeSubscribePacket(packetID uint16, opts []*SubscribeOption) []byte {
	if c.isMQTTv5() {
		v5Opts := make([]v5.SubOption, len(opts))
		for i, opt := range opts {
			v5Opts[i] = v5.SubOption{
				Topic:  opt.Topic,
				MaxQoS: opt.QoS,
			}

			if opt.NoLocal {
				noLocal := true
				v5Opts[i].NoLocal = &noLocal
			}

			if opt.RetainAsPublished {
				rap := true
				v5Opts[i].RetainAsPublished = &rap
			}

			if opt.RetainHandling > 0 {
				v5Opts[i].RetainHandling = &opt.RetainHandling
			}
		}

		pkt := &v5.Subscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
			ID:          packetID,
			Opts:        v5Opts,
		}

		if len(opts) > 0 && opts[0].SubscriptionID > 0 {
			subID := int(opts[0].SubscriptionID)
			pkt.Properties = &v5.SubscribeProperties{
				SubscriptionIdentifier: &subID,
			}
		}

		return pkt.Encode()
	}

	topics := make([]v3.Topic, len(opts))
	for i, opt := range opts {
		topics[i] = v3.Topic{Name: opt.Topic, QoS: opt.QoS}
	}

	return (&v3.Subscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      topics,
	}).Encode()
}

func (c *Client) encodeUnsubscribePacket(packetID uint16, topics []string) []byte {
	if c.isMQTTv5() {
		return (&v5.Unsubscribe{
			FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
			ID:          packetID,
			Topics:      topics,
		}).Encode()
	}

	return (&v3.Unsubscribe{
		FixedHeader: packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1},
		ID:          packetID,
		Topics:      topics,
	}).Encode()
}

func (c *Client) encodeDisconnectPacket(reasonCode byte, sessionExpiry uint32, reasonString string) []byte {
	if c.isMQTTv5() {
		pkt := &v5.Disconnect{
			FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
			ReasonCode:  reasonCode,
		}

		if sessionExpiry > 0 || reasonString != "" {
			pkt.Properties = &v5.DisconnectProperties{}

			if sessionExpiry > 0 {
				pkt.Properties.SessionExpiryInterval = &sessionExpiry
			}

			if reasonString != "" {
				pkt.Properties.ReasonString = reasonString
			}
		}

		return pkt.Encode()
	}

	return (&v3.Disconnect{
		FixedHeader: packets.FixedHeader{PacketType: packets.DisconnectType},
	}).Encode()
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

func messageFromV5Publish(pkt *v5.Publish, topic string) *Message {
	msg := &Message{
		Topic:    topic,
		Payload:  pkt.Payload,
		QoS:      pkt.QoS,
		Retain:   pkt.Retain,
		Dup:      pkt.Dup,
		PacketID: pkt.ID,
	}

	if pkt.Properties == nil {
		return msg
	}

	msg.PayloadFormat = pkt.Properties.PayloadFormat
	msg.MessageExpiry = pkt.Properties.MessageExpiry
	msg.ContentType = pkt.Properties.ContentType
	msg.ResponseTopic = pkt.Properties.ResponseTopic
	msg.CorrelationData = pkt.Properties.CorrelationData

	if len(pkt.Properties.User) > 0 {
		msg.UserProperties = make(map[string]string, len(pkt.Properties.User))
		for _, u := range pkt.Properties.User {
			msg.UserProperties[u.Key] = u.Value
		}
	}

	if pkt.Properties.SubscriptionID != nil {
		msg.SubscriptionIDs = []uint32{uint32(*pkt.Properties.SubscriptionID)}
	}

	return msg
}

func (c *Client) decodeIncomingPublish(pkt packets.ControlPacket) (*Message, bool) {
	if c.isMQTTv5() {
		p, ok := pkt.(*v5.Publish)
		if !ok {
			return nil, false
		}

		topic := p.TopicName

		if c.topicAliases != nil && p.Properties != nil && p.Properties.TopicAlias != nil {
			alias := *p.Properties.TopicAlias
			if topic != "" {
				c.topicAliases.registerInbound(alias, topic)
			} else {
				var resolved bool
				topic, resolved = c.topicAliases.resolveInbound(alias)
				if !resolved {
					// Unknown alias is a protocol error; drop the message.
					return nil, false
				}
			}
		}

		return messageFromV5Publish(p, topic), true
	}

	p, ok := pkt.(*v3.Publish)
	if !ok {
		return nil, false
	}

	return &Message{
		Topic:    p.TopicName,
		Payload:  p.Payload,
		QoS:      p.QoS,
		Retain:   p.Retain,
		Dup:      p.Dup,
		PacketID: p.ID,
	}, true
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
		return messageFromV5Publish(p, p.TopicName), nil
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

func (c *Client) packConnectPacket(conn net.Conn, keepAlive uint16) error {
	if c.isMQTTv5() {
		pkt := &v5.Connect{
			FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
			ClientID:        c.opts.ClientID,
			KeepAlive:       keepAlive,
			ProtocolName:    "MQTT",
			ProtocolVersion: 5,
			CleanStart:      c.opts.CleanSession,
		}

		if c.opts.Username != "" {
			pkt.UsernameFlag = true
			pkt.Username = c.opts.Username
		}
		if c.opts.Password != "" {
			pkt.PasswordFlag = true
			pkt.Password = []byte(c.opts.Password)
		}

		if c.opts.Will != nil {
			pkt.WillFlag = true
			pkt.WillQoS = c.opts.Will.QoS
			pkt.WillRetain = c.opts.Will.Retain
			pkt.WillTopic = c.opts.Will.Topic
			pkt.WillPayload = c.opts.Will.Payload

			if c.opts.Will.WillDelayInterval > 0 || c.opts.Will.PayloadFormat != nil ||
				c.opts.Will.MessageExpiry > 0 || c.opts.Will.ContentType != "" ||
				c.opts.Will.ResponseTopic != "" || len(c.opts.Will.CorrelationData) > 0 ||
				len(c.opts.Will.UserProperties) > 0 {
				pkt.WillProperties = &v5.WillProperties{}

				if c.opts.Will.WillDelayInterval > 0 {
					pkt.WillProperties.WillDelayInterval = &c.opts.Will.WillDelayInterval
				}

				if c.opts.Will.PayloadFormat != nil {
					pkt.WillProperties.PayloadFormat = c.opts.Will.PayloadFormat
				}

				if c.opts.Will.MessageExpiry > 0 {
					pkt.WillProperties.MessageExpiry = &c.opts.Will.MessageExpiry
				}

				if c.opts.Will.ContentType != "" {
					pkt.WillProperties.ContentType = c.opts.Will.ContentType
				}

				if c.opts.Will.ResponseTopic != "" {
					pkt.WillProperties.ResponseTopic = c.opts.Will.ResponseTopic
				}

				if len(c.opts.Will.CorrelationData) > 0 {
					pkt.WillProperties.CorrelationData = c.opts.Will.CorrelationData
				}

				if len(c.opts.Will.UserProperties) > 0 {
					pkt.WillProperties.User = make([]v5.User, 0, len(c.opts.Will.UserProperties))
					for k, v := range c.opts.Will.UserProperties {
						pkt.WillProperties.User = append(pkt.WillProperties.User, v5.User{Key: k, Value: v})
					}
				}
			}
		}

		if c.opts.SessionExpiry > 0 || c.opts.ReceiveMaximum > 0 ||
			c.opts.MaximumPacketSize > 0 || c.opts.TopicAliasMaximum > 0 ||
			c.opts.RequestResponseInfo || !c.opts.RequestProblemInfo ||
			c.opts.AuthMethod != "" {
			if pkt.Properties == nil {
				pkt.Properties = &v5.ConnectProperties{}
			}

			if c.opts.SessionExpiry > 0 {
				pkt.Properties.SessionExpiryInterval = &c.opts.SessionExpiry
			}

			if c.opts.ReceiveMaximum > 0 {
				pkt.Properties.ReceiveMaximum = &c.opts.ReceiveMaximum
			}

			if c.opts.MaximumPacketSize > 0 {
				pkt.Properties.MaximumPacketSize = &c.opts.MaximumPacketSize
			}

			if c.opts.TopicAliasMaximum > 0 {
				pkt.Properties.TopicAliasMaximum = &c.opts.TopicAliasMaximum
			}

			if c.opts.RequestResponseInfo {
				one := byte(1)
				pkt.Properties.RequestResponseInfo = &one
			}

			if !c.opts.RequestProblemInfo {
				zero := byte(0)
				pkt.Properties.RequestProblemInfo = &zero
			}

			if c.opts.AuthMethod != "" {
				pkt.Properties.AuthMethod = c.opts.AuthMethod
				pkt.Properties.AuthData = c.opts.AuthData
			}
		}

		c.updateActivity()
		return pkt.Pack(conn)
	}

	pkt := &v3.Connect{
		FixedHeader:     packets.FixedHeader{PacketType: packets.ConnectType},
		ClientID:        c.opts.ClientID,
		KeepAlive:       keepAlive,
		ProtocolName:    "MQTT",
		ProtocolVersion: 4,
		CleanSession:    c.opts.CleanSession,
	}

	if c.opts.Username != "" {
		pkt.UsernameFlag = true
		pkt.Username = c.opts.Username
	}
	if c.opts.Password != "" {
		pkt.PasswordFlag = true
		pkt.Password = []byte(c.opts.Password)
	}

	if c.opts.Will != nil {
		pkt.WillFlag = true
		pkt.WillQoS = c.opts.Will.QoS
		pkt.WillRetain = c.opts.Will.Retain
		pkt.WillTopic = c.opts.Will.Topic
		pkt.WillMessage = c.opts.Will.Payload
	}

	return pkt.Pack(conn)
}
