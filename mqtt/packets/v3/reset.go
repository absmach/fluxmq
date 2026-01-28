// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import "github.com/absmach/fluxmq/mqtt/packets"

func (c *Connect) Reset() {
	c.FixedHeader = packets.FixedHeader{PacketType: packets.ConnectType}
	c.ProtocolName = ""
	c.ProtocolVersion = 0
	c.UsernameFlag = false
	c.PasswordFlag = false
	c.WillRetain = false
	c.WillQoS = 0
	c.WillFlag = false
	c.CleanSession = false
	c.ReservedBit = 0
	c.KeepAlive = 0
	c.ClientID = ""
	c.WillTopic = ""
	c.WillMessage = nil
	c.Username = ""
	c.Password = nil
}

func (c *ConnAck) Reset() {
	c.FixedHeader = packets.FixedHeader{PacketType: packets.ConnAckType}
	c.SessionPresent = false
	c.ReturnCode = 0
}

func (p *Publish) Reset() {
	p.FixedHeader = packets.FixedHeader{PacketType: packets.PublishType}
	p.TopicName = ""
	p.ID = 0
	p.Payload = nil
}

func (p *PubAck) Reset() {
	p.FixedHeader = packets.FixedHeader{PacketType: packets.PubAckType}
	p.ID = 0
}

func (p *PubRec) Reset() {
	p.FixedHeader = packets.FixedHeader{PacketType: packets.PubRecType}
	p.ID = 0
}

func (p *PubRel) Reset() {
	p.FixedHeader = packets.FixedHeader{PacketType: packets.PubRelType, QoS: 1}
	p.ID = 0
}

func (p *PubComp) Reset() {
	p.FixedHeader = packets.FixedHeader{PacketType: packets.PubCompType}
	p.ID = 0
}

func (s *Subscribe) Reset() {
	s.FixedHeader = packets.FixedHeader{PacketType: packets.SubscribeType, QoS: 1}
	s.ID = 0
	s.Topics = s.Topics[:0]
}

func (s *SubAck) Reset() {
	s.FixedHeader = packets.FixedHeader{PacketType: packets.SubAckType}
	s.ID = 0
	s.ReturnCodes = nil
}

func (u *Unsubscribe) Reset() {
	u.FixedHeader = packets.FixedHeader{PacketType: packets.UnsubscribeType, QoS: 1}
	u.ID = 0
	u.Topics = u.Topics[:0]
}

func (u *UnSubAck) Reset() {
	u.FixedHeader = packets.FixedHeader{PacketType: packets.UnsubAckType}
	u.ID = 0
}

func (p *PingReq) Reset() {
	p.FixedHeader = packets.FixedHeader{PacketType: packets.PingReqType}
}

func (p *PingResp) Reset() {
	p.FixedHeader = packets.FixedHeader{PacketType: packets.PingRespType}
}

func (d *Disconnect) Reset() {
	d.FixedHeader = packets.FixedHeader{PacketType: packets.DisconnectType}
}
