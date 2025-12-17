// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

// Reset clears all fields in the Connect packet for reuse.
func (pkt *Connect) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: ConnectType}
	pkt.ProtocolName = ""
	pkt.ProtocolVersion = 0
	pkt.UsernameFlag = false
	pkt.PasswordFlag = false
	pkt.WillRetain = false
	pkt.WillQoS = 0
	pkt.WillFlag = false
	pkt.CleanStart = false
	pkt.ReservedBit = 0
	pkt.KeepAlive = 0
	pkt.Properties = nil
	pkt.ClientID = ""
	pkt.WillProperties = nil
	pkt.WillTopic = ""
	pkt.WillPayload = nil
	pkt.Username = ""
	pkt.Password = nil
}

// Reset clears all fields in the ConnAck packet for reuse.
func (pkt *ConnAck) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: ConnAckType}
	pkt.SessionPresent = false
	pkt.ReasonCode = 0
	pkt.Properties = nil
}

// Reset clears all fields in the Publish packet for reuse.
func (pkt *Publish) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: PublishType}
	pkt.ID = 0
	pkt.TopicName = ""
	pkt.Properties = nil
	pkt.Payload = nil
}

// Reset clears all fields in the PubAck packet for reuse.
func (pkt *PubAck) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: PubAckType}
	pkt.ID = 0
	pkt.ReasonCode = nil
	pkt.Properties = nil
}

// Reset clears all fields in the PubRec packet for reuse.
func (pkt *PubRec) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: PubRecType}
	pkt.ID = 0
	pkt.ReasonCode = nil
	pkt.Properties = nil
}

// Reset clears all fields in the PubRel packet for reuse.
func (pkt *PubRel) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: PubRelType, QoS: 1}
	pkt.ID = 0
	pkt.ReasonCode = nil
	pkt.Properties = nil
}

// Reset clears all fields in the PubComp packet for reuse.
func (pkt *PubComp) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: PubCompType}
	pkt.ID = 0
	pkt.ReasonCode = nil
	pkt.Properties = nil
}

// Reset clears all fields in the Subscribe packet for reuse.
func (pkt *Subscribe) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: SubscribeType, QoS: 1}
	pkt.ID = 0
	pkt.Properties = nil
	pkt.Opts = pkt.Opts[:0] // Keep capacity, reset length
}

// Reset clears all fields in the SubAck packet for reuse.
func (pkt *SubAck) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: SubAckType}
	pkt.ID = 0
	pkt.Properties = nil
	pkt.ReasonCodes = nil
	pkt.Reason = nil
}

// Reset clears all fields in the Unsubscribe packet for reuse.
func (pkt *Unsubscribe) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: UnsubscribeType, QoS: 1}
	pkt.ID = 0
	pkt.Properties = nil
	pkt.Topics = pkt.Topics[:0] // Keep capacity, reset length
}

// Reset clears all fields in the UnSubAck packet for reuse.
func (pkt *UnSubAck) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: UnsubAckType}
	pkt.ID = 0
	pkt.Properties = nil
	pkt.ReasonCodes = nil
}

// Reset clears all fields in the PingReq packet for reuse.
func (pkt *PingReq) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: PingReqType}
}

// Reset clears all fields in the PingResp packet for reuse.
func (pkt *PingResp) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: PingRespType}
}

// Reset clears all fields in the Disconnect packet for reuse.
func (pkt *Disconnect) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: DisconnectType}
	pkt.ReasonCode = 0
	pkt.Properties = nil
}

// Reset clears all fields in the Auth packet for reuse.
func (pkt *Auth) Reset() {
	pkt.FixedHeader = FixedHeader{PacketType: AuthType}
	pkt.ReasonCode = 0
	pkt.Properties = nil
}
