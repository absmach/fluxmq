package v5

import (
	"bytes"
	"fmt"
	"io"

	"github.com/absmach/mqtt/core/packets"
)

// FixedHeader represents the MQTT fixed header for v5 packets.
// It embeds packets.FixedHeader to allow v5-specific methods.
type FixedHeader = packets.FixedHeader

// Re-export other types from parent package for convenience
type (
	Details       = packets.Details
	ControlPacket = packets.ControlPacket
	User          = packets.User
	Detailer      = packets.Detailer
	Resetter      = packets.Resetter
)

// Re-export packet type constants
const (
	ConnectType     = packets.ConnectType
	ConnAckType     = packets.ConnAckType
	PublishType     = packets.PublishType
	PubAckType      = packets.PubAckType
	PubRecType      = packets.PubRecType
	PubRelType      = packets.PubRelType
	PubCompType     = packets.PubCompType
	SubscribeType   = packets.SubscribeType
	SubAckType      = packets.SubAckType
	UnsubscribeType = packets.UnsubscribeType
	UnsubAckType    = packets.UnsubAckType
	PingReqType     = packets.PingReqType
	PingRespType    = packets.PingRespType
	DisconnectType  = packets.DisconnectType
	AuthType        = packets.AuthType
)

// Re-export protocol version constants
const (
	V31  = packets.V31  // MQTT 3.1
	V311 = packets.V311 // MQTT 3.1.1
	V5   = packets.V5   // MQTT 5.0
)

// PacketNames maps packet type constants to string names
var PacketNames = packets.PacketNames

// NewControlPacket creates a new MQTT 5.0 packet of the specified type.
func NewControlPacket(packetType byte) ControlPacket {
	switch packetType {
	case ConnectType:
		return &Connect{FixedHeader: FixedHeader{PacketType: ConnectType}}
	case ConnAckType:
		return &ConnAck{FixedHeader: FixedHeader{PacketType: ConnAckType}}
	case PublishType:
		return &Publish{FixedHeader: FixedHeader{PacketType: PublishType}}
	case PubAckType:
		return &PubAck{FixedHeader: FixedHeader{PacketType: PubAckType}}
	case PubRecType:
		return &PubRec{FixedHeader: FixedHeader{PacketType: PubRecType}}
	case PubRelType:
		return &PubRel{FixedHeader: FixedHeader{PacketType: PubRelType, QoS: 1}}
	case PubCompType:
		return &PubComp{FixedHeader: FixedHeader{PacketType: PubCompType}}
	case SubscribeType:
		return &Subscribe{FixedHeader: FixedHeader{PacketType: SubscribeType, QoS: 1}}
	case SubAckType:
		return &SubAck{FixedHeader: FixedHeader{PacketType: SubAckType}}
	case UnsubscribeType:
		return &Unsubscribe{FixedHeader: FixedHeader{PacketType: UnsubscribeType, QoS: 1}}
	case UnsubAckType:
		return &UnSubAck{FixedHeader: FixedHeader{PacketType: UnsubAckType}}
	case PingReqType:
		return &PingReq{FixedHeader: FixedHeader{PacketType: PingReqType}}
	case PingRespType:
		return &PingResp{FixedHeader: FixedHeader{PacketType: PingRespType}}
	case DisconnectType:
		return &Disconnect{FixedHeader: FixedHeader{PacketType: DisconnectType}}
	case AuthType:
		return &Auth{FixedHeader: FixedHeader{PacketType: AuthType}}
	}
	return nil
}

// NewControlPacketWithHeader creates a new MQTT 5.0 packet with the given fixed header.
func NewControlPacketWithHeader(fh FixedHeader) (ControlPacket, error) {
	switch fh.PacketType {
	case ConnectType:
		return &Connect{FixedHeader: fh}, nil
	case ConnAckType:
		return &ConnAck{FixedHeader: fh}, nil
	case PublishType:
		return &Publish{FixedHeader: fh}, nil
	case PubAckType:
		return &PubAck{FixedHeader: fh}, nil
	case PubRecType:
		return &PubRec{FixedHeader: fh}, nil
	case PubRelType:
		return &PubRel{FixedHeader: fh}, nil
	case PubCompType:
		return &PubComp{FixedHeader: fh}, nil
	case SubscribeType:
		return &Subscribe{FixedHeader: fh}, nil
	case SubAckType:
		return &SubAck{FixedHeader: fh}, nil
	case UnsubscribeType:
		return &Unsubscribe{FixedHeader: fh}, nil
	case UnsubAckType:
		return &UnSubAck{FixedHeader: fh}, nil
	case PingReqType:
		return &PingReq{FixedHeader: fh}, nil
	case PingRespType:
		return &PingResp{FixedHeader: fh}, nil
	case DisconnectType:
		return &Disconnect{FixedHeader: fh}, nil
	case AuthType:
		return &Auth{FixedHeader: fh}, nil
	}
	return nil, fmt.Errorf("unsupported packet type 0x%x", fh.PacketType)
}

// ReadPacket reads an MQTT 5.0 packet from the reader.
// Returns the packet, encoded fixed header, packet body bytes, and any error.
func ReadPacket(r io.Reader) (ControlPacket, []byte, []byte, error) {
	var fh FixedHeader
	b := make([]byte, 1)

	_, err := io.ReadFull(r, b)
	if err != nil {
		return nil, nil, nil, err
	}

	err = fh.Decode(b[0], r)
	if err != nil {
		return nil, nil, nil, err
	}

	cp, err := NewControlPacketWithHeader(fh)
	if err != nil {
		return nil, nil, nil, err
	}

	packetBytes := make([]byte, fh.RemainingLength)
	n, err := io.ReadFull(r, packetBytes)
	if err != nil {
		return nil, nil, nil, err
	}
	if n != fh.RemainingLength {
		return nil, nil, nil, packets.ErrFailRemaining
	}

	err = cp.Unpack(bytes.NewReader(packetBytes))
	return cp, fh.Encode(), packetBytes, err
}
