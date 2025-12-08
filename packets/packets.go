// Package packets provides shared constants and interfaces for MQTT packet handling.
// Version-specific implementations are in the v3 and v5 subpackages.
package packets

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// ErrFailRemaining indicates remaining data does not match the size of sent data.
var ErrFailRemaining = errors.New("remaining data length does not match data size")

// Protocol version constants.
const (
	V31  byte = 0x03 // MQTT 3.1
	V311 byte = 0x04 // MQTT 3.1.1
	V5   byte = 0x05 // MQTT 5.0
)

// Packet type constants.
const (
	ConnectType = iota + 1 // 0 value is forbidden
	ConnAckType
	PublishType
	PubAckType
	PubRecType
	PubRelType
	PubCompType
	SubscribeType
	SubAckType
	UnsubscribeType
	UnsubAckType
	PingReqType
	PingRespType
	DisconnectType
	AuthType // MQTT 5.0 only
)

// PacketNames maps packet type constants to string names.
var PacketNames = map[byte]string{
	ConnectType:     "CONNECT",
	ConnAckType:     "CONNACK",
	PublishType:     "PUBLISH",
	PubAckType:      "PUBACK",
	PubRecType:      "PUBREC",
	PubRelType:      "PUBREL",
	PubCompType:     "PUBCOMP",
	SubscribeType:   "SUBSCRIBE",
	SubAckType:      "SUBACK",
	UnsubscribeType: "UNSUBSCRIBE",
	UnsubAckType:    "UNSUBACK",
	PingReqType:     "PINGREQ",
	PingRespType:    "PINGRESP",
	DisconnectType:  "DISCONNECT",
	AuthType:        "AUTH",
}

// ControlPacket is the interface for all MQTT control packets.
// Both v3 and v5 implementations satisfy this interface.
type ControlPacket interface {
	// Encode serializes the packet to bytes.
	Encode() []byte

	// Pack writes the encoded packet to the writer.
	Pack(w io.Writer) error

	// Unpack deserializes the packet from the reader.
	// The version parameter indicates the MQTT protocol version.
	Unpack(r io.Reader, v byte) error

	// Type returns the packet type constant.
	Type() byte

	// String returns a human-readable representation.
	String() string
}

// FixedHeader represents the MQTT fixed header present in all packets.
type FixedHeader struct {
	PacketType      byte
	Dup             bool
	QoS             byte
	Retain          bool
	RemainingLength int
}

// Details contains packet metadata useful for QoS handling.
type Details struct {
	Type byte
	ID   uint16
	QoS  byte
}

// Detailer is an optional interface for packets that provide QoS details.
type Detailer interface {
	Details() Details
}

// Resetter is an optional interface for packets that support pooling.
type Resetter interface {
	Reset()
}

// User represents a user property key-value pair (MQTT 5.0).
type User struct {
	Key, Value string
}

// NewControlPacket creates a new packet of the specified type.
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

// NewControlPacketWithHeader creates a new packet with the given fixed header.
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

// ReadPacket reads an MQTT packet from the reader.
// Returns the packet, encoded fixed header, packet body bytes, and any error.
func ReadPacket(r io.Reader, version byte) (ControlPacket, []byte, []byte, error) {
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
		return nil, nil, nil, ErrFailRemaining
	}

	err = cp.Unpack(bytes.NewReader(packetBytes), version)
	return cp, fh.Encode(), packetBytes, err
}
