package packets

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

const (
	V31  byte = 0x3
	V311 byte = 0x4
	V5   byte = 0x05
)

// ErrFailRemaining indicates remaining data does not match the size of sent data.
var ErrFailRemaining = errors.New("remaining data length does not match data size")

// Constants assigned to each of the MQTT packet types
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
	AuthType
)

// PacketNames maps the constants for each of the MQTT packet types
// to a string representation of their name.
var PacketNames = map[uint8]string{
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

// ControlPacket defines the interface for structures intended to hold
// decoded MQTT packets, either from being read or before being written.
type ControlPacket interface {
	Pack(w io.Writer) error
	// Unpack receives an IO reader and a protocol version so
	// the same packet can be reaused for multiple protocol version.
	// In the server implmentation, protocvol version will be bound
	// to the underlying connection (mixing versions is not allowed),
	// but we're trying to keep parsers decoupled from the broker and
	// client implemention.
	Unpack(r io.Reader, v byte) error
	String() string
	Details() Details
}

// Details struct returned by the Details() function called on
// ControlPackets to present details of the Qos and ID
// of the ControlPacket
type Details struct {
	Type byte
	ID   uint16
	Qos  byte
}

// ReadPacket takes an instance of an io.Reader (such as net.Conn) and attempts
// to read an MQTT packet from the stream. It returns a ControlPacket
// representing the decoded MQTT packet and an error. One of these returns will
// always be nil, a nil ControlPacket indicating an error occurred.
func ReadPacket(r io.Reader, v byte) (ControlPacket, error) {
	var fh FixedHeader
	var b [1]byte

	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return nil, err
	}

	err = fh.Decode(b[0], r)
	if err != nil {
		return nil, err
	}

	cp, err := NewControlPacketWithHeader(fh)
	if err != nil {
		return nil, err
	}

	packetBytes := make([]byte, fh.RemainingLength)
	n, err := io.ReadFull(r, packetBytes)
	if err != nil {
		return nil, err
	}
	if n != fh.RemainingLength {
		return nil, ErrFailRemaining
	}
	err = cp.Unpack(bytes.NewReader(packetBytes), v)
	return cp, err
}

// NewControlPacket is used to create a new ControlPacket of the type specified
// by packetType, this is usually done by reference to the packet type constants
// defined in packets.go. The newly created ControlPacket is empty and a pointer
// is returned.
func NewControlPacket(packetType byte) ControlPacket {
	switch packetType {
	case ConnectType:
		return &Connect{FixedHeader: FixedHeader{PacketType: ConnectType}}
	case ConnAckType:
		return &ConnAck{FixedHeader: FixedHeader{PacketType: ConnAckType}}
	case DisconnectType:
		return &Disconnect{FixedHeader: FixedHeader{PacketType: DisconnectType}}
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
	case AuthType:
		return &Auth{FixedHeader: FixedHeader{PacketType: AuthType}}
	}
	return nil
}

// NewControlPacketWithHeader is used to create a new ControlPacket of the type
// specified within the FixedHeader that is passed to the function.
// The newly created ControlPacket is empty and a pointer is returned.
func NewControlPacketWithHeader(fh FixedHeader) (ControlPacket, error) {
	switch fh.PacketType {
	case ConnectType:
		return &Connect{FixedHeader: fh}, nil
	case ConnAckType:
		return &ConnAck{FixedHeader: fh}, nil
	case DisconnectType:
		return &Disconnect{FixedHeader: fh}, nil
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
	}
	return nil, fmt.Errorf("unsupported packet type 0x%x", fh.PacketType)
}
