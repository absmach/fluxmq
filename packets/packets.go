package packets

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	codec "github.com/dborovcanin/mqtt/packets/codec"
)

const headerFormat = "type: %s: dup: %t qos: %d retain: %t remaining_length: %d\n"

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
	AuthType:        "AUTH",
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
}

// FixedHeader is a struct to hold the decoded information from
// the fixed header of an MQTT ControlPacket.
type FixedHeader struct {
	PacketType      byte
	Dup             bool
	Qos             byte
	Retain          bool
	RemainingLength int
}

func (fh FixedHeader) String() string {
	return fmt.Sprintf(headerFormat, PacketNames[fh.PacketType], fh.Dup, fh.Qos, fh.Retain, fh.RemainingLength)
}

func (fh *FixedHeader) encode() bytes.Buffer {
	// ret := []byte{fh.PacketType<<4 | codec.EncodeBool(fh.Dup)<<3 | fh.Qos<<1 | codec.EncodeBool(fh.Retain)}
	// return append(ret, codec.EncodeLength(fh.RemainingLength)...)
	var header bytes.Buffer
	header.WriteByte(fh.PacketType<<4 | codec.EncodeBool(fh.Dup)<<3 | fh.Qos<<1 | codec.EncodeBool(fh.Retain))
	header.Write(codec.EncodeVBI(fh.RemainingLength))
	return header
}

func (fh *FixedHeader) decode(typeAndFlags byte, r io.Reader) error {
	fh.PacketType = typeAndFlags >> 4
	fh.Dup = (typeAndFlags>>3)&0x01 > 0
	fh.Qos = (typeAndFlags >> 1) & 0x03
	fh.Retain = typeAndFlags&0x01 > 0

	var err error
	fh.RemainingLength, err = codec.DecodeVBI(r)
	return err
}

// ControlPacket defines the interface for structures intended to hold
// decoded MQTT packets, either from being read or before being written.
type ControlPacket interface {
	Pack(io.Writer) error
	Unpack(io.Reader) error
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
func ReadPacket(r io.Reader) (ControlPacket, error) {
	var fh FixedHeader
	b := make([]byte, 1)

	n, err := io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, io.ErrUnexpectedEOF
	}

	err = fh.decode(b[0], r)
	if err != nil {
		return nil, err
	}

	cp, err := NewControlPacketWithHeader(fh)
	if err != nil {
		return nil, err
	}

	packetBytes := make([]byte, fh.RemainingLength)
	n, err = io.ReadFull(r, packetBytes)
	if err != nil {
		return nil, err
	}
	if n != fh.RemainingLength {
		return nil, ErrFailRemaining
	}
	err = cp.Unpack(bytes.NewReader(packetBytes))
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
		return &PubRel{FixedHeader: FixedHeader{PacketType: PubRelType, Qos: 1}}
	case PubCompType:
		return &PubComp{FixedHeader: FixedHeader{PacketType: PubCompType}}
	case SubscribeType:
		return &Subscribe{FixedHeader: FixedHeader{PacketType: SubscribeType, Qos: 1}}
	case SubAckType:
		return &SubAck{FixedHeader: FixedHeader{PacketType: SubAckType}}
	case UnsubscribeType:
		return &Unsubscribe{FixedHeader: FixedHeader{PacketType: UnsubscribeType, Qos: 1}}
	case UnsubAckType:
		return &UnSubAck{FixedHeader: FixedHeader{PacketType: UnsubAckType}}
	case PingReqType:
		return &PingReq{FixedHeader: FixedHeader{PacketType: PingReqType}}
	case PingRespType:
		return &PingResp{FixedHeader: FixedHeader{PacketType: PingRespType}}
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
