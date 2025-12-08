// package v5 provides shared constants and interfaces for MQTT packet handling.
// Version-specific implementations are in the v3 and v5 packages.
package packets

import (
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
	Unpack(r io.Reader) error

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

// String returns a human-readable representation of the fixed header.
func (fh FixedHeader) String() string {
	return fmt.Sprintf("type: %s dup: %t qos: %d retain: %t remaining_length: %d",
		PacketNames[fh.PacketType], fh.Dup, fh.QoS, fh.Retain, fh.RemainingLength)
}

// Encode serializes the fixed header to bytes.
func (fh FixedHeader) Encode() []byte {
	var dup, retain byte
	if fh.Dup {
		dup = 1
	}
	if fh.Retain {
		retain = 1
	}
	ret := []byte{fh.PacketType<<4 | dup<<3 | fh.QoS<<1 | retain}
	return append(ret, encodeVBI(fh.RemainingLength)...)
}

// Decode parses the fixed header from the type/flags byte and reader.
func (fh *FixedHeader) Decode(typeAndFlags byte, r io.Reader) error {
	fh.PacketType = typeAndFlags >> 4
	fh.Dup = (typeAndFlags>>3)&0x01 > 0
	fh.QoS = (typeAndFlags >> 1) & 0x03
	fh.Retain = typeAndFlags&0x01 > 0

	var err error
	fh.RemainingLength, err = decodeVBI(r)
	return err
}

// DecodeFromBytes parses the fixed header from a byte slice.
// Returns the number of bytes consumed.
func (fh *FixedHeader) DecodeFromBytes(data []byte) (int, error) {
	if len(data) < 2 {
		return 0, errors.New("buffer too short")
	}

	fh.PacketType = data[0] >> 4
	fh.Dup = (data[0]>>3)&0x01 > 0
	fh.QoS = (data[0] >> 1) & 0x03
	fh.Retain = data[0]&0x01 > 0

	// Decode remaining length (VBI)
	var vbi uint32
	var multiplier uint32
	offset := 1
	for i := 0; i < 4; i++ {
		if offset >= len(data) {
			return 0, errors.New("buffer too short")
		}
		b := data[offset]
		offset++
		vbi |= uint32(b&0x7F) << multiplier
		if (b & 0x80) == 0 {
			fh.RemainingLength = int(vbi)
			return offset, nil
		}
		multiplier += 7
	}
	return 0, errors.New("malformed VBI")
}

// encodeVBI encodes an integer as a Variable Byte Integer.
func encodeVBI(value int) []byte {
	var ret []byte
	for {
		digit := byte(value % 128)
		value /= 128
		if value > 0 {
			digit |= 0x80
		}
		ret = append(ret, digit)
		if value == 0 {
			break
		}
	}
	return ret
}

// decodeVBI decodes a Variable Byte Integer from the reader.
func decodeVBI(r io.Reader) (int, error) {
	var vbi uint32
	var multiplier uint32
	b := make([]byte, 1)
	for i := 0; i < 4; i++ {
		_, err := io.ReadFull(r, b)
		if err != nil {
			return 0, err
		}
		vbi |= uint32(b[0]&0x7F) << multiplier
		if (b[0] & 0x80) == 0 {
			return int(vbi), nil
		}
		multiplier += 7
	}
	return 0, errors.New("malformed VBI")
}
