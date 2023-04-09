package packets

import (
	"bytes"
	"fmt"
	"io"

	"github.com/dborovcanin/mbroker/packets/codec"
)

const headerFormat = "type: %s: dup: %t qos: %d retain: %t remaining_length: %d\n"

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

func (fh *FixedHeader) pack() bytes.Buffer {
	var header bytes.Buffer
	header.WriteByte(fh.PacketType<<4 | codec.EncodeBool(fh.Dup)<<3 | fh.Qos<<1 | codec.EncodeBool(fh.Retain))
	header.Write(codec.EncodeLength(fh.RemainingLength))
	return header
}

func (fh *FixedHeader) unpack(typeAndFlags byte, r io.Reader) error {
	fh.PacketType = typeAndFlags >> 4
	fh.Dup = (typeAndFlags>>3)&0x01 > 0
	fh.Qos = (typeAndFlags >> 1) & 0x03
	fh.Retain = typeAndFlags&0x01 > 0

	var err error
	fh.RemainingLength, err = codec.DecodeLength(r)
	return err
}
