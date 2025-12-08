package packets

import (
	"fmt"
	"io"

	"github.com/dborovcanin/mqtt/packets/codec"
)

const headerFormat = "type: %s dup: %t qos: %d retain: %t remaining_length: %d"

func (fh FixedHeader) String() string {
	return fmt.Sprintf(headerFormat, PacketNames[fh.PacketType], fh.Dup, fh.QoS, fh.Retain, fh.RemainingLength)
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
	return append(ret, codec.EncodeVBI(fh.RemainingLength)...)
}

// Decode parses the fixed header from the type/flags byte and reader.
func (fh *FixedHeader) Decode(typeAndFlags byte, r io.Reader) error {
	fh.PacketType = typeAndFlags >> 4
	fh.Dup = (typeAndFlags>>3)&0x01 > 0
	fh.QoS = (typeAndFlags >> 1) & 0x03
	fh.Retain = typeAndFlags&0x01 > 0

	var err error
	fh.RemainingLength, err = codec.DecodeVBI(r)
	return err
}

// DecodeFromBytes parses the fixed header from a byte slice.
// Returns the number of bytes consumed.
func (fh *FixedHeader) DecodeFromBytes(data []byte) (int, error) {
	if len(data) < 2 {
		return 0, codec.ErrBufferTooShort
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
			return 0, codec.ErrBufferTooShort
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
	return 0, codec.ErrMalformedVBI
}
