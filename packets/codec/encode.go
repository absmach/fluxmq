package codec

import "encoding/binary"

func EncodeBytes(field []byte) []byte {
	fieldLength := make([]byte, 2)
	binary.BigEndian.PutUint16(fieldLength, uint16(len(field)))
	return append(fieldLength, field...)
}

func EncodeByte(b byte) []byte {
	return []byte{b}
}

func EncodeUint16(num uint16) []byte {
	bytes := make([]byte, 2)
	binary.BigEndian.PutUint16(bytes, num)
	return bytes
}

func EncodeUint32(num uint32) []byte {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, num)
	return bytes
}

// EncodeVBI is used for Variable Byte Integers used to
// encode length in a minimal way.
func EncodeVBI(num int) []byte {
	var x int
	ret := [4]byte{}
	v := uint32(num)
	for {
		b := byte(v & 0x7F) // take 7 least significant bits
		v >>= 7
		if v > 0 {
			b |= 0x80 // set continuation bit
		}
		ret[x] = b
		x++
		if v == 0 {
			return ret[:x]
		}
	}
}

func EncodeString(field string) []byte {
	return EncodeBytes([]byte(field))
}

func EncodeBool(b bool) byte {
	if b {
		return 1
	}
	return 0
}
