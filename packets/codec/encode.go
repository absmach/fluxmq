package codec

// Encode methods rewrite some of bigEndian methods
// to avoid unnecessary function calls and checks.

func EncodeBytes(field []byte) []byte {
	v := len(field)
	b := []byte{byte(v >> 8), byte(v)}
	return append(b, field...)
}

func EncodeUint16(num uint16) []byte {
	return []byte{byte(num >> 8), byte(num)}
}

func EncodeUint32(num uint32) []byte {
	b := make([]byte, 4)
	b[0] = byte(num >> 24)
	b[1] = byte(num >> 16)
	b[2] = byte(num >> 8)
	b[3] = byte(num)
	return b
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
