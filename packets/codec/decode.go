package codec

import (
	"encoding/binary"
	"errors"
	"io"
)

// ErrMaxLengthExceeded represents an error for invalid length int size.
// Length is positive integer of variable bytes integer.
var ErrMaxLengthExceeded = errors.New("max length value exceeded")

const maxMultiplier = 128 * 128 * 128

func DecodeByte(r io.Reader) (byte, error) {
	b := make([]byte, 1)
	_, err := io.ReadAtLeast(r, b, 1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func DecodeUint16(r io.Reader) (uint16, error) {
	num := make([]byte, 2)
	_, err := io.ReadAtLeast(r, num, 2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(num), nil
}

func DecodeUint32(r io.Reader) (uint32, error) {
	num := make([]byte, 4)
	_, err := io.ReadAtLeast(r, num, 4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(num), nil
}

func DecodeBytes(r io.Reader) ([]byte, error) {
	fieldLength, err := DecodeUint16(r)
	if err != nil {
		return nil, err
	}

	field := make([]byte, fieldLength)
	_, err = io.ReadAtLeast(r, field, int(fieldLength))
	if err != nil {
		return nil, err
	}

	return field, nil
}

func DecodeString(r io.Reader) (string, error) {
	buf, err := DecodeBytes(r)
	return string(buf), err
}

// DecodeVBI is used for Variable Byte Integers used to
// encode length in a minimal way.
func DecodeVBI(r io.Reader) (int, error) {
	var vbi uint32
	var multiplier uint32
	bytes := make([]byte, 1)

	for {
		_, err := io.ReadAtLeast(r, bytes, 1)
		if err != nil && err != io.EOF {
			return 0, err
		}
		digit := bytes[0]
		vbi |= uint32(digit&0x7F) << multiplier
		if (digit & 0x80) == 0 {
			break
		}
		multiplier += 7
		if multiplier > maxMultiplier {
			return 0, ErrMaxLengthExceeded
		}
	}
	return int(vbi), nil
}
