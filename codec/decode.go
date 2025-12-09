package codec

import (
	"errors"
	"io"
)

// ErrMaxLengthExceeded represents an error for invalid length int size.
// Length is positive integer of variable bytes integer.
var ErrMaxLengthExceeded = errors.New("max length value exceeded")

const maxMultiplier = 128 * 128 * 128

func DecodeByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, err := io.ReadFull(r, b[:])
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func DecodeUint16(r io.Reader) (uint16, error) {
	var num [2]byte
	_, err := io.ReadFull(r, num[:])
	if err != nil {
		return 0, err
	}

	return uint16(num[1]) | uint16(num[0])<<8, nil
}

func DecodeUint32(r io.Reader) (uint32, error) {
	var num [4]byte
	_, err := io.ReadFull(r, num[:])
	if err != nil {
		return 0, err
	}

	return uint32(num[3]) | uint32(num[2])<<8 | uint32(num[1])<<16 | uint32(num[0])<<24, nil
}

func DecodeBytes(r io.Reader) ([]byte, error) {
	fieldLength, err := DecodeUint16(r)
	if err != nil {
		return nil, err
	}
	field := make([]byte, fieldLength)
	_, err = io.ReadFull(r, field)
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
	var b [1]byte
	for {
		_, err := io.ReadFull(r, b[:])
		if err != nil && err != io.EOF {
			return 0, err
		}
		digit := b[0]
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
