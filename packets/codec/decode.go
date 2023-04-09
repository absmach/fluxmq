package codec

import (
	"encoding/binary"
	"errors"
	"io"
)

// ErrMaxLengthExceeded represents an error for invalid length int size.
// Length is positive integer of variable length.
var ErrMaxLengthExceeded = errors.New("max length value exceeded")

const maxMultiplier = 128 * 128 * 128

func DecodeByte(b io.Reader) (byte, error) {
	ret := make([]byte, 1)
	_, err := b.Read(ret)
	if err != nil {
		return 0, err
	}

	return ret[0], nil
}

func DecodeUint16(b io.Reader) (uint16, error) {
	num := make([]byte, 2)
	_, err := b.Read(num)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(num), nil
}

func DecodeBytes(b io.Reader) ([]byte, error) {
	fieldLength, err := DecodeUint16(b)
	if err != nil {
		return nil, err
	}

	field := make([]byte, fieldLength)
	_, err = b.Read(field)
	if err != nil {
		return nil, err
	}

	return field, nil
}

func DecodeString(b io.Reader) (string, error) {
	buf, err := DecodeBytes(b)
	return string(buf), err
}

func DecodeLength(r io.Reader) (int, error) {
	var value uint32
	b := byte(128)
	multiplier := uint32(1)
	buff := make([]byte, 1)
	for b&128 != 0 {
		if multiplier > maxMultiplier {
			return 0, ErrMaxLengthExceeded
		}
		_, err := io.ReadFull(r, buff)
		if err != nil {
			return 0, err
		}
		b = buff[0]
		value += uint32(b&127) * multiplier
		multiplier *= 128
	}
	return int(value), nil
}
