// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// ReadType reads a single AMQP typed value from the reader.
// Returns the decoded Go value and any error.
func ReadType(r io.Reader) (any, error) {
	var code [1]byte
	if _, err := io.ReadFull(r, code[:]); err != nil {
		return nil, err
	}
	return readByCode(r, code[0])
}

func readByCode(r io.Reader, code byte) (any, error) {
	switch code {
	case TypeNull:
		return nil, nil
	case TypeBoolTrue:
		return true, nil
	case TypeBoolFalse:
		return false, nil
	case TypeBool:
		return readBool(r)
	case TypeUbyte:
		return readUbyte(r)
	case TypeUshort:
		return readUshort(r)
	case TypeUint:
		return readUint(r)
	case TypeUintSmall:
		return readUintSmall(r)
	case TypeUint0:
		return uint32(0), nil
	case TypeUlong:
		return readUlong(r)
	case TypeUlongSmall:
		return readUlongSmall(r)
	case TypeUlong0:
		return uint64(0), nil
	case TypeByte:
		return readByte(r)
	case TypeShort:
		return readShort(r)
	case TypeInt:
		return readInt(r)
	case TypeIntSmall:
		return readIntSmall(r)
	case TypeLong:
		return readLong(r)
	case TypeLongSmall:
		return readLongSmall(r)
	case TypeFloat:
		return readFloat(r)
	case TypeDouble:
		return readDouble(r)
	case TypeTimestamp:
		return readTimestamp(r)
	case TypeUUID:
		return readUUID(r)
	case TypeBinaryShort:
		return readVarShort(r)
	case TypeBinaryLong:
		return readVarLong(r)
	case TypeStringShort:
		b, err := readVarShort(r)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case TypeStringLong:
		b, err := readVarLong(r)
		if err != nil {
			return nil, err
		}
		return string(b), nil
	case TypeSymbolShort:
		b, err := readVarShort(r)
		if err != nil {
			return nil, err
		}
		return Symbol(b), nil
	case TypeSymbolLong:
		b, err := readVarLong(r)
		if err != nil {
			return nil, err
		}
		return Symbol(b), nil
	case TypeList0:
		return []any{}, nil
	case TypeList8:
		return readList8(r)
	case TypeList32:
		return readList32(r)
	case TypeMap8:
		return readMap8(r)
	case TypeMap32:
		return readMap32(r)
	case TypeArray8:
		return readArray8(r)
	case TypeArray32:
		return readArray32(r)
	case TypeDescriptor:
		return readDescribed(r)
	default:
		return nil, fmt.Errorf("unknown AMQP type code: 0x%02x", code)
	}
}

func readN(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func readBool(r io.Reader) (bool, error) {
	b, err := readN(r, 1)
	if err != nil {
		return false, err
	}
	return b[0] != 0, nil
}

func readUbyte(r io.Reader) (uint8, error) {
	b, err := readN(r, 1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func readUshort(r io.Reader) (uint16, error) {
	b, err := readN(r, 2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func readUint(r io.Reader) (uint32, error) {
	b, err := readN(r, 4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func readUintSmall(r io.Reader) (uint32, error) {
	b, err := readN(r, 1)
	if err != nil {
		return 0, err
	}
	return uint32(b[0]), nil
}

func readUlong(r io.Reader) (uint64, error) {
	b, err := readN(r, 8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func readUlongSmall(r io.Reader) (uint64, error) {
	b, err := readN(r, 1)
	if err != nil {
		return 0, err
	}
	return uint64(b[0]), nil
}

func readByte(r io.Reader) (int8, error) {
	b, err := readN(r, 1)
	if err != nil {
		return 0, err
	}
	return int8(b[0]), nil
}

func readShort(r io.Reader) (int16, error) {
	b, err := readN(r, 2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func readInt(r io.Reader) (int32, error) {
	b, err := readN(r, 4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func readIntSmall(r io.Reader) (int32, error) {
	b, err := readN(r, 1)
	if err != nil {
		return 0, err
	}
	return int32(int8(b[0])), nil
}

func readLong(r io.Reader) (int64, error) {
	b, err := readN(r, 8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func readLongSmall(r io.Reader) (int64, error) {
	b, err := readN(r, 1)
	if err != nil {
		return 0, err
	}
	return int64(int8(b[0])), nil
}

func readFloat(r io.Reader) (float32, error) {
	b, err := readN(r, 4)
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(binary.BigEndian.Uint32(b)), nil
}

func readDouble(r io.Reader) (float64, error) {
	b, err := readN(r, 8)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
}

func readTimestamp(r io.Reader) (Timestamp, error) {
	b, err := readN(r, 8)
	if err != nil {
		return Timestamp{}, err
	}
	ms := int64(binary.BigEndian.Uint64(b))
	return TimestampFromMillis(ms), nil
}

func readUUID(r io.Reader) (UUID, error) {
	var u UUID
	_, err := io.ReadFull(r, u[:])
	return u, err
}

func readVarShort(r io.Reader) ([]byte, error) {
	b, err := readN(r, 1)
	if err != nil {
		return nil, err
	}
	return readN(r, int(b[0]))
}

func readVarLong(r io.Reader) ([]byte, error) {
	b, err := readN(r, 4)
	if err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(b)
	return readN(r, int(size))
}

func readList8(r io.Reader) ([]any, error) {
	b, err := readN(r, 2) // size, count
	if err != nil {
		return nil, err
	}
	count := int(b[1])
	items := make([]any, count)
	for i := 0; i < count; i++ {
		items[i], err = ReadType(r)
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func readList32(r io.Reader) ([]any, error) {
	b, err := readN(r, 8) // size(4), count(4)
	if err != nil {
		return nil, err
	}
	count := int(binary.BigEndian.Uint32(b[4:]))
	items := make([]any, count)
	for i := 0; i < count; i++ {
		items[i], err = ReadType(r)
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func readMap8(r io.Reader) (map[any]any, error) {
	b, err := readN(r, 2) // size, count
	if err != nil {
		return nil, err
	}
	count := int(b[1]) / 2 // count is number of items, divide by 2 for pairs
	m := make(map[any]any, count)
	for i := 0; i < count; i++ {
		key, err := ReadType(r)
		if err != nil {
			return nil, err
		}
		val, err := ReadType(r)
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return m, nil
}

func readMap32(r io.Reader) (map[any]any, error) {
	b, err := readN(r, 8) // size(4), count(4)
	if err != nil {
		return nil, err
	}
	count := int(binary.BigEndian.Uint32(b[4:])) / 2
	m := make(map[any]any, count)
	for i := 0; i < count; i++ {
		key, err := ReadType(r)
		if err != nil {
			return nil, err
		}
		val, err := ReadType(r)
		if err != nil {
			return nil, err
		}
		m[key] = val
	}
	return m, nil
}

func readArray8(r io.Reader) ([]any, error) {
	b, err := readN(r, 2) // size, count
	if err != nil {
		return nil, err
	}
	count := int(b[1])
	// Read element constructor
	var code [1]byte
	if _, err := io.ReadFull(r, code[:]); err != nil {
		return nil, err
	}
	items := make([]any, count)
	for i := 0; i < count; i++ {
		items[i], err = readByCode(r, code[0])
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func readArray32(r io.Reader) ([]any, error) {
	b, err := readN(r, 8) // size(4), count(4)
	if err != nil {
		return nil, err
	}
	count := int(binary.BigEndian.Uint32(b[4:]))
	var code [1]byte
	if _, err := io.ReadFull(r, code[:]); err != nil {
		return nil, err
	}
	items := make([]any, count)
	for i := 0; i < count; i++ {
		items[i], err = readByCode(r, code[0])
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func readDescribed(r io.Reader) (*Described, error) {
	descVal, err := ReadType(r)
	if err != nil {
		return nil, err
	}

	var descriptor uint64
	switch v := descVal.(type) {
	case uint64:
		descriptor = v
	case uint32:
		descriptor = uint64(v)
	default:
		return nil, fmt.Errorf("unsupported descriptor type: %T", descVal)
	}

	value, err := ReadType(r)
	if err != nil {
		return nil, err
	}

	return &Described{
		Descriptor: descriptor,
		Value:      value,
	}, nil
}

// ReadListFields reads a described list and returns the fields as a slice.
// This is the common pattern for performative decoding.
func ReadListFields(r io.Reader) (uint64, []any, error) {
	val, err := ReadType(r)
	if err != nil {
		return 0, nil, err
	}

	desc, ok := val.(*Described)
	if !ok {
		return 0, nil, fmt.Errorf("expected described type, got %T", val)
	}

	fields, ok := desc.Value.([]any)
	if !ok {
		return 0, nil, fmt.Errorf("expected list value, got %T", desc.Value)
	}

	return desc.Descriptor, fields, nil
}
