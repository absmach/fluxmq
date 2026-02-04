// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// WriteNull writes a null value.
func WriteNull(w io.Writer) error {
	_, err := w.Write([]byte{TypeNull})
	return err
}

// WriteBool writes a boolean value using compact encoding.
func WriteBool(w io.Writer, v bool) error {
	if v {
		_, err := w.Write([]byte{TypeBoolTrue})
		return err
	}
	_, err := w.Write([]byte{TypeBoolFalse})
	return err
}

// WriteUbyte writes an unsigned byte.
func WriteUbyte(w io.Writer, v uint8) error {
	_, err := w.Write([]byte{TypeUbyte, v})
	return err
}

// WriteUshort writes an unsigned 16-bit integer.
func WriteUshort(w io.Writer, v uint16) error {
	var buf [3]byte
	buf[0] = TypeUshort
	binary.BigEndian.PutUint16(buf[1:], v)
	_, err := w.Write(buf[:])
	return err
}

// WriteUint writes an unsigned 32-bit integer with small-encoding optimization.
func WriteUint(w io.Writer, v uint32) error {
	if v == 0 {
		_, err := w.Write([]byte{TypeUint0})
		return err
	}
	if v <= 255 {
		_, err := w.Write([]byte{TypeUintSmall, byte(v)})
		return err
	}
	var buf [5]byte
	buf[0] = TypeUint
	binary.BigEndian.PutUint32(buf[1:], v)
	_, err := w.Write(buf[:])
	return err
}

// WriteUlong writes an unsigned 64-bit integer with small-encoding optimization.
func WriteUlong(w io.Writer, v uint64) error {
	if v == 0 {
		_, err := w.Write([]byte{TypeUlong0})
		return err
	}
	if v <= 255 {
		_, err := w.Write([]byte{TypeUlongSmall, byte(v)})
		return err
	}
	var buf [9]byte
	buf[0] = TypeUlong
	binary.BigEndian.PutUint64(buf[1:], v)
	_, err := w.Write(buf[:])
	return err
}

// WriteByte writes a signed byte.
func WriteByte(w io.Writer, v int8) error {
	_, err := w.Write([]byte{TypeByte, byte(v)})
	return err
}

// WriteShort writes a signed 16-bit integer.
func WriteShort(w io.Writer, v int16) error {
	var buf [3]byte
	buf[0] = TypeShort
	binary.BigEndian.PutUint16(buf[1:], uint16(v))
	_, err := w.Write(buf[:])
	return err
}

// WriteInt writes a signed 32-bit integer with small-encoding optimization.
func WriteInt(w io.Writer, v int32) error {
	if v >= -128 && v <= 127 {
		_, err := w.Write([]byte{TypeIntSmall, byte(v)})
		return err
	}
	var buf [5]byte
	buf[0] = TypeInt
	binary.BigEndian.PutUint32(buf[1:], uint32(v))
	_, err := w.Write(buf[:])
	return err
}

// WriteLong writes a signed 64-bit integer with small-encoding optimization.
func WriteLong(w io.Writer, v int64) error {
	if v >= -128 && v <= 127 {
		_, err := w.Write([]byte{TypeLongSmall, byte(v)})
		return err
	}
	var buf [9]byte
	buf[0] = TypeLong
	binary.BigEndian.PutUint64(buf[1:], uint64(v))
	_, err := w.Write(buf[:])
	return err
}

// WriteFloat writes a 32-bit IEEE 754 float.
func WriteFloat(w io.Writer, v float32) error {
	var buf [5]byte
	buf[0] = TypeFloat
	binary.BigEndian.PutUint32(buf[1:], math.Float32bits(v))
	_, err := w.Write(buf[:])
	return err
}

// WriteDouble writes a 64-bit IEEE 754 double.
func WriteDouble(w io.Writer, v float64) error {
	var buf [9]byte
	buf[0] = TypeDouble
	binary.BigEndian.PutUint64(buf[1:], math.Float64bits(v))
	_, err := w.Write(buf[:])
	return err
}

// WriteTimestamp writes a timestamp (milliseconds since Unix epoch).
func WriteTimestamp(w io.Writer, v Timestamp) error {
	var buf [9]byte
	buf[0] = TypeTimestamp
	binary.BigEndian.PutUint64(buf[1:], uint64(v.Milliseconds()))
	_, err := w.Write(buf[:])
	return err
}

// WriteUUID writes a 16-byte UUID.
func WriteUUID(w io.Writer, v UUID) error {
	var buf [17]byte
	buf[0] = TypeUUID
	copy(buf[1:], v[:])
	_, err := w.Write(buf[:])
	return err
}

// WriteBinary writes a binary value with short/long encoding optimization.
func WriteBinary(w io.Writer, v []byte) error {
	if len(v) <= 255 {
		if _, err := w.Write([]byte{TypeBinaryShort, byte(len(v))}); err != nil {
			return err
		}
	} else {
		var buf [5]byte
		buf[0] = TypeBinaryLong
		binary.BigEndian.PutUint32(buf[1:], uint32(len(v)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
	}
	_, err := w.Write(v)
	return err
}

// WriteString writes a UTF-8 string with short/long encoding optimization.
func WriteString(w io.Writer, v string) error {
	b := []byte(v)
	if len(b) <= 255 {
		if _, err := w.Write([]byte{TypeStringShort, byte(len(b))}); err != nil {
			return err
		}
	} else {
		var buf [5]byte
		buf[0] = TypeStringLong
		binary.BigEndian.PutUint32(buf[1:], uint32(len(b)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
	}
	_, err := w.Write(b)
	return err
}

// WriteSymbol writes a symbolic value with short/long encoding optimization.
func WriteSymbol(w io.Writer, v Symbol) error {
	b := []byte(v)
	if len(b) <= 255 {
		if _, err := w.Write([]byte{TypeSymbolShort, byte(len(b))}); err != nil {
			return err
		}
	} else {
		var buf [5]byte
		buf[0] = TypeSymbolLong
		binary.BigEndian.PutUint32(buf[1:], uint32(len(b)))
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
	}
	_, err := w.Write(b)
	return err
}

// WriteDescriptor writes a described type constructor with a ulong descriptor.
func WriteDescriptor(w io.Writer, code uint64) error {
	if _, err := w.Write([]byte{TypeDescriptor}); err != nil {
		return err
	}
	return WriteUlong(w, code)
}

// WriteList writes a list of pre-encoded field values.
// fields should be encoded with a FieldWriter.
func WriteList(w io.Writer, fields []byte, count int) error {
	if count == 0 && len(fields) == 0 {
		_, err := w.Write([]byte{TypeList0})
		return err
	}

	// list32: type + 4-byte size + 4-byte count + data
	size := uint32(len(fields)) + 4 // count is included in size
	var buf [9]byte
	buf[0] = TypeList32
	binary.BigEndian.PutUint32(buf[1:5], size)
	binary.BigEndian.PutUint32(buf[5:9], uint32(count))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	_, err := w.Write(fields)
	return err
}

// WriteMap writes a map from pre-encoded key-value pairs.
// pairs should be encoded as alternating key, value entries.
func WriteMap(w io.Writer, pairs []byte, count int) error {
	// map32: type + 4-byte size + 4-byte count + data
	// count is number of key-value pairs * 2 (each pair is 2 items)
	size := uint32(len(pairs)) + 4
	var buf [9]byte
	buf[0] = TypeMap32
	binary.BigEndian.PutUint32(buf[1:5], size)
	binary.BigEndian.PutUint32(buf[5:9], uint32(count*2))
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	_, err := w.Write(pairs)
	return err
}

// WriteArray writes an array of uniformly-typed pre-encoded values.
func WriteArray(w io.Writer, elemType byte, elements []byte, count int) error {
	// array32: type + 4-byte size + 4-byte count + constructor + data
	size := uint32(len(elements)) + 4 + 1 // count + constructor byte
	var buf [10]byte
	buf[0] = TypeArray32
	binary.BigEndian.PutUint32(buf[1:5], size)
	binary.BigEndian.PutUint32(buf[5:9], uint32(count))
	buf[9] = elemType
	if _, err := w.Write(buf[:]); err != nil {
		return err
	}
	_, err := w.Write(elements)
	return err
}

// WriteAny writes a Go value as the appropriate AMQP type.
func WriteAny(w io.Writer, v any) error {
	if v == nil {
		return WriteNull(w)
	}
	switch val := v.(type) {
	case bool:
		return WriteBool(w, val)
	case uint8:
		return WriteUbyte(w, val)
	case uint16:
		return WriteUshort(w, val)
	case uint32:
		return WriteUint(w, val)
	case uint64:
		return WriteUlong(w, val)
	case int8:
		return WriteByte(w, val)
	case int16:
		return WriteShort(w, val)
	case int32:
		return WriteInt(w, val)
	case int64:
		return WriteLong(w, val)
	case float32:
		return WriteFloat(w, val)
	case float64:
		return WriteDouble(w, val)
	case string:
		return WriteString(w, val)
	case Symbol:
		return WriteSymbol(w, val)
	case []byte:
		return WriteBinary(w, val)
	case UUID:
		return WriteUUID(w, val)
	case Timestamp:
		return WriteTimestamp(w, val)
	default:
		return fmt.Errorf("unsupported type: %T", v)
	}
}

// WriteSymbolArray writes an AMQP multiple symbol value.
// For a single element, encodes as a bare symbol (per AMQP "multiple" encoding).
// For multiple elements, encodes as an array of symbols.
func WriteSymbolArray(w io.Writer, symbols []Symbol) error {
	if len(symbols) == 0 {
		return WriteNull(w)
	}
	if len(symbols) == 1 {
		return WriteSymbol(w, symbols[0])
	}
	var elems bytes.Buffer
	for _, s := range symbols {
		b := []byte(s)
		if len(b) <= 255 {
			elems.WriteByte(byte(len(b)))
			elems.Write(b)
		} else {
			var buf [4]byte
			binary.BigEndian.PutUint32(buf[:], uint32(len(b)))
			elems.Write(buf[:])
			elems.Write(b)
		}
	}
	// Use sym8 element type for short symbols, sym32 for long
	// For simplicity, determine based on whether all symbols fit in sym8
	allShort := true
	for _, s := range symbols {
		if len([]byte(s)) > 255 {
			allShort = false
			break
		}
	}
	elemType := TypeSymbolShort
	if !allShort {
		elemType = TypeSymbolLong
	}
	return WriteArray(w, elemType, elems.Bytes(), len(symbols))
}

// WriteStringAnyMap writes a map with string keys and any values.
func WriteStringAnyMap(w io.Writer, m map[string]any) error {
	var pairs bytes.Buffer
	for k, v := range m {
		if err := WriteString(&pairs, k); err != nil {
			return err
		}
		if err := WriteAny(&pairs, v); err != nil {
			return err
		}
	}
	return WriteMap(w, pairs.Bytes(), len(m))
}
