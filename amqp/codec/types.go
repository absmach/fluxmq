// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package codec

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

// Constants for AMQP frame types
const (
	FrameMethod    byte = 1
	FrameHeader    byte = 2
	FrameBody      byte = 3
	FrameHeartbeat byte = 8
)

// FrameEnd is the octet that ends all frames.
const FrameEnd = 0xCE

// Decimal represents an AMQP decimal value with scale and unscaled components.
type Decimal struct {
	Scale uint8
	Value int32
}

// ReadOctet reads a single byte from the reader.
func ReadOctet(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

// WriteOctet writes a single byte to the writer.
func WriteOctet(w io.Writer, b byte) error {
	_, err := w.Write([]byte{b})
	return err
}

// ReadShort reads a 16-bit unsigned integer.
func ReadShort(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b[:]), nil
}

// WriteShort writes a 16-bit unsigned integer.
func WriteShort(w io.Writer, i uint16) error {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], i)
	_, err := w.Write(b[:])
	return err
}

// ReadLong reads a 32-bit unsigned integer.
func ReadLong(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

// WriteLong writes a 32-bit unsigned integer.
func WriteLong(w io.Writer, i uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], i)
	_, err := w.Write(b[:])
	return err
}

// ReadLongLong reads a 64-bit unsigned integer.
func ReadLongLong(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

// WriteLongLong writes a 64-bit unsigned integer.
func WriteLongLong(w io.Writer, i uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	_, err := w.Write(b[:])
	return err
}

// ReadShortStr reads a short string.
func ReadShortStr(r io.Reader) (string, error) {
	length, err := ReadOctet(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// WriteShortStr writes a short string.
func WriteShortStr(w io.Writer, s string) error {
	if len(s) > 255 {
		return NewErr(InternalError, "short string too long", nil)
	}
	if err := WriteOctet(w, byte(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

// ReadLongStr reads a long string.
func ReadLongStr(r io.Reader) (string, error) {
	length, err := ReadLong(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// WriteLongStr writes a long string.
func WriteLongStr(w io.Writer, s string) error {
	if err := WriteLong(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := w.Write([]byte(s))
	return err
}

// TODO: Implement ReadTable and WriteTable for field-table support.

// ReadTable reads a field-table from the reader.
func ReadTable(r io.Reader) (map[string]interface{}, error) {
	length, err := ReadLong(r)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	b := new(bytes.Buffer)
	b.Write(buf)

	table := make(map[string]interface{})
	for b.Len() > 0 {
		key, err := ReadShortStr(b)
		if err != nil {
			return nil, err
		}
		value, err := ReadFieldValue(b)
		if err != nil {
			return nil, err
		}
		table[key] = value
	}
	return table, nil
}

// WriteTable writes a field-table to the writer.
func WriteTable(w io.Writer, table map[string]interface{}) error {
	b := new(bytes.Buffer)
	for key, value := range table {
		if err := WriteShortStr(b, key); err != nil {
			return err
		}
		if err := WriteFieldValue(b, value); err != nil {
			return err
		}
	}
	if err := WriteLong(w, uint32(b.Len())); err != nil {
		return err
	}
	_, err := w.Write(b.Bytes())
	return err
}

// ReadFieldValue reads a single field-value from the reader.
func ReadFieldValue(r io.Reader) (interface{}, error) {
	fieldType, err := ReadOctet(r)
	if err != nil {
		return nil, err
	}
	switch fieldType {
	case 't':
		b, err := ReadOctet(r)
		return b == 1, err
	case 'b':
		val, err := ReadOctet(r)
		return int8(val), err
	case 'B':
		return ReadOctet(r)
	case 'u':
		val, err := ReadShort(r)
		return int16(val), err
	case 'U':
		return ReadShort(r)
	case 'I':
		val, err := ReadLong(r)
		return int32(val), err
	case 'i':
		val, err := ReadLong(r)
		return int32(val), err
	case 'l':
		val, err := ReadLongLong(r)
		return int64(val), err
	case 'f':
		val, err := ReadLong(r)
		return math.Float32frombits(val), err
	case 'd':
		val, err := ReadLongLong(r)
		return math.Float64frombits(val), err
	case 'D':
		scale, err := ReadOctet(r)
		if err != nil {
			return nil, err
		}
		val, err := ReadLong(r)
		if err != nil {
			return nil, err
		}
		return Decimal{Scale: scale, Value: int32(val)}, nil
	case 's':
		return ReadShortStr(r)
	case 'S':
		return ReadLongStr(r)
	case 'T':
		return ReadLongLong(r)
	case 'F':
		return ReadTable(r)
	case 'A':
		return ReadArray(r)
	case 'V':
		return nil, nil
	case 'x':
		length, err := ReadLong(r)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf, nil
	default:
		return nil, NewErr(FrameError, "unsupported field type", nil)
	}
}

// WriteFieldValue writes a single field-value to the writer.
func WriteFieldValue(w io.Writer, value interface{}) error {
	switch v := value.(type) {
	case bool:
		if err := WriteOctet(w, 't'); err != nil {
			return err
		}
		if v {
			return WriteOctet(w, 1)
		}
		return WriteOctet(w, 0)
	case int8:
		if err := WriteOctet(w, 'b'); err != nil {
			return err
		}
		return WriteOctet(w, byte(v))
	case byte:
		if err := WriteOctet(w, 'B'); err != nil {
			return err
		}
		return WriteOctet(w, v)
	case int16:
		if err := WriteOctet(w, 'u'); err != nil {
			return err
		}
		return WriteShort(w, uint16(v))
	case uint16:
		if err := WriteOctet(w, 'U'); err != nil {
			return err
		}
		return WriteShort(w, v)
	case int32:
		if err := WriteOctet(w, 'I'); err != nil {
			return err
		}
		return WriteLong(w, uint32(v))
	case int:
		if err := WriteOctet(w, 'I'); err != nil {
			return err
		}
		return WriteLong(w, uint32(v))
	case int64:
		if err := WriteOctet(w, 'l'); err != nil {
			return err
		}
		return WriteLongLong(w, uint64(v))
	case float32:
		if err := WriteOctet(w, 'f'); err != nil {
			return err
		}
		return WriteLong(w, math.Float32bits(v))
	case uint64:
		if err := WriteOctet(w, 'T'); err != nil {
			return err
		}
		return WriteLongLong(w, v)
	case float64:
		if err := WriteOctet(w, 'd'); err != nil {
			return err
		}
		return WriteLongLong(w, math.Float64bits(v))
	case Decimal:
		if err := WriteOctet(w, 'D'); err != nil {
			return err
		}
		if err := WriteOctet(w, v.Scale); err != nil {
			return err
		}
		return WriteLong(w, uint32(v.Value))
	case string:
		if err := WriteOctet(w, 'S'); err != nil {
			return err
		}
		return WriteLongStr(w, v)
	case map[string]interface{}:
		if err := WriteOctet(w, 'F'); err != nil {
			return err
		}
		return WriteTable(w, v)
	case []interface{}:
		if err := WriteOctet(w, 'A'); err != nil {
			return err
		}
		return WriteArray(w, v)
	case []byte:
		if err := WriteOctet(w, 'x'); err != nil {
			return err
		}
		if err := WriteLong(w, uint32(len(v))); err != nil {
			return err
		}
		_, err := w.Write(v)
		return err
	case nil:
		return WriteOctet(w, 'V')
	default:
		return NewErr(FrameError, "unsupported value type", nil)
	}
}

// ReadArray reads a field-array from the reader.
func ReadArray(r io.Reader) ([]interface{}, error) {
	length, err := ReadLong(r)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	b := new(bytes.Buffer)
	b.Write(buf)
	var arr []interface{}
	for b.Len() > 0 {
		value, err := ReadFieldValue(b)
		if err != nil {
			return nil, err
		}
		arr = append(arr, value)
	}
	return arr, nil
}

// WriteArray writes a field-array to the writer.
func WriteArray(w io.Writer, arr []interface{}) error {
	b := new(bytes.Buffer)
	for _, value := range arr {
		if err := WriteFieldValue(b, value); err != nil {
			return err
		}
	}
	if err := WriteLong(w, uint32(b.Len())); err != nil {
		return err
	}
	_, err := w.Write(b.Bytes())
	return err
}
