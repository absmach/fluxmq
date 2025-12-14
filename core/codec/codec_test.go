package codec_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/dborovcanin/mqtt/core/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testString      = "test string"
	testBytes       = []byte("test bytes")
	maxUint16       = uint16(65535)
	maxUint32       = uint32(4294967295)
	maxVBI          = 268435455 // Maximum VBI value per MQTT spec
	emptyString     = ""
	emptyBytes      = []byte{}
	longString      = string(make([]byte, 65535))
	utf8String      = "Hello 世界 🌍"
	utf8Bytes       = []byte(utf8String)
)

func TestEncodeDecodeBytes(t *testing.T) {
	cases := []struct {
		desc  string
		input []byte
		err   error
	}{
		{
			desc:  "encode and decode normal bytes",
			input: testBytes,
			err:   nil,
		},
		{
			desc:  "encode and decode empty bytes",
			input: emptyBytes,
			err:   nil,
		},
		{
			desc:  "encode and decode UTF-8 bytes",
			input: utf8Bytes,
			err:   nil,
		},
		{
			desc:  "encode and decode max length bytes",
			input: make([]byte, 65535),
			err:   nil,
		},
		{
			desc:  "encode and decode single byte",
			input: []byte{0x42},
			err:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := codec.EncodeBytes(tc.input)
			assert.NotNil(t, encoded)
			assert.GreaterOrEqual(t, len(encoded), 2, "encoded bytes should have at least length prefix")

			reader := bytes.NewReader(encoded)
			decoded, err := codec.DecodeBytes(reader)
			assert.Equal(t, tc.err, err)
			if err == nil {
				assert.Equal(t, tc.input, decoded)
			}
		})
	}
}

func TestEncodeDecodeString(t *testing.T) {
	cases := []struct {
		desc  string
		input string
		err   error
	}{
		{
			desc:  "encode and decode normal string",
			input: testString,
			err:   nil,
		},
		{
			desc:  "encode and decode empty string",
			input: emptyString,
			err:   nil,
		},
		{
			desc:  "encode and decode UTF-8 string",
			input: utf8String,
			err:   nil,
		},
		{
			desc:  "encode and decode max length string",
			input: longString,
			err:   nil,
		},
		{
			desc:  "encode and decode string with special characters",
			input: "test/topic/+/#",
			err:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := codec.EncodeString(tc.input)
			assert.NotNil(t, encoded)

			reader := bytes.NewReader(encoded)
			decoded, err := codec.DecodeString(reader)
			assert.Equal(t, tc.err, err)
			if err == nil {
				assert.Equal(t, tc.input, decoded)
			}
		})
	}
}

func TestEncodeDecodeUint16(t *testing.T) {
	cases := []struct {
		desc  string
		input uint16
		err   error
	}{
		{
			desc:  "encode and decode zero",
			input: 0,
			err:   nil,
		},
		{
			desc:  "encode and decode small number",
			input: 42,
			err:   nil,
		},
		{
			desc:  "encode and decode max uint16",
			input: maxUint16,
			err:   nil,
		},
		{
			desc:  "encode and decode packet ID",
			input: 12345,
			err:   nil,
		},
		{
			desc:  "encode and decode boundary value 255",
			input: 255,
			err:   nil,
		},
		{
			desc:  "encode and decode boundary value 256",
			input: 256,
			err:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := codec.EncodeUint16(tc.input)
			assert.Equal(t, 2, len(encoded), "uint16 should encode to 2 bytes")

			reader := bytes.NewReader(encoded)
			decoded, err := codec.DecodeUint16(reader)
			assert.Equal(t, tc.err, err)
			if err == nil {
				assert.Equal(t, tc.input, decoded)
			}
		})
	}
}

func TestEncodeDecodeUint32(t *testing.T) {
	cases := []struct {
		desc  string
		input uint32
		err   error
	}{
		{
			desc:  "encode and decode zero",
			input: 0,
			err:   nil,
		},
		{
			desc:  "encode and decode small number",
			input: 12345,
			err:   nil,
		},
		{
			desc:  "encode and decode max uint32",
			input: maxUint32,
			err:   nil,
		},
		{
			desc:  "encode and decode session expiry interval",
			input: 3600,
			err:   nil,
		},
		{
			desc:  "encode and decode boundary value 65535",
			input: 65535,
			err:   nil,
		},
		{
			desc:  "encode and decode boundary value 65536",
			input: 65536,
			err:   nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := codec.EncodeUint32(tc.input)
			assert.Equal(t, 4, len(encoded), "uint32 should encode to 4 bytes")

			reader := bytes.NewReader(encoded)
			decoded, err := codec.DecodeUint32(reader)
			assert.Equal(t, tc.err, err)
			if err == nil {
				assert.Equal(t, tc.input, decoded)
			}
		})
	}
}

func TestEncodeDecodeVBI(t *testing.T) {
	cases := []struct {
		desc         string
		input        int
		expectedLen  int
		err          error
	}{
		{
			desc:        "encode and decode zero",
			input:       0,
			expectedLen: 1,
			err:         nil,
		},
		{
			desc:        "encode and decode small number",
			input:       127,
			expectedLen: 1,
			err:         nil,
		},
		{
			desc:        "encode and decode boundary 127 (1 byte)",
			input:       127,
			expectedLen: 1,
			err:         nil,
		},
		{
			desc:        "encode and decode boundary 128 (2 bytes)",
			input:       128,
			expectedLen: 2,
			err:         nil,
		},
		{
			desc:        "encode and decode boundary 16383 (2 bytes)",
			input:       16383,
			expectedLen: 2,
			err:         nil,
		},
		{
			desc:        "encode and decode boundary 16384 (3 bytes)",
			input:       16384,
			expectedLen: 3,
			err:         nil,
		},
		{
			desc:        "encode and decode boundary 2097151 (3 bytes)",
			input:       2097151,
			expectedLen: 3,
			err:         nil,
		},
		{
			desc:        "encode and decode boundary 2097152 (4 bytes)",
			input:       2097152,
			expectedLen: 4,
			err:         nil,
		},
		{
			desc:        "encode and decode max VBI value",
			input:       maxVBI,
			expectedLen: 4,
			err:         nil,
		},
		{
			desc:        "encode and decode typical packet size",
			input:       1024,
			expectedLen: 2,
			err:         nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := codec.EncodeVBI(tc.input)
			assert.Equal(t, tc.expectedLen, len(encoded), "VBI length mismatch")

			reader := bytes.NewReader(encoded)
			decoded, err := codec.DecodeVBI(reader)
			assert.Equal(t, tc.err, err)
			if err == nil {
				assert.Equal(t, tc.input, decoded)
			}
		})
	}
}

func TestDecodeVBIMaxLengthExceeded(t *testing.T) {
	malformedVBI := []byte{0xFF, 0xFF, 0xFF, 0xFF}

	reader := bytes.NewReader(malformedVBI)
	_, err := codec.DecodeVBI(reader)
	assert.Equal(t, codec.ErrMaxLengthExceeded, err)
}

func TestDecodeByte(t *testing.T) {
	cases := []struct {
		desc  string
		input []byte
		want  byte
		err   error
	}{
		{
			desc:  "decode byte successfully",
			input: []byte{0x42},
			want:  0x42,
			err:   nil,
		},
		{
			desc:  "decode zero byte",
			input: []byte{0x00},
			want:  0x00,
			err:   nil,
		},
		{
			desc:  "decode max byte value",
			input: []byte{0xFF},
			want:  0xFF,
			err:   nil,
		},
		{
			desc:  "decode from empty reader",
			input: []byte{},
			want:  0,
			err:   io.EOF,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewReader(tc.input)
			decoded, err := codec.DecodeByte(reader)
			if tc.err != nil {
				assert.Equal(t, tc.err, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, decoded)
			}
		})
	}
}

func TestDecodeUint16Errors(t *testing.T) {
	cases := []struct {
		desc  string
		input []byte
		err   error
	}{
		{
			desc:  "decode from empty reader",
			input: []byte{},
			err:   io.EOF,
		},
		{
			desc:  "decode from incomplete data (1 byte)",
			input: []byte{0x42},
			err:   io.ErrUnexpectedEOF,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewReader(tc.input)
			_, err := codec.DecodeUint16(reader)
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestDecodeUint32Errors(t *testing.T) {
	cases := []struct {
		desc  string
		input []byte
		err   error
	}{
		{
			desc:  "decode from empty reader",
			input: []byte{},
			err:   io.EOF,
		},
		{
			desc:  "decode from incomplete data (3 bytes)",
			input: []byte{0x00, 0x00, 0x00},
			err:   io.ErrUnexpectedEOF,
		},
		{
			desc:  "decode from incomplete data (2 bytes)",
			input: []byte{0x00, 0x00},
			err:   io.ErrUnexpectedEOF,
		},
		{
			desc:  "decode from incomplete data (1 byte)",
			input: []byte{0x00},
			err:   io.ErrUnexpectedEOF,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewReader(tc.input)
			_, err := codec.DecodeUint32(reader)
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestDecodeBytesErrors(t *testing.T) {
	cases := []struct {
		desc  string
		input []byte
		err   error
	}{
		{
			desc:  "decode from empty reader",
			input: []byte{},
			err:   io.EOF,
		},
		{
			desc:  "decode with incomplete length prefix",
			input: []byte{0x00},
			err:   io.ErrUnexpectedEOF,
		},
		{
			desc:  "decode with length but no data",
			input: []byte{0x00, 0x05},
			err:   io.EOF,
		},
		{
			desc:  "decode with length larger than available data",
			input: []byte{0x00, 0x10, 0x01, 0x02},
			err:   io.ErrUnexpectedEOF,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewReader(tc.input)
			_, err := codec.DecodeBytes(reader)
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestDecodeStringErrors(t *testing.T) {
	cases := []struct {
		desc  string
		input []byte
		err   error
	}{
		{
			desc:  "decode from empty reader",
			input: []byte{},
			err:   io.EOF,
		},
		{
			desc:  "decode with incomplete length prefix",
			input: []byte{0x00},
			err:   io.ErrUnexpectedEOF,
		},
		{
			desc:  "decode with length but no data",
			input: []byte{0x00, 0x05},
			err:   io.EOF,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			reader := bytes.NewReader(tc.input)
			_, err := codec.DecodeString(reader)
			assert.Equal(t, tc.err, err)
		})
	}
}

func TestEncodeBool(t *testing.T) {
	cases := []struct {
		desc  string
		input bool
		want  byte
	}{
		{
			desc:  "encode true",
			input: true,
			want:  1,
		},
		{
			desc:  "encode false",
			input: false,
			want:  0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			result := codec.EncodeBool(tc.input)
			assert.Equal(t, tc.want, result)
		})
	}
}

func TestEncodeDecodeBinaryData(t *testing.T) {
	cases := []struct {
		desc  string
		input []byte
	}{
		{
			desc:  "encode and decode all byte values",
			input: func() []byte {
				data := make([]byte, 256)
				for i := 0; i < 256; i++ {
					data[i] = byte(i)
				}
				return data
			}(),
		},
		{
			desc:  "encode and decode null bytes",
			input: []byte{0x00, 0x00, 0x00, 0x00},
		},
		{
			desc:  "encode and decode max bytes",
			input: []byte{0xFF, 0xFF, 0xFF, 0xFF},
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := codec.EncodeBytes(tc.input)
			reader := bytes.NewReader(encoded)
			decoded, err := codec.DecodeBytes(reader)
			require.NoError(t, err)
			assert.Equal(t, tc.input, decoded)
		})
	}
}
