// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package codec_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/absmach/fluxmq/amqp/codec"
)

func TestReadWriteOctet(t *testing.T) {
	tests := []struct {
		name  string
		input byte
	}{
		{"zero", 0x00},
		{"max", 0xFF},
		{"arbitrary", 0x42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			if err := codec.WriteOctet(buf, tt.input); err != nil {
				t.Fatalf("WriteOctet failed: %v", err)
			}
			if buf.Len() != 1 {
				t.Fatalf("Expected buffer length 1, got %d", buf.Len())
			}

			readVal, err := codec.ReadOctet(buf)
			if err != nil {
				t.Fatalf("ReadOctet failed: %v", err)
			}
			if readVal != tt.input {
				t.Fatalf("Expected %v, got %v", tt.input, readVal)
			}
		})
	}
}

func TestReadWriteShort(t *testing.T) {
	tests := []struct {
		name  string
		input uint16
	}{
		{"zero", 0},
		{"max", 0xFFFF},
		{"arbitrary", 0x1234},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			if err := codec.WriteShort(buf, tt.input); err != nil {
				t.Fatalf("WriteShort failed: %v", err)
			}
			if buf.Len() != 2 {
				t.Fatalf("Expected buffer length 2, got %d", buf.Len())
			}

			readVal, err := codec.ReadShort(buf)
			if err != nil {
				t.Fatalf("ReadShort failed: %v", err)
			}
			if readVal != tt.input {
				t.Fatalf("Expected %v, got %v", tt.input, readVal)
			}
		})
	}
}

func TestReadWriteLong(t *testing.T) {
	tests := []struct {
		name  string
		input uint32
	}{
		{"zero", 0},
		{"max", 0xFFFFFFFF},
		{"arbitrary", 0x12345678},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			if err := codec.WriteLong(buf, tt.input); err != nil {
				t.Fatalf("WriteLong failed: %v", err)
			}
			if buf.Len() != 4 {
				t.Fatalf("Expected buffer length 4, got %d", buf.Len())
			}

			readVal, err := codec.ReadLong(buf)
			if err != nil {
				t.Fatalf("ReadLong failed: %v", err)
			}
			if readVal != tt.input {
				t.Fatalf("Expected %v, got %v", tt.input, readVal)
			}
		})
	}
}

func TestReadWriteLongLong(t *testing.T) {
	tests := []struct {
		name  string
		input uint64
	}{
		{"zero", 0},
		{"max", 0xFFFFFFFFFFFFFFFF},
		{"arbitrary", 0x123456789ABCDEF0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			if err := codec.WriteLongLong(buf, tt.input); err != nil {
				t.Fatalf("WriteLongLong failed: %v", err)
			}
			if buf.Len() != 8 {
				t.Fatalf("Expected buffer length 8, got %d", buf.Len())
			}

			readVal, err := codec.ReadLongLong(buf)
			if err != nil {
				t.Fatalf("ReadLongLong failed: %v", err)
			}
			if readVal != tt.input {
				t.Fatalf("Expected %v, got %v", tt.input, readVal)
			}
		})
	}
}

func TestReadWriteShortStr(t *testing.T) {
	tests := []struct {
		name  string
		input string
		err   error
	}{
		{"empty", "", nil},
		{"short", "hello", nil},
		{"max_length", string(make([]byte, 255)), nil},
		{"too_long", string(make([]byte, 256)), codec.NewErr(codec.InternalError, "short string too long", nil)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := codec.WriteShortStr(buf, tt.input)

			if tt.err != nil {
				if err == nil || err.Error() != tt.err.Error() {
					t.Fatalf("Expected error %v, got %v", tt.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("WriteShortStr failed: %v", err)
			}

			readVal, err := codec.ReadShortStr(buf)
			if err != nil {
				t.Fatalf("ReadShortStr failed: %v", err)
			}
			if readVal != tt.input {
				t.Fatalf("Expected %q, got %q", tt.input, readVal)
			}
		})
	}
}

func TestReadWriteLongStr(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"short", "hello world"},
		{"long", string(make([]byte, 1024))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			if err := codec.WriteLongStr(buf, tt.input); err != nil {
				t.Fatalf("WriteLongStr failed: %v", err)
			}

			readVal, err := codec.ReadLongStr(buf)
			if err != nil {
				t.Fatalf("ReadLongStr failed: %v", err)
			}
			if readVal != tt.input {
				t.Fatalf("Expected %q, got %q", tt.input, readVal)
			}
		})
	}
}

func TestReadWriteTable(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]interface{}
	}{
		{
			"empty",
			map[string]interface{}{},
		},
		{
			"simple",
			map[string]interface{}{
				"key1": "value1",
				"key2": true,
				"key3": int32(123),
			},
		},
		{
			"nested",
			map[string]interface{}{
				"key1": "value1",
				"nested_table": map[string]interface{}{
					"nested_key1": int32(456),
					"nested_key2": true,
				},
				"key3": "value3",
			},
		},
		{
			"with_array",
			map[string]interface{}{
				"key1": "value1",
				"array_key": []interface{}{
					"arr_val1",
					true,
					int32(789),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			if err := codec.WriteTable(buf, tt.input); err != nil {
				t.Fatalf("WriteTable failed: %v", err)
			}

			readVal, err := codec.ReadTable(buf)
			if err != nil {
				t.Fatalf("ReadTable failed: %v", err)
			}

			if !reflect.DeepEqual(readVal, tt.input) {
				t.Fatalf("Expected %v, got %v", tt.input, readVal)
			}
		})
	}
}

func TestConnectionMethods(t *testing.T) {
	tests := []struct {
		name   string
		method interface {
			Read(*bytes.Reader) error
			Write(io.Writer) error
		}
		expected interface{}
		classID  uint16
		methodID uint16
	}{
		{
			"ConnectionStart",
			&codec.ConnectionStart{
				VersionMajor: 0,
				VersionMinor: 9,
				ServerProperties: map[string]interface{}{
					"product": "FluxMQ",
					"version": "0.1.0",
				},
				Mechanisms: "PLAIN AMQPLAIN",
				Locales:    "en_US",
			},
			&codec.ConnectionStart{
				VersionMajor: 0,
				VersionMinor: 9,
				ServerProperties: map[string]interface{}{
					"product": "FluxMQ",
					"version": "0.1.0",
				},
				Mechanisms: "PLAIN AMQPLAIN",
				Locales:    "en_US",
			},
			codec.ClassConnection,
			codec.MethodConnectionStart,
		},
		{
			"ConnectionStartOk",
			&codec.ConnectionStartOk{
				ClientProperties: map[string]interface{}{
					"product": "Go Client",
					"version": "1.0.0",
				},
				Mechanism: "PLAIN",
				Response:  "some_response",
				Locale:    "en_US",
			},
			&codec.ConnectionStartOk{
				ClientProperties: map[string]interface{}{
					"product": "Go Client",
					"version": "1.0.0",
				},
				Mechanism: "PLAIN",
				Response:  "some_response",
				Locale:    "en_US",
			},
			codec.ClassConnection,
			codec.MethodConnectionStartOk,
		},
		{
			"ConnectionSecure",
			&codec.ConnectionSecure{
				Challenge: "challenge_string",
			},
			&codec.ConnectionSecure{
				Challenge: "challenge_string",
			},
			codec.ClassConnection,
			codec.MethodConnectionSecure,
		},
		{
			"ConnectionSecureOk",
			&codec.ConnectionSecureOk{
				Response: "response_string",
			},
			&codec.ConnectionSecureOk{
				Response: "response_string",
			},
			codec.ClassConnection,
			codec.MethodConnectionSecureOk,
		},
		{
			"ConnectionTune",
			&codec.ConnectionTune{
				ChannelMax: 10,
				FrameMax:   1024,
				Heartbeat:  60,
			},
			&codec.ConnectionTune{
				ChannelMax: 10,
				FrameMax:   1024,
				Heartbeat:  60,
			},
			codec.ClassConnection,
			codec.MethodConnectionTune,
		},
		{
			"ConnectionTuneOk",
			&codec.ConnectionTuneOk{
				ChannelMax: 5,
				FrameMax:   512,
				Heartbeat:  30,
			},
			&codec.ConnectionTuneOk{
				ChannelMax: 5,
				FrameMax:   512,
				Heartbeat:  30,
			},
			codec.ClassConnection,
			codec.MethodConnectionTuneOk,
		},
		{
			"ConnectionOpen",
			&codec.ConnectionOpen{
				VirtualHost:  "/",
				Capabilities: "",
				Insist:       true,
			},
			&codec.ConnectionOpen{
				VirtualHost:  "/",
				Capabilities: "",
				Insist:       true,
			},
			codec.ClassConnection,
			codec.MethodConnectionOpen,
		},
		{
			"ConnectionOpenOk",
			&codec.ConnectionOpenOk{
				KnownHosts: "",
			},
			&codec.ConnectionOpenOk{
				KnownHosts: "",
			},
			codec.ClassConnection,
			codec.MethodConnectionOpenOk,
		},
		{
			"ConnectionClose",
			&codec.ConnectionClose{
				ReplyCode: 200,
				ReplyText: "Goodbye",
				ClassID:   codec.ClassConnection,
				MethodID:  codec.MethodConnectionOpen,
			},
			&codec.ConnectionClose{
				ReplyCode: 200,
				ReplyText: "Goodbye",
				ClassID:   codec.ClassConnection,
				MethodID:  codec.MethodConnectionOpen,
			},
			codec.ClassConnection,
			codec.MethodConnectionClose,
		},
		{
			"ConnectionCloseOk",
			&codec.ConnectionCloseOk{},
			&codec.ConnectionCloseOk{},
			codec.ClassConnection,
			codec.MethodConnectionCloseOk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBuf := new(bytes.Buffer)
			if err := tt.method.Write(payloadBuf); err != nil {
				t.Fatalf("Write method failed for %s: %v", tt.name, err)
			}

			frame := &codec.Frame{
				Type:    codec.FrameMethod,
				Channel: 0,
				Payload: payloadBuf.Bytes(),
			}

			fullFrameBuf := new(bytes.Buffer)
			if err := frame.WriteFrame(fullFrameBuf); err != nil {
				t.Fatalf("WriteFrame failed for %s: %v", tt.name, err)
			}

			readFrame, err := codec.ReadFrame(fullFrameBuf)
			if err != nil {
				t.Fatalf("ReadFrame failed for %s: %v", tt.name, err)
			}

			decodedMethod, err := readFrame.Decode()
			if err != nil {
				t.Fatalf("Decode method failed for %s: %v", tt.name, err)
			}

			if !reflect.DeepEqual(decodedMethod, tt.expected) {
				t.Errorf("Decoded method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, decodedMethod)
			}

			directReadMethod := reflect.New(reflect.TypeOf(tt.expected).Elem()).Interface().(interface {
				Read(*bytes.Reader) error
			})
			directPayloadReader := bytes.NewReader(payloadBuf.Bytes()[4:])
			if err := directReadMethod.Read(directPayloadReader); err != nil {
				t.Fatalf("Direct Read method failed for %s: %v", tt.name, err)
			}
			if !reflect.DeepEqual(directReadMethod, tt.expected) {
				t.Errorf("Direct Read method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, directReadMethod)
			}
		})
	}
}

func TestFieldValueTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"int8", int8(-42)},
		{"byte_uint8", byte(255)},
		{"int16", int16(-1234)},
		{"uint16", uint16(65535)},
		{"int32", int32(-123456)},
		{"int64", int64(-9876543210)},
		{"uint64", uint64(9876543210)},
		{"float32", float32(3.14159)},
		{"float64", float64(2.718281828)},
		{"decimal", codec.Decimal{Scale: 2, Value: 12345}},
		{"nil_void", nil},
		{"byte_array", []byte{0x01, 0x02, 0x03, 0x04, 0x05}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			if err := codec.WriteFieldValue(buf, tt.value); err != nil {
				t.Fatalf("WriteFieldValue failed for %s: %v", tt.name, err)
			}

			readVal, err := codec.ReadFieldValue(buf)
			if err != nil {
				t.Fatalf("ReadFieldValue failed for %s: %v", tt.name, err)
			}

			if !reflect.DeepEqual(readVal, tt.value) {
				t.Errorf("Field value mismatch for %s:\nExpected: %+v (%T)\nGot: %+v (%T)",
					tt.name, tt.value, tt.value, readVal, readVal)
			}
		})
	}
}

func TestChannelMethods(t *testing.T) {
	tests := []struct {
		name   string
		method interface {
			Read(*bytes.Reader) error
			Write(io.Writer) error
		}
		expected interface{}
		classID  uint16
		methodID uint16
	}{
		{
			"ChannelOpen",
			&codec.ChannelOpen{},
			&codec.ChannelOpen{Reserved1: ""},
			codec.ClassChannel,
			codec.MethodChannelOpen,
		},
		{
			"ChannelOpenOk",
			&codec.ChannelOpenOk{},
			&codec.ChannelOpenOk{Reserved1: ""},
			codec.ClassChannel,
			codec.MethodChannelOpenOk,
		},
		{
			"ChannelFlow",
			&codec.ChannelFlow{Active: true},
			&codec.ChannelFlow{Active: true},
			codec.ClassChannel,
			codec.MethodChannelFlow,
		},
		{
			"ChannelFlowOk",
			&codec.ChannelFlowOk{Active: false},
			&codec.ChannelFlowOk{Active: false},
			codec.ClassChannel,
			codec.MethodChannelFlowOk,
		},
		{
			"ChannelClose",
			&codec.ChannelClose{
				ReplyCode: 200,
				ReplyText: "OK",
				ClassID:   20,
				MethodID:  10,
			},
			&codec.ChannelClose{
				ReplyCode: 200,
				ReplyText: "OK",
				ClassID:   20,
				MethodID:  10,
			},
			codec.ClassChannel,
			codec.MethodChannelClose,
		},
		{
			"ChannelCloseOk",
			&codec.ChannelCloseOk{},
			&codec.ChannelCloseOk{},
			codec.ClassChannel,
			codec.MethodChannelCloseOk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBuf := new(bytes.Buffer)
			if err := tt.method.Write(payloadBuf); err != nil {
				t.Fatalf("Write method failed for %s: %v", tt.name, err)
			}

			frame := &codec.Frame{
				Type:    codec.FrameMethod,
				Channel: 0,
				Payload: payloadBuf.Bytes(),
			}

			fullFrameBuf := new(bytes.Buffer)
			if err := frame.WriteFrame(fullFrameBuf); err != nil {
				t.Fatalf("WriteFrame failed for %s: %v", tt.name, err)
			}

			readFrame, err := codec.ReadFrame(fullFrameBuf)
			if err != nil {
				t.Fatalf("ReadFrame failed for %s: %v", tt.name, err)
			}

			decodedMethod, err := readFrame.Decode()
			if err != nil {
				t.Fatalf("Decode method failed for %s: %v", tt.name, err)
			}

			if !reflect.DeepEqual(decodedMethod, tt.expected) {
				t.Errorf("Decoded method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, decodedMethod)
			}

			directReadMethod := reflect.New(reflect.TypeOf(tt.expected).Elem()).Interface().(interface {
				Read(*bytes.Reader) error
			})
			directPayloadReader := bytes.NewReader(payloadBuf.Bytes()[4:])
			if err := directReadMethod.Read(directPayloadReader); err != nil {
				t.Fatalf("Direct Read method failed for %s: %v", tt.name, err)
			}
			if !reflect.DeepEqual(directReadMethod, tt.expected) {
				t.Errorf("Direct Read method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, directReadMethod)
			}
		})
	}
}

func TestExchangeMethods(t *testing.T) {
	tests := []struct {
		name   string
		method interface {
			Read(*bytes.Reader) error
			Write(io.Writer) error
		}
		expected interface{}
		classID  uint16
		methodID uint16
	}{
		{
			"ExchangeDeclare",
			&codec.ExchangeDeclare{
				Exchange:   "test-exchange",
				Type:       "direct",
				Passive:    true,
				Durable:    true,
				AutoDelete: true,
				Internal:   true,
				NoWait:     true,
				Arguments: map[string]interface{}{
					"x-message-ttl": int32(60000),
				},
			},
			&codec.ExchangeDeclare{
				Reserved1:  0,
				Exchange:   "test-exchange",
				Type:       "direct",
				Passive:    true,
				Durable:    true,
				AutoDelete: true,
				Internal:   true,
				NoWait:     true,
				Arguments: map[string]interface{}{
					"x-message-ttl": int32(60000),
				},
			},
			codec.ClassExchange,
			codec.MethodExchangeDeclare,
		},
		{
			"ExchangeDeclareOk",
			&codec.ExchangeDeclareOk{},
			&codec.ExchangeDeclareOk{},
			codec.ClassExchange,
			codec.MethodExchangeDeclareOk,
		},
		{
			"ExchangeDelete",
			&codec.ExchangeDelete{
				Exchange: "test-exchange",
				IfUnused: true,
				NoWait:   true,
			},
			&codec.ExchangeDelete{
				Reserved1: 0,
				Exchange:  "test-exchange",
				IfUnused:  true,
				NoWait:    true,
			},
			codec.ClassExchange,
			codec.MethodExchangeDelete,
		},
		{
			"ExchangeDeleteOk",
			&codec.ExchangeDeleteOk{},
			&codec.ExchangeDeleteOk{},
			codec.ClassExchange,
			codec.MethodExchangeDeleteOk,
		},
		{
			"ExchangeBind",
			&codec.ExchangeBind{
				Destination: "dest-exchange",
				Source:      "source-exchange",
				RoutingKey:  "routing.key",
				NoWait:      true,
				Arguments: map[string]interface{}{
					"key": "value",
				},
			},
			&codec.ExchangeBind{
				Reserved1:   0,
				Destination: "dest-exchange",
				Source:      "source-exchange",
				RoutingKey:  "routing.key",
				NoWait:      true,
				Arguments: map[string]interface{}{
					"key": "value",
				},
			},
			codec.ClassExchange,
			codec.MethodExchangeBind,
		},
		{
			"ExchangeBindOk",
			&codec.ExchangeBindOk{},
			&codec.ExchangeBindOk{},
			codec.ClassExchange,
			codec.MethodExchangeBindOk,
		},
		{
			"ExchangeUnbind",
			&codec.ExchangeUnbind{
				Destination: "dest-exchange",
				Source:      "source-exchange",
				RoutingKey:  "routing.key",
				NoWait:      false,
				Arguments: map[string]interface{}{
					"key": "value",
				},
			},
			&codec.ExchangeUnbind{
				Reserved1:   0,
				Destination: "dest-exchange",
				Source:      "source-exchange",
				RoutingKey:  "routing.key",
				NoWait:      false,
				Arguments: map[string]interface{}{
					"key": "value",
				},
			},
			codec.ClassExchange,
			codec.MethodExchangeUnbind,
		},
		{
			"ExchangeUnbindOk",
			&codec.ExchangeUnbindOk{},
			&codec.ExchangeUnbindOk{},
			codec.ClassExchange,
			codec.MethodExchangeUnbindOk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBuf := new(bytes.Buffer)
			if err := tt.method.Write(payloadBuf); err != nil {
				t.Fatalf("Write method failed for %s: %v", tt.name, err)
			}

			frame := &codec.Frame{
				Type:    codec.FrameMethod,
				Channel: 0,
				Payload: payloadBuf.Bytes(),
			}

			fullFrameBuf := new(bytes.Buffer)
			if err := frame.WriteFrame(fullFrameBuf); err != nil {
				t.Fatalf("WriteFrame failed for %s: %v", tt.name, err)
			}

			readFrame, err := codec.ReadFrame(fullFrameBuf)
			if err != nil {
				t.Fatalf("ReadFrame failed for %s: %v", tt.name, err)
			}

			decodedMethod, err := readFrame.Decode()
			if err != nil {
				t.Fatalf("Decode method failed for %s: %v", tt.name, err)
			}

			if !reflect.DeepEqual(decodedMethod, tt.expected) {
				t.Errorf("Decoded method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, decodedMethod)
			}

			directReadMethod := reflect.New(reflect.TypeOf(tt.expected).Elem()).Interface().(interface {
				Read(*bytes.Reader) error
			})
			directPayloadReader := bytes.NewReader(payloadBuf.Bytes()[4:])
			if err := directReadMethod.Read(directPayloadReader); err != nil {
				t.Fatalf("Direct Read method failed for %s: %v", tt.name, err)
			}
			if !reflect.DeepEqual(directReadMethod, tt.expected) {
				t.Errorf("Direct Read method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, directReadMethod)
			}
		})
	}
}

func TestQueueMethods(t *testing.T) {
	tests := []struct {
		name   string
		method interface {
			Read(*bytes.Reader) error
			Write(io.Writer) error
		}
		expected interface{}
		classID  uint16
		methodID uint16
	}{
		{
			"QueueDeclare",
			&codec.QueueDeclare{
				Queue:      "test-queue",
				Passive:    true,
				Durable:    true,
				Exclusive:  true,
				AutoDelete: true,
				NoWait:     true,
				Arguments: map[string]interface{}{
					"x-max-length": int32(1000),
				},
			},
			&codec.QueueDeclare{
				Reserved1:  0,
				Queue:      "test-queue",
				Passive:    true,
				Durable:    true,
				Exclusive:  true,
				AutoDelete: true,
				NoWait:     true,
				Arguments: map[string]interface{}{
					"x-max-length": int32(1000),
				},
			},
			codec.ClassQueue,
			codec.MethodQueueDeclare,
		},
		{
			"QueueDeclareOk",
			&codec.QueueDeclareOk{
				Queue:         "q1",
				MessageCount:  10,
				ConsumerCount: 2,
			},
			&codec.QueueDeclareOk{
				Queue:         "q1",
				MessageCount:  10,
				ConsumerCount: 2,
			},
			codec.ClassQueue,
			codec.MethodQueueDeclareOk,
		},
		{
			"QueueBind",
			&codec.QueueBind{
				Queue:      "test-queue",
				Exchange:   "test-exchange",
				RoutingKey: "routing.key",
				NoWait:     true,
				Arguments: map[string]interface{}{
					"key": "value",
				},
			},
			&codec.QueueBind{
				Reserved1:  0,
				Queue:      "test-queue",
				Exchange:   "test-exchange",
				RoutingKey: "routing.key",
				NoWait:     true,
				Arguments: map[string]interface{}{
					"key": "value",
				},
			},
			codec.ClassQueue,
			codec.MethodQueueBind,
		},
		{
			"QueueBindOk",
			&codec.QueueBindOk{},
			&codec.QueueBindOk{},
			codec.ClassQueue,
			codec.MethodQueueBindOk,
		},
		{
			"QueuePurge",
			&codec.QueuePurge{
				Queue:  "test-queue",
				NoWait: true,
			},
			&codec.QueuePurge{
				Reserved1: 0,
				Queue:     "test-queue",
				NoWait:    true,
			},
			codec.ClassQueue,
			codec.MethodQueuePurge,
		},
		{
			"QueuePurgeOk",
			&codec.QueuePurgeOk{MessageCount: 5},
			&codec.QueuePurgeOk{MessageCount: 5},
			codec.ClassQueue,
			codec.MethodQueuePurgeOk,
		},
		{
			"QueueDelete",
			&codec.QueueDelete{
				Queue:    "test-queue",
				IfUnused: true,
				IfEmpty:  true,
				NoWait:   true,
			},
			&codec.QueueDelete{
				Reserved1: 0,
				Queue:     "test-queue",
				IfUnused:  true,
				IfEmpty:   true,
				NoWait:    true,
			},
			codec.ClassQueue,
			codec.MethodQueueDelete,
		},
		{
			"QueueDeleteOk",
			&codec.QueueDeleteOk{MessageCount: 3},
			&codec.QueueDeleteOk{MessageCount: 3},
			codec.ClassQueue,
			codec.MethodQueueDeleteOk,
		},
		{
			"QueueUnbind",
			&codec.QueueUnbind{
				Queue:      "test-queue",
				Exchange:   "test-exchange",
				RoutingKey: "routing.key",
				Arguments: map[string]interface{}{
					"key": "value",
				},
			},
			&codec.QueueUnbind{
				Reserved1:  0,
				Queue:      "test-queue",
				Exchange:   "test-exchange",
				RoutingKey: "routing.key",
				Arguments: map[string]interface{}{
					"key": "value",
				},
			},
			codec.ClassQueue,
			codec.MethodQueueUnbind,
		},
		{
			"QueueUnbindOk",
			&codec.QueueUnbindOk{},
			&codec.QueueUnbindOk{},
			codec.ClassQueue,
			codec.MethodQueueUnbindOk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBuf := new(bytes.Buffer)
			if err := tt.method.Write(payloadBuf); err != nil {
				t.Fatalf("Write method failed for %s: %v", tt.name, err)
			}

			frame := &codec.Frame{
				Type:    codec.FrameMethod,
				Channel: 0,
				Payload: payloadBuf.Bytes(),
			}

			fullFrameBuf := new(bytes.Buffer)
			if err := frame.WriteFrame(fullFrameBuf); err != nil {
				t.Fatalf("WriteFrame failed for %s: %v", tt.name, err)
			}

			readFrame, err := codec.ReadFrame(fullFrameBuf)
			if err != nil {
				t.Fatalf("ReadFrame failed for %s: %v", tt.name, err)
			}

			decodedMethod, err := readFrame.Decode()
			if err != nil {
				t.Fatalf("Decode method failed for %s: %v", tt.name, err)
			}

			if !reflect.DeepEqual(decodedMethod, tt.expected) {
				t.Errorf("Decoded method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, decodedMethod)
			}

			directReadMethod := reflect.New(reflect.TypeOf(tt.expected).Elem()).Interface().(interface {
				Read(*bytes.Reader) error
			})
			directPayloadReader := bytes.NewReader(payloadBuf.Bytes()[4:])
			if err := directReadMethod.Read(directPayloadReader); err != nil {
				t.Fatalf("Direct Read method failed for %s: %v", tt.name, err)
			}
			if !reflect.DeepEqual(directReadMethod, tt.expected) {
				t.Errorf("Direct Read method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, directReadMethod)
			}
		})
	}
}

func TestBasicMethods(t *testing.T) {
	tests := []struct {
		name   string
		method interface {
			Read(*bytes.Reader) error
			Write(io.Writer) error
		}
		expected interface{}
		classID  uint16
		methodID uint16
	}{
		{
			"BasicQos",
			&codec.BasicQos{
				PrefetchSize:  1024,
				PrefetchCount: 10,
				Global:        true,
			},
			&codec.BasicQos{
				PrefetchSize:  1024,
				PrefetchCount: 10,
				Global:        true,
			},
			codec.ClassBasic,
			codec.MethodBasicQos,
		},
		{
			"BasicQosOk",
			&codec.BasicQosOk{},
			&codec.BasicQosOk{},
			codec.ClassBasic,
			codec.MethodBasicQosOk,
		},
		{
			"BasicConsume",
			&codec.BasicConsume{
				Queue:       "test-queue",
				ConsumerTag: "consumer-1",
				NoLocal:     true,
				NoAck:       true,
				Exclusive:   true,
				NoWait:      true,
				Arguments: map[string]interface{}{
					"x-priority": int32(5),
				},
			},
			&codec.BasicConsume{
				Reserved1:   0,
				Queue:       "test-queue",
				ConsumerTag: "consumer-1",
				NoLocal:     true,
				NoAck:       true,
				Exclusive:   true,
				NoWait:      true,
				Arguments: map[string]interface{}{
					"x-priority": int32(5),
				},
			},
			codec.ClassBasic,
			codec.MethodBasicConsume,
		},
		{
			"BasicConsumeOk",
			&codec.BasicConsumeOk{ConsumerTag: "consumer-1"},
			&codec.BasicConsumeOk{ConsumerTag: "consumer-1"},
			codec.ClassBasic,
			codec.MethodBasicConsumeOk,
		},
		{
			"BasicCancel",
			&codec.BasicCancel{
				ConsumerTag: "consumer-1",
				NoWait:      true,
			},
			&codec.BasicCancel{
				ConsumerTag: "consumer-1",
				NoWait:      true,
			},
			codec.ClassBasic,
			codec.MethodBasicCancel,
		},
		{
			"BasicCancelOk",
			&codec.BasicCancelOk{ConsumerTag: "consumer-1"},
			&codec.BasicCancelOk{ConsumerTag: "consumer-1"},
			codec.ClassBasic,
			codec.MethodBasicCancelOk,
		},
		{
			"BasicPublish",
			&codec.BasicPublish{
				Exchange:   "test-exchange",
				RoutingKey: "routing.key",
				Mandatory:  true,
				Immediate:  true,
			},
			&codec.BasicPublish{
				Reserved1:  0,
				Exchange:   "test-exchange",
				RoutingKey: "routing.key",
				Mandatory:  true,
				Immediate:  true,
			},
			codec.ClassBasic,
			codec.MethodBasicPublish,
		},
		{
			"BasicReturn",
			&codec.BasicReturn{
				ReplyCode:  312,
				ReplyText:  "NO_ROUTE",
				Exchange:   "test-exchange",
				RoutingKey: "routing.key",
			},
			&codec.BasicReturn{
				ReplyCode:  312,
				ReplyText:  "NO_ROUTE",
				Exchange:   "test-exchange",
				RoutingKey: "routing.key",
			},
			codec.ClassBasic,
			codec.MethodBasicReturn,
		},
		{
			"BasicDeliver",
			&codec.BasicDeliver{
				ConsumerTag: "consumer-1",
				DeliveryTag: 123456,
				Redelivered: true,
				Exchange:    "test-exchange",
				RoutingKey:  "routing.key",
			},
			&codec.BasicDeliver{
				ConsumerTag: "consumer-1",
				DeliveryTag: 123456,
				Redelivered: true,
				Exchange:    "test-exchange",
				RoutingKey:  "routing.key",
			},
			codec.ClassBasic,
			codec.MethodBasicDeliver,
		},
		{
			"BasicGet",
			&codec.BasicGet{
				Queue: "test-queue",
				NoAck: true,
			},
			&codec.BasicGet{
				Reserved1: 0,
				Queue:     "test-queue",
				NoAck:     true,
			},
			codec.ClassBasic,
			codec.MethodBasicGet,
		},
		{
			"BasicGetOk",
			&codec.BasicGetOk{
				DeliveryTag:  123456,
				Redelivered:  true,
				Exchange:     "test-exchange",
				RoutingKey:   "routing.key",
				MessageCount: 42,
			},
			&codec.BasicGetOk{
				DeliveryTag:  123456,
				Redelivered:  true,
				Exchange:     "test-exchange",
				RoutingKey:   "routing.key",
				MessageCount: 42,
			},
			codec.ClassBasic,
			codec.MethodBasicGetOk,
		},
		{
			"BasicGetEmpty",
			&codec.BasicGetEmpty{},
			&codec.BasicGetEmpty{Reserved1: ""},
			codec.ClassBasic,
			codec.MethodBasicGetEmpty,
		},
		{
			"BasicAck",
			&codec.BasicAck{
				DeliveryTag: 123456,
				Multiple:    true,
			},
			&codec.BasicAck{
				DeliveryTag: 123456,
				Multiple:    true,
			},
			codec.ClassBasic,
			codec.MethodBasicAck,
		},
		{
			"BasicReject",
			&codec.BasicReject{
				DeliveryTag: 123456,
				Requeue:     true,
			},
			&codec.BasicReject{
				DeliveryTag: 123456,
				Requeue:     true,
			},
			codec.ClassBasic,
			codec.MethodBasicReject,
		},
		{
			"BasicRecoverAsync",
			&codec.BasicRecoverAsync{Requeue: true},
			&codec.BasicRecoverAsync{Requeue: true},
			codec.ClassBasic,
			codec.MethodBasicRecoverAsync,
		},
		{
			"BasicRecover",
			&codec.BasicRecover{Requeue: true},
			&codec.BasicRecover{Requeue: true},
			codec.ClassBasic,
			codec.MethodBasicRecover,
		},
		{
			"BasicRecoverOk",
			&codec.BasicRecoverOk{},
			&codec.BasicRecoverOk{},
			codec.ClassBasic,
			codec.MethodBasicRecoverOk,
		},
		{
			"BasicNack",
			&codec.BasicNack{
				DeliveryTag: 123456,
				Multiple:    true,
				Requeue:     true,
			},
			&codec.BasicNack{
				DeliveryTag: 123456,
				Multiple:    true,
				Requeue:     true,
			},
			codec.ClassBasic,
			codec.MethodBasicNack,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBuf := new(bytes.Buffer)
			if err := tt.method.Write(payloadBuf); err != nil {
				t.Fatalf("Write method failed for %s: %v", tt.name, err)
			}

			frame := &codec.Frame{
				Type:    codec.FrameMethod,
				Channel: 0,
				Payload: payloadBuf.Bytes(),
			}

			fullFrameBuf := new(bytes.Buffer)
			if err := frame.WriteFrame(fullFrameBuf); err != nil {
				t.Fatalf("WriteFrame failed for %s: %v", tt.name, err)
			}

			readFrame, err := codec.ReadFrame(fullFrameBuf)
			if err != nil {
				t.Fatalf("ReadFrame failed for %s: %v", tt.name, err)
			}

			decodedMethod, err := readFrame.Decode()
			if err != nil {
				t.Fatalf("Decode method failed for %s: %v", tt.name, err)
			}

			if !reflect.DeepEqual(decodedMethod, tt.expected) {
				t.Errorf("Decoded method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, decodedMethod)
			}

			directReadMethod := reflect.New(reflect.TypeOf(tt.expected).Elem()).Interface().(interface {
				Read(*bytes.Reader) error
			})
			directPayloadReader := bytes.NewReader(payloadBuf.Bytes()[4:])
			if err := directReadMethod.Read(directPayloadReader); err != nil {
				t.Fatalf("Direct Read method failed for %s: %v", tt.name, err)
			}
			if !reflect.DeepEqual(directReadMethod, tt.expected) {
				t.Errorf("Direct Read method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, directReadMethod)
			}
		})
	}
}

func TestTxMethods(t *testing.T) {
	tests := []struct {
		name   string
		method interface {
			Read(*bytes.Reader) error
			Write(io.Writer) error
		}
		expected interface{}
		classID  uint16
		methodID uint16
	}{
		{
			"TxSelect",
			&codec.TxSelect{},
			&codec.TxSelect{},
			codec.ClassTx,
			codec.MethodTxSelect,
		},
		{
			"TxSelectOk",
			&codec.TxSelectOk{},
			&codec.TxSelectOk{},
			codec.ClassTx,
			codec.MethodTxSelectOk,
		},
		{
			"TxCommit",
			&codec.TxCommit{},
			&codec.TxCommit{},
			codec.ClassTx,
			codec.MethodTxCommit,
		},
		{
			"TxCommitOk",
			&codec.TxCommitOk{},
			&codec.TxCommitOk{},
			codec.ClassTx,
			codec.MethodTxCommitOk,
		},
		{
			"TxRollback",
			&codec.TxRollback{},
			&codec.TxRollback{},
			codec.ClassTx,
			codec.MethodTxRollback,
		},
		{
			"TxRollbackOk",
			&codec.TxRollbackOk{},
			&codec.TxRollbackOk{},
			codec.ClassTx,
			codec.MethodTxRollbackOk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBuf := new(bytes.Buffer)
			if err := tt.method.Write(payloadBuf); err != nil {
				t.Fatalf("Write method failed for %s: %v", tt.name, err)
			}

			frame := &codec.Frame{
				Type:    codec.FrameMethod,
				Channel: 0,
				Payload: payloadBuf.Bytes(),
			}

			fullFrameBuf := new(bytes.Buffer)
			if err := frame.WriteFrame(fullFrameBuf); err != nil {
				t.Fatalf("WriteFrame failed for %s: %v", tt.name, err)
			}

			readFrame, err := codec.ReadFrame(fullFrameBuf)
			if err != nil {
				t.Fatalf("ReadFrame failed for %s: %v", tt.name, err)
			}

			decodedMethod, err := readFrame.Decode()
			if err != nil {
				t.Fatalf("Decode method failed for %s: %v", tt.name, err)
			}

			if !reflect.DeepEqual(decodedMethod, tt.expected) {
				t.Errorf("Decoded method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, decodedMethod)
			}

			directReadMethod := reflect.New(reflect.TypeOf(tt.expected).Elem()).Interface().(interface {
				Read(*bytes.Reader) error
			})
			directPayloadReader := bytes.NewReader(payloadBuf.Bytes()[4:])
			if err := directReadMethod.Read(directPayloadReader); err != nil {
				t.Fatalf("Direct Read method failed for %s: %v", tt.name, err)
			}
			if !reflect.DeepEqual(directReadMethod, tt.expected) {
				t.Errorf("Direct Read method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, directReadMethod)
			}
		})
	}
}

func TestConfirmMethods(t *testing.T) {
	tests := []struct {
		name   string
		method interface {
			Read(*bytes.Reader) error
			Write(io.Writer) error
		}
		expected interface{}
		classID  uint16
		methodID uint16
	}{
		{
			"ConfirmSelect",
			&codec.ConfirmSelect{NoWait: true},
			&codec.ConfirmSelect{NoWait: true},
			codec.ClassConfirm,
			codec.MethodConfirmSelect,
		},
		{
			"ConfirmSelectOk",
			&codec.ConfirmSelectOk{},
			&codec.ConfirmSelectOk{},
			codec.ClassConfirm,
			codec.MethodConfirmSelectOk,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBuf := new(bytes.Buffer)
			if err := tt.method.Write(payloadBuf); err != nil {
				t.Fatalf("Write method failed for %s: %v", tt.name, err)
			}

			frame := &codec.Frame{
				Type:    codec.FrameMethod,
				Channel: 0,
				Payload: payloadBuf.Bytes(),
			}

			fullFrameBuf := new(bytes.Buffer)
			if err := frame.WriteFrame(fullFrameBuf); err != nil {
				t.Fatalf("WriteFrame failed for %s: %v", tt.name, err)
			}

			readFrame, err := codec.ReadFrame(fullFrameBuf)
			if err != nil {
				t.Fatalf("ReadFrame failed for %s: %v", tt.name, err)
			}

			decodedMethod, err := readFrame.Decode()
			if err != nil {
				t.Fatalf("Decode method failed for %s: %v", tt.name, err)
			}

			if !reflect.DeepEqual(decodedMethod, tt.expected) {
				t.Errorf("Decoded method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, decodedMethod)
			}

			directReadMethod := reflect.New(reflect.TypeOf(tt.expected).Elem()).Interface().(interface {
				Read(*bytes.Reader) error
			})
			directPayloadReader := bytes.NewReader(payloadBuf.Bytes()[4:])
			if err := directReadMethod.Read(directPayloadReader); err != nil {
				t.Fatalf("Direct Read method failed for %s: %v", tt.name, err)
			}
			if !reflect.DeepEqual(directReadMethod, tt.expected) {
				t.Errorf("Direct Read method mismatch for %s:\nExpected: %+v\nGot: %+v", tt.name, tt.expected, directReadMethod)
			}
		})
	}
}

func TestBasicProperties(t *testing.T) {
	props := &codec.BasicProperties{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Headers: map[string]interface{}{
			"x-custom": "value",
			"x-retry":  int32(3),
		},
		DeliveryMode:  2,
		Priority:      5,
		CorrelationID: "corr-123",
		ReplyTo:       "reply-queue",
		Expiration:    "60000",
		MessageID:     "msg-456",
		Timestamp:     1234567890,
		Type:          "test.message",
		UserID:        "user-1",
		AppID:         "test-app",
		ClusterID:     "cluster-1",
	}

	buf := new(bytes.Buffer)
	flags := props.Flags()
	if err := codec.WriteShort(buf, flags); err != nil {
		t.Fatalf("WriteShort flags failed: %v", err)
	}
	if err := props.Write(buf); err != nil {
		t.Fatalf("Write properties failed: %v", err)
	}

	readBuf := bytes.NewReader(buf.Bytes())
	readFlags, err := codec.ReadShort(readBuf)
	if err != nil {
		t.Fatalf("ReadShort flags failed: %v", err)
	}
	if readFlags != flags {
		t.Fatalf("Flags mismatch: expected %d, got %d", flags, readFlags)
	}

	readProps := &codec.BasicProperties{}
	if err := readProps.Read(readBuf, readFlags); err != nil {
		t.Fatalf("Read properties failed: %v", err)
	}

	if !reflect.DeepEqual(readProps, props) {
		t.Errorf("Properties mismatch:\nExpected: %+v\nGot: %+v", props, readProps)
	}
}

func TestContentHeaderFrame(t *testing.T) {
	props := &codec.BasicProperties{
		ContentType:     "text/plain",
		ContentEncoding: "utf-8",
		Headers: map[string]interface{}{
			"x-test": "header-value",
		},
		DeliveryMode:  1,
		Priority:      3,
		CorrelationID: "correlation-id",
		ReplyTo:       "reply-to-queue",
		Expiration:    "30000",
		MessageID:     "message-id-789",
		Timestamp:     9876543210,
		Type:          "test.type",
		UserID:        "test-user",
		AppID:         "test-application",
		ClusterID:     "test-cluster",
	}

	header := &codec.ContentHeader{
		ClassID:    codec.ClassBasic,
		Weight:     0,
		BodySize:   1024,
		Flags:      props.Flags(),
		Properties: *props,
	}

	payloadBuf := new(bytes.Buffer)
	if err := header.WriteContentHeader(payloadBuf); err != nil {
		t.Fatalf("WriteContentHeader failed: %v", err)
	}

	frame := &codec.Frame{
		Type:    codec.FrameHeader,
		Channel: 1,
		Payload: payloadBuf.Bytes(),
	}

	fullFrameBuf := new(bytes.Buffer)
	if err := frame.WriteFrame(fullFrameBuf); err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	readFrame, err := codec.ReadFrame(fullFrameBuf)
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if readFrame.Type != codec.FrameHeader {
		t.Fatalf("Expected frame type %d, got %d", codec.FrameHeader, readFrame.Type)
	}
	if readFrame.Channel != 1 {
		t.Fatalf("Expected channel 1, got %d", readFrame.Channel)
	}

	decodedHeader, err := readFrame.Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	readHeader, ok := decodedHeader.(*codec.ContentHeader)
	if !ok {
		t.Fatalf("Expected *ContentHeader, got %T", decodedHeader)
	}

	if readHeader.ClassID != header.ClassID {
		t.Errorf("ClassID mismatch: expected %d, got %d", header.ClassID, readHeader.ClassID)
	}
	if readHeader.Weight != header.Weight {
		t.Errorf("Weight mismatch: expected %d, got %d", header.Weight, readHeader.Weight)
	}
	if readHeader.BodySize != header.BodySize {
		t.Errorf("BodySize mismatch: expected %d, got %d", header.BodySize, readHeader.BodySize)
	}
	if readHeader.Flags != header.Flags {
		t.Errorf("Flags mismatch: expected %d, got %d", header.Flags, readHeader.Flags)
	}

	if !reflect.DeepEqual(readHeader.Properties, header.Properties) {
		t.Errorf("Properties mismatch:\nExpected: %+v\nGot: %+v", header.Properties, readHeader.Properties)
	}
}
