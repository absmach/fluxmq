// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package message

import (
	"testing"

	"github.com/absmach/fluxmq/amqp1/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageDataOnly(t *testing.T) {
	m := &Message{
		Data: [][]byte{[]byte("hello world")},
	}

	encoded, err := m.Encode()
	require.NoError(t, err)

	decoded, err := Decode(encoded)
	require.NoError(t, err)
	require.Len(t, decoded.Data, 1)
	assert.Equal(t, []byte("hello world"), decoded.Data[0])
}

func TestMessageWithHeader(t *testing.T) {
	m := &Message{
		Header: &Header{
			Durable:  true,
			Priority: 4,
			TTL:      60000,
		},
		Data: [][]byte{[]byte("payload")},
	}

	encoded, err := m.Encode()
	require.NoError(t, err)

	decoded, err := Decode(encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded.Header)
	assert.True(t, decoded.Header.Durable)
	assert.Equal(t, uint8(4), decoded.Header.Priority)
	assert.Equal(t, uint32(60000), decoded.Header.TTL)
}

func TestMessageWithProperties(t *testing.T) {
	m := &Message{
		Properties: &Properties{
			MessageID:   "msg-1",
			To:          "test/topic",
			Subject:     "test subject",
			ContentType: "application/json",
		},
		Data: [][]byte{[]byte(`{"key":"value"}`)},
	}

	encoded, err := m.Encode()
	require.NoError(t, err)

	decoded, err := Decode(encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded.Properties)
	assert.Equal(t, "msg-1", decoded.Properties.MessageID)
	assert.Equal(t, "test/topic", decoded.Properties.To)
	assert.Equal(t, "test subject", decoded.Properties.Subject)
	assert.Equal(t, types.Symbol("application/json"), decoded.Properties.ContentType)
}

func TestMessageWithAppProperties(t *testing.T) {
	m := &Message{
		ApplicationProperties: map[string]any{
			"queue":      "orders",
			"message-id": "id-123",
		},
		Data: [][]byte{[]byte("data")},
	}

	encoded, err := m.Encode()
	require.NoError(t, err)

	decoded, err := Decode(encoded)
	require.NoError(t, err)
	require.NotNil(t, decoded.ApplicationProperties)
	assert.Equal(t, "orders", decoded.ApplicationProperties["queue"])
	assert.Equal(t, "id-123", decoded.ApplicationProperties["message-id"])
}

func TestMessageWithAnnotations(t *testing.T) {
	m := &Message{
		MessageAnnotations: map[types.Symbol]any{
			"x-opt-routing-key": "test.routing",
		},
		Data: [][]byte{[]byte("data")},
	}

	encoded, err := m.Encode()
	require.NoError(t, err)

	decoded, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, "test.routing", decoded.MessageAnnotations["x-opt-routing-key"])
}

func TestMessageMultipleData(t *testing.T) {
	m := &Message{
		Data: [][]byte{[]byte("part1"), []byte("part2")},
	}

	encoded, err := m.Encode()
	require.NoError(t, err)

	decoded, err := Decode(encoded)
	require.NoError(t, err)
	require.Len(t, decoded.Data, 2)
	assert.Equal(t, []byte("part1"), decoded.Data[0])
	assert.Equal(t, []byte("part2"), decoded.Data[1])
}

func TestMessageFull(t *testing.T) {
	m := &Message{
		Header: &Header{Durable: true, Priority: 4},
		Properties: &Properties{
			MessageID: "full-msg",
			To:        "$queue/orders",
		},
		ApplicationProperties: map[string]any{
			"type": "order.created",
		},
		Data: [][]byte{[]byte(`{"orderId": 123}`)},
	}

	encoded, err := m.Encode()
	require.NoError(t, err)

	decoded, err := Decode(encoded)
	require.NoError(t, err)
	assert.True(t, decoded.Header.Durable)
	assert.Equal(t, "full-msg", decoded.Properties.MessageID)
	assert.Equal(t, "$queue/orders", decoded.Properties.To)
	assert.Equal(t, "order.created", decoded.ApplicationProperties["type"])
	assert.Equal(t, []byte(`{"orderId": 123}`), decoded.Data[0])
}
