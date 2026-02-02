// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/absmach/fluxmq/amqp/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagementHandlerNoQueueManager(t *testing.T) {
	b := New(nil, nil, nil)
	h := newManagementHandler(b)

	req := &message.Message{
		Properties: &message.Properties{MessageID: "req-1"},
		ApplicationProperties: map[string]any{
			"operation": "CREATE",
			"type":      "queue",
			"name":      "test-queue",
		},
	}

	resp := h.handleRequest(req)
	require.NotNil(t, resp)

	code, _ := resp.ApplicationProperties["statusCode"].(int32)
	assert.Equal(t, statusError, code)
}

func TestManagementHandlerMissingProperties(t *testing.T) {
	b := New(nil, nil, nil)
	h := newManagementHandler(b)

	req := &message.Message{
		Properties: &message.Properties{},
	}

	resp := h.handleRequest(req)
	require.NotNil(t, resp)

	code, _ := resp.ApplicationProperties["statusCode"].(int32)
	assert.Equal(t, statusError, code)
}

func TestManagementHandlerUnsupportedType(t *testing.T) {
	b := New(nil, nil, nil)
	h := newManagementHandler(b)

	req := &message.Message{
		Properties: &message.Properties{},
		ApplicationProperties: map[string]any{
			"operation": "CREATE",
			"type":      "exchange",
			"name":      "test",
		},
	}

	resp := h.handleRequest(req)
	require.NotNil(t, resp)

	code, _ := resp.ApplicationProperties["statusCode"].(int32)
	assert.Equal(t, statusError, code)
	desc, _ := resp.ApplicationProperties["statusDescription"].(string)
	assert.Contains(t, desc, "unsupported type")
}

func TestManagementHandlerUnsupportedOperation(t *testing.T) {
	b := New(nil, nil, nil)
	h := newManagementHandler(b)

	req := &message.Message{
		Properties: &message.Properties{},
		ApplicationProperties: map[string]any{
			"operation": "PURGE",
			"type":      "queue",
			"name":      "test",
		},
	}

	resp := h.handleRequest(req)
	require.NotNil(t, resp)

	code, _ := resp.ApplicationProperties["statusCode"].(int32)
	assert.Equal(t, statusError, code)
}

func TestManagementResponseCorrelation(t *testing.T) {
	b := New(nil, nil, nil)
	h := newManagementHandler(b)

	req := &message.Message{
		Properties: &message.Properties{
			MessageID: "msg-123",
			ReplyTo:   "$management/reply/0",
		},
		ApplicationProperties: map[string]any{
			"operation": "READ",
			"type":      "queue",
			"name":      "nonexistent",
		},
	}

	resp := h.handleRequest(req)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Properties)
	assert.Equal(t, "msg-123", resp.Properties.CorrelationID)
	assert.Equal(t, "$management/reply/0", resp.Properties.To)
}
