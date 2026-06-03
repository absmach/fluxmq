// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/absmach/fluxmq/amqp1/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPropName  = "name"
	testQueueName = "test-queue"
)

func TestManagementHandlerNoQueueManager(t *testing.T) {
	b := New(nil, nil, nil)
	h := newManagementHandler(b)

	req := &message.Message{
		Properties: &message.Properties{MessageID: "req-1"},
		ApplicationProperties: map[string]any{
			testKeyOperation: opCreate,
			testKeyType:      entityTypeQueue,
			testPropName:     testQueueName,
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
			testKeyOperation: opCreate,
			testKeyType:      "exchange",
			testPropName:     "test",
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
			testKeyOperation: "PURGE",
			testKeyType:      entityTypeQueue,
			testPropName:     "test",
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
			testKeyOperation: "READ",
			testKeyType:      entityTypeQueue,
			testPropName:     "nonexistent",
		},
	}

	resp := h.handleRequest(req)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Properties)
	assert.Equal(t, "msg-123", resp.Properties.CorrelationID)
	assert.Equal(t, "$management/reply/0", resp.Properties.To)
}
