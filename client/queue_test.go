// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueuePublishOptions(t *testing.T) {
	opts := &QueuePublishOptions{
		QueueName: "tasks/image",
		Payload:   []byte("test payload"),
		Properties: map[string]string{
			"priority": "high",
		},
		QoS: 1,
	}

	assert.Equal(t, "tasks/image", opts.QueueName)
	assert.Equal(t, []byte("test payload"), opts.Payload)
	assert.Equal(t, "high", opts.Properties["priority"])
	assert.Equal(t, byte(1), opts.QoS)
}

func TestPublishToQueueWithNilOptions(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, err := New(opts)
	require.NoError(t, err)
	client.state.set(StateConnected)

	err = client.PublishToQueueWithOptions(nil)
	assert.Equal(t, ErrInvalidMessage, err)
}

func TestQueueMessage_Ack(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, err := New(opts)
	require.NoError(t, err)
	client.state.set(StateConnected)

	msg := &Message{
		Topic:   "$queue/test",
		Payload: []byte("test"),
	}

	qm := &QueueMessage{
		Message:   msg,
		MessageID: "msg-123",
		client:    client,
		queueName: "test",
	}

	// Ack should fail when not connected (we're connected but no conn object)
	err = qm.Ack()
	assert.Error(t, err) // Will fail because conn is nil
}

func TestQueueMessage_NoClient(t *testing.T) {
	qm := &QueueMessage{
		Message:   &Message{Topic: "$queue/test"},
		MessageID: "msg-123",
		queueName: "test",
	}

	err := qm.Ack()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client not set")

	err = qm.Nack()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client not set")

	err = qm.Reject()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client not set")
}

func TestQueueSubscriptions_AddGetRemove(t *testing.T) {
	qs := newQueueSubscriptions()

	handler := func(msg *QueueMessage) {
		// Handler function
	}

	sub := &queueSubscription{
		queueName:     "test",
		consumerGroup: "workers",
		handler:       handler,
	}

	// Add subscription
	qs.add("$queue/test", sub)

	// Get subscription
	retrieved, ok := qs.get("$queue/test")
	assert.True(t, ok)
	assert.Equal(t, "test", retrieved.queueName)
	assert.Equal(t, "workers", retrieved.consumerGroup)
	assert.NotNil(t, retrieved.handler)

	// Get non-existent subscription
	_, ok = qs.get("$queue/nonexistent")
	assert.False(t, ok)

	// Remove subscription
	qs.remove("$queue/test")
	_, ok = qs.get("$queue/test")
	assert.False(t, ok)
}

func TestQueueSubscriptions_Concurrent(t *testing.T) {
	qs := newQueueSubscriptions()

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent adds
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			sub := &queueSubscription{
				queueName:     "test",
				consumerGroup: "workers",
			}
			qs.add("$queue/test", sub)
		}(i)
	}

	wg.Wait()

	// Should have one subscription (last write wins)
	sub, ok := qs.get("$queue/test")
	assert.True(t, ok)
	assert.NotNil(t, sub)
}

func TestIsQueueTopic(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		expected bool
	}{
		{"Queue topic", "$queue/test", true},
		{"Queue topic with path", "$queue/tasks/image", true},
		{"Queue ack topic", "$queue/test/$ack", true},
		{"Normal topic", "sensors/temperature", false},
		{"Share topic", "$share/workers/tasks", false},
		{"Short topic", "$queue", false},
		{"Empty topic", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isQueueTopic(tt.topic)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestClient_HandleQueueMessage(t *testing.T) {
	client := &Client{
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
		opts:      NewOptions(),
	}

	// Test with no handler registered - should fall through to OnMessageV2
	var receivedMsg *Message
	client.opts.OnMessageV2 = func(msg *Message) {
		receivedMsg = msg
	}

	msg := &Message{
		Topic:   "$queue/test",
		Payload: []byte("test payload"),
		UserProperties: map[string]string{
			"message-id": "msg-123",
			"group-id":   "workers",
			"offset":     "42",
		},
	}

	client.handleQueueMessage(msg)
	assert.NotNil(t, receivedMsg)
	assert.Equal(t, "$queue/test", receivedMsg.Topic)
}

func TestClient_HandleQueueMessage_WithHandler(t *testing.T) {
	client := &Client{
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
		opts:      NewOptions(),
	}

	var receivedQueueMsg *QueueMessage
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(msg *QueueMessage) {
		receivedQueueMsg = msg
		wg.Done()
	}

	// Register queue subscription
	client.queueSubs.add("$queue/test", &queueSubscription{
		queueName:     "test",
		consumerGroup: "workers",
		handler:       handler,
	})

	msg := &Message{
		Topic:   "$queue/test",
		Payload: []byte("test payload"),
		UserProperties: map[string]string{
			"message-id": "msg-123",
			"group-id":   "workers",
			"offset":     "42",
		},
	}

	client.handleQueueMessage(msg)

	// Wait for handler to be called
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Handler was called
	case <-time.After(1 * time.Second):
		t.Fatal("Handler was not called within timeout")
	}

	require.NotNil(t, receivedQueueMsg)
	assert.Equal(t, "msg-123", receivedQueueMsg.MessageID)
	assert.Equal(t, "workers", receivedQueueMsg.GroupID)
	assert.Equal(t, uint64(42), receivedQueueMsg.Offset)
	assert.Equal(t, uint64(42), receivedQueueMsg.Sequence)
	assert.Equal(t, "test", receivedQueueMsg.queueName)
	assert.Equal(t, client, receivedQueueMsg.client)
}

func TestClient_DeliverMessage_RouteToQueueHandler(t *testing.T) {
	client := &Client{
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
		opts:      NewOptions(),
	}

	var receivedQueueMsg *QueueMessage
	handler := func(msg *QueueMessage) {
		receivedQueueMsg = msg
	}

	client.queueSubs.add("$queue/test", &queueSubscription{
		queueName: "test",
		handler:   handler,
	})

	msg := &Message{
		Topic:          "$queue/test",
		Payload:        []byte("test"),
		UserProperties: map[string]string{"message-id": "msg-1"},
	}

	client.deliverMessage(msg)

	assert.NotNil(t, receivedQueueMsg)
	assert.Equal(t, "msg-1", receivedQueueMsg.MessageID)
}

func TestClient_DeliverMessage_OnMessageV2(t *testing.T) {
	client := &Client{
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
		opts:      NewOptions(),
	}

	var receivedMsg *Message
	client.opts.OnMessageV2 = func(msg *Message) {
		receivedMsg = msg
	}

	msg := &Message{
		Topic:   "sensors/temp",
		Payload: []byte("test"),
	}

	client.deliverMessage(msg)

	assert.NotNil(t, receivedMsg)
	assert.Equal(t, "sensors/temp", receivedMsg.Topic)
}

func TestClient_DeliverMessage_OnMessage_Fallback(t *testing.T) {
	client := &Client{
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
		opts:      NewOptions(),
	}

	var receivedTopic string
	var receivedPayload []byte
	client.opts.OnMessage = func(topic string, payload []byte, qos byte) {
		receivedTopic = topic
		receivedPayload = payload
	}

	msg := &Message{
		Topic:   "sensors/temp",
		Payload: []byte("test"),
	}

	client.deliverMessage(msg)

	assert.Equal(t, "sensors/temp", receivedTopic)
	assert.Equal(t, []byte("test"), receivedPayload)
}

func TestClient_DeliverMessage_OnMessageV2_TakesPrecedence(t *testing.T) {
	client := &Client{
		state:     newStateManager(),
		queueSubs: newQueueSubscriptions(),
		opts:      NewOptions(),
	}

	var v2Called bool
	var v1Called bool

	client.opts.OnMessageV2 = func(msg *Message) {
		v2Called = true
	}

	client.opts.OnMessage = func(topic string, payload []byte, qos byte) {
		v1Called = true
	}

	msg := &Message{
		Topic:   "sensors/temp",
		Payload: []byte("test"),
	}

	client.deliverMessage(msg)

	assert.True(t, v2Called, "OnMessageV2 should be called")
	assert.False(t, v1Called, "OnMessage should NOT be called when OnMessageV2 is set")
}

func TestQueueMessage_AckNackReject_Integration(t *testing.T) {
	// This is a basic test - full integration would require a real connection
	opts := NewOptions().SetClientID("test-client").SetProtocolVersion(5)
	client, err := New(opts)
	require.NoError(t, err)
	client.state.set(StateConnected)

	qm := &QueueMessage{
		Message: &Message{
			Topic:   "$queue/test",
			Payload: []byte("test"),
		},
		MessageID: "msg-123",
		client:    client,
		queueName: "test",
	}

	// These will fail because we don't have a real connection
	// But they should not panic and should return errors
	err = qm.Ack()
	assert.Error(t, err)

	err = qm.Nack()
	assert.Error(t, err)

	err = qm.Reject()
	assert.Error(t, err)
}
