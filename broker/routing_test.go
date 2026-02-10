// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve_PubSub(t *testing.T) {
	r := NewRoutingResolver()
	result := r.Resolve("sensors/temperature")
	assert.Equal(t, RoutePubSub, result.Kind)
	assert.Empty(t, result.QueueName)
	assert.Empty(t, result.Pattern)
}

func TestResolve_Queue(t *testing.T) {
	r := NewRoutingResolver()
	tests := []struct {
		topic     string
		queueName string
		pattern   string
	}{
		{"$queue/tasks", "tasks", ""},
		{"$queue/tasks/images", "tasks", "images"},
		{"$queue/tasks/images/#", "tasks", "images/#"},
		{"$queue/orders/region/eu", "orders", "region/eu"},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			result := r.Resolve(tt.topic)
			assert.Equal(t, RouteQueue, result.Kind)
			assert.Equal(t, tt.queueName, result.QueueName)
			assert.Equal(t, tt.pattern, result.Pattern)
			assert.Equal(t, tt.topic, result.PublishTopic)
		})
	}
}

func TestResolve_QueueAck(t *testing.T) {
	r := NewRoutingResolver()
	tests := []struct {
		topic     string
		ackKind   AckKind
		queueName string
	}{
		{"$queue/orders/$ack", AckAccept, "orders"},
		{"$queue/orders/images/$nack", AckNack, "orders"},
		{"$queue/orders/$reject", AckReject, "orders"},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			result := r.Resolve(tt.topic)
			assert.Equal(t, RouteQueueAck, result.Kind)
			assert.Equal(t, tt.ackKind, result.AckKind)
			assert.Equal(t, tt.queueName, result.QueueName)
		})
	}
}

func TestResolve_QueueCommit(t *testing.T) {
	r := NewRoutingResolver()
	result := r.Resolve("$queue/mystream/$commit")
	assert.Equal(t, RouteQueueCommit, result.Kind)
	assert.Equal(t, "mystream", result.QueueName)
}

func TestResolve_EmptyQueuePrefix(t *testing.T) {
	r := NewRoutingResolver()
	result := r.Resolve("$queue/")
	assert.Equal(t, RouteQueue, result.Kind)
	assert.Empty(t, result.QueueName)
}

func TestResolve_QueueAckEmptyName(t *testing.T) {
	r := NewRoutingResolver()
	result := r.Resolve("$queue/$ack")
	assert.Equal(t, RouteQueueAck, result.Kind)
	assert.Empty(t, result.QueueName)
}

func TestIsQueueTopic(t *testing.T) {
	r := NewRoutingResolver()
	assert.True(t, r.IsQueueTopic("$queue/test"))
	assert.True(t, r.IsQueueTopic("$queue/"))
	assert.False(t, r.IsQueueTopic("sensors/temp"))
	assert.False(t, r.IsQueueTopic(""))
}

func TestQueueTopic(t *testing.T) {
	r := NewRoutingResolver()
	assert.Equal(t, "$queue/tasks", r.QueueTopic("tasks"))
	assert.Equal(t, "$queue/tasks/images", r.QueueTopic("tasks", "images"))
	assert.Equal(t, "$queue/tasks/#", r.QueueTopic("tasks", "#"))
	// Empty part is skipped
	assert.Equal(t, "$queue/tasks", r.QueueTopic("tasks", ""))
}

func TestParseQueueFilter(t *testing.T) {
	tests := []struct {
		filter    string
		queueName string
		pattern   string
	}{
		{"$queue/tasks", "tasks", ""},
		{"$queue/tasks/images", "tasks", "images"},
		{"$queue/tasks/images/#", "tasks", "images/#"},
		{"$queue/", "", ""},
		{"sensors/temp", "", ""},
		{"", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.filter, func(t *testing.T) {
			qn, pat := ParseQueueFilter(tt.filter)
			assert.Equal(t, tt.queueName, qn)
			assert.Equal(t, tt.pattern, pat)
		})
	}
}

func TestDeliveryTargetFunc(t *testing.T) {
	// Test that DeliveryTargetFunc in queue package would satisfy the interface.
	// Here we just test the ParseQueueFilter round-trip with QueueTopic.
	r := NewRoutingResolver()
	topic := r.QueueTopic("orders", "region/eu")
	require.Equal(t, "$queue/orders/region/eu", topic)
	qn, pat := ParseQueueFilter(topic)
	assert.Equal(t, "orders", qn)
	assert.Equal(t, "region/eu", pat)
}
