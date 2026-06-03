// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package topics_test

import (
	"testing"

	"github.com/absmach/fluxmq/topics"
)

func TestAMQPFilterToMQTT(t *testing.T) {
	tests := []struct {
		name   string
		filter string
		want   string
	}{
		{name: "amqp wildcard single", filter: "user.*.created", want: testUserPlusCreated},
		{name: "amqp wildcard multi", filter: "sensor.#", want: "sensor/#"},
		{name: "mixed", filter: "v1.*.sensor.#", want: "v1/+/sensor/#"},
		{name: "already mqtt", filter: testUserPlusCreated, want: testUserPlusCreated},
		{name: testEmpty, filter: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := topics.AMQPFilterToMQTT(tt.filter); got != tt.want {
				t.Fatalf("AMQPFilterToMQTT(%q) = %q, want %q", tt.filter, got, tt.want)
			}
		})
	}
}

func TestAMQPTopicToMQTT(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  string
	}{
		{name: "amqp topic", topic: testOrdersEuCreated, want: testOrdersEuMqtt},
		{name: "single segment", topic: testOrders, want: testOrders},
		{name: "already mqtt", topic: testOrdersEuMqtt, want: testOrdersEuMqtt},
		{name: testEmpty, topic: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := topics.AMQPTopicToMQTT(tt.topic); got != tt.want {
				t.Fatalf("AMQPTopicToMQTT(%q) = %q, want %q", tt.topic, got, tt.want)
			}
		})
	}
}

func TestMQTTTopicToAMQP(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  string
	}{
		{name: "mqtt topic", topic: testOrdersEuMqtt, want: testOrdersEuCreated},
		{name: "single segment", topic: testOrders, want: testOrders},
		{name: "already amqp", topic: testOrdersEuCreated, want: testOrdersEuCreated},
		{name: testEmpty, topic: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := topics.MQTTTopicToAMQP(tt.topic); got != tt.want {
				t.Fatalf("MQTTTopicToAMQP(%q) = %q, want %q", tt.topic, got, tt.want)
			}
		})
	}
}
