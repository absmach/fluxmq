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
		{name: "amqp wildcard single", filter: "user.*.created", want: "user/+/created"},
		{name: "amqp wildcard multi", filter: "sensor.#", want: "sensor/#"},
		{name: "mixed", filter: "v1.*.sensor.#", want: "v1/+/sensor/#"},
		{name: "already mqtt", filter: "user/+/created", want: "user/+/created"},
		{name: "empty", filter: "", want: ""},
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
		{name: "amqp topic", topic: "orders.eu.created", want: "orders/eu/created"},
		{name: "single segment", topic: "orders", want: "orders"},
		{name: "already mqtt", topic: "orders/eu/created", want: "orders/eu/created"},
		{name: "empty", topic: "", want: ""},
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
		{name: "mqtt topic", topic: "orders/eu/created", want: "orders.eu.created"},
		{name: "single segment", topic: "orders", want: "orders"},
		{name: "already amqp", topic: "orders.eu.created", want: "orders.eu.created"},
		{name: "empty", topic: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := topics.MQTTTopicToAMQP(tt.topic); got != tt.want {
				t.Fatalf("MQTTTopicToAMQP(%q) = %q, want %q", tt.topic, got, tt.want)
			}
		})
	}
}
