// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddClientIDProperty(t *testing.T) {
	tests := []struct {
		name          string
		initial       map[string]string
		clientID      string
		wantClientID  string
		wantPublisher string
		wantNil       bool
	}{
		{
			name:          "first publish sets both",
			initial:       nil,
			clientID:      "device-1",
			wantClientID:  "device-1",
			wantPublisher: "device-1",
		},
		{
			name: "republish overwrites client_id, preserves publisher",
			initial: map[string]string{
				ClientIDProperty:  "device-1",
				PublisherProperty: "device-1",
			},
			clientID:      "amqp091-rules-engine",
			wantClientID:  "amqp091-rules-engine",
			wantPublisher: "device-1",
		},
		{
			name: "third hop still preserves publisher",
			initial: map[string]string{
				ClientIDProperty:  "amqp091-rules-engine",
				PublisherProperty: "device-1",
			},
			clientID:      "amqp091-timescale-writer",
			wantClientID:  "amqp091-timescale-writer",
			wantPublisher: "device-1",
		},
		{
			name:     "empty clientID is no-op on nil props",
			initial:  nil,
			clientID: "",
			wantNil:  true,
		},
		{
			name: "empty clientID is no-op on existing props",
			initial: map[string]string{
				ClientIDProperty:  "device-1",
				PublisherProperty: "device-1",
			},
			clientID:      "",
			wantClientID:  "device-1",
			wantPublisher: "device-1",
		},
		{
			name:          "first publish with existing empty map",
			initial:       map[string]string{},
			clientID:      "coap:sensor-5",
			wantClientID:  "coap:sensor-5",
			wantPublisher: "coap:sensor-5",
		},
		{
			name: "preserves unrelated properties",
			initial: map[string]string{
				"content-type": "application/json",
			},
			clientID:      "device-1",
			wantClientID:  "device-1",
			wantPublisher: "device-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := AddClientIDProperty(tc.initial, tc.clientID)

			if tc.wantNil {
				assert.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			assert.Equal(t, tc.wantClientID, result[ClientIDProperty])
			assert.Equal(t, tc.wantPublisher, result[PublisherProperty])
		})
	}
}

func TestClientIDFromProperties(t *testing.T) {
	assert.Equal(t, "", ClientIDFromProperties(nil))
	assert.Equal(t, "", ClientIDFromProperties(map[string]string{}))
	assert.Equal(t, "device-1", ClientIDFromProperties(map[string]string{
		ClientIDProperty: "device-1",
	}))
}

func TestPublisherIDFromProperties(t *testing.T) {
	assert.Equal(t, "", PublisherIDFromProperties(nil))
	assert.Equal(t, "", PublisherIDFromProperties(map[string]string{}))
	assert.Equal(t, "device-1", PublisherIDFromProperties(map[string]string{
		PublisherProperty: "device-1",
	}))
}
