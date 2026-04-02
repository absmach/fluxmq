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
		name              string
		initial           map[string]string
		clientID          string
		wantClientID      string
		wantExternalID    string
		wantExternalIDSet bool
		wantNil           bool
	}{
		{
			name:         "first publish sets client_id only",
			initial:      nil,
			clientID:     "device-1",
			wantClientID: "device-1",
		},
		{
			name: "republish overwrites client_id, preserves explicit external_id",
			initial: map[string]string{
				ClientIDProperty:   "device-1",
				ExternalIDProperty: "ext-123",
			},
			clientID:          "amqp091:rules-engine",
			wantClientID:      "amqp091:rules-engine",
			wantExternalID:    "ext-123",
			wantExternalIDSet: true,
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
				ClientIDProperty:   "device-1",
				ExternalIDProperty: "ext-123",
			},
			clientID:          "",
			wantClientID:      "device-1",
			wantExternalID:    "ext-123",
			wantExternalIDSet: true,
		},
		{
			name:         "first publish with existing empty map",
			initial:      map[string]string{},
			clientID:     "coap:sensor-5",
			wantClientID: "coap:sensor-5",
		},
		{
			name: "preserves unrelated properties",
			initial: map[string]string{
				"content-type": "application/json",
			},
			clientID:     "device-1",
			wantClientID: "device-1",
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

			externalID, ok := result[ExternalIDProperty]
			assert.Equal(t, tc.wantExternalIDSet, ok)
			if tc.wantExternalIDSet {
				assert.Equal(t, tc.wantExternalID, externalID)
			}
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

func TestExternalIDFromProperties(t *testing.T) {
	assert.Equal(t, "", ExternalIDFromProperties(nil))
	assert.Equal(t, "", ExternalIDFromProperties(map[string]string{}))
	assert.Equal(t, "device-1", ExternalIDFromProperties(map[string]string{
		ExternalIDProperty: "device-1",
	}))
}
