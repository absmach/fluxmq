// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectValidate(t *testing.T) {
	tests := []struct {
		name    string
		connect *Connect
		wantErr error
	}{
		{
			name: "valid v3.1.1 connect",
			connect: &Connect{
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
			},
		},
		{
			name: "valid v3.1 connect",
			connect: &Connect{
				ProtocolName:    "MQIsdp",
				ProtocolVersion: 3,
			},
		},
		{
			name: "password without username",
			connect: &Connect{
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				PasswordFlag:    true,
			},
			wantErr: ErrInvalidConnectFlags,
		},
		{
			name: "reserved bit set",
			connect: &Connect{
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				ReservedBit:     1,
			},
			wantErr: ErrInvalidConnectFlags,
		},
		{
			name: "will qos without will flag",
			connect: &Connect{
				ProtocolName:    "MQTT",
				ProtocolVersion: 4,
				WillQoS:         1,
			},
			wantErr: ErrInvalidConnectFlags,
		},
		{
			name: "invalid protocol name for v4",
			connect: &Connect{
				ProtocolName:    "MQIsdp",
				ProtocolVersion: 4,
			},
			wantErr: ErrInvalidProtocolName,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.connect.Validate()
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
