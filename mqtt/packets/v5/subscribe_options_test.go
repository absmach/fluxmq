// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package v5

import (
	"bytes"
	"testing"

	"github.com/absmach/fluxmq/mqtt/codec"
	"github.com/stretchr/testify/require"
)

func TestSubOptionEncode_BoolPointersRespectValue(t *testing.T) {
	no := false
	opt := SubOption{
		Topic:             "sensors/temperature",
		MaxQoS:            1,
		NoLocal:           &no,
		RetainAsPublished: &no,
	}

	encoded := opt.Encode()
	flags := encoded[len(encoded)-1]
	require.Equal(t, byte(0x01), flags)
}

func TestSubOptionUnpack_InvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		flags byte
	}{
		{name: "invalid qos", flags: 0x03},
		{name: "invalid retain handling", flags: 0x30},
		{name: "reserved bit 6", flags: 0x40},
		{name: "reserved bit 7", flags: 0x80},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := append(codec.EncodeBytes([]byte("topic/one")), tc.flags)
			var opt SubOption
			err := opt.Unpack(bytes.NewReader(raw))
			require.Error(t, err)
		})
	}
}
