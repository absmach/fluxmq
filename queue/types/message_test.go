// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types_test

import (
	"encoding/json"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessage_IsExpired(t *testing.T) {
	tests := []struct {
		name      string
		expiresAt time.Time
		expected  bool
	}{
		{
			name:      "zero value is non-expiring",
			expiresAt: time.Time{},
			expected:  false,
		},
		{
			name:      "future expiry is not expired",
			expiresAt: time.Now().Add(time.Hour),
			expected:  false,
		},
		{
			name:      "past expiry is expired",
			expiresAt: time.Now().Add(-time.Second),
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &types.Message{ExpiresAt: tt.expiresAt}
			assert.Equal(t, tt.expected, msg.IsExpired())
		})
	}
}

func TestMessageJSONMaterializesPayloadBuf(t *testing.T) {
	msg := &types.Message{ID: "msg-1", Topic: "$queue/events"}
	msg.SetPayloadFromBuffer(core.GetBufferWithData([]byte("queued payload")))
	defer msg.ReleasePayload()

	data, err := json.Marshal(msg)
	require.NoError(t, err)
	require.NotContains(t, string(data), "PayloadBuf")

	var restored types.Message
	require.NoError(t, json.Unmarshal(data, &restored))
	require.Equal(t, "queued payload", string(restored.GetPayload()))
	require.Equal(t, "queued payload", string(msg.GetPayload()), "marshal must not consume the live buffer")
}

// TestMessageJSONValueMarshalPreservesPayloadBuf guards the value receiver on
// MarshalJSON: a non-addressable Message value (here a map entry) must still
// route through MarshalJSON rather than dropping the buffer-backed payload.
func TestMessageJSONValueMarshalPreservesPayloadBuf(t *testing.T) {
	msg := types.Message{ID: "msg-1", Topic: "$queue/events"}
	msg.SetPayloadFromBuffer(core.GetBufferWithData([]byte("queued payload")))
	defer msg.ReleasePayload()

	data, err := json.Marshal(map[string]types.Message{"m": msg})
	require.NoError(t, err)

	var restored map[string]types.Message
	require.NoError(t, json.Unmarshal(data, &restored))
	got := restored["m"]
	require.Equal(t, "queued payload", string(got.GetPayload()))
}
