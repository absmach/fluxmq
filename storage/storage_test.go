// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package storage_test

import (
	"encoding/json"
	"testing"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

// TestMessage_PayloadBufNotSerialized_PayloadIs guards finding #1: the zero-copy
// PayloadBuf is excluded from JSON (json:"-"), so persistence (badger) relies on
// the Payload byte slice. A message carrying only a PayloadBuf loses its payload
// unless Payload is materialised first.
func TestMessage_PayloadBufNotSerialized_PayloadIs(t *testing.T) {
	msg := &storage.Message{}
	msg.SetPayloadFromBuffer(core.GetBufferWithData([]byte("hello")))
	require.Equal(t, "hello", string(msg.GetPayload()))

	// Round-trip without materialising Payload: payload is lost.
	data, err := json.Marshal(msg)
	require.NoError(t, err)
	var restored storage.Message
	require.NoError(t, json.Unmarshal(data, &restored))
	require.Nil(t, restored.PayloadBuf, "PayloadBuf must not be serialized")
	require.Empty(t, restored.GetPayload())

	// Materialising Payload (as the persistence paths now do) preserves it.
	msg.Payload = append([]byte(nil), msg.GetPayload()...)
	data, err = json.Marshal(msg)
	require.NoError(t, err)
	var restored2 storage.Message
	require.NoError(t, json.Unmarshal(data, &restored2))
	require.Equal(t, "hello", string(restored2.GetPayload()))
}
