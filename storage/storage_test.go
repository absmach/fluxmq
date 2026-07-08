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

func TestMessage_JSONMaterializesPayloadBuf(t *testing.T) {
	msg := &storage.Message{}
	msg.SetPayloadFromBuffer(core.GetBufferWithData([]byte("hello")))
	defer msg.ReleasePayload()
	require.Equal(t, "hello", string(msg.GetPayload()))

	data, err := json.Marshal(msg)
	require.NoError(t, err)
	var restored storage.Message
	require.NoError(t, json.Unmarshal(data, &restored))
	require.Nil(t, restored.PayloadBuf, "PayloadBuf must not be serialized")
	require.Equal(t, "hello", string(restored.GetPayload()))
	require.Equal(t, "hello", string(msg.GetPayload()), "marshal must not consume the live buffer")
}

func TestMessagePoolReuseClearsInflightMetadata(t *testing.T) {
	msg := storage.AcquireMessage()
	msg.InflightDirection = 1
	msg.InflightState = 1
	msg.Reset()
	require.Zero(t, msg.InflightDirection)
	require.Zero(t, msg.InflightState)
	storage.ReleaseMessage(msg)

	reused := storage.AcquireMessage()
	t.Cleanup(func() { storage.ReleaseMessage(reused) })
	require.Zero(t, reused.InflightDirection)
	require.Zero(t, reused.InflightState)
}
