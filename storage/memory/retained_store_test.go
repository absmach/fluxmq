// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/require"
)

func TestRetainedStoreSet_WithPayloadBuffer(t *testing.T) {
	s := NewRetainedStore()
	ctx := context.Background()

	msg := &storage.Message{Topic: "devices/one"}
	msg.SetPayloadFromBytes([]byte("payload"))
	require.NoError(t, s.Set(ctx, "devices/one", msg))

	got, err := s.Get(ctx, "devices/one")
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), got.GetPayload())
}
