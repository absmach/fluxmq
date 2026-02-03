// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/queue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdapter_ReadBatch(t *testing.T) {
	dir := t.TempDir()
	adapter, err := NewAdapter(dir, DefaultAdapterConfig())
	require.NoError(t, err)
	defer adapter.Close()

	ctx := context.Background()

	msgs := []*types.Message{
		{ID: "1", Topic: "t", Payload: []byte("a")},
		{ID: "2", Topic: "t", Payload: []byte("b")},
		{ID: "3", Topic: "t", Payload: []byte("c")},
		{ID: "4", Topic: "t", Payload: []byte("d")},
	}

	_, err = adapter.AppendBatch(ctx, "q1", msgs[:3])
	require.NoError(t, err)
	_, err = adapter.Append(ctx, "q1", msgs[3])
	require.NoError(t, err)

	got, err := adapter.ReadBatch(ctx, "q1", 1, 2)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, []byte("b"), got[0].Payload)
	assert.Equal(t, []byte("c"), got[1].Payload)

	got, err = adapter.ReadBatch(ctx, "q1", 2, 10)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, []byte("c"), got[0].Payload)
	assert.Equal(t, []byte("d"), got[1].Payload)

	got, err = adapter.ReadBatch(ctx, "q1", 10, 10)
	require.NoError(t, err)
	assert.Len(t, got, 0)
}
