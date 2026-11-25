// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"fmt"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A queue that never quite empties, as under steady load, must not keep the
// slots of items already taken out: its storage stays proportional to what
// it holds, not to everything that has passed through it.
func TestPendingQueueStorageStaysBounded(t *testing.T) {
	const capacity = 2
	q := newPendingQueue(capacity)
	require.True(t, q.push(pendingItem{}))

	for range 100_000 {
		require.True(t, q.push(pendingItem{}))
		_, ok := q.pop()
		require.True(t, ok)
	}

	assert.LessOrEqual(t, cap(q.items), 4*capacity, "storage grew with throughput")
	_, ok := q.pop()
	assert.True(t, ok, "the item kept across the cycles is still there")
	_, ok = q.pop()
	assert.False(t, ok)
}

func TestPendingQueueIsFIFOAndBounded(t *testing.T) {
	q := newPendingQueue(3)
	items := make([]*message.Envelope, 5)
	for i := range items {
		items[i] = message.New(fmt.Sprintf("t/%d", i), nil)
		t.Cleanup(func() { message.Release(items[i]) })
	}

	for _, msg := range items[:3] {
		require.True(t, q.push(pendingItem{msg: msg}))
	}
	assert.False(t, q.push(pendingItem{msg: items[3]}), "a full queue refuses more")

	got, ok := q.pop()
	require.True(t, ok)
	assert.Same(t, items[0], got.msg)
	require.True(t, q.push(pendingItem{msg: items[4]}), "a pop makes room")

	for _, want := range []*message.Envelope{items[1], items[2], items[4]} {
		got, ok := q.pop()
		require.True(t, ok)
		assert.Same(t, want, got.msg)
	}
	_, ok = q.pop()
	assert.False(t, ok)
}
