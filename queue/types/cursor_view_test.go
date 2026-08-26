// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The view is a copy. Writing to it must not reach the group, which is the
// whole reason the live pointer stopped being handed out.
func TestCursorViewIsACopy(t *testing.T) {
	group := NewConsumerGroupState("orders", "workers", "#")
	group.SetCursor(5, 3)

	view := group.CursorView()
	view.Cursor = 99
	view.Committed = 99

	assert.Equal(t, uint64(5), group.CursorView().Cursor, "the group must be unchanged")
	assert.Equal(t, uint64(3), group.CursorView().Committed)
}

// A group that has never had a cursor reports zero rather than allocating one,
// so reading is not a write.
func TestCursorViewOnGroupWithoutCursor(t *testing.T) {
	group := &ConsumerGroup{ID: "workers", QueueName: "orders"}

	assert.Equal(t, QueueCursor{}, group.CursorView())
	assert.Nil(t, group.Cursor, "reading must not create state")
}

func TestCursorMutators(t *testing.T) {
	tests := []struct {
		name    string
		start   QueueCursor
		apply   func(*ConsumerGroup)
		want    QueueCursor
		comment string
	}{
		{
			name:  "set cursor position leaves committed",
			start: QueueCursor{Cursor: 4, Committed: 2},
			apply: func(g *ConsumerGroup) { g.SetCursorPosition(9) },
			want:  QueueCursor{Cursor: 9, Committed: 2},
		},
		{
			name:  "set committed leaves cursor",
			start: QueueCursor{Cursor: 4, Committed: 2},
			apply: func(g *ConsumerGroup) { g.SetCommitted(3) },
			want:  QueueCursor{Cursor: 4, Committed: 3},
		},
		{
			name:  "advance committed pulls the cursor up",
			start: QueueCursor{Cursor: 4, Committed: 2},
			apply: func(g *ConsumerGroup) { g.AdvanceCommitted(7) },
			want:  QueueCursor{Cursor: 7, Committed: 7},
		},
		{
			name:  "advance committed behind the cursor leaves it",
			start: QueueCursor{Cursor: 8, Committed: 2},
			apply: func(g *ConsumerGroup) { g.AdvanceCommitted(5) },
			want:  QueueCursor{Cursor: 8, Committed: 5},
		},
		{
			name:  "set cursor writes both",
			start: QueueCursor{Cursor: 4, Committed: 2},
			apply: func(g *ConsumerGroup) { g.SetCursor(11, 10) },
			want:  QueueCursor{Cursor: 11, Committed: 10},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			group := NewConsumerGroupState("orders", "workers", "#")
			group.SetCursor(tc.start.Cursor, tc.start.Committed)

			tc.apply(group)

			assert.Equal(t, tc.want, group.CursorView())
		})
	}
}

// Cursor writes and reads must be safe together: the pointer used to escape,
// so every reader raced every advance.
func TestCursorAccessIsRaceFree(t *testing.T) {
	group := NewConsumerGroupState("orders", "workers", "#")

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 100 {
				switch i % 4 {
				case 0:
					group.SetCursorPosition(uint64(j))
				case 1:
					group.SetCommitted(uint64(j))
				case 2:
					group.AdvanceCommitted(uint64(j))
				default:
					_ = group.CursorView()
				}
			}
		}()
	}
	wg.Wait()

	require.NotNil(t, group.Cursor)
}
