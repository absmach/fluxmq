// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"container/heap"
	"sync"
	"time"

	"github.com/absmach/fluxmq/queue/types"
)

// PendingHeap is a min-heap of pending entries ordered by claim time.
// This allows O(1) access to the oldest pending entry for work stealing.
type PendingHeap struct {
	entries []*heapEntry
	index   map[uint64]int // Maps offset -> heap index
	mu      sync.RWMutex
}

type heapEntry struct {
	entry *types.PendingEntry
	index int // Index in the heap slice
}

// NewPendingHeap creates a new pending entry heap.
func NewPendingHeap() *PendingHeap {
	return &PendingHeap{
		entries: make([]*heapEntry, 0),
		index:   make(map[uint64]int),
	}
}

// Len returns the number of entries in the heap.
func (h *PendingHeap) Len() int {
	return len(h.entries)
}

// Less compares two entries by claim time (oldest first).
func (h *PendingHeap) Less(i, j int) bool {
	return h.entries[i].entry.ClaimedAt.Before(h.entries[j].entry.ClaimedAt)
}

// Swap swaps two entries in the heap.
func (h *PendingHeap) Swap(i, j int) {
	h.entries[i], h.entries[j] = h.entries[j], h.entries[i]
	h.entries[i].index = i
	h.entries[j].index = j

	// Update index map
	h.index[h.entries[i].entry.Offset] = i
	h.index[h.entries[j].entry.Offset] = j
}

// Push adds an entry to the heap.
func (h *PendingHeap) Push(x interface{}) {
	entry := x.(*types.PendingEntry)
	he := &heapEntry{
		entry: entry,
		index: len(h.entries),
	}
	h.entries = append(h.entries, he)
	h.index[entry.Offset] = he.index
}

// Pop removes and returns the oldest entry from the heap.
func (h *PendingHeap) Pop() interface{} {
	n := len(h.entries)
	if n == 0 {
		return nil
	}

	he := h.entries[n-1]
	h.entries = h.entries[:n-1]
	delete(h.index, he.entry.Offset)

	return he.entry
}

// Add adds a pending entry to the heap (thread-safe).
func (h *PendingHeap) Add(entry *types.PendingEntry) {
	h.mu.Lock()
	defer h.mu.Unlock()

	heap.Push(h, entry)
}

// Remove removes a specific entry from the heap by offset (thread-safe).
func (h *PendingHeap) Remove(offset uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	idx, ok := h.index[offset]
	if !ok {
		return false
	}

	heap.Remove(h, idx)
	return true
}

// Peek returns the oldest entry without removing it (thread-safe).
func (h *PendingHeap) Peek() *types.PendingEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.entries) == 0 {
		return nil
	}

	return h.entries[0].entry
}

// PopOldest removes and returns the oldest entry (thread-safe).
func (h *PendingHeap) PopOldest() *types.PendingEntry {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.entries) == 0 {
		return nil
	}

	entry := heap.Pop(h)
	if entry == nil {
		return nil
	}

	return entry.(*types.PendingEntry)
}

// GetStealable returns entries older than the visibility timeout.
func (h *PendingHeap) GetStealable(visibilityTimeout time.Duration, excludeConsumer string, limit int) []*types.PendingEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()

	cutoff := time.Now().Add(-visibilityTimeout)
	var result []*types.PendingEntry

	// Since heap is ordered by claim time, we can stop early
	for _, he := range h.entries {
		if he.entry.ClaimedAt.After(cutoff) {
			break // No more stealable entries
		}

		if he.entry.ConsumerID == excludeConsumer {
			continue
		}

		result = append(result, he.entry)
		if len(result) >= limit {
			break
		}
	}

	return result
}

// UpdateClaimTime updates the claim time for an entry and reheapifies.
func (h *PendingHeap) UpdateClaimTime(offset uint64, newTime time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	idx, ok := h.index[offset]
	if !ok {
		return false
	}

	h.entries[idx].entry.ClaimedAt = newTime
	heap.Fix(h, idx)
	return true
}

// Size returns the number of entries (thread-safe).
func (h *PendingHeap) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.entries)
}

// Clear removes all entries from the heap.
func (h *PendingHeap) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.entries = h.entries[:0]
	h.index = make(map[uint64]int)
}

// QueueHeapManager manages a single heap for queue-based consumer groups.
// There's only one log per queue.
type QueueHeapManager struct {
	heap *PendingHeap
	mu   sync.RWMutex
}

// NewQueueHeapManager creates a new queue heap manager.
func NewQueueHeapManager() *QueueHeapManager {
	return &QueueHeapManager{
		heap: NewPendingHeap(),
	}
}

// Get returns the heap.
func (m *QueueHeapManager) Get() *PendingHeap {
	return m.heap
}

// Add adds an entry to the heap.
func (m *QueueHeapManager) Add(entry *types.PendingEntry) {
	m.heap.Add(entry)
}

// Remove removes an entry from the heap.
func (m *QueueHeapManager) Remove(offset uint64) bool {
	return m.heap.Remove(offset)
}

// TotalSize returns total entries in the heap.
func (m *QueueHeapManager) TotalSize() int {
	return m.heap.Size()
}
