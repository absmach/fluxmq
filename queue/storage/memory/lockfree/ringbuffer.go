// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package lockfree

import (
	"sync/atomic"

	"github.com/absmach/fluxmq/queue/storage"
)

// RingBuffer is a lock-free SPSC (Single-Producer-Single-Consumer) ring buffer.
// It uses atomic operations for synchronization instead of mutexes, providing
// better performance and predictable latency.
//
// Key properties:
// - Single producer (enqueue operations)
// - Single consumer (dequeue operations)
// - Fixed capacity (power of 2 for fast modulo)
// - Cache-line padding to prevent false sharing
// - Zero-copy (stores messages by value)
type RingBuffer struct {
	buffer   []storage.Message
	capacity uint64
	mask     uint64 // capacity - 1, for fast modulo

	// Cache-line padding to prevent false sharing
	// Modern CPUs have 64-byte cache lines = 8 uint64s
	// We separate head and tail by full cache lines

	head      uint64    // Consumer position (read index)
	_padding1 [7]uint64 // Padding to fill cache line

	tail      uint64    // Producer position (write index)
	_padding2 [7]uint64 // Padding to fill cache line
}

// NewRingBuffer creates a new lock-free ring buffer with the specified capacity.
// Capacity must be a power of 2 for efficient modulo operations.
func NewRingBuffer(capacity uint64) *RingBuffer {
	// Ensure capacity is power of 2
	if capacity == 0 || (capacity&(capacity-1)) != 0 {
		// Round up to next power of 2
		capacity = roundUpPowerOf2(capacity)
	}

	return &RingBuffer{
		buffer:   make([]storage.Message, capacity),
		capacity: capacity,
		mask:     capacity - 1,
	}
}

// Enqueue adds a message to the ring buffer (producer operation).
// Returns false if the buffer is full.
//
// This is lock-free because:
// - Only one producer calls this
// - Uses atomic loads/stores for synchronization
// - No CAS loops needed (SPSC guarantees)
func (rb *RingBuffer) Enqueue(msg storage.Message) bool {
	// Load current positions
	tail := atomic.LoadUint64(&rb.tail)
	head := atomic.LoadUint64(&rb.head)

	// Check if buffer is full
	// We keep one slot empty to distinguish full from empty
	// So max fill level is capacity - 1
	if tail-head >= rb.capacity-1 {
		return false
	}

	// Write message to buffer
	// Safe: single producer, no race on this slot
	idx := tail & rb.mask
	rb.buffer[idx] = msg

	atomic.StoreUint64(&rb.tail, tail+1)

	return true
}

// Dequeue removes and returns a message from the ring buffer (consumer operation).
// Returns (message, true) if successful, (zero, false) if empty.
//
// This is lock-free because:
// - Only one consumer calls this
// - Uses atomic loads/stores for synchronization
// - No CAS loops needed (SPSC guarantees)
func (rb *RingBuffer) Dequeue() (storage.Message, bool) {
	// Load current positions
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	// Check if buffer is empty
	if head >= tail {
		return storage.Message{}, false
	}

	// Read message from buffer
	// Safe: single consumer, no race on this slot
	idx := head & rb.mask
	msg := rb.buffer[idx]

	atomic.StoreUint64(&rb.head, head+1)

	return msg, true
}

// DequeueBatch removes up to 'limit' messages from the ring buffer.
// Returns a slice of messages (may be less than limit if buffer doesn't have enough).
//
// This provides better performance than calling Dequeue() in a loop because:
// - Fewer atomic operations (one head update for entire batch)
// - Better cache utilization
// - Reduced function call overhead
func (rb *RingBuffer) DequeueBatch(limit int) []storage.Message {
	if limit <= 0 {
		return nil
	}

	// Load current positions
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)

	// Calculate available messages
	available := tail - head
	if available == 0 {
		return nil
	}

	// Limit to requested amount
	count := available
	if uint64(limit) < count {
		count = uint64(limit)
	}

	// Pre-allocate result slice
	messages := make([]storage.Message, count)

	// Copy messages from ring buffer
	for i := uint64(0); i < count; i++ {
		idx := (head + i) & rb.mask
		messages[i] = rb.buffer[idx]
	}

	// Update head position atomically
	atomic.StoreUint64(&rb.head, head+count)

	return messages
}

// Len returns the approximate number of messages in the buffer.
// This is approximate because head/tail may change between the two reads.
// Useful for monitoring, not for synchronization.
func (rb *RingBuffer) Len() int {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	return int(tail - head)
}

// Cap returns the capacity of the ring buffer.
func (rb *RingBuffer) Cap() int {
	return int(rb.capacity)
}

// IsFull returns true if the buffer is full (approximate).
func (rb *RingBuffer) IsFull() bool {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	return tail-head >= rb.capacity-1
}

// IsEmpty returns true if the buffer is empty (approximate).
func (rb *RingBuffer) IsEmpty() bool {
	head := atomic.LoadUint64(&rb.head)
	tail := atomic.LoadUint64(&rb.tail)
	return head >= tail
}

// Reset clears the ring buffer by resetting head and tail to 0.
// NOT thread-safe - should only be called when no concurrent access is happening.
func (rb *RingBuffer) Reset() {
	atomic.StoreUint64(&rb.head, 0)
	atomic.StoreUint64(&rb.tail, 0)
}

// roundUpPowerOf2 rounds up to the next power of 2.
func roundUpPowerOf2(n uint64) uint64 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}
