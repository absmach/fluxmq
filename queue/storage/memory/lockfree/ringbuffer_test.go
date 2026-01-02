// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package lockfree

import (
	"sync"
	"testing"
	"time"

	"github.com/absmach/mqtt/queue/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRingBuffer(t *testing.T) {
	tests := []struct {
		name     string
		capacity uint64
		expected uint64
		isFull   bool
	}{
		{"power of 2", 1024, 1024, false},
		{"non-power of 2", 1000, 1024, false},
		{"zero", 0, 1, true},    // Capacity 1 can hold 0 items (always full)
		{"one", 1, 1, true},      // Capacity 1 can hold 0 items (always full)
		{"small", 7, 8, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := NewRingBuffer(tt.capacity)
			assert.Equal(t, int(tt.expected), rb.Cap())
			assert.True(t, rb.IsEmpty())
			assert.Equal(t, tt.isFull, rb.IsFull())
		})
	}
}

func TestRingBuffer_EnqueueDequeue(t *testing.T) {
	rb := NewRingBuffer(4)

	// Enqueue a message
	msg1 := storage.QueueMessage{
		ID:      "msg-1",
		Payload: []byte("test payload"),
		Sequence: 1,
	}

	ok := rb.Enqueue(msg1)
	assert.True(t, ok)
	assert.Equal(t, 1, rb.Len())

	// Dequeue the message
	got, ok := rb.Dequeue()
	assert.True(t, ok)
	assert.Equal(t, msg1.ID, got.ID)
	assert.Equal(t, msg1.Sequence, got.Sequence)
	assert.Equal(t, 0, rb.Len())
}

func TestRingBuffer_FIFO(t *testing.T) {
	rb := NewRingBuffer(8)

	// Enqueue multiple messages
	for i := 1; i <= 5; i++ {
		msg := storage.QueueMessage{
			ID:       string(rune('0' + i)),
			Sequence: uint64(i),
		}
		ok := rb.Enqueue(msg)
		require.True(t, ok)
	}

	assert.Equal(t, 5, rb.Len())

	// Dequeue and verify FIFO order
	for i := 1; i <= 5; i++ {
		msg, ok := rb.Dequeue()
		require.True(t, ok)
		assert.Equal(t, uint64(i), msg.Sequence)
	}

	assert.True(t, rb.IsEmpty())
}

func TestRingBuffer_Full(t *testing.T) {
	rb := NewRingBuffer(4)

	// Fill the buffer (capacity - 1 because we keep one slot empty)
	for i := 0; i < 3; i++ {
		msg := storage.QueueMessage{Sequence: uint64(i)}
		ok := rb.Enqueue(msg)
		require.True(t, ok)
	}

	// Buffer should be full now
	assert.True(t, rb.IsFull())

	// Enqueue should fail
	msg := storage.QueueMessage{Sequence: 999}
	ok := rb.Enqueue(msg)
	assert.False(t, ok)

	// Dequeue one
	_, ok = rb.Dequeue()
	require.True(t, ok)

	// Now we should be able to enqueue again
	ok = rb.Enqueue(msg)
	assert.True(t, ok)
}

func TestRingBuffer_Empty(t *testing.T) {
	rb := NewRingBuffer(4)

	// Dequeue from empty buffer
	_, ok := rb.Dequeue()
	assert.False(t, ok)

	// Enqueue and dequeue
	msg := storage.QueueMessage{ID: "test"}
	rb.Enqueue(msg)
	rb.Dequeue()

	// Should be empty again
	_, ok = rb.Dequeue()
	assert.False(t, ok)
}

func TestRingBuffer_Wraparound(t *testing.T) {
	rb := NewRingBuffer(4)

	// Enqueue and dequeue multiple times to test wraparound
	for round := 0; round < 10; round++ {
		// Fill buffer
		for i := 0; i < 3; i++ {
			msg := storage.QueueMessage{
				Sequence: uint64(round*10 + i),
			}
			ok := rb.Enqueue(msg)
			require.True(t, ok, "round %d, enqueue %d", round, i)
		}

		// Empty buffer
		for i := 0; i < 3; i++ {
			msg, ok := rb.Dequeue()
			require.True(t, ok, "round %d, dequeue %d", round, i)
			assert.Equal(t, uint64(round*10+i), msg.Sequence)
		}

		assert.True(t, rb.IsEmpty())
	}
}

func TestRingBuffer_DequeueBatch(t *testing.T) {
	rb := NewRingBuffer(16)

	// Enqueue 10 messages
	for i := 1; i <= 10; i++ {
		msg := storage.QueueMessage{Sequence: uint64(i)}
		ok := rb.Enqueue(msg)
		require.True(t, ok)
	}

	// Dequeue batch of 5
	messages := rb.DequeueBatch(5)
	assert.Len(t, messages, 5)
	for i, msg := range messages {
		assert.Equal(t, uint64(i+1), msg.Sequence)
	}
	assert.Equal(t, 5, rb.Len())

	// Dequeue batch larger than available
	messages = rb.DequeueBatch(10)
	assert.Len(t, messages, 5)
	for i, msg := range messages {
		assert.Equal(t, uint64(i+6), msg.Sequence)
	}
	assert.True(t, rb.IsEmpty())
}

func TestRingBuffer_DequeueBatch_Empty(t *testing.T) {
	rb := NewRingBuffer(8)

	messages := rb.DequeueBatch(5)
	assert.Nil(t, messages)
}

func TestRingBuffer_DequeueBatch_Zero(t *testing.T) {
	rb := NewRingBuffer(8)
	rb.Enqueue(storage.QueueMessage{ID: "test"})

	messages := rb.DequeueBatch(0)
	assert.Nil(t, messages)
}

func TestRingBuffer_Reset(t *testing.T) {
	rb := NewRingBuffer(8)

	// Add some messages
	for i := 0; i < 5; i++ {
		rb.Enqueue(storage.QueueMessage{Sequence: uint64(i)})
	}
	assert.Equal(t, 5, rb.Len())

	// Reset
	rb.Reset()
	assert.True(t, rb.IsEmpty())
	assert.Equal(t, 0, rb.Len())

	// Should be able to use after reset
	msg := storage.QueueMessage{ID: "after-reset"}
	ok := rb.Enqueue(msg)
	assert.True(t, ok)
}

func TestRingBuffer_ConcurrentSPSC(t *testing.T) {
	rb := NewRingBuffer(1024)
	const numMessages = 100000

	var wg sync.WaitGroup
	wg.Add(2)

	// Producer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < numMessages; i++ {
			msg := storage.QueueMessage{
				ID:       "msg",
				Sequence: uint64(i),
			}
			// Retry if full (shouldn't happen with large buffer)
			for !rb.Enqueue(msg) {
				time.Sleep(time.Microsecond)
			}
		}
	}()

	// Consumer goroutine
	received := make([]uint64, 0, numMessages)
	go func() {
		defer wg.Done()
		for len(received) < numMessages {
			if msg, ok := rb.Dequeue(); ok {
				received = append(received, msg.Sequence)
			} else {
				// Buffer empty, yield
				time.Sleep(time.Microsecond)
			}
		}
	}()

	wg.Wait()

	// Verify all messages received in order
	assert.Len(t, received, numMessages)
	for i, seq := range received {
		assert.Equal(t, uint64(i), seq, "message %d out of order", i)
	}
}

func TestRingBuffer_ConcurrentBatchSPSC(t *testing.T) {
	rb := NewRingBuffer(2048)
	const numMessages = 100000
	const batchSize = 100

	var wg sync.WaitGroup
	wg.Add(2)

	// Producer goroutine
	go func() {
		defer wg.Done()
		for i := 0; i < numMessages; i++ {
			msg := storage.QueueMessage{
				ID:       "msg",
				Sequence: uint64(i),
			}
			for !rb.Enqueue(msg) {
				time.Sleep(time.Microsecond)
			}
		}
	}()

	// Consumer goroutine using batch dequeue
	received := make([]uint64, 0, numMessages)
	go func() {
		defer wg.Done()
		for len(received) < numMessages {
			messages := rb.DequeueBatch(batchSize)
			if len(messages) > 0 {
				for _, msg := range messages {
					received = append(received, msg.Sequence)
				}
			} else {
				time.Sleep(time.Microsecond)
			}
		}
	}()

	wg.Wait()

	// Verify all messages received in order
	assert.Len(t, received, numMessages)
	for i, seq := range received {
		assert.Equal(t, uint64(i), seq, "message %d out of order", i)
	}
}

// Benchmark tests
func BenchmarkRingBuffer_Enqueue(b *testing.B) {
	rb := NewRingBuffer(uint64(b.N))
	msg := storage.QueueMessage{
		ID:       "bench-msg",
		Payload:  make([]byte, 100),
		Sequence: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Enqueue(msg)
	}
}

func BenchmarkRingBuffer_Dequeue(b *testing.B) {
	rb := NewRingBuffer(uint64(b.N))
	msg := storage.QueueMessage{
		ID:       "bench-msg",
		Payload:  make([]byte, 100),
		Sequence: 1,
	}

	// Pre-fill buffer
	for i := 0; i < b.N; i++ {
		rb.Enqueue(msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Dequeue()
	}
}

func BenchmarkRingBuffer_DequeueBatch(b *testing.B) {
	rb := NewRingBuffer(102400)
	msg := storage.QueueMessage{
		ID:       "bench-msg",
		Payload:  make([]byte, 100),
		Sequence: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Pre-fill
		for j := 0; j < 100; j++ {
			rb.Enqueue(msg)
		}
		// Batch dequeue
		rb.DequeueBatch(100)
	}
}

func BenchmarkRingBuffer_SPSC_Parallel(b *testing.B) {
	rb := NewRingBuffer(4096)
	msg := storage.QueueMessage{
		ID:       "bench-msg",
		Payload:  make([]byte, 100),
		Sequence: 1,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Each goroutine alternates between enqueue and dequeue
			rb.Enqueue(msg)
			rb.Dequeue()
		}
	})
}
