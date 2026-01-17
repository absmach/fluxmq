// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"fmt"
	"os"
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageStore_StoreInflight(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	msg := &storage.Message{
		Topic:    "test/topic",
		Payload:  []byte("test payload"),
		QoS:      1,
		PacketID: 100,
	}

	key := "client-1/inflight/100"
	err := store.Store(key, msg)
	require.NoError(t, err)

	// Verify stored
	retrieved, err := store.Get(key)
	require.NoError(t, err)
	assert.Equal(t, msg.Topic, retrieved.Topic)
	assert.Equal(t, msg.Payload, retrieved.Payload)
	assert.Equal(t, msg.QoS, retrieved.QoS)
	assert.Equal(t, msg.PacketID, retrieved.PacketID)
}

func TestMessageStore_StoreQueue(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	msg := &storage.Message{
		Topic:   "test/topic",
		Payload: []byte("queued message"),
		QoS:     2,
	}

	key := "client-1/queue/0"
	err := store.Store(key, msg)
	require.NoError(t, err)

	retrieved, err := store.Get(key)
	require.NoError(t, err)
	assert.Equal(t, msg.Topic, retrieved.Topic)
	assert.Equal(t, msg.Payload, retrieved.Payload)
}

func TestMessageStore_GetNotFound(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	_, err := store.Get("nonexistent/key")
	assert.Error(t, err)
}

func TestMessageStore_Delete(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	msg := &storage.Message{
		Topic:   "test/topic",
		Payload: []byte("test"),
		QoS:     1,
	}

	key := "client-1/inflight/200"
	err := store.Store(key, msg)
	require.NoError(t, err)

	err = store.Delete(key)
	require.NoError(t, err)

	_, err = store.Get(key)
	assert.Error(t, err)
}

func TestMessageStore_ListByPrefix(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	// Store multiple messages with same prefix
	for i := 0; i < 5; i++ {
		msg := &storage.Message{
			Topic:   fmt.Sprintf("test/topic/%d", i),
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
			QoS:     1,
		}
		key := fmt.Sprintf("client-1/queue/%d", i)
		err := store.Store(key, msg)
		require.NoError(t, err)
	}

	// List by prefix
	messages, err := store.List("client-1/queue/")
	require.NoError(t, err)
	assert.Len(t, messages, 5)
}

func TestMessageStore_DeleteByPrefix(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	// Store multiple messages
	for i := 0; i < 3; i++ {
		msg := &storage.Message{
			Topic:   "test/topic",
			Payload: []byte("test"),
			QoS:     1,
		}
		key := fmt.Sprintf("client-2/inflight/%d", i)
		err := store.Store(key, msg)
		require.NoError(t, err)
	}

	// Delete by prefix
	err := store.DeleteByPrefix("client-2/inflight/")
	require.NoError(t, err)

	// Verify deleted
	messages, err := store.List("client-2/inflight/")
	require.NoError(t, err)
	assert.Empty(t, messages)
}

func TestMessageStore_ConcurrentOperations(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	done := make(chan bool, 20)

	// Concurrent writes
	for i := 0; i < 10; i++ {
		go func(id int) {
			msg := &storage.Message{
				Topic:   fmt.Sprintf("concurrent/topic/%d", id),
				Payload: []byte("test"),
				QoS:     1,
			}
			key := fmt.Sprintf("concurrent-client/inflight/%d", id)
			err := store.Store(key, msg)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func(id int) {
			key := fmt.Sprintf("concurrent-client/inflight/%d", id)
			_, _ = store.Get(key) // May or may not exist yet
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}
}

func TestMessageStore_QoSLevels(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	tests := []struct {
		name string
		qos  byte
	}{
		{"QoS 0", 0},
		{"QoS 1", 1},
		{"QoS 2", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &storage.Message{
				Topic:   "test/topic",
				Payload: []byte("test"),
				QoS:     tt.qos,
			}
			key := fmt.Sprintf("client/inflight/%d", tt.qos)
			err := store.Store(key, msg)
			require.NoError(t, err)

			retrieved, err := store.Get(key)
			require.NoError(t, err)
			assert.Equal(t, tt.qos, retrieved.QoS)
		})
	}
}

func TestMessageStore_LargePayload(t *testing.T) {
	store := setupMessageStore(t)
	defer cleanupMessageStore(t, store)

	// 1MB payload
	largePayload := make([]byte, 1024*1024)
	for i := range largePayload {
		largePayload[i] = byte(i % 256)
	}

	msg := &storage.Message{
		Topic:   "test/large",
		Payload: largePayload,
		QoS:     2,
	}

	key := "client/large/1"
	err := store.Store(key, msg)
	require.NoError(t, err)

	retrieved, err := store.Get(key)
	require.NoError(t, err)
	assert.Equal(t, len(largePayload), len(retrieved.Payload))
	assert.Equal(t, largePayload, retrieved.Payload)
}

// Helper functions

func setupMessageStore(t *testing.T) *MessageStore {
	tmpDir, err := os.MkdirTemp("", "badger-msg-test-*")
	require.NoError(t, err)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)

	return &MessageStore{db: store.db}
}

func cleanupMessageStore(t *testing.T, store *MessageStore) {
	if store != nil && store.db != nil {
		dir := store.db.Opts().Dir
		store.db.Close()
		os.RemoveAll(dir)
	}
}
