// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package badger

import (
	"context"
	"testing"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestRetainedStore_Set(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := message.NewDelivery(testTopic, []byte("retained message"), 1, true)

	err := store.Set(ctx, testTopic, msg)
	require.NoError(t, err)

	retrieved, err := store.Get(ctx, testTopic)
	require.NoError(t, err)
	assert.Equal(t, msg.Topic, retrieved.Topic)
	assert.Equal(t, msg.PayloadBytes(), retrieved.PayloadBytes())
	assert.Equal(t, msg.Broker.Delivery.QoS, retrieved.Broker.Delivery.QoS)
}

func TestRetainedStore_SetWithPayloadBuffer(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := message.NewDelivery(testTopic, []byte("buffered retained"), 1, true)
	defer msg.ReleasePayload()

	err := store.Set(ctx, testTopic, msg)
	require.NoError(t, err)

	retrieved, err := store.Get(ctx, testTopic)
	require.NoError(t, err)
	assert.Equal(t, []byte("buffered retained"), retrieved.PayloadBytes())
	assert.Equal(t, byte(1), retrieved.Broker.Delivery.QoS)
}

func TestRetainedStore_SetEmptyPayload(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := message.NewDelivery(testTopic, []byte("initial message"), 1, true)

	err := store.Set(ctx, testTopic, msg)
	require.NoError(t, err)

	emptyMsg := message.NewDelivery(testTopic, nil, 0, true)
	err = store.Set(ctx, testTopic, emptyMsg)
	require.NoError(t, err)

	_, err = store.Get(ctx, testTopic)
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestRetainedStore_Get(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := message.NewDelivery("sensor/temperature", []byte("25.5"), 2, true)

	err := store.Set(ctx, "sensor/temperature", msg)
	require.NoError(t, err)

	retrieved, err := store.Get(ctx, "sensor/temperature")
	require.NoError(t, err)
	assert.Equal(t, "sensor/temperature", retrieved.Topic)
	assert.Equal(t, []byte("25.5"), retrieved.PayloadBytes())
	assert.Equal(t, byte(2), retrieved.Broker.Delivery.QoS)
}

func TestRetainedStore_GetNotFound(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	_, err := store.Get(ctx, "nonexistent/topic")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestRetainedStore_Delete(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := message.NewDelivery("test/delete", []byte("to be deleted"), 1, true)

	err := store.Set(ctx, "test/delete", msg)
	require.NoError(t, err)

	err = store.Delete(ctx, "test/delete")
	require.NoError(t, err)

	_, err = store.Get(ctx, "test/delete")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestRetainedStore_MatchExact(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := message.NewDelivery("sensor/temp", []byte("20"), 1, true)

	err := store.Set(ctx, "sensor/temp", msg)
	require.NoError(t, err)

	matched, err := store.Match(ctx, "sensor/temp")
	require.NoError(t, err)
	assert.Len(t, matched, 1)
	assert.Equal(t, "sensor/temp", matched[0].Topic)
}

func TestRetainedStore_MatchSingleLevelWildcard(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	messages := []*message.Envelope{
		message.NewDelivery("sensor/temp/room1", []byte("20"), 1, true),
		message.NewDelivery("sensor/temp/room2", []byte("21"), 1, true),
		message.NewDelivery("sensor/humidity/room1", []byte("60"), 1, true),
	}

	for _, msg := range messages {
		err := store.Set(ctx, msg.Topic, msg)
		require.NoError(t, err)
	}

	matched, err := store.Match(ctx, "sensor/temp/+")
	require.NoError(t, err)
	assert.Len(t, matched, 2)
}

func TestRetainedStore_MatchMultiLevelWildcard(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	messages := []*message.Envelope{
		message.NewDelivery("sensor/temp/room1", []byte("20"), 1, true),
		message.NewDelivery("sensor/temp/room2", []byte("21"), 1, true),
		message.NewDelivery("sensor/humidity/room1", []byte("60"), 1, true),
		message.NewDelivery(testAlertsCritical, []byte("fire"), 2, true),
	}

	for _, msg := range messages {
		err := store.Set(ctx, msg.Topic, msg)
		require.NoError(t, err)
	}

	matched, err := store.Match(ctx, "sensor/#")
	require.NoError(t, err)
	assert.Len(t, matched, 3)

	matchAll, err := store.Match(ctx, "#")
	require.NoError(t, err)
	assert.Len(t, matchAll, 4)
}

func TestRetainedStore_MatchEmpty(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	matched, err := store.Match(ctx, "nonexistent/#")
	require.NoError(t, err)
	assert.Empty(t, matched)
}

func TestRetainedStore_ConcurrentSetGet(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func(id int) {
			msg := message.NewDelivery("concurrent/topic", []byte("message"), 1, true)
			err := store.Set(ctx, "concurrent/topic", msg)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		go func() {
			_, _ = store.Get(ctx, "concurrent/topic")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestRetainedStore_UpdateExisting(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	original := message.NewDelivery("test/update", []byte("original"), 1, true)

	err := store.Set(ctx, "test/update", original)
	require.NoError(t, err)

	updated := message.NewDelivery("test/update", []byte("updated"), 2, true)

	err = store.Set(ctx, "test/update", updated)
	require.NoError(t, err)

	retrieved, err := store.Get(ctx, "test/update")
	require.NoError(t, err)
	assert.Equal(t, []byte("updated"), retrieved.PayloadBytes())
	assert.Equal(t, byte(2), retrieved.Broker.Delivery.QoS)
}

func setupRetainedStore(t *testing.T) *RetainedStore {
	store, err := New(Config{Dir: t.TempDir()})
	require.NoError(t, err)

	return &RetainedStore{db: store.db}
}

func cleanupRetainedStore(t *testing.T, store *RetainedStore) {
	if store != nil && store.db != nil {
		store.db.Close() //nolint:errcheck // test teardown
	}
}
