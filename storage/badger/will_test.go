package badger

import (
	"os"
	"testing"
	"time"

	"github.com/absmach/mqtt/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWillStore_Set(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	will := &storage.WillMessage{
		Topic:   "client/status",
		Payload: []byte("offline"),
		QoS:     1,
		Retain:  true,
		Delay:   5,
	}

	err := store.Set("client-1", will)
	require.NoError(t, err)

	retrieved, err := store.Get("client-1")
	require.NoError(t, err)
	assert.Equal(t, will.Topic, retrieved.Topic)
	assert.Equal(t, will.Payload, retrieved.Payload)
	assert.Equal(t, will.QoS, retrieved.QoS)
	assert.Equal(t, will.Retain, retrieved.Retain)
	assert.Equal(t, will.Delay, retrieved.Delay)
}

func TestWillStore_Get(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	will := &storage.WillMessage{
		Topic:   "device/last-will",
		Payload: []byte("device disconnected"),
		QoS:     2,
		Retain:  false,
		Delay:   10,
	}

	err := store.Set("device-123", will)
	require.NoError(t, err)

	retrieved, err := store.Get("device-123")
	require.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, "device/last-will", retrieved.Topic)
	assert.Equal(t, uint32(10), retrieved.Delay)
}

func TestWillStore_GetNotFound(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	_, err := store.Get("nonexistent-client")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestWillStore_Delete(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	will := &storage.WillMessage{
		Topic:   "test/will",
		Payload: []byte("goodbye"),
		QoS:     1,
	}

	err := store.Set("client-delete", will)
	require.NoError(t, err)

	err = store.Delete("client-delete")
	require.NoError(t, err)

	_, err = store.Get("client-delete")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestWillStore_GetPendingNoDelay(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	will := &storage.WillMessage{
		Topic:   "client/offline",
		Payload: []byte("disconnected"),
		QoS:     1,
		Delay:   0,
	}

	err := store.Set("client-immediate", will)
	require.NoError(t, err)

	pending, err := store.GetPending(time.Now().Add(1 * time.Second))
	require.NoError(t, err)
	assert.Len(t, pending, 1)
	assert.Equal(t, "client/offline", pending[0].Topic)
}

func TestWillStore_GetPendingWithDelay(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	will := &storage.WillMessage{
		Topic:   "client/delayed",
		Payload: []byte("delayed disconnect"),
		QoS:     1,
		Delay:   5,
	}

	err := store.Set("client-delayed", will)
	require.NoError(t, err)

	before := time.Now().Add(3 * time.Second)
	pending, err := store.GetPending(before)
	require.NoError(t, err)
	assert.Empty(t, pending)

	after := time.Now().Add(10 * time.Second)
	pending, err = store.GetPending(after)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
}

func TestWillStore_GetPendingMultiple(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	wills := []*storage.WillMessage{
		{Topic: "client1/status", Payload: []byte("offline"), QoS: 1, Delay: 0},
		{Topic: "client2/status", Payload: []byte("offline"), QoS: 1, Delay: 2},
		{Topic: "client3/status", Payload: []byte("offline"), QoS: 1, Delay: 10},
	}

	for i, will := range wills {
		err := store.Set(string(rune('A'+i)), will)
		require.NoError(t, err)
	}

	checkTime := time.Now().Add(5 * time.Second)
	pending, err := store.GetPending(checkTime)
	require.NoError(t, err)
	assert.Len(t, pending, 2)
}

func TestWillStore_GetPendingEmpty(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	pending, err := store.GetPending(time.Now())
	require.NoError(t, err)
	assert.Empty(t, pending)
}

func TestWillStore_ConcurrentOperations(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func(id int) {
			will := &storage.WillMessage{
				Topic:   "concurrent/will",
				Payload: []byte("message"),
				QoS:     1,
				Delay:   0,
			}
			err := store.Set("concurrent-client", will)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		go func() {
			_, _ = store.Get("concurrent-client")
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestWillStore_UpdateExisting(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

	original := &storage.WillMessage{
		Topic:   "client/will",
		Payload: []byte("original"),
		QoS:     1,
		Delay:   5,
	}

	err := store.Set("client-update", original)
	require.NoError(t, err)

	updated := &storage.WillMessage{
		Topic:   "client/will/updated",
		Payload: []byte("updated"),
		QoS:     2,
		Delay:   10,
	}

	err = store.Set("client-update", updated)
	require.NoError(t, err)

	retrieved, err := store.Get("client-update")
	require.NoError(t, err)
	assert.Equal(t, "client/will/updated", retrieved.Topic)
	assert.Equal(t, []byte("updated"), retrieved.Payload)
	assert.Equal(t, byte(2), retrieved.QoS)
	assert.Equal(t, uint32(10), retrieved.Delay)
}

func TestWillStore_QoSLevels(t *testing.T) {
	store := setupWillStore(t)
	defer cleanupWillStore(t, store)

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
			will := &storage.WillMessage{
				Topic:   "test/qos",
				Payload: []byte("test"),
				QoS:     tt.qos,
			}

			err := store.Set("client-qos", will)
			require.NoError(t, err)

			retrieved, err := store.Get("client-qos")
			require.NoError(t, err)
			assert.Equal(t, tt.qos, retrieved.QoS)
		})
	}
}

func setupWillStore(t *testing.T) *WillStore {
	tmpDir, err := os.MkdirTemp("", "badger-will-test-*")
	require.NoError(t, err)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)

	return &WillStore{db: store.db}
}

func cleanupWillStore(t *testing.T, store *WillStore) {
	if store != nil && store.db != nil {
		dir := store.db.Opts().Dir
		store.db.Close()
		os.RemoveAll(dir)
	}
}
