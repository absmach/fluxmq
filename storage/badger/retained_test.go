package badger

import (
	"os"
	"testing"

	"github.com/absmach/mqtt/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetainedStore_Set(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := &storage.Message{
		Topic:   "test/topic",
		Payload: []byte("retained message"),
		QoS:     1,
	}

	err := store.Set("test/topic", msg)
	require.NoError(t, err)

	retrieved, err := store.Get("test/topic")
	require.NoError(t, err)
	assert.Equal(t, msg.Topic, retrieved.Topic)
	assert.Equal(t, msg.Payload, retrieved.Payload)
	assert.Equal(t, msg.QoS, retrieved.QoS)
}

func TestRetainedStore_SetEmptyPayload(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := &storage.Message{
		Topic:   "test/topic",
		Payload: []byte("initial message"),
		QoS:     1,
	}

	err := store.Set("test/topic", msg)
	require.NoError(t, err)

	emptyMsg := &storage.Message{
		Topic:   "test/topic",
		Payload: []byte{},
		QoS:     0,
	}
	err = store.Set("test/topic", emptyMsg)
	require.NoError(t, err)

	_, err = store.Get("test/topic")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestRetainedStore_Get(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := &storage.Message{
		Topic:   "sensor/temperature",
		Payload: []byte("25.5"),
		QoS:     2,
	}

	err := store.Set("sensor/temperature", msg)
	require.NoError(t, err)

	retrieved, err := store.Get("sensor/temperature")
	require.NoError(t, err)
	assert.Equal(t, "sensor/temperature", retrieved.Topic)
	assert.Equal(t, []byte("25.5"), retrieved.Payload)
	assert.Equal(t, byte(2), retrieved.QoS)
}

func TestRetainedStore_GetNotFound(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	_, err := store.Get("nonexistent/topic")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestRetainedStore_Delete(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := &storage.Message{
		Topic:   "test/delete",
		Payload: []byte("to be deleted"),
		QoS:     1,
	}

	err := store.Set("test/delete", msg)
	require.NoError(t, err)

	err = store.Delete("test/delete")
	require.NoError(t, err)

	_, err = store.Get("test/delete")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestRetainedStore_MatchExact(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	msg := &storage.Message{
		Topic:   "sensor/temp",
		Payload: []byte("20"),
		QoS:     1,
	}

	err := store.Set("sensor/temp", msg)
	require.NoError(t, err)

	matched, err := store.Match("sensor/temp")
	require.NoError(t, err)
	assert.Len(t, matched, 1)
	assert.Equal(t, "sensor/temp", matched[0].Topic)
}

func TestRetainedStore_MatchSingleLevelWildcard(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	messages := []*storage.Message{
		{Topic: "sensor/temp/room1", Payload: []byte("20"), QoS: 1},
		{Topic: "sensor/temp/room2", Payload: []byte("21"), QoS: 1},
		{Topic: "sensor/humidity/room1", Payload: []byte("60"), QoS: 1},
	}

	for _, msg := range messages {
		err := store.Set(msg.Topic, msg)
		require.NoError(t, err)
	}

	matched, err := store.Match("sensor/temp/+")
	require.NoError(t, err)
	assert.Len(t, matched, 2)
}

func TestRetainedStore_MatchMultiLevelWildcard(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	messages := []*storage.Message{
		{Topic: "sensor/temp/room1", Payload: []byte("20"), QoS: 1},
		{Topic: "sensor/temp/room2", Payload: []byte("21"), QoS: 1},
		{Topic: "sensor/humidity/room1", Payload: []byte("60"), QoS: 1},
		{Topic: "alerts/critical", Payload: []byte("fire"), QoS: 2},
	}

	for _, msg := range messages {
		err := store.Set(msg.Topic, msg)
		require.NoError(t, err)
	}

	matched, err := store.Match("sensor/#")
	require.NoError(t, err)
	assert.Len(t, matched, 3)

	matchAll, err := store.Match("#")
	require.NoError(t, err)
	assert.Len(t, matchAll, 4)
}

func TestRetainedStore_MatchEmpty(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	matched, err := store.Match("nonexistent/#")
	require.NoError(t, err)
	assert.Empty(t, matched)
}

func TestRetainedStore_ConcurrentSetGet(t *testing.T) {
	store := setupRetainedStore(t)
	defer cleanupRetainedStore(t, store)

	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func(id int) {
			msg := &storage.Message{
				Topic:   "concurrent/topic",
				Payload: []byte("message"),
				QoS:     1,
			}
			err := store.Set("concurrent/topic", msg)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	for i := 0; i < 5; i++ {
		go func() {
			_, _ = store.Get("concurrent/topic")
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

	original := &storage.Message{
		Topic:   "test/update",
		Payload: []byte("original"),
		QoS:     1,
	}

	err := store.Set("test/update", original)
	require.NoError(t, err)

	updated := &storage.Message{
		Topic:   "test/update",
		Payload: []byte("updated"),
		QoS:     2,
	}

	err = store.Set("test/update", updated)
	require.NoError(t, err)

	retrieved, err := store.Get("test/update")
	require.NoError(t, err)
	assert.Equal(t, []byte("updated"), retrieved.Payload)
	assert.Equal(t, byte(2), retrieved.QoS)
}

func setupRetainedStore(t *testing.T) *RetainedStore {
	tmpDir, err := os.MkdirTemp("", "badger-retained-test-*")
	require.NoError(t, err)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)

	return &RetainedStore{db: store.db}
}

func cleanupRetainedStore(t *testing.T, store *RetainedStore) {
	if store != nil && store.db != nil {
		dir := store.db.Opts().Dir
		store.db.Close()
		os.RemoveAll(dir)
	}
}
