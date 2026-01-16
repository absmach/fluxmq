package badger

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionStore_Save(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	info := storage.Session{
		ClientID:       "test-client-1",
		Version:        5,
		CleanStart:     false,
		ExpiryInterval: 300,
		Connected:      true,
		ConnectedAt:    time.Now(),
	}

	err := store.Save(&info)
	require.NoError(t, err)

	// Verify saved
	retrieved, err := store.Get("test-client-1")
	require.NoError(t, err)
	assert.Equal(t, info.ClientID, retrieved.ClientID)
	assert.Equal(t, info.Version, retrieved.Version)
	assert.Equal(t, info.CleanStart, retrieved.CleanStart)
	assert.Equal(t, info.ExpiryInterval, retrieved.ExpiryInterval)
}

func TestSessionStore_Get(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	info := storage.Session{
		ClientID:       "test-client-2",
		Version:        5,
		CleanStart:     false,
		ExpiryInterval: 600,
	}

	err := store.Save(&info)
	require.NoError(t, err)

	retrieved, err := store.Get("test-client-2")
	require.NoError(t, err)
	assert.NotNil(t, retrieved)
	assert.Equal(t, "test-client-2", retrieved.ClientID)
	assert.Equal(t, uint32(600), retrieved.ExpiryInterval)
}

func TestSessionStore_GetNotFound(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	_, err := store.Get("nonexistent-client")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestSessionStore_Delete(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	info := storage.Session{
		ClientID:   "test-client-3",
		Version:    5,
		CleanStart: true,
	}

	err := store.Save(&info)
	require.NoError(t, err)

	err = store.Delete("test-client-3")
	require.NoError(t, err)

	_, err = store.Get("test-client-3")
	assert.Error(t, err)
	assert.Equal(t, storage.ErrNotFound, err)
}

func TestSessionStore_List(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	// Save multiple sessions
	sessions := []storage.Session{
		{ClientID: "client-1", Version: 5},
		{ClientID: "client-2", Version: 5},
		{ClientID: "client-3", Version: 5},
	}

	for _, session := range sessions {
		sess := session
		err := store.Save(&sess)
		require.NoError(t, err)
	}

	list, err := store.List()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(list), 3)

	// Verify all sessions are in the list
	clientIDs := make(map[string]bool)
	for _, s := range list {
		clientIDs[s.ClientID] = true
	}
	assert.True(t, clientIDs["client-1"])
	assert.True(t, clientIDs["client-2"])
	assert.True(t, clientIDs["client-3"])
}

func TestSessionStore_ListEmpty(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	list, err := store.List()
	require.NoError(t, err)
	assert.Empty(t, list)
}

func TestSessionStore_Update(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	info := storage.Session{
		ClientID:       "test-client-4",
		Version:        5,
		ExpiryInterval: 300,
		Connected:      true,
	}

	err := store.Save(&info)
	require.NoError(t, err)

	// Update
	info.Connected = false
	info.DisconnectedAt = time.Now()
	info.ExpiryInterval = 600

	err = store.Save(&info)
	require.NoError(t, err)

	// Verify update
	retrieved, err := store.Get("test-client-4")
	require.NoError(t, err)
	assert.False(t, retrieved.Connected)
	assert.Equal(t, uint32(600), retrieved.ExpiryInterval)
}

func TestSessionStore_ConcurrentSaveGet(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	done := make(chan bool, 10)

	// Concurrent writes
	for i := 0; i < 5; i++ {
		go func(id int) {
			info := storage.Session{
				ClientID: fmt.Sprintf("concurrent-client-%d", id),
				Version:  5,
			}
			err := store.Save(&info)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		go func(id int) {
			time.Sleep(10 * time.Millisecond) // Let some writes happen
			_, _ = store.Get(fmt.Sprintf("concurrent-client-%d", id))
			// Error OK if not yet saved
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestSessionStore_ExpiryInterval(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	info := storage.Session{
		ClientID:       "test-client-5",
		Version:        5,
		ExpiryInterval: 300, // 5 minutes
	}

	err := store.Save(&info)
	require.NoError(t, err)

	// Verify expiry is set correctly
	retrieved, err := store.Get("test-client-5")
	require.NoError(t, err)
	assert.Equal(t, uint32(300), retrieved.ExpiryInterval)
}

func TestSessionStore_GetExpired(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	now := time.Now()

	sessions := []storage.Session{
		{
			ClientID:       "active-client",
			Version:        5,
			Connected:      true,
			ExpiryInterval: 60,
			DisconnectedAt: now.Add(-120 * time.Second),
		},
		{
			ClientID:       "expired-client-1",
			Version:        5,
			Connected:      false,
			ExpiryInterval: 30,
			DisconnectedAt: now.Add(-60 * time.Second),
		},
		{
			ClientID:       "expired-client-2",
			Version:        5,
			Connected:      false,
			ExpiryInterval: 10,
			DisconnectedAt: now.Add(-20 * time.Second),
		},
		{
			ClientID:       "not-yet-expired",
			Version:        5,
			Connected:      false,
			ExpiryInterval: 300,
			DisconnectedAt: now.Add(-30 * time.Second),
		},
	}

	for _, sess := range sessions {
		s := sess
		err := store.Save(&s)
		require.NoError(t, err)
	}

	expired, err := store.GetExpired(now)
	require.NoError(t, err)
	assert.Len(t, expired, 2)
	assert.Contains(t, expired, "expired-client-1")
	assert.Contains(t, expired, "expired-client-2")
}

func TestSessionStore_GetExpiredEmpty(t *testing.T) {
	store := setupSessionStore(t)
	defer cleanupSessionStore(t, store)

	expired, err := store.GetExpired(time.Now())
	require.NoError(t, err)
	assert.Empty(t, expired)
}

// Helper functions

func setupSessionStore(t *testing.T) *SessionStore {
	tmpDir, err := os.MkdirTemp("", "badger-test-*")
	require.NoError(t, err)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)

	return &SessionStore{db: store.db}
}

func cleanupSessionStore(t *testing.T, store *SessionStore) {
	if store != nil && store.db != nil {
		dir := store.db.Opts().Dir
		store.db.Close()
		os.RemoveAll(dir)
	}
}
