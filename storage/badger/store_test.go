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

func TestStore_New(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)
	require.NotNil(t, store)
	defer store.Close()

	assert.NotNil(t, store.db)
}

func TestStore_Getters(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)
	defer store.Close()

	// Test all getters return non-nil stores
	assert.NotNil(t, store.Messages())
	assert.NotNil(t, store.Sessions())
	assert.NotNil(t, store.Subscriptions())
	assert.NotNil(t, store.Retained())
	assert.NotNil(t, store.Wills())

	// Verify they're the same instances on repeated calls
	assert.Equal(t, store.Messages(), store.Messages())
	assert.Equal(t, store.Sessions(), store.Sessions())
	assert.Equal(t, store.Subscriptions(), store.Subscriptions())
	assert.Equal(t, store.Retained(), store.Retained())
	assert.Equal(t, store.Wills(), store.Wills())
}

func TestStore_Close(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-store-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)

	// Close should not error
	err = store.Close()
	assert.NoError(t, err)

	// Second close should not panic (idempotent)
	err = store.Close()
	assert.NoError(t, err)
}

func TestStore_IntegrationSaveAndRetrieve(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-integration-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create store and save data
	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)

	// Save session
	session := &storage.Session{
		ClientID:       "integration-client",
		Version:        5,
		CleanStart:     false,
		ExpiryInterval: 300,
		Connected:      true,
		ConnectedAt:    time.Now(),
	}
	err = store.Sessions().Save(session)
	require.NoError(t, err)

	// Save message
	msg := &storage.Message{
		Topic:    "integration/topic",
		Payload:  []byte("integration test"),
		QoS:      1,
		PacketID: 123,
	}
	err = store.Messages().Store("integration-client/inflight/123", msg)
	require.NoError(t, err)

	// Save subscription
	sub := &storage.Subscription{
		ClientID: "integration-client",
		Filter:   "integration/#",
		QoS:      1,
	}
	err = store.Subscriptions().Add(sub)
	require.NoError(t, err)

	// Save retained message
	retainedMsg := &storage.Message{
		Topic:   "integration/retained",
		Payload: []byte("retained"),
		QoS:     0,
	}
	err = store.Retained().Set(ctx, "integration/retained", retainedMsg)
	require.NoError(t, err)

	// Save will message
	will := &storage.WillMessage{
		Topic:   "integration/will",
		Payload: []byte("offline"),
		QoS:     1,
		Delay:   0,
	}
	err = store.Wills().Set(ctx, "integration-client", will)
	require.NoError(t, err)

	// Close and reopen
	err = store.Close()
	require.NoError(t, err)

	store, err = New(Config{Dir: tmpDir})
	require.NoError(t, err)
	defer store.Close()

	// Verify all data persisted
	retrievedSession, err := store.Sessions().Get("integration-client")
	require.NoError(t, err)
	assert.Equal(t, session.ClientID, retrievedSession.ClientID)
	assert.Equal(t, session.Version, retrievedSession.Version)

	retrievedMsg, err := store.Messages().Get("integration-client/inflight/123")
	require.NoError(t, err)
	assert.Equal(t, msg.Topic, retrievedMsg.Topic)
	assert.Equal(t, msg.Payload, retrievedMsg.Payload)

	retrievedSubs, err := store.Subscriptions().GetForClient("integration-client")
	require.NoError(t, err)
	assert.Len(t, retrievedSubs, 1)
	assert.Equal(t, sub.Filter, retrievedSubs[0].Filter)

	retrievedRetained, err := store.Retained().Get(ctx, "integration/retained")
	require.NoError(t, err)
	assert.Equal(t, retainedMsg.Payload, retrievedRetained.Payload)

	retrievedWill, err := store.Wills().Get(ctx, "integration-client")
	require.NoError(t, err)
	assert.Equal(t, will.Topic, retrievedWill.Topic)
}

func TestStore_GarbageCollection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-gc-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)
	defer store.Close()

	// Write and delete some data to create garbage
	for i := 0; i < 100; i++ {
		session := &storage.Session{
			ClientID: "gc-test",
			Version:  5,
		}
		err = store.Sessions().Save(session)
		require.NoError(t, err)

		err = store.Sessions().Delete("gc-test")
		require.NoError(t, err)
	}

	// Trigger GC manually (normally runs in background)
	// This tests that runGC doesn't crash
	err = store.db.RunValueLogGC(0.5)
	// GC may return error if no GC was needed - that's OK
	if err != nil {
		t.Logf("GC returned: %v (expected if no garbage to collect)", err)
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-concurrent-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)
	defer store.Close()

	done := make(chan bool, 30)

	// Concurrent access to different stores
	for i := 0; i < 10; i++ {
		go func(id int) {
			session := &storage.Session{
				ClientID: "concurrent",
				Version:  5,
			}
			_ = store.Sessions().Save(session)
			done <- true
		}(i)

		go func(id int) {
			msg := &storage.Message{
				Topic:   "test",
				Payload: []byte("data"),
				QoS:     1,
			}
			_ = store.Messages().Store("test/key", msg)
			done <- true
		}(i)

		go func(id int) {
			sub := &storage.Subscription{
				ClientID: "test",
				Filter:   "topic/#",
				QoS:      1,
			}
			_ = store.Subscriptions().Add(sub)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 30; i++ {
		<-done
	}
}

func TestStore_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large dataset test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "badger-large-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)
	defer store.Close()

	// Write 1000 unique sessions
	count := 1000
	for i := 0; i < count; i++ {
		session := &storage.Session{
			ClientID: fmt.Sprintf("large-test-client-%d", i),
			Version:  5,
		}
		err = store.Sessions().Save(session)
		require.NoError(t, err)
	}

	// Verify count
	sessions, err := store.Sessions().List()
	require.NoError(t, err)
	assert.Equal(t, count, len(sessions))
}

func TestStore_ErrorHandling(t *testing.T) {
	// Test with invalid directory
	_, err := New(Config{Dir: "/invalid/nonexistent/path/that/should/fail"})
	assert.Error(t, err)
}

func TestStore_ConfigDefaults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "badger-config-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Test with minimal config
	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)
	defer store.Close()

	assert.NotNil(t, store)
	assert.NotNil(t, store.db)

	// Verify DB is writable
	session := &storage.Session{
		ClientID: "config-test",
		Version:  5,
	}
	err = store.Sessions().Save(session)
	assert.NoError(t, err)
}
