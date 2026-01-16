package badger

import (
	"fmt"
	"os"
	"testing"

	"github.com/absmach/fluxmq/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionStore_Add(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	sub := &storage.Subscription{
		ClientID: "client-1",
		Filter:   "test/topic",
		QoS:      1,
		Options: storage.SubscribeOptions{
			NoLocal:           false,
			RetainAsPublished: true,
		},
	}

	err := store.Add(sub)
	require.NoError(t, err)

	// Verify added
	subs, err := store.GetForClient("client-1")
	require.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.Equal(t, "test/topic", subs[0].Filter)
	assert.Equal(t, byte(1), subs[0].QoS)
}

func TestSubscriptionStore_AddMultiple(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	subs := []*storage.Subscription{
		{ClientID: "client-1", Filter: "sensor/+/temp", QoS: 0},
		{ClientID: "client-1", Filter: "sensor/#", QoS: 1},
		{ClientID: "client-1", Filter: "alerts/critical", QoS: 2},
	}

	for _, sub := range subs {
		err := store.Add(sub)
		require.NoError(t, err)
	}

	retrieved, err := store.GetForClient("client-1")
	require.NoError(t, err)
	assert.Len(t, retrieved, 3)
}

func TestSubscriptionStore_Remove(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	sub := &storage.Subscription{
		ClientID: "client-2",
		Filter:   "test/remove",
		QoS:      1,
	}

	err := store.Add(sub)
	require.NoError(t, err)

	err = store.Remove("client-2", "test/remove")
	require.NoError(t, err)

	subs, err := store.GetForClient("client-2")
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestSubscriptionStore_RemoveNonExistent(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	// Should not error when removing non-existent subscription
	err := store.Remove("nonexistent-client", "nonexistent/filter")
	assert.NoError(t, err)
}

func TestSubscriptionStore_GetForClient(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	// Add subscriptions for multiple clients
	clients := []string{"client-1", "client-2", "client-3"}
	for _, clientID := range clients {
		for i := 0; i < 3; i++ {
			sub := &storage.Subscription{
				ClientID: clientID,
				Filter:   fmt.Sprintf("topic/%s/%d", clientID, i),
				QoS:      byte(i % 3),
			}
			err := store.Add(sub)
			require.NoError(t, err)
		}
	}

	// Verify each client has only their subscriptions
	for _, clientID := range clients {
		subs, err := store.GetForClient(clientID)
		require.NoError(t, err)
		assert.Len(t, subs, 3)

		for _, sub := range subs {
			assert.Equal(t, clientID, sub.ClientID)
		}
	}
}

func TestSubscriptionStore_GetForClientEmpty(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	subs, err := store.GetForClient("nonexistent-client")
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestSubscriptionStore_RemoveAll(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	// Add multiple subscriptions for a client
	for i := 0; i < 5; i++ {
		sub := &storage.Subscription{
			ClientID: "client-bulk",
			Filter:   fmt.Sprintf("topic/%d", i),
			QoS:      1,
		}
		err := store.Add(sub)
		require.NoError(t, err)
	}

	// Remove all
	err := store.RemoveAll("client-bulk")
	require.NoError(t, err)

	// Verify all removed
	subs, err := store.GetForClient("client-bulk")
	require.NoError(t, err)
	assert.Empty(t, subs)
}

func TestSubscriptionStore_Count(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	// Initially zero
	count := store.Count()
	assert.Equal(t, 0, count)

	// Add subscriptions
	for i := 0; i < 10; i++ {
		sub := &storage.Subscription{
			ClientID: fmt.Sprintf("client-%d", i),
			Filter:   "test/topic",
			QoS:      1,
		}
		err := store.Add(sub)
		require.NoError(t, err)
	}

	// Verify count
	count = store.Count()
	assert.Equal(t, 10, count)

	// Remove some
	err := store.RemoveAll("client-0")
	require.NoError(t, err)

	count = store.Count()
	assert.Equal(t, 9, count)
}

func TestSubscriptionStore_DuplicateAdd(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	sub := &storage.Subscription{
		ClientID: "client-dup",
		Filter:   "test/topic",
		QoS:      1,
	}

	// Add first time
	err := store.Add(sub)
	require.NoError(t, err)

	// Add again with different QoS (should update)
	sub.QoS = 2
	err = store.Add(sub)
	require.NoError(t, err)

	// Verify only one subscription with updated QoS
	subs, err := store.GetForClient("client-dup")
	require.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.Equal(t, byte(2), subs[0].QoS)
}

func TestSubscriptionStore_WildcardFilters(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	wildcards := []string{
		"sensor/+/temp",
		"sensor/#",
		"+/+/status",
		"#",
	}

	for i, filter := range wildcards {
		sub := &storage.Subscription{
			ClientID: fmt.Sprintf("client-%d", i),
			Filter:   filter,
			QoS:      1,
		}
		err := store.Add(sub)
		require.NoError(t, err)
	}

	// Verify all wildcards stored correctly
	for i, filter := range wildcards {
		subs, err := store.GetForClient(fmt.Sprintf("client-%d", i))
		require.NoError(t, err)
		assert.Len(t, subs, 1)
		assert.Equal(t, filter, subs[0].Filter)
	}
}

func TestSubscriptionStore_ConcurrentAdd(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	done := make(chan bool, 20)

	// Concurrent adds for different clients
	for i := 0; i < 10; i++ {
		go func(id int) {
			sub := &storage.Subscription{
				ClientID: fmt.Sprintf("concurrent-client-%d", id),
				Filter:   "test/topic",
				QoS:      1,
			}
			err := store.Add(sub)
			assert.NoError(t, err)
			done <- true
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 10; i++ {
		go func(id int) {
			_, _ = store.GetForClient(fmt.Sprintf("concurrent-client-%d", id))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}
}

func TestSubscriptionStore_Options(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	sub := &storage.Subscription{
		ClientID: "client-opts",
		Filter:   "test/topic",
		QoS:      1,
		Options: storage.SubscribeOptions{
			NoLocal:           true,
			RetainAsPublished: true,
			RetainHandling:    2,
		},
	}

	err := store.Add(sub)
	require.NoError(t, err)

	subs, err := store.GetForClient("client-opts")
	require.NoError(t, err)
	assert.Len(t, subs, 1)
	assert.True(t, subs[0].Options.NoLocal)
	assert.True(t, subs[0].Options.RetainAsPublished)
	assert.Equal(t, byte(2), subs[0].Options.RetainHandling)
}

func TestSubscriptionStore_MatchExact(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	subs := []*storage.Subscription{
		{ClientID: "client-1", Filter: "sensor/temp", QoS: 1},
		{ClientID: "client-2", Filter: "sensor/humidity", QoS: 1},
	}

	for _, sub := range subs {
		err := store.Add(sub)
		require.NoError(t, err)
	}

	matched, err := store.Match("sensor/temp")
	require.NoError(t, err)
	assert.Len(t, matched, 1)
	assert.Equal(t, "client-1", matched[0].ClientID)
}

func TestSubscriptionStore_MatchSingleLevelWildcard(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	subs := []*storage.Subscription{
		{ClientID: "client-1", Filter: "sensor/+/temp", QoS: 1},
		{ClientID: "client-2", Filter: "sensor/room1/humidity", QoS: 1},
	}

	for _, sub := range subs {
		err := store.Add(sub)
		require.NoError(t, err)
	}

	matched, err := store.Match("sensor/room1/temp")
	require.NoError(t, err)
	assert.Len(t, matched, 1)
	assert.Equal(t, "client-1", matched[0].ClientID)
}

func TestSubscriptionStore_MatchMultiLevelWildcard(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	subs := []*storage.Subscription{
		{ClientID: "client-1", Filter: "sensor/#", QoS: 1},
		{ClientID: "client-2", Filter: "#", QoS: 1},
		{ClientID: "client-3", Filter: "alerts/critical", QoS: 1},
	}

	for _, sub := range subs {
		err := store.Add(sub)
		require.NoError(t, err)
	}

	matched, err := store.Match("sensor/room1/temp")
	require.NoError(t, err)
	assert.Len(t, matched, 2)

	clientIDs := make(map[string]bool)
	for _, m := range matched {
		clientIDs[m.ClientID] = true
	}
	assert.True(t, clientIDs["client-1"])
	assert.True(t, clientIDs["client-2"])
}

func TestSubscriptionStore_MatchEmpty(t *testing.T) {
	store := setupSubscriptionStore(t)
	defer cleanupSubscriptionStore(t, store)

	matched, err := store.Match("nonexistent/topic")
	require.NoError(t, err)
	assert.Empty(t, matched)
}

// Helper functions

func setupSubscriptionStore(t *testing.T) *SubscriptionStore {
	tmpDir, err := os.MkdirTemp("", "badger-sub-test-*")
	require.NoError(t, err)

	store, err := New(Config{Dir: tmpDir})
	require.NoError(t, err)

	return &SubscriptionStore{db: store.db}
}

func cleanupSubscriptionStore(t *testing.T, store *SubscriptionStore) {
	if store != nil && store.db != nil {
		dir := store.db.Opts().Dir
		store.db.Close()
		os.RemoveAll(dir)
	}
}
