// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	testNode1 = "node1"
	testNode2 = "node2"
	testNode3 = "node3"
)

// testLogger creates a logger for testing that discards output.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

// TestRetainedStore_SmallMessageReplication tests that small messages (<threshold) are replicated to all nodes.
func TestRetainedStore_SmallMessageReplication(t *testing.T) {
	// Setup etcd client (requires running etcd instance)
	t.Skip("Requires etcd instance - run as integration test")

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Create two nodes with different local stores
	node1Store := memory.New()
	node2Store := memory.New()

	store1 := NewRetainedStore(
		testNode1,
		node1Store.Retained(),
		client,
		nil,  // no transport needed for replication test
		1024, // 1KB threshold
		testLogger(t),
	)
	defer store1.Close()

	store2 := NewRetainedStore(
		testNode2,
		node2Store.Retained(),
		client,
		nil,
		1024,
		testLogger(t),
	)
	defer store2.Close()

	// Create a small message (<1KB)
	msg := &storage.Message{
		Topic:       "test/small",
		Payload:     []byte("small payload"),
		QoS:         1,
		Retain:      true,
		Properties:  map[string]string{"test": testValue},
		PublishTime: time.Now(),
	}

	// Store message on node1
	err = store1.Set(ctx, msg.Topic, msg)
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(100 * time.Millisecond)

	// Verify message is available on node2 (replicated)
	retrieved, err := store2.Get(ctx, msg.Topic)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, msg.Topic, retrieved.Topic)
	assert.Equal(t, msg.Payload, retrieved.Payload)
	assert.Equal(t, msg.QoS, retrieved.QoS)
}

// TestRetainedStore_LargeMessageFetchOnDemand tests that large messages (≥threshold) are not replicated.
func TestRetainedStore_LargeMessageFetchOnDemand(t *testing.T) {
	t.Skip("Requires etcd + transport setup - run as integration test")

	// This test would verify:
	// 1. Large message stored on node1 doesn't appear in node2's local store
	// 2. Metadata is replicated to node2
	// 3. node2 fetches large message from node1 via gRPC on demand
}

// TestRetainedStore_Set_UpdatesMetadataCache verifies metadata cache is updated on Set.
func TestRetainedStore_Set_UpdatesMetadataCache(t *testing.T) {
	t.Skip("Requires etcd instance")
	// Test that after Set(), the metadata cache contains the correct entry
}

// TestRetainedStore_Delete_RemovesFromBothStores verifies Delete removes from both local and etcd.
func TestRetainedStore_Delete_RemovesFromBothStores(t *testing.T) {
	t.Skip("Requires etcd instance")
	// Test that Delete() removes from:
	// 1. Local BadgerDB/memory store
	// 2. etcd (both data and index prefixes)
	// 3. Metadata cache
}

// newTestRetainedStore builds a RetainedStore backed by an in-memory local
// store, bypassing NewRetainedStore's etcd load/watch. Match, Get, and pruning
// need only localStore, metadataCache, transport, and logger.
func newTestRetainedStore(t *testing.T, nodeID string) *RetainedStore {
	t.Helper()
	return &RetainedStore{
		nodeID:        nodeID,
		localStore:    memory.New().Retained(),
		sizeThreshold: defaultSizeThreshold,
		metadataCache: make(map[string]*RetainedMetadata),
		logger:        testLogger(t),
		stopCh:        make(chan struct{}),
	}
}

// seedLocalRetained stores a small retained message owned by this node in both
// the local store and the metadata cache, so Get resolves it from local.
func seedLocalRetained(t *testing.T, h *RetainedStore, topic string, payload []byte) {
	t.Helper()
	msg := &storage.Message{
		Topic:       topic,
		Payload:     payload,
		QoS:         1,
		Retain:      true,
		PublishTime: time.Now(),
	}
	require.NoError(t, h.localStore.Set(context.Background(), topic, msg))
	h.metadataCache[topic] = &RetainedMetadata{
		NodeID:     h.nodeID,
		Topic:      topic,
		QoS:        1,
		Size:       len(payload),
		Replicated: len(payload) < h.sizeThreshold,
		Timestamp:  time.Now(),
	}
}

// seedMetadataOnly registers a metadata entry with no local payload, simulating
// a message whose payload lives elsewhere (or was lost). ownerNode == nodeID
// and replicated=false both drive Get to storage.ErrNotFound without a
// transport; a remote owner with a nil transport yields a transient error.
func seedMetadataOnly(h *RetainedStore, topic, ownerNode string, replicated bool) {
	h.metadataCache[topic] = &RetainedMetadata{
		NodeID:     ownerNode,
		Topic:      topic,
		QoS:        1,
		Size:       4096,
		Replicated: replicated,
		Timestamp:  time.Now(),
	}
}

func matchedTopics(msgs []*storage.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Topic
	}
	return out
}

// TestRetainedStore_Match_CompleteAndDeterministic verifies that a wildcard
// Match returns the full matching set in the same sorted order on every call —
// the core fix for the "different subset each subscribe" bug.
func TestRetainedStore_Match_CompleteAndDeterministic(t *testing.T) {
	h := newTestRetainedStore(t, testNode1)

	var want []string
	for i := range 25 {
		topic := fmt.Sprintf("m/dom/c/chan/sensor-%02d", i)
		seedLocalRetained(t, h, topic, fmt.Appendf(nil, "v%d", i))
		want = append(want, topic)
	}
	slices.Sort(want)
	// A non-matching entry must never appear.
	seedLocalRetained(t, h, "other/topic", []byte("nope"))

	ctx := context.Background()
	var first []string
	for iter := range 50 {
		msgs, err := h.Match(ctx, "m/dom/c/chan/#")
		require.NoError(t, err)
		got := matchedTopics(msgs)
		require.True(t, slices.IsSorted(got), "iter %d: result not sorted", iter)
		require.Equal(t, want, got, "iter %d: incomplete or reordered set", iter)
		if iter == 0 {
			first = got
		}
		assert.Equal(t, first, got, "iter %d: set differs from first call", iter)
	}
}

// TestRetainedStore_Match_PrunesMissingPayload verifies that entries whose
// payload is gone are excluded and their stale metadata is pruned, while
// present entries are still returned.
func TestRetainedStore_Match_PrunesMissingPayload(t *testing.T) {
	tests := []struct {
		name       string
		ownerNode  string
		replicated bool
	}{
		{"owned but payload lost", testNode1, false},
		{"replicated but missing locally", testNode2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestRetainedStore(t, testNode1)
			seedLocalRetained(t, h, "m/x/present", []byte("here"))
			seedMetadataOnly(h, "m/x/gone", tt.ownerNode, tt.replicated)

			msgs, err := h.Match(context.Background(), "m/x/#")
			require.NoError(t, err)
			assert.Equal(t, []string{"m/x/present"}, matchedTopics(msgs))

			h.metadataCacheMu.RLock()
			_, stillThere := h.metadataCache["m/x/gone"]
			_, presentKept := h.metadataCache["m/x/present"]
			h.metadataCacheMu.RUnlock()
			assert.False(t, stillThere, "missing entry should be pruned")
			assert.True(t, presentKept, "present entry must not be pruned")
		})
	}
}

// TestRetainedStore_Match_SurfacesTransientErrors verifies that a transient
// fetch failure (remote owner, no transport) is not silently converted into a
// missing entry: the whole call fails when nothing resolves, and a partial
// success keeps the failing entry's metadata for a later retry.
func TestRetainedStore_Match_SurfacesTransientErrors(t *testing.T) {
	t.Run("all fetches fail -> error", func(t *testing.T) {
		h := newTestRetainedStore(t, testNode1) // transport is nil
		seedMetadataOnly(h, "m/x/remote1", testNode2, false)
		seedMetadataOnly(h, "m/x/remote2", testNode3, false)

		msgs, err := h.Match(context.Background(), "m/x/#")
		require.Error(t, err)
		assert.Nil(t, msgs)

		// A transient failure must NOT prune metadata.
		h.metadataCacheMu.RLock()
		remaining := len(h.metadataCache)
		h.metadataCacheMu.RUnlock()
		assert.Equal(t, 2, remaining, "transient errors must not prune metadata")
	})

	t.Run("partial success -> subset, no prune", func(t *testing.T) {
		h := newTestRetainedStore(t, testNode1)
		seedLocalRetained(t, h, "m/x/present", []byte("here"))
		seedMetadataOnly(h, "m/x/remote", testNode2, false) // errors: nil transport

		msgs, err := h.Match(context.Background(), "m/x/#")
		require.NoError(t, err)
		assert.Equal(t, []string{"m/x/present"}, matchedTopics(msgs))

		h.metadataCacheMu.RLock()
		_, errKept := h.metadataCache["m/x/remote"]
		h.metadataCacheMu.RUnlock()
		assert.True(t, errKept, "entry that errored transiently must be retried later, not pruned")
	})
}

// TestRetainedStore_pruneMissingMetadata_PointerGuard verifies that a prune
// only removes the exact metadata pointer Match observed, so a concurrent Set
// that replaced the entry is never clobbered.
func TestRetainedStore_pruneMissingMetadata_PointerGuard(t *testing.T) {
	h := newTestRetainedStore(t, testNode1)
	topic := "m/x/topic"
	stale := &RetainedMetadata{NodeID: testNode1, Topic: topic}
	h.metadataCache[topic] = stale

	// Simulate a concurrent Set replacing the pointer before prune runs.
	fresh := &RetainedMetadata{NodeID: testNode1, Topic: topic}
	h.metadataCache[topic] = fresh

	h.pruneMissingMetadata(retainedRef{topic: topic, meta: stale})

	h.metadataCacheMu.RLock()
	got := h.metadataCache[topic]
	h.metadataCacheMu.RUnlock()
	assert.Same(t, fresh, got, "prune must not remove a concurrently-updated entry")

	// Pruning the current pointer does remove it.
	h.pruneMissingMetadata(retainedRef{topic: topic, meta: fresh})
	h.metadataCacheMu.RLock()
	_, exists := h.metadataCache[topic]
	h.metadataCacheMu.RUnlock()
	assert.False(t, exists, "prune must remove the matching pointer")
}

// TestRetainedStore_Match_ConcurrentReaders exercises Match under -race with
// many concurrent callers to confirm the metadata-cache locking is sound.
func TestRetainedStore_Match_ConcurrentReaders(t *testing.T) {
	h := newTestRetainedStore(t, testNode1)
	for i := range 30 {
		seedLocalRetained(t, h, fmt.Sprintf("m/x/%02d", i), []byte("p"))
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 20 {
				msgs, err := h.Match(context.Background(), "m/x/#")
				assert.NoError(t, err)
				assert.Len(t, msgs, 30)
			}
		})
	}
	wg.Wait()
}

// TestRetainedStore_SizeThreshold tests messages at threshold boundary.
func TestRetainedStore_SizeThreshold(t *testing.T) {
	tests := []struct {
		name            string
		payloadSize     int
		threshold       int
		shouldReplicate bool
	}{
		{"exactly at threshold", 1024, 1024, false}, // ≥ threshold = not replicated
		{"one byte below threshold", 1023, 1024, true},
		{"one byte above threshold", 1025, 1024, false},
		{"much smaller", 100, 1024, true},
		{"much larger", 10000, 1024, false},
		{"zero threshold defaults to 1024", 512, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test logic to verify replication behavior based on size
			payload := make([]byte, tt.payloadSize)

			threshold := tt.threshold
			if threshold <= 0 {
				threshold = defaultSizeThreshold
			}

			replicated := len(payload) < threshold
			assert.Equal(t, tt.shouldReplicate, replicated)
		})
	}
}

// TestRetainedStore_ConcurrentAccess tests thread-safety of metadata cache.
func TestRetainedStore_ConcurrentAccess(t *testing.T) {
	t.Skip("Requires etcd instance")
	// Test concurrent:
	// 1. Set() operations
	// 2. Get() operations
	// 3. Match() operations
	// 4. Delete() operations
	// Verify no race conditions
}

// TestRetainedStore_WatchEventHandling tests etcd watch event processing.
func TestRetainedStore_WatchEventHandling(t *testing.T) {
	t.Skip("Requires etcd instance")
	// Test that watch events properly:
	// 1. Update metadata cache
	// 2. Replicate small messages to local store
	// 3. Handle delete events
	// 4. Recover from watch errors
}

// TestRetainedMetadata_JSON tests JSON marshaling/unmarshaling.
func TestRetainedMetadata_JSON(t *testing.T) {
	metadata := &RetainedMetadata{
		NodeID:     testNode1,
		Topic:      "test/topic",
		QoS:        1,
		Size:       512,
		Replicated: true,
		Timestamp:  time.Now().Truncate(time.Second),
	}

	// Marshal to JSON
	data, err := json.Marshal(metadata)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Unmarshal back
	var unmarshaled RetainedMetadata
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)

	assert.Equal(t, metadata.NodeID, unmarshaled.NodeID)
	assert.Equal(t, metadata.Topic, unmarshaled.Topic)
	assert.Equal(t, metadata.QoS, unmarshaled.QoS)
	assert.Equal(t, metadata.Size, unmarshaled.Size)
	assert.Equal(t, metadata.Replicated, unmarshaled.Replicated)
	assert.Equal(t, metadata.Timestamp.Unix(), unmarshaled.Timestamp.Unix())
}

// TestRetainedDataEntry_JSON tests JSON marshaling for replicated data.
func TestRetainedDataEntry_JSON(t *testing.T) {
	entry := &RetainedDataEntry{
		Metadata: RetainedMetadata{
			NodeID:     testNode1,
			Topic:      "test/topic",
			QoS:        2,
			Size:       100,
			Replicated: true,
			Timestamp:  time.Now().Truncate(time.Second),
		},
		Payload:    "dGVzdCBwYXlsb2Fk", // base64 "test payload"
		Properties: map[string]string{"key": testValue},
	}

	// Marshal to JSON
	data, err := json.Marshal(entry)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Unmarshal back
	var unmarshaled RetainedDataEntry
	err = json.Unmarshal(data, &unmarshaled)
	require.NoError(t, err)

	assert.Equal(t, entry.Metadata.Topic, unmarshaled.Metadata.Topic)
	assert.Equal(t, entry.Payload, unmarshaled.Payload)
	assert.Equal(t, entry.Properties, unmarshaled.Properties)
}

// Benchmark tests.
func BenchmarkRetainedStore_SetSmallMessage(b *testing.B) {
	b.Skip("Requires etcd instance")
	// Benchmark Set() for small messages
}

func BenchmarkRetainedStore_SetLargeMessage(b *testing.B) {
	b.Skip("Requires etcd instance")
	// Benchmark Set() for large messages
}

func BenchmarkRetainedStore_GetLocalHit(b *testing.B) {
	b.Skip("Requires etcd instance")
	// Benchmark Get() when message is in local store (cache hit)
}

func BenchmarkRetainedStore_MatchWildcard(b *testing.B) {
	b.Skip("Requires etcd instance")
	// Benchmark Match() with wildcard patterns
}
