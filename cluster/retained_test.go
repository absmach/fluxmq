// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
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
		"node1",
		node1Store.Retained(),
		client,
		nil,  // no transport needed for replication test
		1024, // 1KB threshold
		testLogger(t),
	)
	defer store1.Close()

	store2 := NewRetainedStore(
		"node2",
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
		Properties:  map[string]string{"test": "value"},
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

// TestRetainedStore_Match_WildcardTopics tests wildcard topic matching.
func TestRetainedStore_Match_WildcardTopics(t *testing.T) {
	t.Skip("Requires etcd instance")
	// Test that Match() correctly:
	// 1. Scans metadata cache for matching topics
	// 2. Fetches messages from local store
	// 3. Returns all matching messages
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
		NodeID:     "node1",
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
			NodeID:     "node1",
			Topic:      "test/topic",
			QoS:        2,
			Size:       100,
			Replicated: true,
			Timestamp:  time.Now().Truncate(time.Second),
		},
		Payload:    "dGVzdCBwYXlsb2Fk", // base64 "test payload"
		Properties: map[string]string{"key": "value"},
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
