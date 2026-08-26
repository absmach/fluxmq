// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/types"
	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var updateRaftGolden = flag.Bool("update-raft-golden", false,
	"rewrite the golden queue Raft encodings in testdata")

// conformanceTime is fixed rather than time-dependent so the golden bytes are
// reproducible.
var conformanceTime = time.Date(2026, 8, 26, 10, 11, 12, 13, time.UTC)

// The round-trip tests would all still pass if the schema and the codec moved
// together in a way that rewrote every entry already on disk. These pin the
// bytes themselves, so a format change has to be a visible diff in the review.
func TestGoldenRaftEncodings(t *testing.T) {
	operation, err := marshalOperation(conformanceOperation())
	require.NoError(t, err)

	logEntry, err := marshalLogEntry(&hraft.Log{
		Index: 42, Term: 7, Type: hraft.LogCommand,
		Data: operation, Extensions: []byte("extension"), AppendedAt: conformanceTime,
	})
	require.NoError(t, err)

	snapshot, err := conformanceSnapshotBytes()
	require.NoError(t, err)

	goldens := map[string][]byte{
		"testdata/operation-v1.bin": operation,
		"testdata/log-entry-v1.bin": logEntry,
		"testdata/snapshot-v1.bin":  snapshot,
	}

	for path, encoded := range goldens {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if *updateRaftGolden {
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
				require.NoError(t, os.WriteFile(path, encoded, 0o644))
				t.Logf("wrote %s (%d bytes)", path, len(encoded))
				return
			}

			golden, err := os.ReadFile(path)
			require.NoError(t, err)
			assert.Equal(t, golden, encoded,
				"the persisted queue Raft format changed.\n"+
					"Every entry already written uses the old bytes. If the change is intended, run:\n"+
					"  go test ./queue/raft -run TestGoldenRaftEncodings -update-raft-golden\n"+
					"and put the schema change and the golden diff in the same review.")
		})
	}
}

// conformanceOperation carries a create-queue command because it reaches the
// widest part of the schema: every queue config section, both enums, and the
// duration and timestamp wrappers.
func conformanceOperation() *Operation {
	config := conformanceQueueConfig()
	return &Operation{
		Type:        OpCreateQueue,
		Timestamp:   conformanceTime,
		QueueName:   testOperationQueue,
		QueueConfig: &config,
	}
}

// conformanceSnapshotBytes pins the framing as well as the messages: a snapshot
// is a stream of length-delimited frames, and collapsing it back into a single
// message would be invisible to a test that only compared decoded values.
func conformanceSnapshotBytes() ([]byte, error) {
	config := conformanceQueueConfig()
	var buf bytes.Buffer
	writer := newSnapshotWriter(&buf)
	if err := writer.WriteHeader(conformanceTime); err != nil {
		return nil, err
	}
	if err := writer.WriteQueue(QueueSnapshotData{
		QueueName:   testOperationQueue,
		QueueConfig: &config,
		Groups:      []*types.ConsumerGroup{conformanceConsumerGroup(conformanceTime)},
		Head:        3,
		Tail:        5,
	}); err != nil {
		return nil, err
	}
	for offset := uint64(3); offset < 5; offset++ {
		if err := writer.WriteRecord(offset, []byte{byte(offset), 0x01, 0x02}); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// conformanceQueueConfig populates every field the schema declares, which is
// what makes the golden comparison exhaustive. It is also a config the broker
// would accept, so the FSM tests can put it through a real store.
func conformanceQueueConfig() types.QueueConfig {
	config := types.DefaultQueueConfig(testOperationQueue, "jobs/#", "priority/#")
	config.Reserved = true
	config.Type = types.QueueTypeStream
	config.PrimaryGroup = testOperationGroup
	config.Durable = true
	config.AckDurability = "fsync"
	config.ExpiresAfter = 3 * time.Minute
	config.LastConsumerDisconnect = conformanceTime
	config.MaxMessageSize = 1 << 20
	config.MaxDepth = 4096
	config.MessageTTL = 2 * time.Hour
	config.DeliveryTimeout = 30 * time.Second
	config.BatchSize = 64
	config.HeartbeatTimeout = 15 * time.Second
	config.RetryPolicy = types.RetryPolicy{
		MaxRetries: 5, InitialBackoff: time.Second, MaxBackoff: time.Minute,
		BackoffMultiplier: 2.5, TotalTimeout: 10 * time.Minute,
	}
	config.DLQConfig = types.DLQConfig{Enabled: true, Topic: "$dlq/jobs", AlertWebhook: "https://example.test/alert"}
	config.Replication = types.ReplicationConfig{
		Enabled: true, Group: testOperationQueue, ReplicationFactor: 3, Mode: types.ReplicationSync,
		MinInSyncReplicas: 2, AckTimeout: 4 * time.Second, HeartbeatTimeout: time.Second,
		ElectionTimeout: 3 * time.Second, SnapshotInterval: 5 * time.Minute, SnapshotThreshold: 1000,
	}
	config.Retention = types.RetentionPolicy{
		RetentionTime: time.Hour, TimeCheckInterval: time.Minute, RetentionBytes: 4096,
		RetentionMessages: 100, SizeCheckEvery: 5, CompactionEnabled: true,
		CompactionKey: "key", CompactionLag: time.Minute, CompactionInterval: 2 * time.Minute,
	}
	return config
}

func conformanceConsumerGroup(now time.Time) *types.ConsumerGroup {
	return &types.ConsumerGroup{
		ID:         testOperationGroup,
		QueueName:  testOperationQueue,
		Pattern:    "jobs/#",
		Mode:       types.GroupModeQueue,
		AutoCommit: true,
		Cursor:     &types.QueueCursor{Cursor: 19, Committed: 17},
		PEL: map[string][]*types.PendingEntry{
			testOperationConsumerB: {{Offset: 18, ConsumerID: testOperationConsumerB, ClaimedAt: now.Add(time.Second), DeliveryCount: 2}},
			testOperationConsumerA: {{Offset: 17, ConsumerID: testOperationConsumerA, ClaimedAt: now, DeliveryCount: 1}},
		},
		Consumers: map[string]*types.ConsumerInfo{
			testOperationConsumerB: {ID: testOperationConsumerB, ClientID: "client-b", ProxyNodeID: "node-2", RegisteredAt: now, LastHeartbeat: now.Add(time.Second)},
			testOperationConsumerA: {ID: testOperationConsumerA, ClientID: "client-a", ProxyNodeID: "node-1", RegisteredAt: now, LastHeartbeat: now.Add(2 * time.Second)},
		},
		CreatedAt: now.Add(-time.Hour),
		UpdatedAt: now,
	}
}
