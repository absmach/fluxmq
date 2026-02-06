// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

//go:build ignore

// TODO: enable when queue/storage/badger packages are implemented

package raft

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	badgerstore "github.com/absmach/fluxmq/queue/storage/badger"
	"github.com/absmach/fluxmq/queue/types"
	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
)

func setupTestFSM(t *testing.T) (*PartitionFSM, storage.MessageStore, func()) {
	t.Helper()

	dir, err := os.MkdirTemp("", "fsm-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	opts := badger.DefaultOptions(dir)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("failed to open badger: %v", err)
	}

	store := badgerstore.New(db)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	fsm := NewPartitionFSM("test-queue", 0, store, logger)

	cleanup := func() {
		db.Close()
		os.RemoveAll(dir)
	}

	return fsm, store, cleanup
}

func TestFSM_ApplyEnqueue(t *testing.T) {
	fsm, store, cleanup := setupTestFSM(t)
	defer cleanup()

	msg := &types.Message{
		ID:       "msg-1",
		Sequence: 1,
		Payload:  []byte("test payload"),
		State:    types.StateQueued,
	}

	op := Operation{
		Type:    OpEnqueue,
		Message: msg,
	}

	data, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("failed to marshal operation: %v", err)
	}

	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  data,
	}

	result := fsm.Apply(log)
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("apply enqueue failed: %v", err)
	}

	// Verify message was stored
	ctx := context.Background()
	retrieved, err := store.GetMessage(ctx, "test-queue", "msg-1")
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}

	if string(retrieved.Payload) != "test payload" {
		t.Errorf("expected payload 'test payload', got %s", retrieved.Payload)
	}
}

func TestFSM_ApplyAck(t *testing.T) {
	fsm, store, cleanup := setupTestFSM(t)
	defer cleanup()

	ctx := context.Background()

	// First enqueue a message
	msg := &types.Message{
		ID:       "msg-1",
		Sequence: 1,
		Payload:  []byte("test payload"),
		State:    types.StateQueued,
	}

	if err := store.Enqueue(ctx, "test-queue", msg); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Mark as inflight
	deliveryState := &types.DeliveryState{
		MessageID:   "msg-1",
		QueueName:   "test-queue",
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}
	if err := store.MarkInflight(ctx, deliveryState); err != nil {
		t.Fatalf("failed to mark inflight: %v", err)
	}

	// Apply ACK operation
	op := Operation{
		Type:      OpAck,
		MessageID: "msg-1",
	}

	data, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("failed to marshal operation: %v", err)
	}

	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  data,
	}

	result := fsm.Apply(log)
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("apply ack failed: %v", err)
	}

	// Verify message was deleted
	_, err = store.GetMessage(ctx, "test-queue", "msg-1")
	if err != storage.ErrMessageNotFound {
		t.Errorf("expected ErrMessageNotFound, got %v", err)
	}
}

func TestFSM_ApplyNack(t *testing.T) {
	fsm, store, cleanup := setupTestFSM(t)
	defer cleanup()

	ctx := context.Background()

	// First enqueue a message
	msg := &types.Message{
		ID:       "msg-1",
		Sequence: 1,
		Payload:  []byte("test payload"),
		State:    types.StateQueued,
	}

	if err := store.Enqueue(ctx, "test-queue", msg); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Mark as inflight
	deliveryState := &types.DeliveryState{
		MessageID:   "msg-1",
		QueueName:   "test-queue",
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}
	if err := store.MarkInflight(ctx, deliveryState); err != nil {
		t.Fatalf("failed to mark inflight: %v", err)
	}

	// Apply NACK operation
	op := Operation{
		Type:      OpNack,
		MessageID: "msg-1",
		Reason:    "delivery failed",
	}

	data, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("failed to marshal operation: %v", err)
	}

	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  data,
	}

	result := fsm.Apply(log)
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("apply nack failed: %v", err)
	}

	// Verify message state was updated to retry
	retrieved, err := store.GetMessage(ctx, "test-queue", "msg-1")
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}

	if retrieved.State != types.StateRetry {
		t.Errorf("expected state %v, got %v", types.StateRetry, retrieved.State)
	}

	if retrieved.RetryCount != 1 {
		t.Errorf("expected retry count 1, got %d", retrieved.RetryCount)
	}

	if retrieved.FailureReason != "delivery failed" {
		t.Errorf("expected reason 'delivery failed', got %s", retrieved.FailureReason)
	}
}

func TestFSM_ApplyReject(t *testing.T) {
	fsm, store, cleanup := setupTestFSM(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue a message
	msg := &types.Message{
		ID:       "msg-1",
		Sequence: 1,
		Payload:  []byte("test payload"),
		State:    types.StateQueued,
	}

	if err := store.Enqueue(ctx, "test-queue", msg); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Mark as inflight
	deliveryState := &types.DeliveryState{
		MessageID:   "msg-1",
		QueueName:   "test-queue",
		PartitionID: 0,
		ConsumerID:  "consumer-1",
		DeliveredAt: time.Now(),
		Timeout:     time.Now().Add(30 * time.Second),
	}
	if err := store.MarkInflight(ctx, deliveryState); err != nil {
		t.Fatalf("failed to mark inflight: %v", err)
	}

	// Apply REJECT operation
	op := Operation{
		Type:      OpReject,
		MessageID: "msg-1",
		Reason:    "max retries exceeded",
	}

	data, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("failed to marshal operation: %v", err)
	}

	log := &raft.Log{
		Index: 1,
		Term:  1,
		Type:  raft.LogCommand,
		Data:  data,
	}

	result := fsm.Apply(log)
	if err, ok := result.(error); ok && err != nil {
		t.Fatalf("apply reject failed: %v", err)
	}

	// Verify message was removed from main queue
	_, err = store.GetMessage(ctx, "test-queue", "msg-1")
	if err != storage.ErrMessageNotFound {
		t.Errorf("expected ErrMessageNotFound, got %v", err)
	}
}

func TestFSM_SnapshotRestore(t *testing.T) {
	fsm, store, cleanup := setupTestFSM(t)
	defer cleanup()

	ctx := context.Background()

	// Enqueue some messages to test snapshot contains actual data
	messages := []*types.Message{
		{
			ID:          "msg-1",
			Sequence:    1,
			PartitionID: 0,
			Payload:     []byte("payload 1"),
			State:       types.StateQueued,
			CreatedAt:   time.Now(),
		},
		{
			ID:          "msg-2",
			Sequence:    2,
			PartitionID: 0,
			Payload:     []byte("payload 2"),
			State:       types.StateQueued,
			CreatedAt:   time.Now(),
		},
		{
			ID:          "msg-3",
			Sequence:    3,
			PartitionID: 0,
			Payload:     []byte("payload 3"),
			State:       types.StateQueued,
			CreatedAt:   time.Now(),
		},
	}

	for _, msg := range messages {
		if err := store.Enqueue(ctx, "test-queue", msg); err != nil {
			t.Fatalf("failed to enqueue message %s: %v", msg.ID, err)
		}
	}

	// Create snapshot
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("failed to create snapshot: %v", err)
	}

	// Persist snapshot
	sink := &mockSnapshotSink{data: make([]byte, 0)}
	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("failed to persist snapshot: %v", err)
	}

	// Verify snapshot contains data
	if len(sink.data) == 0 {
		t.Fatal("snapshot data is empty")
	}

	// Decode snapshot to verify it contains our messages
	var snapshotData SnapshotData
	if err := json.Unmarshal(sink.data, &snapshotData); err != nil {
		t.Fatalf("failed to unmarshal snapshot: %v", err)
	}

	if len(snapshotData.Messages) != len(messages) {
		t.Errorf("expected %d messages in snapshot, got %d", len(messages), len(snapshotData.Messages))
	}

	// Create new FSM for restore
	dir2, err := os.MkdirTemp("", "fsm-restore-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir2)

	opts := badger.DefaultOptions(dir2)
	opts.Logger = nil
	db2, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("failed to open badger: %v", err)
	}
	defer db2.Close()

	store2 := badgerstore.New(db2)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	fsm2 := NewPartitionFSM("test-queue", 0, store2, logger)

	// Restore from snapshot
	if err := fsm2.Restore(&mockReadCloser{data: sink.data}); err != nil {
		t.Fatalf("failed to restore: %v", err)
	}

	// Verify all messages were restored
	for _, orig := range messages {
		restored, err := store2.GetMessage(ctx, "test-queue", orig.ID)
		if err != nil {
			t.Errorf("failed to get restored message %s: %v", orig.ID, err)
			continue
		}

		if string(restored.Payload) != string(orig.Payload) {
			t.Errorf("message %s payload mismatch: expected %s, got %s",
				orig.ID, string(orig.Payload), string(restored.Payload))
		}

		if restored.Sequence != orig.Sequence {
			t.Errorf("message %s sequence mismatch: expected %d, got %d",
				orig.ID, orig.Sequence, restored.Sequence)
		}
	}
}

func TestFSM_ISRTracking(t *testing.T) {
	fsm, _, cleanup := setupTestFSM(t)
	defer cleanup()

	// Update ISR
	fsm.UpdateISR("node1")
	fsm.UpdateISR("node2")
	fsm.UpdateISR("node3")

	// Check ISR count
	count := fsm.ISRCount()
	if count != 3 {
		t.Errorf("expected ISR count 3, got %d", count)
	}

	// Get ISR list
	replicas := fsm.GetISR()
	if len(replicas) != 3 {
		t.Errorf("expected 3 replicas, got %d", len(replicas))
	}

	// Verify all replicas are present
	replicaMap := make(map[string]bool)
	for _, r := range replicas {
		replicaMap[r] = true
	}

	for _, expected := range []string{"node1", "node2", "node3"} {
		if !replicaMap[expected] {
			t.Errorf("expected replica %s not found", expected)
		}
	}
}

// Mock implementations for testing

type mockSnapshotSink struct {
	data []byte
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *mockSnapshotSink) Close() error {
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snapshot"
}

func (m *mockSnapshotSink) Cancel() error {
	m.data = nil
	return nil
}

type mockReadCloser struct {
	data []byte
	pos  int
}

func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	if m.pos >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.pos:])
	m.pos += n
	return n, nil
}

func (m *mockReadCloser) Close() error {
	return nil
}
