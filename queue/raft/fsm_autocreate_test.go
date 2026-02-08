// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

type noopGroupStore struct{}

func (noopGroupStore) CreateConsumerGroup(context.Context, *types.ConsumerGroup) error {
	return nil
}
func (noopGroupStore) GetConsumerGroup(context.Context, string, string) (*types.ConsumerGroup, error) {
	return nil, storage.ErrConsumerNotFound
}
func (noopGroupStore) UpdateConsumerGroup(context.Context, *types.ConsumerGroup) error {
	return nil
}
func (noopGroupStore) DeleteConsumerGroup(context.Context, string, string) error {
	return nil
}
func (noopGroupStore) ListConsumerGroups(context.Context, string) ([]*types.ConsumerGroup, error) {
	return nil, nil
}
func (noopGroupStore) AddPendingEntry(context.Context, string, string, *types.PendingEntry) error {
	return nil
}
func (noopGroupStore) RemovePendingEntry(context.Context, string, string, string, uint64) error {
	return nil
}
func (noopGroupStore) GetPendingEntries(context.Context, string, string, string) ([]*types.PendingEntry, error) {
	return nil, nil
}
func (noopGroupStore) GetAllPendingEntries(context.Context, string, string) ([]*types.PendingEntry, error) {
	return nil, nil
}
func (noopGroupStore) TransferPendingEntry(context.Context, string, string, uint64, string, string) error {
	return nil
}
func (noopGroupStore) UpdateCursor(context.Context, string, string, uint64) error {
	return nil
}
func (noopGroupStore) UpdateCommitted(context.Context, string, string, uint64) error {
	return nil
}
func (noopGroupStore) RegisterConsumer(context.Context, string, string, *types.ConsumerInfo) error {
	return nil
}
func (noopGroupStore) UnregisterConsumer(context.Context, string, string, string) error {
	return nil
}
func (noopGroupStore) ListConsumers(context.Context, string, string) ([]*types.ConsumerInfo, error) {
	return nil, nil
}

func newTestLogFSM() (*LogFSM, *memlog.Store) {
	queueStore := memlog.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewLogFSM(queueStore, noopGroupStore{}, logger), queueStore
}

func TestLogFSM_ApplyAppendAutoCreatesMissingQueue(t *testing.T) {
	fsm, store := newTestLogFSM()
	ctx := context.Background()
	queueName := "demo-events"

	result := fsm.applyAppend(ctx, &Operation{
		QueueName: queueName,
		Message: &types.Message{
			ID:        "msg-1",
			Topic:     "$queue/" + queueName,
			Payload:   []byte("payload-1"),
			State:     types.StateQueued,
			CreatedAt: time.Now(),
		},
	})
	if result.Error != nil {
		t.Fatalf("applyAppend returned error: %v", result.Error)
	}
	if result.Offset != 0 {
		t.Fatalf("expected first offset to be 0, got %d", result.Offset)
	}

	if _, err := store.GetQueue(ctx, queueName); err != nil {
		t.Fatalf("expected queue %q to be auto-created, got error: %v", queueName, err)
	}

	msg, err := store.Read(ctx, queueName, 0)
	if err != nil {
		t.Fatalf("expected appended message at offset 0, got error: %v", err)
	}
	if got := string(msg.GetPayload()); got != "payload-1" {
		t.Fatalf("unexpected payload: %q", got)
	}
}

func TestLogFSM_ApplyAppendBatchAutoCreatesMissingQueue(t *testing.T) {
	fsm, store := newTestLogFSM()
	ctx := context.Background()
	queueName := "demo-batch"

	result := fsm.applyAppendBatch(ctx, &Operation{
		QueueName: queueName,
		Messages: []*types.Message{
			{
				ID:        "msg-1",
				Topic:     "$queue/" + queueName,
				Payload:   []byte("one"),
				State:     types.StateQueued,
				CreatedAt: time.Now(),
			},
			{
				ID:        "msg-2",
				Topic:     "$queue/" + queueName,
				Payload:   []byte("two"),
				State:     types.StateQueued,
				CreatedAt: time.Now(),
			},
		},
	})
	if result.Error != nil {
		t.Fatalf("applyAppendBatch returned error: %v", result.Error)
	}
	if result.Offset != 0 {
		t.Fatalf("expected first offset to be 0, got %d", result.Offset)
	}

	count, err := store.Count(ctx, queueName)
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 messages, got %d", count)
	}
}
