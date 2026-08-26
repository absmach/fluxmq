// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package raft

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/message"
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

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// encodeOperationEnvelope encodes an envelope the way a producer does before it
// reaches the Raft log, and releases the caller's copy.
func encodeOperationEnvelope(t *testing.T, envelope *message.Envelope) []byte {
	t.Helper()

	encoded, err := message.MarshalBinary(envelope)
	if err != nil {
		t.Fatalf("encode operation envelope: %v", err)
	}
	message.Release(envelope)

	return encoded
}

func newTestLogFSM() (*LogFSM, *memlog.Store) {
	queueStore := memlog.New()
	return NewLogFSM(queueStore, noopGroupStore{}, discardLogger()), queueStore
}

func newQueuedEnvelope(id, topic string, data []byte) *message.Envelope {
	envelope := message.New(topic, data)
	envelope.PublisherMeta.MessageID = id
	envelope.BrokerMeta.Queue.State = message.QueueStateQueued
	envelope.BrokerMeta.Queue.CreatedAt = time.Now()
	return envelope
}

func TestLogFSM_ApplyAppendAutoCreatesMissingQueue(t *testing.T) {
	fsm, store := newTestLogFSM()
	ctx := context.Background()
	queueName := "demo-events"

	result := fsm.applyAppend(ctx, &Operation{
		QueueName: queueName,
		Message:   encodeOperationEnvelope(t, newQueuedEnvelope("msg-1", "$queue/"+queueName, []byte("payload-1"))),
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
	if got := string(msg.PayloadBytes()); got != "payload-1" {
		t.Fatalf("unexpected payload: %q", got)
	}
}

// The operation carries the canonical encoded envelope as protobuf bytes. This
// pins that the pooled payload survives the wire round trip.
func TestLogFSM_ApplyAppendWithPayloadBufferAfterOperationRoundTrip(t *testing.T) {
	fsm, store := newTestLogFSM()
	ctx := context.Background()
	queueName := "demo-buffered"

	msg := newQueuedEnvelope("buffered-msg-1", "$queue/"+queueName, []byte("buffered payload"))

	data, err := marshalOperation(&Operation{
		Type:      OpAppend,
		QueueName: queueName,
		Message:   encodeOperationEnvelope(t, msg),
	})
	if err != nil {
		t.Fatalf("marshal append operation failed: %v", err)
	}

	decoded, err := unmarshalOperation(data)
	if err != nil {
		t.Fatalf("unmarshal append operation failed: %v", err)
	}

	result := fsm.applyAppend(ctx, decoded)
	if result.Error != nil {
		t.Fatalf("applyAppend returned error: %v", result.Error)
	}

	got, err := store.Read(ctx, queueName, 0)
	if err != nil {
		t.Fatalf("expected appended message at offset 0, got error: %v", err)
	}
	if gotPayload := string(got.PayloadBytes()); gotPayload != "buffered payload" {
		t.Fatalf("expected buffered payload, got %q", gotPayload)
	}
}
