// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/queue/consumer"
	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

type stateMachineContractBackend struct {
	queue storage.QueueStore
	group storage.ConsumerGroupStore
}

func TestStateMachineStorageContract(t *testing.T) {
	for name, factory := range stateMachineBackendFactories() {
		t.Run(name, func(t *testing.T) {
			runStateMachineStorageContract(t, factory(t))
		})
	}
}

func stateMachineBackendFactories() map[string]func(*testing.T) stateMachineContractBackend {
	return map[string]func(*testing.T) stateMachineContractBackend{
		"memory": func(*testing.T) stateMachineContractBackend {
			return stateMachineContractBackend{queue: memlog.New(), group: newMockGroupStore()}
		},
		"logstorage": func(t *testing.T) stateMachineContractBackend {
			store, err := logstorage.NewAdapter(t.TempDir(), logstorage.DefaultAdapterConfig())
			if err != nil {
				t.Fatalf("create logstorage adapter: %v", err)
			}
			t.Cleanup(func() {
				if err := store.Close(); err != nil {
					t.Errorf("close logstorage adapter: %v", err)
				}
			})
			return stateMachineContractBackend{queue: store, group: store}
		},
	}
}

func TestMQTTAndAMQPManagerAdapterContract(t *testing.T) {
	for name, factory := range stateMachineBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			ctx := context.Background()
			manager := NewManager(
				backend.queue,
				backend.group,
				nil,
				DefaultConfig(),
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				nil,
			)
			config := types.DefaultQueueConfig(testQueueJobs, "jobs/#")
			config.DLQConfig.Enabled = true
			if err := manager.CreateQueue(ctx, config); err != nil {
				t.Fatalf("create queue: %v", err)
			}
			group := types.NewConsumerGroupState(testQueueJobs, testGroupWorkers, "")
			if err := backend.group.CreateConsumerGroup(ctx, group); err != nil {
				t.Fatalf("create group: %v", err)
			}
			for _, id := range []string{testConsumerOne, testConsumerTwo} {
				if err := backend.group.RegisterConsumer(ctx, testQueueJobs, testGroupWorkers, &types.ConsumerInfo{ID: id, ClientID: id}); err != nil {
					t.Fatalf("register %s: %v", id, err)
				}
			}
			if _, err := manager.StateMachine().Append(ctx, AppendCommand{
				QueueName: testQueueJobs,
				Messages: []types.PublishRequest{
					{Topic: "jobs/0", Payload: []byte("zero")},
					{Topic: "jobs/1", Payload: []byte("one")},
					{Topic: "jobs/2", Payload: []byte("two")},
				},
				AtomicBatch: true,
			}); err != nil {
				t.Fatalf("append: %v", err)
			}
			if _, err := manager.StateMachine().Consume(ctx, ConsumeCommand{
				QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerOne, Limit: 3,
			}); err != nil {
				t.Fatalf("consume: %v", err)
			}

			// MQTT and both AMQP adapters call these frozen Manager methods.
			if err := manager.Nack(ctx, testQueueJobs, "jobs:0", testGroupWorkers); err != nil {
				t.Fatalf("legacy nack: %v", err)
			}
			claimed, err := manager.StateMachine().Claim(ctx, ClaimCommand{
				QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerTwo, MinIdle: time.Second, Limit: 1,
			})
			if err != nil || len(claimed.Offsets) != 1 || claimed.Offsets[0] != 0 {
				t.Fatalf("claim after legacy nack = %+v, error = %v", claimed, err)
			}
			if err := manager.Ack(ctx, testQueueJobs, "jobs:0", testGroupWorkers); err != nil {
				t.Fatalf("legacy ack: %v", err)
			}
			if err := manager.Reject(ctx, testQueueJobs, "jobs:1", testGroupWorkers, "invalid"); err != nil {
				t.Fatalf("legacy reject: %v", err)
			}
			if err := manager.Ack(ctx, testQueueJobs, "jobs:2", testGroupWorkers); err != nil {
				t.Fatalf("legacy final ack: %v", err)
			}
			final, err := backend.group.GetConsumerGroup(ctx, testQueueJobs, testGroupWorkers)
			if err != nil {
				t.Fatalf("get final group: %v", err)
			}
			if final.PendingCount() != 0 || final.GetCursor().Committed != 3 {
				t.Fatalf("final group pending=%d cursor=%+v", final.PendingCount(), final.GetCursor())
			}
		})
	}
}

func runStateMachineStorageContract(t *testing.T, backend stateMachineContractBackend) {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager := NewManager(backend.queue, backend.group, nil, DefaultConfig(), logger, nil)
	machine := manager.StateMachine()

	config := types.DefaultQueueConfig(testQueueJobs, "jobs/#")
	config.DLQConfig.Enabled = true
	if err := manager.CreateQueue(ctx, config); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	group := types.NewConsumerGroupState(testQueueJobs, testGroupWorkers, "")
	if err := backend.group.CreateConsumerGroup(ctx, group); err != nil {
		t.Fatalf("create consumer group: %v", err)
	}
	for _, id := range []string{testConsumerOne, testConsumerTwo} {
		if err := backend.group.RegisterConsumer(ctx, testQueueJobs, testGroupWorkers, &types.ConsumerInfo{ID: id, ClientID: id}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	appended, err := machine.Append(ctx, AppendCommand{
		QueueName:   testQueueJobs,
		AtomicBatch: true,
		Messages: []types.PublishRequest{
			{Topic: "jobs/0", Payload: []byte("zero")},
			{Topic: "jobs/1", Payload: []byte("one")},
			{Topic: "jobs/2", Payload: []byte("two")},
			{Topic: "jobs/3", Payload: []byte("three")},
		},
	})
	if err != nil {
		t.Fatalf("append batch: %v", err)
	}
	if appended.FirstOffset != 0 || appended.LastOffset != 3 || appended.Count != 4 {
		t.Fatalf("append outcome = %+v, want offsets 0..3", appended)
	}

	seek, err := machine.Seek(ctx, SeekCommand{QueueName: testQueueJobs, Kind: SeekOffset, Offset: 99})
	if err != nil {
		t.Fatalf("seek offset: %v", err)
	}
	if seek.Offset != 4 {
		t.Fatalf("seek offset = %d, want tail 4", seek.Offset)
	}
	first, err := backend.queue.Read(ctx, testQueueJobs, 0)
	if err != nil {
		t.Fatalf("read first message: %v", err)
	}
	seek, err = machine.Seek(ctx, SeekCommand{QueueName: testQueueJobs, Kind: SeekTimestamp, Timestamp: first.CreatedAt})
	if err != nil {
		t.Fatalf("seek timestamp: %v", err)
	}
	if seek.Offset != 0 || !seek.ExactMatch {
		t.Fatalf("timestamp seek = %+v, want exact offset 0", seek)
	}

	consumed, err := machine.Consume(ctx, ConsumeCommand{
		QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerOne, Limit: 4,
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if consumed.Mode != types.GroupModeQueue || consumed.CommitRequired || len(consumed.Messages) != 4 || consumed.NextOffset != 4 {
		t.Fatalf("consume outcome = %+v", consumed)
	}

	acked, err := machine.Ack(ctx, AckCommand{
		QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerOne, Offsets: []uint64{0},
	})
	if err != nil {
		t.Fatalf("ack offset 0: %v", err)
	}
	if acked.Committed != 1 {
		t.Fatalf("committed after ack = %d, want 1", acked.Committed)
	}
	if _, err := machine.Nack(ctx, NackCommand{
		QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerTwo, Offsets: []uint64{1},
	}); !errors.Is(err, consumer.ErrConsumerNotFound) {
		t.Fatalf("wrong-owner nack error = %v, want consumer not found", err)
	}

	if _, err := machine.Nack(ctx, NackCommand{
		QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerOne, Offsets: []uint64{1},
	}); err != nil {
		t.Fatalf("nack offset 1: %v", err)
	}
	if _, err := machine.Nack(ctx, NackCommand{
		QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerOne, Offsets: []uint64{2}, Delay: time.Second,
	}); err != nil {
		t.Fatalf("delayed nack offset 2: %v", err)
	}

	claimed, err := machine.Claim(ctx, ClaimCommand{
		QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerTwo, MinIdle: time.Second, Limit: 1,
	})
	if err != nil {
		t.Fatalf("claim nacked message: %v", err)
	}
	if len(claimed.Offsets) != 1 || claimed.Offsets[0] != 1 {
		t.Fatalf("claimed offsets = %v, want [1]", claimed.Offsets)
	}

	if _, err := machine.Reject(ctx, RejectCommand{
		QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: testConsumerOne, Offsets: []uint64{2}, Reason: "invalid",
	}); err != nil {
		t.Fatalf("reject offset 2: %v", err)
	}
	if count, err := backend.queue.Count(ctx, "$dlq/jobs"); err != nil || count != 1 {
		t.Fatalf("DLQ count = %d, err = %v; want 1", count, err)
	}

	for consumerID, offset := range map[string]uint64{testConsumerTwo: 1, testConsumerOne: 3} {
		if _, err := machine.Ack(ctx, AckCommand{
			QueueName: testQueueJobs, GroupID: testGroupWorkers, ConsumerID: consumerID, Offsets: []uint64{offset},
		}); err != nil {
			t.Fatalf("ack offset %d as %s: %v", offset, consumerID, err)
		}
	}
	final, err := backend.group.GetConsumerGroup(ctx, testQueueJobs, testGroupWorkers)
	if err != nil {
		t.Fatalf("get final group: %v", err)
	}
	if final.PendingCount() != 0 || final.GetCursor().Committed != 4 {
		t.Fatalf("final group pending=%d cursor=%+v", final.PendingCount(), final.GetCursor())
	}
}
