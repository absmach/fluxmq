// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

// benchGroupStore adapts the in-memory log store to storage.ConsumerGroupStore.
// It is declared here rather than shared so this benchmark compiles unchanged
// against revisions that predate the keyed-locking work, which is what makes a
// before/after comparison possible.
type benchGroupStore struct {
	*memlog.Store
}

func (s *benchGroupStore) RegisterConsumer(context.Context, string, string, *types.ConsumerInfo) error {
	return nil
}

func (s *benchGroupStore) UnregisterConsumer(context.Context, string, string, string) error {
	return nil
}

func (s *benchGroupStore) ListConsumers(ctx context.Context, queueName, groupID string) ([]*types.ConsumerInfo, error) {
	consumers, err := s.Store.ListConsumers(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}
	infos := make([]*types.ConsumerInfo, 0, len(consumers))
	for _, c := range consumers {
		infos = append(infos, &types.ConsumerInfo{ID: c.ID, ClientID: c.ClientID})
	}
	return infos, nil
}

const benchGroups = 64

// benchFixture builds benchGroups independent consumer groups, each on its own
// queue, so parallel operations never touch the same group state.
func benchFixture(b *testing.B) (*Manager, []string) {
	b.Helper()

	ctx := context.Background()
	store := &benchGroupStore{Store: memlog.New()}

	names := make([]string, benchGroups)
	for i := range names {
		name := "bench-queue-" + strconv.Itoa(i)
		names[i] = name

		if err := store.CreateQueue(ctx, types.DefaultQueueConfig(name, name+"/#")); err != nil {
			b.Fatalf("create queue: %v", err)
		}
		envelope := message.New(name, []byte("payload"))
		if _, err := store.Append(ctx, name, envelope); err != nil {
			b.Fatalf("append: %v", err)
		}
		group := types.NewConsumerGroupState(name, "workers", "")
		// CommitOffset is a stream-mode operation; it is used here because it is
		// the shortest path that takes the manager lock and then touches the
		// group store.
		group.Mode = types.GroupModeStream
		if err := store.CreateConsumerGroup(ctx, group); err != nil {
			b.Fatalf("create group: %v", err)
		}
	}

	manager := NewManager(store, store, Config{
		VisibilityTimeout:  time.Minute,
		MaxDeliveryCount:   5,
		ClaimBatchSize:     10,
		StealBatchSize:     5,
		AutoCommitInterval: 0,
		MaxPELSize:         1000,
	})

	return manager, names
}

// BenchmarkConsumerGroupContention measures whether operations on unrelated
// consumer groups proceed independently.
//
// Every iteration targets a different group, so a correctly scoped lock lets
// them run concurrently. A single manager-wide mutex serialises them all, and
// the cost of that shows up here rather than in the single-group benchmarks.
func BenchmarkConsumerGroupContention(b *testing.B) {
	manager, names := benchFixture(b)
	ctx := context.Background()

	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			name := names[next.Add(1)%benchGroups]
			if err := manager.CommitOffset(ctx, name, "workers", 0); err != nil {
				b.Errorf("commit offset: %v", err)
				return
			}
		}
	})
}

// BenchmarkConsumerSingleGroupContention is the control: every iteration hits
// the same group, so it stays serialised however the lock is scoped. It exists
// so a change that only moves contention around is distinguishable from one
// that removes it.
func BenchmarkConsumerSingleGroupContention(b *testing.B) {
	manager, names := benchFixture(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := manager.CommitOffset(ctx, names[0], "workers", 0); err != nil {
				b.Errorf("commit offset: %v", err)
				return
			}
		}
	})
}

// benchDurableFixture is benchFixture against the persistent log store, so the
// critical section is the storage work a deployment actually performs rather
// than a map lookup. The lock overhead is fixed; what changes is what it is
// being compared against.
func benchDurableFixture(b *testing.B) (*Manager, []string) {
	b.Helper()

	ctx := context.Background()
	store, err := logstorage.NewAdapter(b.TempDir(), logstorage.DefaultAdapterConfig())
	if err != nil {
		b.Fatalf("create adapter: %v", err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Errorf("close adapter: %v", err)
		}
	})

	names := make([]string, benchGroups)
	for i := range names {
		name := "bench-queue-" + strconv.Itoa(i)
		names[i] = name

		if err := store.CreateQueue(ctx, types.DefaultQueueConfig(name, name+"/#")); err != nil {
			b.Fatalf("create queue: %v", err)
		}
		if _, err := store.Append(ctx, name, message.New(name, []byte("payload"))); err != nil {
			b.Fatalf("append: %v", err)
		}
		group := types.NewConsumerGroupState(name, "workers", "")
		group.Mode = types.GroupModeStream
		if err := store.CreateConsumerGroup(ctx, group); err != nil {
			b.Fatalf("create group: %v", err)
		}
	}

	manager := NewManager(store, store, Config{
		VisibilityTimeout:  time.Minute,
		MaxDeliveryCount:   5,
		ClaimBatchSize:     10,
		StealBatchSize:     5,
		AutoCommitInterval: 0,
		MaxPELSize:         1000,
	})

	return manager, names
}

func BenchmarkConsumerGroupContentionDurable(b *testing.B) {
	manager, names := benchDurableFixture(b)
	ctx := context.Background()

	var next atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			name := names[next.Add(1)%benchGroups]
			if err := manager.CommitOffset(ctx, name, "workers", 0); err != nil {
				b.Errorf("commit offset: %v", err)
				return
			}
		}
	})
}

func BenchmarkConsumerSingleGroupContentionDurable(b *testing.B) {
	manager, names := benchDurableFixture(b)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := manager.CommitOffset(ctx, names[0], "workers", 0); err != nil {
				b.Errorf("commit offset: %v", err)
				return
			}
		}
	})
}

// benchStealFixture builds one group holding a full pending list whose entries
// are all stealable, which is what the steal sweep walks.
func benchStealFixture(b *testing.B, pending int) (*Manager, *types.ConsumerGroup) {
	b.Helper()

	ctx := context.Background()
	store := &benchGroupStore{Store: memlog.New()}
	name := "steal-queue"

	if err := store.CreateQueue(ctx, types.DefaultQueueConfig(name, name+"/#")); err != nil {
		b.Fatalf("create queue: %v", err)
	}

	entries := make([]*types.PendingEntry, 0, pending)
	for i := range pending {
		if _, err := store.Append(ctx, name, message.New(name, []byte("payload"))); err != nil {
			b.Fatalf("append: %v", err)
		}
		entries = append(entries, &types.PendingEntry{
			Offset:     uint64(i),
			ConsumerID: "owner",
			ClaimedAt:  time.Now().Add(-time.Hour),
		})
	}

	group := types.NewConsumerGroupState(name, "workers", "")
	group.ReplacePEL(map[string][]*types.PendingEntry{"owner": entries})
	if err := store.CreateConsumerGroup(ctx, group); err != nil {
		b.Fatalf("create group: %v", err)
	}

	manager := NewManager(store, store, Config{
		VisibilityTimeout: time.Millisecond,
		MaxDeliveryCount:  1000,
		ClaimBatchSize:    10,
		StealBatchSize:    5,
		MaxPELSize:        100000,
	})

	return manager, group
}

// BenchmarkStealSweepWithoutPoison measures the steal path for a group with no
// poison entries, which is the overwhelmingly common case and the one the
// poison gauge bookkeeping must not tax. A group with nothing tracked must not
// pay to walk its pending list.
func BenchmarkStealSweepWithoutPoison(b *testing.B) {
	manager, group := benchStealFixture(b, 512)
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		msg, err := manager.stealWork(ctx, group, "thief", nil)
		if err != nil {
			b.Fatalf("steal: %v", err)
		}
		message.Release(msg)
		// Hand it back so the next iteration has the same work to do.
		group.TransferPending(msg.Broker.Queue.Offset, "thief", "owner")
	}
}

// BenchmarkStealSweepAllPoison measures the claim path for a group whose whole
// pending list is poison and waiting to be swept.
//
// It is not an A/B against the old inline transfer: that version drained the
// pending list as it went, so it measured a shrinking population while this one
// measures a fixed one. What it is good for is the cost of re-examining entries
// that are waiting, which the claim path pays on every claim until a sweep
// clears them.
func BenchmarkStealSweepAllPoison(b *testing.B) {
	manager, group := benchStealFixture(b, 128)
	manager.config.MaxDeliveryCount = 0
	manager.config.OnDLQ = func(context.Context, string, string, *message.Envelope, uint64, int, string) error {
		return nil
	}
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if _, err := manager.stealWork(ctx, group, "thief", nil); err != nil && !errors.Is(err, ErrNoMessages) {
			b.Fatalf("steal: %v", err)
		}
	}
}
