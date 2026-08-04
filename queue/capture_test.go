// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/absmach/fluxmq/queue/storage"
	memlog "github.com/absmach/fluxmq/queue/storage/memory/log"
	"github.com/absmach/fluxmq/queue/types"
)

// blockingQueueStore holds every append until it is released, standing in for a
// store whose disk has stalled.
type blockingQueueStore struct {
	storage.QueueStore
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (s *blockingQueueStore) Append(ctx context.Context, queueName string, msg *types.Message) (uint64, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	return s.QueueStore.Append(ctx, queueName, msg)
}

// The whole point of dispatching capture is that a queue whose store has stalled
// cannot hold up the publisher or the subscribers of a matching topic. Nothing
// else in the package proves that: an inline capture would simply never return.
func TestCaptureDoesNotBlockThePublishPath(t *testing.T) {
	blocking := &blockingQueueStore{
		QueueStore: memlog.New(),
		release:    make(chan struct{}),
		entered:    make(chan struct{}),
	}
	mgr := NewManager(blocking, newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	mgr.capture.Start(ctx)
	t.Cleanup(func() {
		close(blocking.release)
		mgr.capture.Stop()
	})

	// Publish once and wait for the worker to be stuck inside the store, so the
	// stall is real rather than assumed by timing.
	if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
		Topic:   testCapturedTopic,
		Payload: []byte("payload"),
	}); err != nil {
		t.Fatalf("PublishToMatchingQueues failed: %v", err)
	}
	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("capture worker never reached the store")
	}

	// With one worker wedged, further publishes must still return promptly.
	// They are only queued, so this measures the publish path and nothing else.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
				Topic:   testCapturedTopic,
				Payload: []byte("payload"),
			}); err != nil {
				t.Errorf("PublishToMatchingQueues failed: %v", err)
				return
			}
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a stalled queue store blocked the publish path")
	}
}

// A backlog that cannot drain must bound its memory by dropping, and every drop
// has to be visible: it is a lost message that no error reports.
func TestCaptureDropsAndCountsWhenBacklogIsFull(t *testing.T) {
	const depth = 4

	blocking := &blockingQueueStore{
		QueueStore: memlog.New(),
		release:    make(chan struct{}),
		entered:    make(chan struct{}),
	}
	config := DefaultConfig()
	// One lane so the wedged worker is the only one, and a shallow backlog so
	// saturation is reached deterministically.
	config.CaptureWorkers = 1
	config.CaptureQueueDepth = depth
	mgr := NewManager(blocking, newMockGroupStore(), nil, config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	mgr.capture.Start(ctx)
	t.Cleanup(func() {
		close(blocking.release)
		mgr.capture.Stop()
	})

	publish := func() {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Topic:   testCapturedTopic,
			Payload: []byte("payload"),
		}); err != nil {
			t.Fatalf("PublishToMatchingQueues failed: %v", err)
		}
	}

	publish()
	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("capture worker never reached the store")
	}

	// The worker holds one job; the lane buffers depth more. Everything past
	// that has nowhere to go.
	const overflow = 10
	for range depth + overflow {
		publish()
	}

	if got := mgr.GetMetrics().CaptureDropped; got != overflow {
		t.Fatalf("capture dropped = %d, want %d", got, overflow)
	}
	if got := mgr.GetMetrics().CaptureFailures; got != 0 {
		t.Fatalf("capture failures = %d, want 0; a drop is not an append failure", got)
	}
}

// A queue is always handled by the same lane, so its appends keep publish order.
func TestCapturePreservesPerQueueOrder(t *testing.T) {
	const messages = 200

	logStore := memlog.New()
	mgr := NewManager(logStore, newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}

	flushCapture(t, mgr, func() {
		for i := range messages {
			if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
				Topic:   testCapturedTopic,
				Payload: []byte{byte(i % 256)},
			}); err != nil {
				t.Fatalf("PublishToMatchingQueues failed: %v", err)
			}
		}
	})

	count, err := logStore.Count(ctx, testCaptureQueue)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != messages {
		t.Fatalf("stored %d messages, want %d", count, messages)
	}
	for i := range messages {
		msg, err := logStore.Read(ctx, testCaptureQueue, uint64(i))
		if err != nil {
			t.Fatalf("Read offset %d failed: %v", i, err)
		}
		if got := msg.GetPayload()[0]; got != byte(i%256) {
			t.Fatalf("offset %d holds payload %d, want %d; capture reordered a queue", i, got, i%256)
		}
	}
}
