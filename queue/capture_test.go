// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
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

// countingQueueStore records how many appends actually reached storage.
type countingQueueStore struct {
	storage.QueueStore
	appended atomic.Int64
}

func (s *countingQueueStore) Append(ctx context.Context, queueName string, msg *types.Message) (uint64, error) {
	s.appended.Add(1)
	return s.QueueStore.Append(ctx, queueName, msg)
}

// Every capture is either written or counted as dropped. A job accepted by a
// send that raced shutdown would be neither: it would sit in a lane no worker
// reads again, lost with nothing to show for it.
func TestCaptureAccountsForEveryJobAcrossShutdown(t *testing.T) {
	const publishers = 8
	const perPublisher = 250

	counting := &countingQueueStore{QueueStore: memlog.New()}
	mgr := NewManager(counting, newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	mgr.capture.Start(ctx)

	// Publish hard from several goroutines while shutdown runs underneath them,
	// so sends land on both sides of the moment acceptance closes.
	var wg sync.WaitGroup
	for range publishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perPublisher {
				if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
					Topic:   testCapturedTopic,
					Payload: []byte("payload"),
				}); err != nil {
					t.Errorf("PublishToMatchingQueues failed: %v", err)
					return
				}
			}
		}()
	}

	mgr.capture.Stop()
	wg.Wait()
	// Publishes that finished after Stop are refused and counted; sweep anything
	// their sends left behind.
	mgr.capture.Stop()

	written := counting.appended.Load()
	dropped := int64(mgr.GetMetrics().CaptureDropped)
	if total := written + dropped; total != publishers*perPublisher {
		t.Fatalf("wrote %d and dropped %d, accounting for %d of %d publishes; %d went missing",
			written, dropped, total, publishers*perPublisher, publishers*perPublisher-total)
	}
}

// The window between checking for shutdown and sending is a few instructions
// wide, so the concurrent test above rarely lands in it. This states the
// guarantee that closes it directly: once Stop has returned, no job is ever
// accepted, so none can be stranded in a lane nothing reads.
func TestCaptureRefusesAndCountsJobsAfterStop(t *testing.T) {
	counting := &countingQueueStore{QueueStore: memlog.New()}
	mgr := NewManager(counting, newMockGroupStore(), nil, DefaultConfig(), slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	mgr.capture.Start(ctx)
	mgr.capture.Stop()

	const after = 5
	for range after {
		if err := mgr.PublishToMatchingQueues(ctx, types.PublishRequest{
			Topic:   testCapturedTopic,
			Payload: []byte("payload"),
		}); err != nil {
			t.Fatalf("PublishToMatchingQueues failed: %v", err)
		}
	}

	if got := mgr.GetMetrics().CaptureDropped; got != after {
		t.Fatalf("capture dropped = %d, want %d; a job after shutdown was accepted rather than counted", got, after)
	}
	if got := counting.appended.Load(); got != 0 {
		t.Fatalf("%d appends ran after shutdown", got)
	}
}

// Shutdown must not wait on an append that cannot be interrupted. The store
// takes no context, so a wedged worker would otherwise hold the broker open for
// as long as the storage stall lasts.
func TestCaptureStopIsBoundedByDrainTimeout(t *testing.T) {
	blocking := &blockingQueueStore{
		QueueStore: memlog.New(),
		release:    make(chan struct{}),
		entered:    make(chan struct{}),
	}
	config := DefaultConfig()
	config.CaptureWorkers = 1
	config.CaptureDrainTimeout = 200 * time.Millisecond
	mgr := NewManager(blocking, newMockGroupStore(), nil, config, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	ctx := context.Background()

	if err := mgr.CreateQueue(ctx, types.DefaultQueueConfig(testCaptureQueue, "m/#")); err != nil {
		t.Fatalf("CreateQueue failed: %v", err)
	}
	mgr.capture.Start(ctx)
	t.Cleanup(func() { close(blocking.release) })

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

	stopped := make(chan struct{})
	go func() {
		mgr.capture.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop waited on an uninterruptible append instead of its drain timeout")
	}
}
