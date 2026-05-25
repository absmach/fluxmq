// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncEventHookConfig configures an AsyncEventHook.
type AsyncEventHookConfig struct {
	// Workers is the number of goroutines draining the event queue.
	// Defaults to 1 when <= 0.
	Workers int
	// QueueSize is the bounded capacity of the event channel. Defaults to
	// 1024 when <= 0. Publish events that arrive when the queue is full
	// are dropped (newest is rejected) and counted in DroppedCount.
	QueueSize int
	// DispatchTimeout bounds the time a worker spends in the underlying
	// EventHook call. Zero means no timeout (not recommended).
	DispatchTimeout time.Duration
	// ShutdownTimeout bounds how long Close() waits for the queue to drain.
	ShutdownTimeout time.Duration
	// Logger is used for dropped-event warnings and dispatch errors.
	Logger *slog.Logger
}

func (c AsyncEventHookConfig) withDefaults() AsyncEventHookConfig {
	if c.Workers <= 0 {
		c.Workers = 1
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 1024
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// AsyncEventHookStats is a point-in-time snapshot of dispatcher counters.
type AsyncEventHookStats struct {
	Enqueued   uint64
	Dispatched uint64
	Dropped    uint64
	Errors     uint64
}

type eventKind int

const (
	evConnect eventKind = iota
	evDisconnect
	evSubscribe
	evUnsubscribe
	evPublish
)

type asyncEvent struct {
	kind     eventKind
	clientID string
	str1     string // username|reason|topic|filter|topic
	str2     string // protocol (connect only)
	qos      byte
	payload  []byte
}

// AsyncEventHook wraps an EventHook so that events are queued and dispatched
// by a small worker pool. The publish hot path is decoupled from hook latency.
// Lifecycle events (Connect/Disconnect/Subscribe/Unsubscribe) block on enqueue
// because they are infrequent and ordering matters; Publish events drop when
// the queue is full so a slow hook cannot stall the broker.
//
// AsyncEventHook is safe for concurrent use.
type AsyncEventHook struct {
	inner   EventHook
	cfg     AsyncEventHookConfig
	queue   chan asyncEvent
	wg      sync.WaitGroup
	closed  atomic.Bool
	closeCh chan struct{}

	enqueued   atomic.Uint64
	dispatched atomic.Uint64
	dropped    atomic.Uint64
	errors     atomic.Uint64
}

// NewAsyncEventHook wraps inner so events dispatch on a worker pool.
// Returns inner unchanged when inner is nil.
func NewAsyncEventHook(inner EventHook, cfg AsyncEventHookConfig) *AsyncEventHook {
	if inner == nil {
		return nil
	}
	cfg = cfg.withDefaults()
	h := &AsyncEventHook{
		inner:   inner,
		cfg:     cfg,
		queue:   make(chan asyncEvent, cfg.QueueSize),
		closeCh: make(chan struct{}),
	}
	h.wg.Add(cfg.Workers)
	for range cfg.Workers {
		go h.run()
	}
	return h
}

// Stats returns a snapshot of dispatcher counters.
func (h *AsyncEventHook) Stats() AsyncEventHookStats {
	return AsyncEventHookStats{
		Enqueued:   h.enqueued.Load(),
		Dispatched: h.dispatched.Load(),
		Dropped:    h.dropped.Load(),
		Errors:     h.errors.Load(),
	}
}

func (h *AsyncEventHook) enqueueBlocking(ev asyncEvent) error {
	if h.closed.Load() {
		return nil
	}
	select {
	case h.queue <- ev:
		h.enqueued.Add(1)
	case <-h.closeCh:
	}
	return nil
}

func (h *AsyncEventHook) enqueueOrDrop(ev asyncEvent) error {
	if h.closed.Load() {
		return nil
	}
	select {
	case h.queue <- ev:
		h.enqueued.Add(1)
	default:
		dropped := h.dropped.Add(1)
		// Log first drop in every batch of 1024 to avoid log floods.
		if dropped == 1 || dropped%1024 == 0 {
			h.cfg.Logger.Warn("async event hook queue full, dropping publish event",
				slog.Uint64("dropped_total", dropped),
				slog.String("topic", ev.str1))
		}
	}
	return nil
}

func (h *AsyncEventHook) OnConnect(_ context.Context, clientID, username, protocol string) error {
	return h.enqueueBlocking(asyncEvent{kind: evConnect, clientID: clientID, str1: username, str2: protocol})
}

func (h *AsyncEventHook) OnDisconnect(_ context.Context, clientID, reason string) error {
	return h.enqueueBlocking(asyncEvent{kind: evDisconnect, clientID: clientID, str1: reason})
}

func (h *AsyncEventHook) OnSubscribe(_ context.Context, clientID, topic string, qos byte) error {
	return h.enqueueBlocking(asyncEvent{kind: evSubscribe, clientID: clientID, str1: topic, qos: qos})
}

func (h *AsyncEventHook) OnUnsubscribe(_ context.Context, clientID, topic string) error {
	return h.enqueueBlocking(asyncEvent{kind: evUnsubscribe, clientID: clientID, str1: topic})
}

func (h *AsyncEventHook) OnPublish(_ context.Context, clientID, topic string, qos byte, payload []byte) error {
	// Copy payload because the caller may release the underlying buffer
	// before a worker dispatches the event.
	var p []byte
	if len(payload) > 0 {
		p = make([]byte, len(payload))
		copy(p, payload)
	}
	return h.enqueueOrDrop(asyncEvent{kind: evPublish, clientID: clientID, str1: topic, qos: qos, payload: p})
}

// Close stops dispatcher workers. Drains the queue up to ShutdownTimeout,
// then closes the inner hook. Safe to call multiple times.
func (h *AsyncEventHook) Close() error {
	if !h.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(h.closeCh)
	close(h.queue)

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(h.cfg.ShutdownTimeout):
		h.cfg.Logger.Warn("async event hook shutdown timed out",
			slog.Duration("timeout", h.cfg.ShutdownTimeout),
			slog.Uint64("pending", uint64(len(h.queue))))
	}
	return h.inner.Close()
}

func (h *AsyncEventHook) run() {
	defer h.wg.Done()
	for ev := range h.queue {
		h.dispatch(ev)
	}
}

func (h *AsyncEventHook) dispatch(ev asyncEvent) {
	ctx := context.Background()
	if h.cfg.DispatchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.cfg.DispatchTimeout)
		defer cancel()
	}

	defer func() {
		if r := recover(); r != nil {
			h.errors.Add(1)
			h.cfg.Logger.Error("async event hook panic",
				slog.Any("recover", r),
				slog.String("client_id", ev.clientID))
		}
	}()

	var err error
	switch ev.kind {
	case evConnect:
		err = h.inner.OnConnect(ctx, ev.clientID, ev.str1, ev.str2)
	case evDisconnect:
		err = h.inner.OnDisconnect(ctx, ev.clientID, ev.str1)
	case evSubscribe:
		err = h.inner.OnSubscribe(ctx, ev.clientID, ev.str1, ev.qos)
	case evUnsubscribe:
		err = h.inner.OnUnsubscribe(ctx, ev.clientID, ev.str1)
	case evPublish:
		err = h.inner.OnPublish(ctx, ev.clientID, ev.str1, ev.qos, ev.payload)
	}

	h.dispatched.Add(1)
	if err != nil {
		h.errors.Add(1)
		h.cfg.Logger.Warn("async event hook dispatch error",
			slog.String("client_id", ev.clientID),
			slog.String("error", err.Error()))
	}
}
