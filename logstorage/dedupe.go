// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/absmach/fluxmq/internal/keylock"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
)

// DefaultDeduplicationWindow bounds how far back a repeated key is recognised.
//
// The index is rebuilt by reading records, so an unbounded window would mean
// reading the whole queue at startup and holding a key for every record ever
// written. The transfers this protects — a dead-letter move retried after a
// failed settlement — repeat within seconds, so a window of recent records
// covers the case that matters at a cost that does not grow with the log.
const DefaultDeduplicationWindow = 4096

// dedupeIndex maps deduplication keys to the offsets already carrying them, for
// one queue.
//
// It holds no authority of its own: the keys live in the records, written as
// the envelope's transfer identity, and this index is only a lookup rebuilt
// from them. A crash costs the index, not the guarantee — the next AppendOnce
// on that queue rebuilds it from the log before answering.
type dedupeIndex struct {
	mu      sync.Mutex
	built   bool
	offsets map[string]uint64
}

// dedupeIndexes holds one index per queue, guarded per queue so a rebuild on
// one does not block appends to another.
type dedupeIndexes struct {
	locks   keylock.Sharded
	mu      sync.RWMutex
	byQueue map[string]*dedupeIndex
}

func newDedupeIndexes() *dedupeIndexes {
	return &dedupeIndexes{byQueue: make(map[string]*dedupeIndex)}
}

func (d *dedupeIndexes) forQueue(queueName string) *dedupeIndex {
	d.mu.RLock()
	index, ok := d.byQueue[queueName]
	d.mu.RUnlock()
	if ok {
		return index
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if index, ok := d.byQueue[queueName]; ok {
		return index
	}
	index = &dedupeIndex{offsets: make(map[string]uint64)}
	d.byQueue[queueName] = index
	return index
}

func (d *dedupeIndexes) forget(queueName string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.byQueue, queueName)
}

// AppendOnce implements storage.DeduplicatingQueueStore.
//
// The check and the append are serialised per queue, so two concurrent retries
// of the same transfer cannot both observe an empty index and both append.
func (a *Adapter) AppendOnce(ctx context.Context, queueName, dedupeKey string, msg *message.Envelope) (uint64, bool, error) {
	if dedupeKey == "" {
		return 0, false, storage.ErrDeduplicationKeyRequired
	}
	if err := a.queueConfigExists(queueName); err != nil {
		return 0, false, err
	}

	queueLock := a.dedupe.locks.Key(queueName)
	queueLock.Lock()
	defer queueLock.Unlock()

	index := a.dedupe.forQueue(queueName)
	if err := a.ensureDedupeIndex(ctx, queueName, index); err != nil {
		return 0, false, err
	}

	index.mu.Lock()
	offset, seen := index.offsets[dedupeKey]
	index.mu.Unlock()
	if seen {
		// The envelope is consumed either way. Releasing it here is what lets a
		// caller retry without tracking whether its previous attempt landed.
		message.Release(msg)
		return offset, true, nil
	}

	// The key must reach the record, or a rebuild after a crash cannot see it.
	msg.Broker.Transfer.ID = dedupeKey

	appended, err := a.Append(ctx, queueName, msg)
	if err != nil {
		return 0, false, err
	}

	index.mu.Lock()
	index.offsets[dedupeKey] = appended
	index.mu.Unlock()

	return appended, false, nil
}

// DeduplicationWindow implements storage.DeduplicatingQueueStore.
func (a *Adapter) DeduplicationWindow() int { return DefaultDeduplicationWindow }

// ensureDedupeIndex rebuilds a queue's index from the tail of its log the first
// time the queue is used after startup.
//
// Reading the records back is what makes the guarantee survive a crash: the
// index is derived state, and the keys it holds were written into the records
// by the append that created them.
func (a *Adapter) ensureDedupeIndex(ctx context.Context, queueName string, index *dedupeIndex) error {
	index.mu.Lock()
	defer index.mu.Unlock()
	if index.built {
		return nil
	}

	head, err := a.store.Head(queueName)
	if err != nil {
		return fmt.Errorf("read head for deduplication rebuild: %w", err)
	}
	tail, err := a.store.Tail(queueName)
	if err != nil {
		return fmt.Errorf("read tail for deduplication rebuild: %w", err)
	}

	start := head
	if window := uint64(DefaultDeduplicationWindow); tail > start+window {
		start = tail - window
	}

	for offset := start; offset < tail; {
		batch, err := a.ReadBatch(ctx, queueName, offset, dedupeRebuildBatch)
		if err != nil {
			if errors.Is(err, storage.ErrOffsetOutOfRange) {
				break
			}
			return fmt.Errorf("read records for deduplication rebuild: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, envelope := range batch {
			if key := envelope.Broker.Transfer.ID; key != "" {
				index.offsets[key] = envelope.Broker.Queue.Offset
			}
		}
		offset = batch[len(batch)-1].Broker.Queue.Offset + 1
		for _, envelope := range batch {
			message.Release(envelope)
		}
	}

	index.built = true
	return nil
}

// dedupeRebuildBatch bounds how many records a rebuild reads at a time.
const dedupeRebuildBatch = 256
