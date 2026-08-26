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

// A deduplication key lives exactly as long as the record that carries it.
//
// There is one rule, and truncation, deletion and recovery all enforce it: a key
// is recognised while its record is retained, and forgotten when the record goes.
// An earlier version bounded recovery to a fixed number of recent records, which
// made the live index and a rebuilt one disagree — ordinary appends move the tail
// arbitrarily far while a key stays live in memory, so the same retry was
// deduplicated before a restart and duplicated after one. Retention already
// bounds the population, so the window bounded nothing that was not bounded
// anyway and cost correctness for it.
//
// The rebuild reads the queue from its head on first use after startup. That is
// proportional to what the queue retains, which for a dead-letter queue is what
// its retention policy allows.

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

// pruneBelow drops keys whose record is no longer at or after minOffset.
//
// A key that outlives its record is worse than a missing key: AppendOnce would
// report the transfer already present, at an offset holding nothing, and the
// caller would settle its source against a record that does not exist. That
// loses the message, which is the failure this whole mechanism exists to
// prevent.
func (d *dedupeIndexes) pruneBelow(queueName string, minOffset uint64) {
	d.mu.RLock()
	index, ok := d.byQueue[queueName]
	d.mu.RUnlock()
	if !ok {
		return
	}

	index.mu.Lock()
	defer index.mu.Unlock()
	for key, offset := range index.offsets {
		if offset < minOffset {
			delete(index.offsets, key)
		}
	}
}

// AppendOnce implements storage.DeduplicatingQueueStore.
//
// The check and the append are serialised per queue, so two concurrent retries
// of the same transfer cannot both observe an empty index and both append.
func (a *Adapter) AppendOnce(ctx context.Context, queueName, dedupeKey string, msg *message.Envelope) (uint64, bool, error) {
	return a.appendOnce(ctx, queueName, dedupeKey, msg, a.Append)
}

// appendOnce performs the check and the append as one operation, writing
// through the supplied append so the caller's durability policy survives the
// deduplication rather than being replaced by it.
func (a *Adapter) appendOnce(
	ctx context.Context,
	queueName, dedupeKey string,
	msg *message.Envelope,
	append func(context.Context, string, *message.Envelope) (uint64, error),
) (uint64, bool, error) {
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

	appended, err := append(ctx, queueName, msg)
	if err != nil {
		return 0, false, err
	}

	index.mu.Lock()
	index.offsets[dedupeKey] = appended
	index.mu.Unlock()

	return appended, false, nil
}

// AppendOnceAndSync implements storage.DeduplicatingQueueStore.
func (a *Adapter) AppendOnceAndSync(ctx context.Context, queueName, dedupeKey string, msg *message.Envelope) (uint64, bool, error) {
	return a.appendOnce(ctx, queueName, dedupeKey, msg, a.AppendAndSync)
}

// DeduplicationWindow implements storage.DeduplicatingQueueStore. Zero: every
// record the queue retains is covered, because a key is dropped only when its
// record is.
func (a *Adapter) DeduplicationWindow() int { return 0 }

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

	for offset := head; offset < tail; {
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
