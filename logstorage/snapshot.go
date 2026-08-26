// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"context"
	"errors"
	"fmt"

	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// snapshotReadBatch bounds one read while a queue is captured. The records are
// held until Persist has written them, so this only paces the reads; the FSM
// holds the whole set either way.
const snapshotReadBatch = 256

// SnapshotQueue implements storage.SnapshotableQueueStore.
//
// The range is fixed to the head and tail observed at the start. Retention runs
// outside the raft group and can truncate underneath a capture; a record that
// has gone by the time it is read fails the snapshot rather than silently
// producing one with a hole, since raft compacts the log against whatever this
// returns.
func (a *Adapter) SnapshotQueue(ctx context.Context, queueName string) (uint64, []*message.Envelope, error) {
	if err := a.queueConfigExists(queueName); err != nil {
		return 0, nil, err
	}

	head, err := a.store.Head(queueName)
	if err != nil {
		return 0, nil, translateQueueErr(err)
	}
	tail, err := a.store.Tail(queueName)
	if err != nil {
		return 0, nil, translateQueueErr(err)
	}
	if tail < head {
		return 0, nil, fmt.Errorf("queue %q tail %d precedes head %d", queueName, tail, head)
	}

	records := make([]*message.Envelope, 0, tail-head)
	for offset := head; offset < tail; {
		limit := min(int(tail-offset), snapshotReadBatch)

		batch, readErr := a.ReadBatch(ctx, queueName, offset, limit)
		if readErr != nil {
			releaseEnvelopes(records)
			return 0, nil, fmt.Errorf("read queue %q at offset %d: %w", queueName, offset, readErr)
		}
		if len(batch) == 0 {
			releaseEnvelopes(records)
			return 0, nil, fmt.Errorf("queue %q lost offset %d while it was being captured", queueName, offset)
		}

		records = append(records, batch...)
		offset += uint64(len(batch))
	}

	return head, records, nil
}

// RestoreQueue implements storage.SnapshotableQueueStore.
//
// The queue is recreated and then truncated to head. Truncating an empty log
// drops the segment it was created with and opens a replacement based at that
// offset, which is what lets restored records keep the offsets they were
// written at after a prefix had been retained away.
func (a *Adapter) RestoreQueue(ctx context.Context, config types.QueueConfig, head uint64) error {
	if err := config.Validate(); err != nil {
		return err
	}

	// The public methods each take the queue's deduplication lock, so they are
	// called in sequence rather than under one. Restore has the store to itself:
	// raft installs a snapshot in place of applying entries, never beside it.
	if err := a.DeleteQueue(ctx, config.Name); err != nil &&
		!errors.Is(err, ErrQueueNotFound) && !errors.Is(err, storage.ErrQueueNotFound) {
		return fmt.Errorf("drop queue %q before restore: %w", config.Name, err)
	}

	if err := a.CreateQueue(ctx, config); err != nil {
		return fmt.Errorf("create queue %q for restore: %w", config.Name, err)
	}

	if head > 0 {
		if err := a.Truncate(ctx, config.Name, head); err != nil {
			return fmt.Errorf("position queue %q at offset %d: %w", config.Name, head, err)
		}
	}

	return nil
}

// RestoreRecord implements storage.SnapshotableQueueStore.
//
// The deduplication index is written here as well as the record. It is durable
// derived state that a plain append does not touch, so a queue restored without
// it would accept a transfer it already holds — appending a second copy of a
// record the rest of the group has exactly one of.
func (a *Adapter) RestoreRecord(ctx context.Context, queueName string, offset uint64, msg *message.Envelope) error {
	if err := a.queueConfigExists(queueName); err != nil {
		message.Release(msg)
		return err
	}

	queueLock := a.dedupe.locks.Key(queueName)
	queueLock.Lock()
	defer queueLock.Unlock()

	value, key, headers, err := encodeMessage(msg)
	if err != nil {
		message.Release(msg)
		return err
	}
	dedupeKey := msg.BrokerMeta.Transfer.ID

	assigned, err := a.store.Append(queueName, value, key, headers)
	if err != nil {
		message.Release(msg)
		return fmt.Errorf("restore record %d of queue %q: %w", offset, queueName, err)
	}
	message.Release(msg)

	// A snapshot describes a contiguous log. An offset that does not continue it
	// means the stream is damaged, and every later record would land wrong.
	if assigned != offset {
		return fmt.Errorf("%w: restoring queue %q expected offset %d, log assigned %d",
			storage.ErrOffsetOutOfRange, queueName, offset, assigned)
	}

	if dedupeKey != "" {
		if err := a.dedupe.state.put(queueName, dedupeKey, dedupeEntry{offset: assigned, state: dedupeConfirmed}); err != nil {
			return fmt.Errorf("restore deduplication key for queue %q offset %d: %w", queueName, assigned, err)
		}
	}

	return nil
}

// ResetForRestore implements storage.SnapshotableQueueStore.
//
// Only the named queues are dropped. One adapter backs every raft group in the
// process along with the queues no group replicates, so clearing it wholesale
// would delete state the caller does not own.
func (a *Adapter) ResetForRestore(ctx context.Context, queueNames []string) error {
	for _, queueName := range queueNames {
		if err := a.DeleteQueue(ctx, queueName); err != nil &&
			!errors.Is(err, ErrQueueNotFound) && !errors.Is(err, storage.ErrQueueNotFound) {
			return fmt.Errorf("clear queue %q for restore: %w", queueName, err)
		}
	}
	return nil
}

func translateQueueErr(err error) error {
	if errors.Is(err, ErrQueueNotFound) {
		return storage.ErrQueueNotFound
	}
	return err
}
