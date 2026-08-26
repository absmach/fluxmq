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

// OpenQueueSnapshot implements storage.SnapshotableQueueStore.
//
// Only the offset range is captured. Segments are append-only files, so the
// records in [head, tail) do not change under the reader; the one thing that
// can remove them is retention, and a record that has gone by the time it is
// read fails the snapshot rather than leaving a hole in it, since raft compacts
// the log against whatever the snapshot reports.
//
// Reading is deferred so that taking a snapshot does not scan the queue on the
// raft goroutine, which cannot apply entries while it runs.
func (a *Adapter) OpenQueueSnapshot(ctx context.Context, queueName string) (storage.QueueSnapshotReader, error) {
	if err := a.queueConfigExists(queueName); err != nil {
		return nil, err
	}

	// The segment manager is captured once and everything afterwards goes
	// through it. Resolving the queue by name on each read would leave a window
	// between deciding the queue is still the captured one and reading it, and
	// a delete-and-recreate landing in that window would hand back the new
	// queue's records under the old queue's configuration and groups.
	manager, err := a.store.getQueue(queueName)
	if err != nil {
		return nil, translateQueueErr(err)
	}

	head, tail := manager.Head(), manager.Tail()
	if tail < head {
		return nil, fmt.Errorf("queue %q tail %d precedes head %d", queueName, tail, head)
	}

	return &adapterQueueSnapshot{
		queueName: queueName, manager: manager,
		head: head, tail: tail, next: head,
	}, nil
}

// adapterQueueSnapshot reads a fixed offset range from the segment manager it
// was opened on, in batches, so the memory it needs is one batch rather than
// the queue.
//
// Nothing here consults the store by name. A queue that is deleted closes the
// manager, so a capture that outlives its queue fails rather than following the
// name onto a different log.
type adapterQueueSnapshot struct {
	queueName string
	manager   *SegmentManager
	head      uint64
	tail      uint64
	next      uint64
	buffered  []*message.Envelope
}

func (r *adapterQueueSnapshot) Head() uint64 { return r.head }
func (r *adapterQueueSnapshot) Tail() uint64 { return r.tail }

func (r *adapterQueueSnapshot) Next(context.Context) (uint64, *message.Envelope, bool, error) {
	if len(r.buffered) == 0 {
		if r.next >= r.tail {
			return 0, nil, false, nil
		}
		if err := r.fill(); err != nil {
			return 0, nil, false, err
		}
	}

	envelope := r.buffered[0]
	r.buffered = r.buffered[1:]
	offset := r.next
	r.next++
	return offset, envelope, true, nil
}

func (r *adapterQueueSnapshot) fill() error {
	messages, err := r.manager.ReadRange(r.next, r.tail, snapshotReadBatch)
	if err != nil {
		return fmt.Errorf("queue %q was replaced or removed while it was being captured: %w", r.queueName, err)
	}
	if len(messages) == 0 {
		return fmt.Errorf("queue %q lost offset %d while it was being captured", r.queueName, r.next)
	}

	buffered := make([]*message.Envelope, 0, len(messages))
	for i := range messages {
		if messages[i].Offset != r.next+uint64(i) {
			releaseEnvelopes(buffered)
			return fmt.Errorf("queue %q returned offset %d where %d was expected while it was being captured",
				r.queueName, messages[i].Offset, r.next+uint64(i))
		}
		envelope, decodeErr := logMessageToEnvelope(&messages[i])
		if decodeErr != nil {
			releaseEnvelopes(buffered)
			return fmt.Errorf("decode record %d of queue %q: %w", messages[i].Offset, r.queueName, decodeErr)
		}
		buffered = append(buffered, envelope)
	}

	r.buffered = buffered
	return nil
}

func (r *adapterQueueSnapshot) Close() error {
	releaseEnvelopes(r.buffered)
	r.buffered = nil
	return nil
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

	// The offset is checked against the tail before anything is written. A
	// snapshot describes a contiguous log, so an offset that does not continue
	// it means the stream is damaged — and raft reports a failed install
	// without moving lastApplied, so a record appended before the rejection
	// would be left behind in a store the FSM believes it never touched.
	tail, err := a.store.Tail(queueName)
	if err != nil {
		message.Release(msg)
		return translateQueueErr(err)
	}
	if offset != tail {
		message.Release(msg)
		return fmt.Errorf("%w: restoring queue %q expected offset %d, snapshot carried %d",
			storage.ErrOffsetOutOfRange, queueName, tail, offset)
	}

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
