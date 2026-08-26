// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package logstorage

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/absmach/fluxmq/internal/keylock"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
)

// A deduplication key lives exactly as long as the record that carries it.
// The durable index is derived state: every entry is checked against the raw
// record before it can settle a source, and truncation removes entries with
// their records. A pre-append reservation records the old tail, so a crash in
// the only non-atomic gap recovers by scanning that uncertain suffix rather
// than decoding the queue's retained history.
type dedupeIndexes struct {
	locks keylock.Sharded
	state *dedupeStateStore
}

func newDedupeIndexes(baseDir string) (*dedupeIndexes, error) {
	state, err := openDedupeStateStore(baseDir)
	if err != nil {
		return nil, err
	}
	return &dedupeIndexes{state: state}, nil
}

type dedupeAppend func(queueName string, value, key []byte, headers map[string][]byte) (uint64, error)

// AppendOnce implements storage.DeduplicatingQueueStore.
func (a *Adapter) AppendOnce(ctx context.Context, queueName, dedupeKey string, msg *message.Envelope) (uint64, bool, error) {
	return a.appendOnce(ctx, queueName, dedupeKey, msg, a.store.Append)
}

// AppendOnceAndSync implements storage.DeduplicatingQueueStore.
func (a *Adapter) AppendOnceAndSync(ctx context.Context, queueName, dedupeKey string, msg *message.Envelope) (uint64, bool, error) {
	return a.appendOnce(ctx, queueName, dedupeKey, msg, a.store.AppendAndSync)
}

// appendOnce serializes the identity check, reservation and append per queue.
// It writes the log directly so envelope ownership changes only after both the
// record and its durable identity entry are confirmed.
func (a *Adapter) appendOnce(
	ctx context.Context,
	queueName, dedupeKey string,
	msg *message.Envelope,
	appendRecord dedupeAppend,
) (uint64, bool, error) {
	if dedupeKey == "" {
		return 0, false, storage.ErrDeduplicationKeyRequired
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if err := a.queueConfigExists(queueName); err != nil {
		return 0, false, err
	}

	queueLock := a.dedupe.locks.Key(queueName)
	queueLock.Lock()
	defer queueLock.Unlock()

	entry, found, err := a.dedupe.state.lookup(queueName, dedupeKey)
	if err != nil {
		return 0, false, fmt.Errorf("read deduplication state: %w", err)
	}
	if found {
		entry, found, err = a.resolveDedupeEntry(ctx, queueName, dedupeKey, entry)
		if err != nil {
			return 0, false, err
		}
		if found {
			if entry.state != dedupeConfirmed {
				if err := a.SyncQueue(ctx, queueName); err != nil {
					return entry.offset, false, fmt.Errorf("%w: confirming deduplicated record at offset %d: %w",
						storage.ErrDurabilityUnconfirmed, entry.offset, err)
				}
				entry.state = dedupeConfirmed
				if err := a.dedupe.state.put(queueName, dedupeKey, entry); err != nil {
					return entry.offset, false, fmt.Errorf("%w: confirm recovered key: %v",
						storage.ErrDeduplicationStateUnconfirmed, err)
				}
			}

			message.Release(msg)
			return entry.offset, true, nil
		}
		if err := a.dedupe.state.remove(queueName, dedupeKey); err != nil {
			return 0, false, fmt.Errorf("remove stale deduplication state: %w", err)
		}
	}

	tail, err := a.store.Tail(queueName)
	if err != nil {
		return 0, false, fmt.Errorf("read tail before deduplicated append: %w", err)
	}
	if _, found, err := a.dedupe.state.reserve(queueName, dedupeKey, tail); err != nil {
		return 0, false, fmt.Errorf("reserve deduplication key: %w", err)
	} else if found {
		return 0, false, fmt.Errorf("deduplication key appeared while queue lock was held")
	}

	// The raw header lets the exceptional pending-reservation recovery path find
	// the identity without unmarshalling envelope metadata or payloads.
	msg.BrokerMeta.Transfer.ID = dedupeKey
	value, key, headers, err := encodeMessage(msg)
	if err != nil {
		_ = a.dedupe.state.remove(queueName, dedupeKey)
		return 0, false, err
	}

	offset, appendErr := appendRecord(queueName, value, key, headers)
	if appendErr != nil {
		if errors.Is(appendErr, storage.ErrDurabilityUnconfirmed) {
			entry = dedupeEntry{offset: offset, state: dedupeUnconfirmed}
			if stateErr := a.dedupe.state.put(queueName, dedupeKey, entry); stateErr != nil {
				return offset, false, errors.Join(appendErr,
					fmt.Errorf("%w: persist accepted offset: %v", storage.ErrDeduplicationStateUnconfirmed, stateErr))
			}
			return offset, false, appendErr
		}
		if stateErr := a.dedupe.state.remove(queueName, dedupeKey); stateErr != nil {
			return offset, false, errors.Join(appendErr, fmt.Errorf("clear deduplication reservation: %w", stateErr))
		}
		return offset, false, appendErr
	}

	entry = dedupeEntry{offset: offset, state: dedupeConfirmed}
	if err := a.dedupe.state.put(queueName, dedupeKey, entry); err != nil {
		// The reservation remains authoritative enough to make a retry safe: it
		// points recovery at the suffix that can contain this record.
		return offset, false, fmt.Errorf("%w: persist accepted offset %d: %v",
			storage.ErrDeduplicationStateUnconfirmed, offset, err)
	}

	message.Release(msg)
	return offset, false, nil
}

// resolveDedupeEntry validates derived state against the raw log. Confirmed
// entries cost one indexed record read. Pending entries are the crash-only path
// and scan from the pre-append tail until they find the key or reach the tail.
func (a *Adapter) resolveDedupeEntry(ctx context.Context, queueName, dedupeKey string, entry dedupeEntry) (dedupeEntry, bool, error) {
	if entry.state == dedupePending {
		offset, found, err := a.findDedupeRecord(ctx, queueName, dedupeKey, entry.offset)
		if err != nil || !found {
			return entry, found, err
		}
		return dedupeEntry{offset: offset, state: dedupeUnconfirmed}, true, nil
	}

	record, err := a.store.Read(queueName, entry.offset)
	if errors.Is(err, ErrOffsetOutOfRange) {
		return entry, false, nil
	}
	if err != nil {
		return entry, false, fmt.Errorf("validate deduplication record at offset %d: %w", entry.offset, err)
	}
	return entry, bytes.Equal(record.Headers[headerDedupeKey], []byte(dedupeKey)), nil
}

func (a *Adapter) findDedupeRecord(ctx context.Context, queueName, dedupeKey string, start uint64) (uint64, bool, error) {
	head, err := a.store.Head(queueName)
	if err != nil {
		return 0, false, fmt.Errorf("read head for pending deduplication recovery: %w", err)
	}
	if start < head {
		start = head
	}
	tail, err := a.store.Tail(queueName)
	if err != nil {
		return 0, false, fmt.Errorf("read tail for pending deduplication recovery: %w", err)
	}

	for start < tail {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		messages, err := a.store.ReadRange(queueName, start, tail, dedupeRecoveryBatch)
		if err != nil {
			return 0, false, fmt.Errorf("read pending deduplication suffix: %w", err)
		}
		if len(messages) == 0 {
			break
		}
		for i := range messages {
			if bytes.Equal(messages[i].Headers[headerDedupeKey], []byte(dedupeKey)) {
				return messages[i].Offset, true, nil
			}
		}
		start = messages[len(messages)-1].Offset + 1
	}
	return 0, false, nil
}

// DeduplicationWindow implements storage.DeduplicatingQueueStore. Zero means
// every retained record is covered.
func (a *Adapter) DeduplicationWindow() int { return 0 }

const dedupeRecoveryBatch = 256
