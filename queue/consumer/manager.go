// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/absmach/fluxmq/internal/keylock"
	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
)

// Manager errors.
var (
	ErrNoMessages                    = errors.New("no messages available")
	ErrGroupNotFound                 = errors.New("consumer group not found")
	ErrConsumerNotFound              = errors.New("consumer not found")
	ErrMessageNotPending             = errors.New("message not in pending list")
	ErrInvalidOffset                 = errors.New("invalid offset")
	ErrGroupModeMismatch             = errors.New("consumer group mode mismatch")
	ErrCommitOffsetOnlyForStreamMode = errors.New("commit offset only supported for stream groups")
	ErrPELFull                       = errors.New("pending entry list at capacity")
	ErrDLQHandlerUnavailable         = errors.New("dead-letter queue handler unavailable")
	ErrDelayedNackUnsupported        = errors.New("delayed nack is not supported")
)

// Manager handles consumer group operations including claiming,
// acknowledging, and work stealing for the  queue.
type Manager struct {
	queueStore storage.QueueStore
	groupStore storage.ConsumerGroupStore
	config     Config

	// groupLocks serialises operations per consumer group. Every exported
	// operation names exactly one (queue, group) pair, so a lock over the whole
	// manager would serialise groups that share no state: one group waiting on
	// storage stalls every other group on the node.
	groupLocks keylock.Sharded

	// stateMu guards the two maps below, which are keyed by group but shared
	// across them. Under groupLocks alone, two goroutines holding different
	// group locks would race on these.
	stateMu    sync.Mutex
	lastCommit map[string]time.Time

	// dlqRetryAfter rate-limits dead-letter transfer retries per pending entry.
	// It is deliberately in-memory: this is a retry throttle, not durable state,
	// and losing it on restart costs one immediate retry per stuck entry.
	dlqRetryAfter map[string]time.Time
}

// groupKey names the lock and map entries for one consumer group.
func groupKey(queueName, groupID string) string {
	return queueName + "\x00" + groupID
}

// DLQHandler is called when a message exceeds MaxDeliveryCount.
// The handler receives a stable source offset so retries can preserve transfer
// identity. It must return nil only after the DLQ append has succeeded.
type DLQHandler func(ctx context.Context, queueName, groupID string, msg *message.Envelope, offset uint64, deliveryCount int, reason string) error

// Config defines configuration for the consumer group manager.
type Config struct {
	// VisibilityTimeout is how long a message stays claimed before it can be stolen.
	VisibilityTimeout time.Duration

	// MaxDeliveryCount is the maximum number of times a message can be delivered
	// before being sent to the DLQ.
	MaxDeliveryCount int

	// ClaimBatchSize is the maximum number of messages to claim at once.
	ClaimBatchSize int

	// StealBatchSize is the maximum number of messages to steal at once.
	StealBatchSize int

	// AutoCommitInterval controls how often stream groups auto-commit offsets.
	// Zero means commit on every delivery batch.
	AutoCommitInterval time.Duration

	// MaxPELSize is the maximum number of pending entries per consumer group.
	// When reached, new claims are rejected until entries are acknowledged.
	// Zero means unlimited (not recommended for production).
	MaxPELSize int

	// OnDLQ is called when a message exceeds MaxDeliveryCount during work
	// stealing. A nil handler, or one reporting that the queue has no
	// dead-letter destination, returns the message to ordinary redelivery. Any
	// other error keeps the entry pending and retries the transfer later.
	OnDLQ DLQHandler

	// DLQUnavailable reports whether an OnDLQ error means "this queue has no
	// dead-letter destination" rather than "the transfer failed this time". The
	// two are handled differently: the first cannot be retried into success, the
	// second can. A nil func treats every error as transient.
	DLQUnavailable func(error) bool

	// DLQRetryBackoff is the minimum interval between dead-letter transfer
	// attempts for one pending entry. It stops a permanently failing transfer
	// from consuming a steal slot on every cycle. Zero selects
	// defaultDLQRetryBackoff.
	DLQRetryBackoff time.Duration

	// Metrics records dead-letter transfer outcomes. May be nil.
	Metrics *Metrics

	// Logger reports dead-letter transfer failures. Nil selects slog.Default.
	Logger *slog.Logger
}

// defaultDLQRetryBackoff throttles retries of a failing dead-letter transfer.
const defaultDLQRetryBackoff = 30 * time.Second

// DefaultConfig returns default manager configuration.
func DefaultConfig() Config {
	return Config{
		VisibilityTimeout:  30 * time.Second,
		MaxDeliveryCount:   5,
		ClaimBatchSize:     10,
		StealBatchSize:     5,
		AutoCommitInterval: 5 * time.Second,
		MaxPELSize:         100_000,
	}
}

// NewManager creates a new consumer group manager.
func NewManager(queueStore storage.QueueStore, groupStore storage.ConsumerGroupStore, config Config) *Manager {
	if config.DLQRetryBackoff <= 0 {
		config.DLQRetryBackoff = defaultDLQRetryBackoff
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Manager{
		queueStore:    queueStore,
		groupStore:    groupStore,
		config:        config,
		lastCommit:    make(map[string]time.Time),
		dlqRetryAfter: make(map[string]time.Time),
	}
}

// GetOrCreateGroup retrieves or creates a consumer group.
func (m *Manager) GetOrCreateGroup(ctx context.Context, queueName, groupID, pattern string, mode types.ConsumerGroupMode, autoCommit bool) (*types.ConsumerGroup, error) {
	// Try to get existing group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err == nil {
		if mode == "" {
			return group, nil
		}
		if group.Mode == "" {
			group.Mode = mode
			group.AutoCommit = autoCommit
			if err := m.groupStore.UpdateConsumerGroup(ctx, group); err != nil {
				return nil, err
			}
			return group, nil
		}
		if group.Mode != mode {
			return nil, ErrGroupModeMismatch
		}
		return group, nil
	}

	// Check for "not found" errors from various storage implementations
	if !errors.Is(err, storage.ErrConsumerNotFound) && !errors.Is(err, logstorage.ErrGroupNotFound) {
		return nil, err
	}

	// Create new group
	group = types.NewConsumerGroupState(queueName, groupID, pattern)
	if mode != "" {
		group.Mode = mode
	}
	group.AutoCommit = autoCommit

	if err := m.groupStore.CreateConsumerGroup(ctx, group); err != nil {
		// Handle race condition - another process might have created it
		if err == storage.ErrConsumerGroupExists {
			return m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
		}
		return nil, err
	}

	return group, nil
}

// RegisterConsumer adds a consumer to a group.
func (m *Manager) RegisterConsumer(ctx context.Context, queueName, groupID, consumerID, clientID, proxyNodeID string) error {
	consumer := &types.ConsumerInfo{
		ID:            consumerID,
		ClientID:      clientID,
		ProxyNodeID:   proxyNodeID,
		RegisteredAt:  time.Now(),
		LastHeartbeat: time.Now(),
	}

	return m.groupStore.RegisterConsumer(ctx, queueName, groupID, consumer)
}

// UnregisterConsumer removes a consumer from a group.
func (m *Manager) UnregisterConsumer(ctx context.Context, queueName, groupID, consumerID string) error {
	return m.groupStore.UnregisterConsumer(ctx, queueName, groupID, consumerID)
}

// Claim retrieves the next available message for a consumer.
// It first tries to get a new message from the log, then falls back to work stealing.
func (m *Manager) Claim(ctx context.Context, queueName, groupID, consumerID string, filter *Filter) (*message.Envelope, error) {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	// Get consumer group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	// Try to claim from cursor position
	msg, err := m.claimFromCursor(ctx, group, consumerID, filter)
	if err == nil {
		return msg, nil
	}

	if err != ErrNoMessages {
		return nil, err
	}

	// No new messages - try work stealing
	return m.stealWork(ctx, group, consumerID, filter)
}

// ClaimBatch retrieves multiple messages for a consumer.
func (m *Manager) ClaimBatch(ctx context.Context, queueName, groupID, consumerID string, filter *Filter, limit int) ([]*message.Envelope, error) {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	if limit <= 0 {
		limit = m.config.ClaimBatchSize
	}

	// Get consumer group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	var messages []*message.Envelope

	// Claim from cursor
	for len(messages) < limit {
		msg, err := m.claimFromCursor(ctx, group, consumerID, filter)
		if err != nil {
			if !errors.Is(err, ErrNoMessages) && !errors.Is(err, ErrPELFull) {
				return messages, err
			}
			break
		}
		messages = append(messages, msg)
	}

	// Try work stealing if we didn't get enough
	for len(messages) < limit {
		msg, err := m.stealWork(ctx, group, consumerID, filter)
		if err != nil {
			if !errors.Is(err, ErrNoMessages) {
				return messages, err
			}
			break
		}
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil, ErrNoMessages
	}

	return messages, nil
}

// ClaimPendingBatch transfers pending messages idle for at least minIdle to a
// consumer. Unlike ClaimBatch it never consumes new log records. Entries are
// considered oldest-first so all storage backends expose the same order.
func (m *Manager) ClaimPendingBatch(ctx context.Context, queueName, groupID, consumerID string, minIdle time.Duration, limit int) ([]*message.Envelope, error) {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	if limit <= 0 {
		limit = m.config.ClaimBatchSize
	}
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}
	if group.Mode == types.GroupModeStream {
		return nil, ErrGroupModeMismatch
	}

	entries := group.StealableEntries(minIdle, consumerID)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].ClaimedAt.Equal(entries[j].ClaimedAt) {
			return entries[i].Offset < entries[j].Offset
		}
		return entries[i].ClaimedAt.Before(entries[j].ClaimedAt)
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}

	messages := make([]*message.Envelope, 0, len(entries))
	for _, entry := range entries {
		msg, err := m.queueStore.Read(ctx, queueName, entry.Offset)
		if err != nil {
			releaseMessages(messages)
			return nil, err
		}
		if msg.IsExpired() {
			message.Release(msg)
			if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, entry.ConsumerID, entry.Offset); err != nil {
				releaseMessages(messages)
				return nil, err
			}
			continue
		}
		if err := m.groupStore.TransferPendingEntry(ctx, queueName, groupID, entry.Offset, entry.ConsumerID, consumerID); err != nil {
			message.Release(msg)
			releaseMessages(messages)
			return nil, err
		}
		messages = append(messages, msg)
	}
	if len(messages) == 0 {
		return nil, ErrNoMessages
	}
	return messages, nil
}

// ClaimBatchStream retrieves multiple messages for a stream consumer without PEL tracking.
// It advances the cursor once per batch for efficiency.
func (m *Manager) ClaimBatchStream(ctx context.Context, queueName, groupID, consumerID string, filter *Filter, limit int) ([]*message.Envelope, error) {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	if limit <= 0 {
		limit = m.config.ClaimBatchSize
	}

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	messages, newCursor, err := m.peekBatchStreamLocked(ctx, group, filter, limit)
	if err != nil {
		return nil, err
	}

	if err := m.updateStreamCursorLocked(ctx, group, newCursor); err != nil {
		releaseMessages(messages)
		return nil, err
	}

	return messages, nil
}

// PeekBatchStream retrieves stream messages without advancing the consumer
// group cursor. Call CommitStreamCursor after successful delivery.
func (m *Manager) PeekBatchStream(ctx context.Context, queueName, groupID, _ string, filter *Filter, limit int) ([]*message.Envelope, uint64, error) {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	if limit <= 0 {
		limit = m.config.ClaimBatchSize
	}

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, 0, err
	}

	return m.peekBatchStreamLocked(ctx, group, filter, limit)
}

// CommitStreamCursor advances a stream consumer group's cursor after delivery
// has succeeded.
func (m *Manager) CommitStreamCursor(ctx context.Context, queueName, groupID string, cursor uint64) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}
	if group.Mode != types.GroupModeStream {
		return ErrCommitOffsetOnlyForStreamMode
	}

	return m.updateStreamCursorLocked(ctx, group, cursor)
}

func (m *Manager) peekBatchStreamLocked(ctx context.Context, group *types.ConsumerGroup, filter *Filter, limit int) ([]*message.Envelope, uint64, error) {
	cursor := group.CursorView()
	tail, err := m.queueStore.Tail(ctx, group.QueueName)
	if err != nil {
		return nil, cursor.Cursor, err
	}

	var messages []*message.Envelope
	var newCursor uint64 = cursor.Cursor

	for newCursor < tail && len(messages) < limit {
		offset := newCursor
		newCursor++

		msg, err := m.queueStore.Read(ctx, group.QueueName, offset)
		if err != nil {
			if err == storage.ErrOffsetOutOfRange {
				continue
			}
			releaseMessages(messages)
			return nil, cursor.Cursor, err
		}

		// Skip expired messages
		if msg.IsExpired() {
			message.Release(msg)
			continue
		}

		if filter != nil {
			queueRoot := "$queue/" + group.QueueName
			routingKey := types.ExtractRoutingKey(msg.Topic, queueRoot)
			if !filter.Matches(routingKey) {
				message.Release(msg)
				continue
			}
		}

		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil, cursor.Cursor, ErrNoMessages
	}

	return messages, newCursor, nil
}

func (m *Manager) updateStreamCursorLocked(ctx context.Context, group *types.ConsumerGroup, newCursor uint64) error {
	cursor := group.CursorView()
	if newCursor <= cursor.Cursor {
		return nil
	}
	if err := m.groupStore.UpdateCursor(ctx, group.QueueName, group.ID, newCursor); err != nil {
		return err
	}

	if !group.AutoCommit {
		return nil
	}
	if m.config.AutoCommitInterval <= 0 {
		return m.groupStore.UpdateCommitted(ctx, group.QueueName, group.ID, newCursor)
	}

	// lastCommit is shared across groups, so it needs its own lock even though
	// the caller already holds this group's.
	if !m.autoCommitDue(groupKey(group.QueueName, group.ID)) {
		return nil
	}
	if err := m.groupStore.UpdateCommitted(ctx, group.QueueName, group.ID, newCursor); err != nil {
		// The commit did not happen, so the interval must not be treated as
		// spent; otherwise a failing store would suppress commits for a whole
		// interval each time it failed.
		m.clearAutoCommit(groupKey(group.QueueName, group.ID))
		return err
	}
	return nil
}

// autoCommitDue reports whether this group's auto-commit interval has elapsed,
// recording the attempt when it has.
func (m *Manager) autoCommitDue(key string) bool {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	now := time.Now()
	if last, ok := m.lastCommit[key]; ok && now.Sub(last) < m.config.AutoCommitInterval {
		return false
	}
	m.lastCommit[key] = now
	return true
}

func (m *Manager) clearAutoCommit(key string) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	delete(m.lastCommit, key)
}

// claimFromCursor tries to claim a message from the cursor position.
func (m *Manager) claimFromCursor(ctx context.Context, group *types.ConsumerGroup, consumerID string, filter *Filter) (*message.Envelope, error) {
	// Check PEL capacity before claiming
	if m.config.MaxPELSize > 0 {
		pelCount := group.PendingCount()
		if pelCount >= m.config.MaxPELSize {
			return nil, ErrPELFull
		}
	}

	cursor := group.CursorView()

	// Get log tail
	tail, err := m.queueStore.Tail(ctx, group.QueueName)
	if err != nil {
		return nil, err
	}

	// Scan from cursor until we find a matching message or hit tail
	for cursor.Cursor < tail {
		offset := cursor.Cursor
		cursor.Cursor++

		// Read message
		msg, err := m.queueStore.Read(ctx, group.QueueName, offset)
		if err != nil {
			if err == storage.ErrOffsetOutOfRange {
				continue // Message was truncated, skip
			}
			return nil, err
		}

		// Skip expired messages
		if msg.IsExpired() {
			message.Release(msg)
			continue
		}

		// Check if message matches filter
		if filter != nil {
			queueRoot := "$queue/" + group.QueueName
			routingKey := types.ExtractRoutingKey(msg.Topic, queueRoot)
			if !filter.Matches(routingKey) {
				message.Release(msg)
				continue // Skip non-matching messages
			}
		}

		// Add to PEL
		entry := &types.PendingEntry{
			Offset:        offset,
			ConsumerID:    consumerID,
			ClaimedAt:     time.Now(),
			DeliveryCount: 1,
		}

		if err := m.groupStore.AddPendingEntry(ctx, group.QueueName, group.ID, entry); err != nil {
			message.Release(msg)
			return nil, err
		}

		// Update cursor
		if err := m.groupStore.UpdateCursor(ctx, group.QueueName, group.ID, cursor.Cursor); err != nil {
			message.Release(msg)
			return nil, err
		}

		return msg, nil
	}

	return nil, ErrNoMessages
}

// stealWork tries to steal a message from another consumer's PEL.
func (m *Manager) stealWork(ctx context.Context, group *types.ConsumerGroup, consumerID string, filter *Filter) (*message.Envelope, error) {
	// Get stealable entries
	stealable := group.StealableEntries(m.config.VisibilityTimeout, consumerID)
	if len(stealable) == 0 {
		return nil, ErrNoMessages
	}

	// Try to steal the oldest entry
	for _, entry := range stealable {
		// Poison message: exceeded max delivery attempts.
		if entry.DeliveryCount >= m.config.MaxDeliveryCount {
			if m.transferPoisonEntry(ctx, group, entry) {
				continue
			}
			// No dead-letter destination exists, so the entry falls through to
			// ordinary redelivery below rather than holding a pending slot for
			// a transfer that can never happen.
		}

		// Read message
		msg, err := m.queueStore.Read(ctx, group.QueueName, entry.Offset)
		if err != nil {
			continue // Message might be truncated
		}

		// Remove expired messages from PEL instead of redelivering
		if msg.IsExpired() {
			_ = m.groupStore.RemovePendingEntry(ctx, group.QueueName, group.ID, entry.ConsumerID, entry.Offset)
			message.Release(msg)
			continue
		}

		// Check filter
		if filter != nil {
			queueRoot := "$queue/" + group.QueueName
			routingKey := types.ExtractRoutingKey(msg.Topic, queueRoot)
			if !filter.Matches(routingKey) {
				message.Release(msg)
				continue
			}
		}

		// Transfer pending entry
		err = m.groupStore.TransferPendingEntry(
			ctx,
			group.QueueName,
			group.ID,
			entry.Offset,
			entry.ConsumerID,
			consumerID,
		)
		if err != nil {
			message.Release(msg)
			continue
		}

		return msg, nil
	}

	return nil, ErrNoMessages
}

// transferPoisonEntry moves one exhausted entry to the dead-letter queue.
//
// It reports whether the entry is settled or should stay pending — in both cases
// the caller skips it. It returns false only when the queue has no dead-letter
// destination at all, which is the one case where continuing to redeliver beats
// holding the entry forever: blocking would occupy a pending slot for a transfer
// that can never succeed, and eventually stall the group on MaxPELSize.
//
// A transient failure keeps the entry pending and retries later under backoff,
// because a destination does exist and redelivering now would duplicate a
// message the transfer may still deliver.
func (m *Manager) transferPoisonEntry(ctx context.Context, group *types.ConsumerGroup, entry *types.PendingEntry) (handled bool) {
	if m.config.OnDLQ == nil {
		m.reportPoisonWithoutDLQ(group, entry, ErrDLQHandlerUnavailable)
		return false
	}
	if !m.dlqTransferDue(group, entry) {
		// Throttled: the entry stays pending and is retried on a later cycle.
		return true
	}

	msg, err := m.queueStore.Read(ctx, group.QueueName, entry.Offset)
	if err != nil {
		m.reportDLQTransferFailure(group, entry, "read source record", err)
		return true
	}

	err = m.config.OnDLQ(ctx, group.QueueName, group.ID, msg, entry.Offset, entry.DeliveryCount, "max delivery count exceeded")
	message.Release(msg)
	if err != nil {
		if m.dlqUnavailable(err) {
			m.reportPoisonWithoutDLQ(group, entry, err)
			return false
		}
		m.reportDLQTransferFailure(group, entry, "append to dead-letter queue", err)
		return true
	}

	if err := m.groupStore.RemovePendingEntry(ctx, group.QueueName, group.ID, entry.ConsumerID, entry.Offset); err != nil {
		// The transfer succeeded, so the record is not lost. The entry is
		// retried, and the destination's deduplication keeps that from
		// producing a second dead-letter record.
		m.reportDLQTransferFailure(group, entry, "remove settled pending entry", err)
		return true
	}

	m.clearDLQRetry(group, entry)
	if m.config.Metrics != nil {
		m.config.Metrics.RecordDLQ()
	}
	return true
}

func (m *Manager) dlqUnavailable(err error) bool {
	if errors.Is(err, ErrDLQHandlerUnavailable) {
		return true
	}
	if m.config.DLQUnavailable == nil {
		return false
	}
	return m.config.DLQUnavailable(err)
}

// dlqTransferDue reports whether this entry's transfer may be attempted now,
// and records the attempt when it may.
func (m *Manager) dlqTransferDue(group *types.ConsumerGroup, entry *types.PendingEntry) bool {
	// dlqRetryAfter is shared across groups, so it needs its own lock even
	// though the caller already holds this group's.
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	key := dlqRetryKey(group, entry)
	if retryAt, throttled := m.dlqRetryAfter[key]; throttled && time.Now().Before(retryAt) {
		return false
	}
	m.dlqRetryAfter[key] = time.Now().Add(m.config.DLQRetryBackoff)
	return true
}

func (m *Manager) clearDLQRetry(group *types.ConsumerGroup, entry *types.PendingEntry) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	delete(m.dlqRetryAfter, dlqRetryKey(group, entry))
}

func dlqRetryKey(group *types.ConsumerGroup, entry *types.PendingEntry) string {
	return group.QueueName + "\x00" + group.ID + "\x00" + strconv.FormatUint(entry.Offset, 10)
}

func (m *Manager) reportDLQTransferFailure(group *types.ConsumerGroup, entry *types.PendingEntry, stage string, err error) {
	if m.config.Metrics != nil {
		m.config.Metrics.RecordDLQTransferFailure()
	}
	m.config.Logger.Warn("dead-letter transfer failed; entry stays pending",
		slog.String("stage", stage),
		slog.String("queue", group.QueueName),
		slog.String("group", group.ID),
		slog.String("consumer", entry.ConsumerID),
		slog.Uint64("offset", entry.Offset),
		slog.Int("delivery_count", entry.DeliveryCount),
		slog.Duration("retry_after", m.config.DLQRetryBackoff),
		slog.String("error", err.Error()))
}

func (m *Manager) reportPoisonWithoutDLQ(group *types.ConsumerGroup, entry *types.PendingEntry, err error) {
	if m.config.Metrics != nil {
		m.config.Metrics.RecordPoisonWithoutDLQ()
	}
	// Throttled on the same schedule as a transfer retry: without a destination
	// this entry is redelivered indefinitely, and one line per delivery would
	// drown the log.
	if !m.dlqTransferDue(group, entry) {
		return
	}
	m.config.Logger.Warn("poison message has no dead-letter destination; continuing redelivery",
		slog.String("queue", group.QueueName),
		slog.String("group", group.ID),
		slog.String("consumer", entry.ConsumerID),
		slog.Uint64("offset", entry.Offset),
		slog.Int("delivery_count", entry.DeliveryCount),
		slog.String("error", err.Error()))
}

// Ack acknowledges successful processing of a message.
func (m *Manager) Ack(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	// Remove from PEL
	if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset); err != nil {
		return err
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Advance committed offset if possible
	return m.advanceCommitted(ctx, group)
}

// AckBatch acknowledges multiple messages.
func (m *Manager) AckBatch(ctx context.Context, queueName, groupID, consumerID string, offsets []uint64) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	for _, offset := range offsets {
		if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset); err != nil {
			// Continue even if some fail
			continue
		}
	}

	// Get group to update committed offset
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	return m.advanceCommitted(ctx, group)
}

// Nack negatively acknowledges a message, making it available for redelivery.
func (m *Manager) Nack(ctx context.Context, queueName, groupID, consumerID string, offset uint64) error {
	return m.NackWithDelay(ctx, queueName, groupID, consumerID, offset, 0)
}

// NackWithDelay negatively acknowledges a message and controls when it becomes
// eligible for work stealing.
func (m *Manager) NackWithDelay(ctx context.Context, queueName, groupID, consumerID string, offset uint64, delay time.Duration) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	// Get group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Find the pending entry
	entry, ownerID := group.FindPending(offset)
	if entry == nil {
		return ErrMessageNotPending
	}

	// Only the owner can nack
	if ownerID != consumerID {
		return ErrConsumerNotFound
	}

	requeuer, ok := m.groupStore.(storage.PendingEntryRequeuer)
	if !ok {
		return ErrDelayedNackUnsupported
	}
	attemptedAt := time.Now().Add(delay)
	if delay == 0 {
		attemptedAt = attemptedAt.Add(-m.config.VisibilityTimeout - time.Second)
	}
	return requeuer.RequeuePendingEntry(ctx, queueName, groupID, consumerID, offset, attemptedAt)
}

// Reject rejects a message, moving it to the DLQ.
func (m *Manager) Reject(ctx context.Context, queueName, groupID, consumerID string, offset uint64, reason string) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}
	entry, ownerID := group.FindPending(offset)
	if entry == nil {
		return ErrMessageNotPending
	}
	if ownerID != consumerID {
		return ErrConsumerNotFound
	}
	if m.config.OnDLQ == nil {
		return ErrDLQHandlerUnavailable
	}
	msg, err := m.queueStore.Read(ctx, queueName, offset)
	if err != nil {
		return err
	}
	defer message.Release(msg)
	if err := m.config.OnDLQ(ctx, queueName, groupID, msg, offset, entry.DeliveryCount, reason); err != nil {
		return err
	}

	// The source delivery is removed only after the DLQ write succeeds.
	if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, consumerID, offset); err != nil {
		return err
	}

	group, err = m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	// Advance committed offset if possible
	return m.advanceCommitted(ctx, group)
}

func releaseMessages(envelopes []*message.Envelope) {
	for _, envelope := range envelopes {
		message.Release(envelope)
	}
}

// advanceCommitted updates the committed offset to the minimum pending offset.
func (m *Manager) advanceCommitted(ctx context.Context, group *types.ConsumerGroup) error {
	cursor := group.CursorView()

	// Find minimum pending offset
	minOffset, found := group.MinPendingOffset()

	committed := cursor.Committed
	switch {
	case !found:
		// No pending entries - committed = cursor
		committed = cursor.Cursor
	case minOffset > cursor.Committed:
		committed = minOffset
	}

	// The store owns the write; mutating the cursor here as well would race
	// every reader of the group.
	return m.groupStore.UpdateCommitted(ctx, group.QueueName, group.ID, committed)
}

// GetPendingCount returns the number of pending messages for a group.
func (m *Manager) GetPendingCount(ctx context.Context, queueName, groupID string) (int, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	return group.PendingCount(), nil
}

// GetLag returns the consumer lag (unprocessed messages).
func (m *Manager) GetLag(ctx context.Context, queueName, groupID string) (uint64, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	cursor := group.CursorView()

	tail, err := m.queueStore.Tail(ctx, queueName)
	if err != nil {
		return 0, err
	}

	// Lag = messages not yet delivered + pending messages
	if tail > cursor.Committed {
		return tail - cursor.Committed, nil
	}

	return 0, nil
}

// GetCommittedOffset returns the committed offset.
// This is the safe point for log truncation.
func (m *Manager) GetCommittedOffset(ctx context.Context, queueName, groupID string) (uint64, error) {
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return 0, err
	}

	cursor := group.CursorView()
	return cursor.Committed, nil
}

// CommitOffset explicitly commits an offset for a stream consumer group.
// This is used when AutoCommit is disabled for manual commit control.
func (m *Manager) CommitOffset(ctx context.Context, queueName, groupID string, offset uint64) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	if group.Mode != types.GroupModeStream {
		return ErrCommitOffsetOnlyForStreamMode
	}

	cursor := group.CursorView()
	if offset > cursor.Cursor {
		return ErrInvalidOffset
	}

	return m.groupStore.UpdateCommitted(ctx, queueName, groupID, offset)
}

// GetMinCommittedOffset returns the minimum committed offset across all groups for a stream.
// This is the global safe point for log truncation.
func (m *Manager) GetMinCommittedOffset(ctx context.Context, queueName string) (uint64, error) {
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return 0, err
	}

	if len(groups) == 0 {
		// No consumers - return tail (can truncate everything)
		return m.queueStore.Tail(ctx, queueName)
	}

	var minCommitted uint64
	first := true

	for _, group := range groups {
		cursor := group.CursorView()
		if first || cursor.Committed < minCommitted {
			minCommitted = cursor.Committed
			first = false
		}
	}

	return minCommitted, nil
}

// GetMinCommittedOffsetByMode returns the minimum committed offset for groups matching mode.
// If no groups of that mode exist, returns the queue tail.
func (m *Manager) GetMinCommittedOffsetByMode(ctx context.Context, queueName string, mode types.ConsumerGroupMode) (uint64, error) {
	groups, err := m.groupStore.ListConsumerGroups(ctx, queueName)
	if err != nil {
		return 0, err
	}

	var minCommitted uint64
	first := true

	for _, group := range groups {
		if mode != "" && group.Mode != mode {
			continue
		}
		cursor := group.CursorView()
		if first || cursor.Committed < minCommitted {
			minCommitted = cursor.Committed
			first = false
		}
	}

	if first {
		return m.queueStore.Tail(ctx, queueName)
	}

	return minCommitted, nil
}

// UpdateHeartbeat updates the heartbeat timestamp for a consumer.
func (m *Manager) UpdateHeartbeat(ctx context.Context, queueName, groupID, consumerID string) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	consumer := group.GetConsumer(consumerID)
	if consumer == nil {
		return ErrConsumerNotFound
	}

	consumer.LastHeartbeat = time.Now()
	return m.groupStore.RegisterConsumer(ctx, queueName, groupID, consumer)
}

// CleanupStaleConsumers removes consumers that haven't sent a heartbeat within the timeout.
func (m *Manager) CleanupStaleConsumers(ctx context.Context, queueName, groupID string, timeout time.Duration) ([]string, error) {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().Add(-timeout)
	var removed []string

	// Collect stale consumer IDs
	group.ForEachConsumer(func(id string, consumer *types.ConsumerInfo) bool {
		if consumer.LastHeartbeat.Before(cutoff) {
			removed = append(removed, id)
		}
		return true
	})

	// Delete stale consumers
	for _, id := range removed {
		group.DeleteConsumer(id)
	}

	if len(removed) > 0 {
		if err := m.groupStore.UpdateConsumerGroup(ctx, group); err != nil {
			return nil, err
		}
	}

	return removed, nil
}
