// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package consumer

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/absmach/fluxmq/internal/keylock"
	"github.com/absmach/fluxmq/logstorage"
	"github.com/absmach/fluxmq/message"
	"github.com/absmach/fluxmq/queue/storage"
	"github.com/absmach/fluxmq/queue/types"
	"golang.org/x/sync/errgroup"
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
	ErrCommitOffsetNotMonotonic      = errors.New("commit offset cannot move behind the committed position")
	ErrNackNotSupportedForStream     = errors.New("nack is not supported for stream groups; commit or seek instead")
	ErrPELFull                       = errors.New("pending entry list at capacity")
	ErrDLQHandlerUnavailable         = errors.New("dead-letter queue handler unavailable")
	ErrTransferInProgress            = errors.New("dead-letter transfer already in progress for this entry")
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

	// transfersMu guards transfers, which is keyed per entry and consulted from
	// operations holding different group locks.
	transfersMu sync.Mutex

	// transfers reserves entries whose dead-letter transfer is running with the
	// group lock released. The destination write cannot be covered by that lock
	// without stalling every consumer in the group for its duration — a Raft
	// round trip when the destination is replicated — but the entry must not be
	// settled or stolen while the write is in flight, or one message ends up
	// both dead-lettered and redelivered.
	//
	// The reservation is process-local, which is the scope the group lock had:
	// a group driven from two nodes was never serialised by it either.
	transfers map[transferKey]struct{}

	// stateMu guards the two maps below, which are keyed by group but shared
	// across them. Under groupLocks alone, two goroutines holding different
	// group locks would race on these.
	stateMu    sync.Mutex
	lastCommit map[string]time.Time

	// poison tracks pending entries that have exhausted their delivery budget
	// and have not reached a dead-letter queue, keyed by group and then offset.
	//
	// It serves two purposes: it rate-limits transfer retries per entry, and its
	// population is what the poison gauges report. Keying by group rather than
	// flattening to one map is what makes both cheap — a sweep can prune one
	// group's stale entries without walking every other group's.
	//
	// Deliberately in-memory. This is a throttle and a gauge, not durable state;
	// losing it on restart costs one immediate retry per stuck entry.
	poison map[string]map[uint64]poisonState

	// poisonTotal and poisonNoDestination mirror the map's population so the
	// gauges can be published without walking it.
	poisonTotal         int
	poisonNoDestination int
}

// poisonState records why one pending entry has not been dead-lettered.
type poisonState struct {
	retryAfter time.Time

	// noDestination separates the two situations an operator handles
	// differently: a transfer that keeps failing resolves itself when the
	// destination recovers, while a queue with no dead-letter destination at
	// all never resolves without a configuration change.
	noDestination bool
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
		queueStore: queueStore,
		groupStore: groupStore,
		config:     config,
		lastCommit: make(map[string]time.Time),
		poison:     make(map[string]map[uint64]poisonState),
		transfers:  make(map[transferKey]struct{}),
	}
}

// GetOrCreateGroup retrieves or creates a consumer group.
func (m *Manager) GetOrCreateGroup(ctx context.Context, queueName, groupID, pattern string, mode types.ConsumerGroupMode, autoCommit bool) (*types.ConsumerGroup, error) {
	group, _, err := m.getOrCreateGroup(ctx, queueName, groupID, pattern, mode, autoCommit, false)
	return group, err
}

// GetOrCreateConfiguredGroup retrieves or creates a consumer group and applies
// an explicitly requested stream auto-commit policy. The returned bool reports
// whether this call created the group, so subscription delivery policies can
// initialize a durable cursor without resetting it on reconnect.
func (m *Manager) GetOrCreateConfiguredGroup(ctx context.Context, queueName, groupID, pattern string, mode types.ConsumerGroupMode, autoCommit bool, configureAutoCommit bool) (*types.ConsumerGroup, bool, error) {
	return m.getOrCreateGroup(ctx, queueName, groupID, pattern, mode, autoCommit, configureAutoCommit)
}

func (m *Manager) getOrCreateGroup(ctx context.Context, queueName, groupID, pattern string, mode types.ConsumerGroupMode, autoCommit bool, configureAutoCommit bool) (*types.ConsumerGroup, bool, error) {
	// Same serialization as every other transition on this group. Without it,
	// two subscribers arriving together can both find no group and both create
	// one, and a mode negotiated by one can be overwritten by the other.
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	// Try to get existing group
	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err == nil {
		if mode != "" && group.Mode == "" {
			// Naming the mode for the first time settles nothing and migrates
			// nothing: the group has not been running under either contract, so
			// only the policy itself is recorded.
			group.Mode = mode
			group.SetAutoCommit(autoCommit)
			if err := m.groupStore.UpdateConsumerGroup(ctx, group); err != nil {
				return nil, false, err
			}
			return group, false, nil
		}
		if mode != "" && group.Mode != mode {
			return nil, false, ErrGroupModeMismatch
		}
		// Checked even when the caller named no mode: an explicit auto-commit
		// policy is a property of the subscription, and dropping it because the
		// mode was left to the stored group silently gave the caller the
		// opposite settlement contract to the one it asked for.
		if configureAutoCommit && group.AutoCommitEnabled() != autoCommit {
			m.applyAutoCommitLocked(group, autoCommit)
			if err := m.groupStore.UpdateConsumerGroup(ctx, group); err != nil {
				return nil, false, err
			}
		}
		return group, false, nil
	}

	// Check for "not found" errors from various storage implementations
	if !errors.Is(err, storage.ErrConsumerNotFound) && !errors.Is(err, logstorage.ErrGroupNotFound) {
		return nil, false, err
	}

	// Create new group
	group = types.NewConsumerGroupState(queueName, groupID, pattern)
	if mode != "" {
		group.Mode = mode
	}
	group.SetAutoCommit(autoCommit)

	if err := m.groupStore.CreateConsumerGroup(ctx, group); err != nil {
		// Handle race condition - another process might have created it
		if errors.Is(err, storage.ErrConsumerGroupExists) {
			group, getErr := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
			return group, false, getErr
		}
		return nil, false, err
	}

	return group, true, nil
}

// applyAutoCommitLocked moves an established stream group from one settlement
// contract to the other. The caller holds the group lock and performs the
// single write that persists it: UpdateConsumerGroup replicates the whole
// group, so a separate cursor write either side of it is replayed away on any
// store that does not hand back the live group pointer.
func (m *Manager) applyAutoCommitLocked(group *types.ConsumerGroup, autoCommit bool) {
	if group.Mode != types.GroupModeStream {
		// Auto-commit describes a stream cursor. A queue group settles through
		// its pending list either way, and migrating it here would drop entries
		// its consumers still hold.
		group.SetAutoCommit(autoCommit)
		return
	}

	// Everything read under the old contract was already exposed to the
	// consumer, and neither contract can settle it now: under auto-commit
	// delivery was the commit, and after the switch to explicit settlement only
	// later deliveries enter the pending list. Either way that boundary is the
	// safe point.
	if cursor := group.CursorView(); cursor.Committed < cursor.Cursor {
		group.SetCommitted(cursor.Cursor)
	}
	if autoCommit {
		// Nothing settles a pending entry once delivery commits on its own, so
		// entries carried over from manual settlement would never leave the
		// group: leaked state, a pending count that never returns to zero, and
		// stale attempt counts if the group is ever switched back.
		if cleared := group.ClearPending(); cleared > 0 {
			m.config.Logger.Warn("dropped unsettled deliveries on switch to auto-commit",
				slog.String("queue", group.QueueName),
				slog.String("group", group.ID),
				slog.Int("entries", cleared))
		}
	}
	group.SetAutoCommit(autoCommit)
}

// RegisterConsumer adds a consumer to a group.
func (m *Manager) RegisterConsumer(ctx context.Context, queueName, groupID, consumerID, clientID, proxyNodeID string) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

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
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

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

	if !errors.Is(err, ErrNoMessages) {
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

// ClaimManualStream retrieves one explicitly settled stream delivery.
//
// A consumer holds at most one unsettled delivery at a time, whatever batch
// size the caller asked for. The stream is ordered and settlement is explicit,
// so handing out the next record while the current one is unsettled would leave
// a nack of the current one unable to redeliver ahead of a record the consumer
// already has. Consumers in the group progress independently.
//
// A delivery the consumer already owns is redelivered before any new record is
// claimed, so a reconnecting consumer resumes on the message it left unsettled.
func (m *Manager) ClaimManualStream(ctx context.Context, queueName, groupID, consumerID string, filter *Filter) (*message.Envelope, error) {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return nil, err
	}
	if group.Mode != types.GroupModeStream || group.AutoCommitEnabled() {
		return nil, ErrGroupModeMismatch
	}

	if msg := m.redeliverOwnLocked(ctx, group, consumerID, filter); msg != nil {
		return msg, nil
	}
	// An entry the consumer still owns but could not be handed back — its
	// record is gone, or it is mid dead-letter transfer — must not fall through
	// to a new record, or the group would hold two unsettled deliveries for one
	// consumer and lose the ordering the manual contract rests on.
	if group.PendingCountFor(consumerID) > 0 {
		return nil, ErrNoMessages
	}

	msg, err := m.stealWork(ctx, group, consumerID, filter)
	if err == nil {
		return msg, nil
	}
	if !errors.Is(err, ErrNoMessages) {
		return nil, err
	}

	return m.claimFromCursor(ctx, group, consumerID, filter)
}

// redeliverOwnLocked hands back the oldest entry consumerID still holds,
// renewing its visibility lease and counting the delivery attempt. It returns
// nil when the consumer holds nothing redeliverable. The caller holds the group
// lock.
//
// The visibility timeout is deliberately not consulted. It exists to stop one
// consumer stealing from another that is still working, which is not what a
// consumer asking for its own unsettled entry back is doing; making the owner
// wait it out stalled every reconnect for the full timeout.
//
// An entry whose record cannot be read is left pending rather than settled:
// only the consumer settles a manual delivery.
func (m *Manager) redeliverOwnLocked(ctx context.Context, group *types.ConsumerGroup, consumerID string, filter *Filter) *message.Envelope {
	queueRoot := "$queue/" + group.QueueName
	for _, entry := range group.OwnedPending(consumerID) {
		if m.transferring(group.QueueName, group.ID, entry.Offset) {
			// A dead-letter transfer is writing this entry to its destination
			// with the group lock released. Redelivering it now would put the
			// same message in two places.
			continue
		}
		if entry.DeliveryCount >= m.config.MaxDeliveryCount {
			// Same rule as work stealing: an exhausted entry goes to the
			// dead-letter queue rather than being handed back.
			// deferPoisonTransfer reports false only when the queue has no
			// dead-letter destination, where redelivering beats holding a
			// pending slot for a transfer that can never happen.
			if m.deferPoisonTransfer(group, &entry) {
				continue
			}
		}
		msg, err := m.queueStore.Read(ctx, group.QueueName, entry.Offset)
		if err != nil {
			continue // Message might be truncated
		}
		if msg.IsExpired() {
			if err := m.groupStore.RemovePendingEntry(ctx, group.QueueName, group.ID, consumerID, entry.Offset); err != nil {
				m.config.Logger.Warn("failed to drop expired pending entry",
					slog.String("queue", group.QueueName),
					slog.String("group", group.ID),
					slog.Uint64("offset", entry.Offset),
					slog.String("error", err.Error()))
			}
			message.Release(msg)
			continue
		}
		if filter != nil && !filter.Matches(types.ExtractRoutingKey(msg.Topic, queueRoot)) {
			message.Release(msg)
			continue
		}
		// A transfer to the current owner, which is what renews the lease and
		// records the attempt. Handing the record back without it would leave
		// the entry stealable by another consumer while this one works on it.
		if err := m.groupStore.TransferPendingEntry(ctx, group.QueueName, group.ID, entry.Offset, consumerID, consumerID); err != nil {
			message.Release(msg)
			continue
		}
		return msg
	}

	return nil
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
			if errors.Is(err, storage.ErrOffsetOutOfRange) {
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

	if !group.AutoCommitEnabled() {
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
			if errors.Is(err, storage.ErrOffsetOutOfRange) {
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

	// Drop tracked poison entries this group no longer holds. An entry settled
	// by an ordinary ack leaves no other signal, so without this the gauge only
	// ever counts up.
	m.prunePoison(group)

	if len(stealable) == 0 {
		return nil, ErrNoMessages
	}

	// Try to steal the oldest entry
	for _, entry := range stealable {
		if m.transferring(group.QueueName, group.ID, entry.Offset) {
			// A dead-letter transfer is writing this entry to its destination
			// with the group lock released. Redelivering it now would put the
			// same message in two places.
			continue
		}

		// Poison message: exceeded max delivery attempts.
		if entry.DeliveryCount >= m.config.MaxDeliveryCount {
			if m.deferPoisonTransfer(group, entry) {
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

// deferPoisonTransfer hands an exhausted entry to the background sweeper and
// reports whether the sweeper owns it now.
//
// The transfer itself does not happen here. It reaches storage and, for a
// replicated dead-letter queue, a full Raft round trip bounded only by
// AckTimeout — and this runs under the group's lock, inside a claim that may
// walk up to a batch of entries in one acquisition. Performing transfers on the
// claim path meant one poison message could hold an entire consumer group for
// seconds, and a batch of them for considerably longer.
//
// It returns false only when the queue has no dead-letter destination at all,
// which is the one case where continuing to redeliver beats holding the entry
// forever: blocking would occupy a pending slot for a transfer that can never
// succeed, and eventually stall the group on MaxPELSize.
func (m *Manager) deferPoisonTransfer(group *types.ConsumerGroup, entry *types.PendingEntry) (handled bool) {
	if m.config.OnDLQ == nil {
		m.reportPoisonWithoutDLQ(group, entry, ErrDLQHandlerUnavailable)
		return false
	}
	// One lock acquisition, not two. A group holding many poison entries has
	// every one of them re-examined on each claim until a sweep drains them, so
	// this runs per entry per claim and the difference is measurable.
	return m.trackPoisonForSweep(group, entry)
}

// SweepPoison performs the dead-letter transfers the claim path deferred.
//
// It runs off the delivery path, so a slow destination costs dead-letter
// latency rather than consumer throughput. Each transfer takes the owning
// group's lock only to validate and to settle; the destination write happens
// with the lock released, guarded by the same reservation that keeps a
// concurrent ack or steal from touching the entry.
func (m *Manager) SweepPoison(ctx context.Context) {
	if m.config.OnDLQ == nil {
		return
	}

	refs := m.poisonWorkList()
	if len(refs) == 0 {
		return
	}

	// Bounded concurrency, not a serial walk. One destination that is slow or
	// down would otherwise hold every other queue's transfers behind it for as
	// long as it takes.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(poisonSweepConcurrency)
	for _, ref := range refs {
		if groupCtx.Err() != nil {
			break
		}
		group.Go(func() error {
			m.sweepPoisonEntry(groupCtx, ref)
			return nil
		})
	}
	// The workers never return an error; this waits for them.
	_ = group.Wait()
}

// poisonSweepConcurrency bounds how many dead-letter transfers run at once. It
// is small deliberately: the destinations are queues on this broker, and the
// point is to stop one slow destination blocking the others, not to saturate
// storage with transfers of messages nobody is waiting for.
const poisonSweepConcurrency = 8

// poisonRef names one entry awaiting a dead-letter transfer.
type poisonRef struct {
	queueName string
	groupID   string
	offset    uint64
}

// poisonWorkList copies the tracked entries so the sweep does not hold stateMu
// while transferring.
func (m *Manager) poisonWorkList() []poisonRef {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	refs := make([]poisonRef, 0, m.poisonTotal)
	for key, entries := range m.poison {
		queueName, groupID, ok := splitGroupKey(key)
		if !ok {
			continue
		}
		for offset, state := range entries {
			if time.Now().Before(state.retryAfter) {
				continue
			}
			refs = append(refs, poisonRef{queueName: queueName, groupID: groupID, offset: offset})
		}
	}
	return refs
}

func (m *Manager) sweepPoisonEntry(ctx context.Context, ref poisonRef) {
	group, entry, msg, ok := m.preparePoisonTransfer(ctx, ref)
	if !ok {
		return
	}

	err := m.config.OnDLQ(ctx, ref.queueName, ref.groupID, msg, ref.offset, entry.DeliveryCount, "max delivery count exceeded")
	message.Release(msg)

	m.finishPoisonTransfer(ctx, ref, group, &entry, err)
}

// preparePoisonTransfer validates the entry, reserves it and reads its record
// under the group's lock, so the destination write can run without it.
func (m *Manager) preparePoisonTransfer(ctx context.Context, ref poisonRef) (*types.ConsumerGroup, types.PendingEntry, *message.Envelope, bool) {
	groupLock := m.groupLocks.KeyPair(ref.queueName, ref.groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, ref.queueName, ref.groupID)
	if err != nil {
		m.forgetPoison(ref.queueName, ref.groupID, ref.offset)
		return nil, types.PendingEntry{}, nil, false
	}

	entry, owner := group.FindPending(ref.offset)
	if owner == "" {
		// Settled while it waited; nothing left to transfer.
		m.forgetPoison(ref.queueName, ref.groupID, ref.offset)
		return nil, types.PendingEntry{}, nil, false
	}
	if !m.beginTransfer(transferKey{queueName: ref.queueName, groupID: ref.groupID, offset: ref.offset}) {
		return nil, types.PendingEntry{}, nil, false
	}

	msg, err := m.queueStore.Read(ctx, ref.queueName, ref.offset)
	if err != nil {
		m.endTransfer(transferKey{queueName: ref.queueName, groupID: ref.groupID, offset: ref.offset})
		m.scheduleNextPoisonAttempt(ref)
		m.reportDLQTransferFailure(group, &entry, "read source record", err)
		return nil, types.PendingEntry{}, nil, false
	}

	return group, entry, msg, true
}

// finishPoisonTransfer releases the reservation and settles the source, but
// only once the destination write succeeded. A failure leaves the entry pending
// and scheduled for a later sweep, which is what keeps the move loss-safe.
func (m *Manager) finishPoisonTransfer(ctx context.Context, ref poisonRef, group *types.ConsumerGroup, entry *types.PendingEntry, transferErr error) {
	groupLock := m.groupLocks.KeyPair(ref.queueName, ref.groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	m.endTransfer(transferKey{queueName: ref.queueName, groupID: ref.groupID, offset: ref.offset})

	if transferErr != nil {
		if m.dlqUnavailable(transferErr) {
			// No destination at all: stop sweeping it and let the claim path
			// redeliver rather than hold a pending slot forever.
			m.reportPoisonWithoutDLQ(group, entry, transferErr)
			return
		}
		m.scheduleNextPoisonAttempt(ref)
		m.reportDLQTransferFailure(group, entry, "append to dead-letter queue", transferErr)
		return
	}

	if err := m.groupStore.RemovePendingEntry(ctx, ref.queueName, ref.groupID, entry.ConsumerID, ref.offset); err != nil {
		// The transfer succeeded, so the record is not lost. The entry is swept
		// again, and the destination's deduplication keeps that from producing
		// a second dead-letter record.
		m.scheduleNextPoisonAttempt(ref)
		m.reportDLQTransferFailure(group, entry, "remove settled pending entry", err)
		return
	}

	m.clearDLQRetry(group, entry)
	if m.config.Metrics != nil {
		m.config.Metrics.RecordDLQ()
	}
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
// and records the attempt so the next one waits out the backoff.
func (m *Manager) dlqTransferDue(group *types.ConsumerGroup, entry *types.PendingEntry) bool {
	// poison is shared across groups, so it needs its own lock even though the
	// caller already holds this group's.
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	key := groupKey(group.QueueName, group.ID)
	entries, ok := m.poison[key]
	if !ok {
		entries = make(map[uint64]poisonState)
		m.poison[key] = entries
	}

	state, tracked := entries[entry.Offset]
	if tracked && time.Now().Before(state.retryAfter) {
		return false
	}
	if !tracked {
		m.poisonTotal++
	}
	state.retryAfter = time.Now().Add(m.config.DLQRetryBackoff)
	entries[entry.Offset] = state
	m.publishPoisonGaugesLocked()

	return true
}

// trackPoisonForSweep puts an entry on the sweeper's work list and reports
// whether the sweeper owns it.
//
// It answers both questions under one lock because the claim path asks them
// together, once per poison entry per claim, for as long as those entries sit
// waiting to be swept.
func (m *Manager) trackPoisonForSweep(group *types.ConsumerGroup, entry *types.PendingEntry) bool {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	key := groupKey(group.QueueName, group.ID)
	entries, ok := m.poison[key]
	if !ok {
		entries = make(map[uint64]poisonState)
		m.poison[key] = entries
	}

	if state, tracked := entries[entry.Offset]; tracked {
		// A sweep that found no destination hands the entry back to redelivery.
		return !state.noDestination
	}

	// A newly tracked entry is due immediately; the backoff applies to retries.
	entries[entry.Offset] = poisonState{}
	m.poisonTotal++
	m.publishPoisonGaugesLocked()

	return true
}

// scheduleNextPoisonAttempt makes a failed transfer wait out the backoff before
// the sweeper tries it again. Without it a destination that is down is retried
// on every sweep, which is the cost the backoff exists to avoid.
func (m *Manager) scheduleNextPoisonAttempt(ref poisonRef) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	entries, ok := m.poison[groupKey(ref.queueName, ref.groupID)]
	if !ok {
		return
	}
	state, tracked := entries[ref.offset]
	if !tracked {
		return
	}
	state.retryAfter = time.Now().Add(m.config.DLQRetryBackoff)
	entries[ref.offset] = state
}

// splitGroupKey reverses groupKey.
func splitGroupKey(key string) (queueName, groupID string, ok bool) {
	queueName, groupID, ok = strings.Cut(key, "\x00")
	return queueName, groupID, ok
}

// markPoisonWithoutDestination records that this entry has nowhere to go, and
// reports whether that is newly true. Callers count the transition rather than
// the observation: without a destination the entry is examined on every steal
// cycle, and counting those measures how often consumers looked rather than how
// many messages are stuck.
func (m *Manager) markPoisonWithoutDestination(group *types.ConsumerGroup, entry *types.PendingEntry) bool {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	key := groupKey(group.QueueName, group.ID)
	entries, ok := m.poison[key]
	if !ok {
		entries = make(map[uint64]poisonState)
		m.poison[key] = entries
	}

	// The entry may not be tracked yet: a queue with no destination at all is
	// reported before any transfer is attempted, so this is where it enters the
	// population rather than dlqTransferDue.
	state, tracked := entries[entry.Offset]
	if !tracked {
		m.poisonTotal++
	}
	if state.noDestination {
		return false
	}

	state.noDestination = true
	entries[entry.Offset] = state
	m.poisonNoDestination++
	m.publishPoisonGaugesLocked()

	return true
}

// clearDLQRetry forgets an entry that is no longer poison, because it reached a
// dead-letter queue or was settled some other way.
func (m *Manager) clearDLQRetry(group *types.ConsumerGroup, entry *types.PendingEntry) {
	m.forgetPoison(group.QueueName, group.ID, entry.Offset)
}

func (m *Manager) forgetPoison(queueName, groupID string, offset uint64) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	m.forgetPoisonLocked(queueName, groupID, offset)
	m.publishPoisonGaugesLocked()
}

func (m *Manager) forgetPoisonLocked(queueName, groupID string, offset uint64) {
	key := groupKey(queueName, groupID)
	entries, ok := m.poison[key]
	if !ok {
		return
	}
	state, tracked := entries[offset]
	if !tracked {
		return
	}

	delete(entries, offset)
	m.poisonTotal--
	if state.noDestination {
		m.poisonNoDestination--
	}
	if len(entries) == 0 {
		delete(m.poison, key)
	}
}

// prunePoison drops tracked entries that are no longer pending in this group.
//
// Without it the map grows for the lifetime of the process: an entry settled by
// an ordinary ack leaves no other signal, and a gauge that only ever counts up
// is worse than none.
//
// The pending set is read only when this group actually has tracked entries.
// Building it means walking the whole pending list, and the overwhelmingly
// common case is a group with no poison at all — this runs on every steal
// sweep, so paying for that walk unconditionally would tax the delivery path
// to maintain a gauge that is almost always zero.
func (m *Manager) prunePoison(group *types.ConsumerGroup) {
	key := groupKey(group.QueueName, group.ID)

	m.stateMu.Lock()
	tracked := len(m.poison[key])
	m.stateMu.Unlock()
	if tracked == 0 {
		return
	}

	pending := group.PendingOffsets()

	m.stateMu.Lock()
	defer m.stateMu.Unlock()

	for offset := range m.poison[key] {
		if _, stillPending := pending[offset]; !stillPending {
			m.forgetPoisonLocked(group.QueueName, group.ID, offset)
		}
	}
	m.publishPoisonGaugesLocked()
}

// publishPoisonGaugesLocked reports the current population. Callers hold stateMu.
func (m *Manager) publishPoisonGaugesLocked() {
	if m.config.Metrics == nil {
		return
	}
	m.config.Metrics.SetPoisonPending(uint64(m.poisonTotal), uint64(m.poisonNoDestination))
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
	// Counted on the transition into the state, not on every observation. This
	// entry is examined on every steal cycle for as long as it is stuck, so
	// counting observations would measure how often consumers looked rather
	// than how many messages have nowhere to go.
	if m.markPoisonWithoutDestination(group, entry) && m.config.Metrics != nil {
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

	if m.transferring(queueName, groupID, offset) {
		return ErrTransferInProgress
	}

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
	_, ownerID := group.FindPending(offset)
	if ownerID == "" {
		return ErrMessageNotPending
	}

	// Only the owner can nack
	if ownerID != consumerID {
		return ErrConsumerNotFound
	}

	if m.transferring(queueName, groupID, offset) {
		return ErrTransferInProgress
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

// transferKey names one pending entry undergoing a dead-letter transfer.
type transferKey struct {
	queueName string
	groupID   string
	offset    uint64
}

// beginTransfer reserves an entry, reporting false when one is already running.
func (m *Manager) beginTransfer(key transferKey) bool {
	m.transfersMu.Lock()
	defer m.transfersMu.Unlock()

	if _, running := m.transfers[key]; running {
		return false
	}
	m.transfers[key] = struct{}{}
	return true
}

func (m *Manager) endTransfer(key transferKey) {
	m.transfersMu.Lock()
	defer m.transfersMu.Unlock()

	delete(m.transfers, key)
}

// transferring reports whether an entry is reserved. Settling or stealing a
// reserved entry would race the destination write that is already in flight.
func (m *Manager) transferring(queueName, groupID string, offset uint64) bool {
	m.transfersMu.Lock()
	defer m.transfersMu.Unlock()

	_, running := m.transfers[transferKey{queueName: queueName, groupID: groupID, offset: offset}]
	return running
}

// prepareTransfer validates the entry, reserves it and reads its record, all
// under the group lock, so the caller can perform the destination write without
// holding it. The returned envelope belongs to the caller.
func (m *Manager) prepareTransfer(ctx context.Context, key transferKey, consumerID string) (*message.Envelope, int, error) {
	groupLock := m.groupLocks.KeyPair(key.queueName, key.groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, key.queueName, key.groupID)
	if err != nil {
		return nil, 0, err
	}
	entry, ownerID := group.FindPending(key.offset)
	if ownerID == "" {
		return nil, 0, ErrMessageNotPending
	}
	if ownerID != consumerID {
		return nil, 0, ErrConsumerNotFound
	}
	if m.config.OnDLQ == nil {
		return nil, 0, ErrDLQHandlerUnavailable
	}
	if !m.beginTransfer(key) {
		return nil, 0, ErrTransferInProgress
	}

	msg, err := m.queueStore.Read(ctx, key.queueName, key.offset)
	if err != nil {
		m.endTransfer(key)
		return nil, 0, err
	}
	return msg, entry.DeliveryCount, nil
}

// finishTransfer releases the reservation and settles the source, but only once
// the destination write has succeeded. A failed write leaves the entry pending,
// which is what keeps the transition loss-safe.
func (m *Manager) finishTransfer(ctx context.Context, key transferKey, consumerID string, transferErr error) error {
	groupLock := m.groupLocks.KeyPair(key.queueName, key.groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	m.endTransfer(key)
	if transferErr != nil {
		return transferErr
	}

	if err := m.groupStore.RemovePendingEntry(ctx, key.queueName, key.groupID, consumerID, key.offset); err != nil {
		return err
	}

	group, err := m.groupStore.GetConsumerGroup(ctx, key.queueName, key.groupID)
	if err != nil {
		return err
	}

	return m.advanceCommitted(ctx, group)
}

// Reject rejects a message, moving it to the DLQ.
func (m *Manager) Reject(ctx context.Context, queueName, groupID, consumerID string, offset uint64, reason string) error {
	key := transferKey{queueName: queueName, groupID: groupID, offset: offset}

	msg, deliveryCount, err := m.prepareTransfer(ctx, key, consumerID)
	if err != nil {
		return err
	}

	// The destination write runs without the group lock. It reaches storage and,
	// for a replicated dead-letter queue, a full Raft round trip bounded only by
	// AckTimeout; holding the group lock across it stalls every consumer in the
	// group for that long. The reservation taken above is what keeps the entry
	// from being settled or stolen in the meantime, and the destination's
	// deduplication is what makes a retry after a failure here safe.
	transferErr := m.config.OnDLQ(ctx, queueName, groupID, msg, offset, deliveryCount, reason)
	message.Release(msg)

	// The source delivery is removed only after the DLQ write succeeds.
	return m.finishTransfer(ctx, key, consumerID, transferErr)
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
// AdvanceCommitted moves a stream group's committed position forward under the
// group's lock, and only forward.
//
// The read and the write have to be one operation. Split, two acknowledgements
// both read the old position and the lower one writes last, so the committed
// offset goes backwards and the group is redelivered messages it already
// settled. Both stores assign the value unconditionally, so nothing downstream
// catches it.
//
// A commit that is already covered is not an error: acknowledging twice is
// ordinary client behaviour, and the second one has nothing to do.
func (m *Manager) AdvanceCommitted(ctx context.Context, queueName, groupID string, committed uint64) error {
	groupLock := m.groupLocks.KeyPair(queueName, groupID)
	groupLock.Lock()
	defer groupLock.Unlock()

	group, err := m.groupStore.GetConsumerGroup(ctx, queueName, groupID)
	if err != nil {
		return err
	}

	cursor := group.CursorView()
	if committed > cursor.Cursor {
		return ErrInvalidOffset
	}
	if committed <= cursor.Committed {
		return nil
	}

	return m.groupStore.UpdateCommitted(ctx, queueName, groupID, committed)
}

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
	if offset < cursor.Committed {
		// Rewinding is not a commit. Nothing today needs replay, and letting a
		// commit move the safe point backwards silently redelivers settled
		// messages; a rewind should be its own named operation if it is wanted.
		return ErrCommitOffsetNotMonotonic
	}
	if !group.AutoCommitEnabled() {
		// Manual stream delivery is represented in the existing PEL. An
		// offset commit settles every delivery before the committed position,
		// preserving the client API that predates per-delivery AMQP ACKs.
		for owner, entries := range group.Snapshot().PEL {
			for _, entry := range entries {
				if entry == nil || entry.Offset >= offset {
					continue
				}
				if err := m.groupStore.RemovePendingEntry(ctx, queueName, groupID, owner, entry.Offset); err != nil {
					return err
				}
			}
		}
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

	// Through the group's lock: the heartbeat used to be written through a
	// pointer GetConsumer handed out, with nothing serialising it against the
	// encoder that persists the group.
	consumer, ok := group.TouchConsumer(consumerID, time.Now())
	if !ok {
		return ErrConsumerNotFound
	}

	return m.groupStore.RegisterConsumer(ctx, queueName, groupID, &consumer)
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
