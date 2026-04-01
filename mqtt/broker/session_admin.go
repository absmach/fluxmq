// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/absmach/fluxmq/mqtt/session"
	"github.com/absmach/fluxmq/storage"
)

// SessionListFilter controls session listing.
type SessionListFilter struct {
	Prefix    string
	State     string
	Limit     int
	PageToken string
}

// SessionSubscription describes a session subscription in the admin API.
type SessionSubscription struct {
	Filter            string
	QoS               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	ConsumerGroup     string
	SubscriptionID    *uint32
}

// SessionSnapshot is a point-in-time session view for management APIs.
type SessionSnapshot struct {
	ClientID          string
	ExternalID        string
	State             string
	Connected         bool
	Version           byte
	CleanStart        bool
	ExpiryInterval    uint32
	ConnectedAt       time.Time
	DisconnectedAt    time.Time
	ReceiveMaximum    uint16
	MaxPacketSize     uint32
	TopicAliasMax     uint16
	RequestResponse   bool
	RequestProblem    bool
	HasWill           bool
	SubscriptionCount int
	InflightCount     int
	OfflineQueueDepth int
	Subscriptions     []SessionSubscription
}

type sessionListEntry struct {
	clientID  string
	connected bool
}

// ListSessions returns paginated sessions for the current node.
//
// The result is eventually consistent: storage and live session state are read
// non-atomically, so a session that connects or disconnects during the call may
// appear in either state. Live in-memory state takes precedence when available.
func (b *Broker) ListSessions(ctx context.Context, filter SessionListFilter) ([]SessionSnapshot, string, error) {
	entries, err := b.collectSessionListEntries()
	if err != nil {
		return nil, "", err
	}

	state := normalizeSessionState(filter.State)
	limit := filter.Limit

	filtered := make([]sessionListEntry, 0, len(entries))
	for _, entry := range entries {
		if filter.Prefix != "" && !strings.HasPrefix(entry.clientID, filter.Prefix) {
			continue
		}
		if state != "" {
			if state == sessionStateConnected && !entry.connected {
				continue
			}
			if state == sessionStateDisconnected && entry.connected {
				continue
			}
		}
		filtered = append(filtered, entry)
	}

	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].clientID < filtered[j].clientID
	})

	start := 0
	if filter.PageToken != "" {
		start = len(filtered)
		for i, entry := range filtered {
			if entry.clientID > filter.PageToken {
				start = i
				break
			}
		}
	}

	end := len(filtered)
	// A limit of 0 means "no limit".
	if limit > 0 && start+limit < end {
		end = start + limit
	}

	pageEntries := filtered[start:end]
	snapshots := make([]SessionSnapshot, 0, len(pageEntries))
	for _, entry := range pageEntries {
		snapshot, err := b.GetSessionSnapshot(ctx, entry.clientID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, "", err
		}
		snapshot.Subscriptions = nil
		snapshots = append(snapshots, *snapshot)
	}

	nextPageToken := ""
	if end < len(filtered) && len(pageEntries) > 0 {
		nextPageToken = pageEntries[len(pageEntries)-1].clientID
	}

	return snapshots, nextPageToken, nil
}

// GetSessionSnapshot returns a detailed session snapshot for a client.
func (b *Broker) GetSessionSnapshot(ctx context.Context, clientID string) (*SessionSnapshot, error) {
	if clientID == "" {
		return nil, storage.ErrNotFound
	}

	if live := b.sessionsMap.Get(clientID); live != nil {
		return b.snapshotFromLiveSession(live)
	}

	if b.stores.sessions == nil {
		return nil, storage.ErrNotFound
	}

	stored, err := b.stores.sessions.Get(clientID)
	if err != nil {
		return nil, err
	}

	return b.snapshotFromStoredSession(ctx, stored)
}

// collectSessionListEntries merges stored and live sessions into a single list.
//
// The read is not atomic across the two sources: storage is queried first, then
// the live session cache is iterated under per-shard read locks. A session that
// connects or disconnects between these two reads may briefly show stale state.
// Live state is overlaid on top of stored state so that connected sessions
// always reflect the in-memory truth. This is acceptable for a monitoring
// endpoint — callers should treat results as eventually consistent.
func (b *Broker) collectSessionListEntries() ([]sessionListEntry, error) {
	entries := make([]sessionListEntry, 0)
	indexByClientID := make(map[string]int)

	if b.stores.sessions != nil {
		stored, err := b.stores.sessions.List()
		if err != nil {
			return nil, err
		}
		for _, item := range stored {
			indexByClientID[item.ClientID] = len(entries)
			entries = append(entries, sessionListEntry{
				clientID:  item.ClientID,
				connected: item.Connected,
			})
		}
	}

	// Overlay live session state. ForEach holds per-shard read locks so
	// individual session reads are race-free, but the overall snapshot
	// is not atomic across shards or relative to the storage read above.
	b.sessionsMap.ForEach(func(s *session.Session) {
		connected := s.IsConnected()
		if idx, ok := indexByClientID[s.ID]; ok {
			entries[idx].connected = connected
			return
		}
		indexByClientID[s.ID] = len(entries)
		entries = append(entries, sessionListEntry{
			clientID:  s.ID,
			connected: connected,
		})
	})

	return entries, nil
}

func (b *Broker) snapshotFromLiveSession(s *session.Session) (*SessionSnapshot, error) {
	info := s.Info()
	subs, err := b.subscriptionsForClient(s.ID, s)
	if err != nil {
		return nil, err
	}

	return &SessionSnapshot{
		ClientID:          s.ID,
		ExternalID:        s.ExternalID,
		State:             s.State().String(),
		Connected:         s.IsConnected(),
		Version:           info.Version,
		CleanStart:        info.CleanStart,
		ExpiryInterval:    info.ExpiryInterval,
		ConnectedAt:       info.ConnectedAt,
		DisconnectedAt:    info.DisconnectedAt,
		ReceiveMaximum:    info.ReceiveMaximum,
		MaxPacketSize:     info.MaxPacketSize,
		TopicAliasMax:     info.TopicAliasMax,
		RequestResponse:   info.RequestResponse,
		RequestProblem:    info.RequestProblem,
		HasWill:           s.GetWill() != nil,
		SubscriptionCount: len(subs),
		InflightCount:     len(s.Inflight().GetAll()),
		OfflineQueueDepth: s.OfflineQueue().Len(),
		Subscriptions:     subs,
	}, nil
}

func (b *Broker) snapshotFromStoredSession(ctx context.Context, stored *storage.Session) (*SessionSnapshot, error) {
	snapshot := &SessionSnapshot{
		ClientID:        stored.ClientID,
		ExternalID:      stored.ExternalID,
		State:           disconnectedState(stored.Connected),
		Connected:       stored.Connected,
		Version:         stored.Version,
		CleanStart:      stored.CleanStart,
		ExpiryInterval:  stored.ExpiryInterval,
		ConnectedAt:     stored.ConnectedAt,
		DisconnectedAt:  stored.DisconnectedAt,
		ReceiveMaximum:  stored.ReceiveMaximum,
		MaxPacketSize:   stored.MaxPacketSize,
		TopicAliasMax:   stored.TopicAliasMax,
		RequestResponse: stored.RequestResponse,
		RequestProblem:  stored.RequestProblem,
	}

	if b.stores.subscriptions != nil {
		subs, err := b.stores.subscriptions.GetForClient(stored.ClientID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		snapshot.Subscriptions = storedSubscriptions(subs)
		snapshot.SubscriptionCount = len(snapshot.Subscriptions)
	}

	if b.stores.messages != nil {
		inflight, err := b.stores.messages.List(stored.ClientID + inflightPrefix)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		queue, err := b.stores.messages.List(stored.ClientID + queuePrefix)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
		snapshot.InflightCount = len(inflight)
		snapshot.OfflineQueueDepth = len(queue)
	}

	if b.stores.wills != nil {
		if will, err := b.stores.wills.Get(ctx, stored.ClientID); err == nil && will != nil {
			snapshot.HasWill = true
		} else if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}

	return snapshot, nil
}

func normalizeSessionState(state string) string {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "", "all":
		return ""
	case sessionStateConnected:
		return sessionStateConnected
	case sessionStateDisconnected:
		return sessionStateDisconnected
	default:
		return ""
	}
}

func disconnectedState(connected bool) string {
	if connected {
		return sessionStateConnected
	}
	return sessionStateDisconnected
}

func liveSubscriptions(s *session.Session) []SessionSubscription {
	subs := s.GetSubscriptions()
	if len(subs) == 0 {
		return nil
	}

	result := make([]SessionSubscription, 0, len(subs))
	for filter, opts := range subs {
		result = append(result, SessionSubscription{
			Filter:            filter,
			QoS:               0,
			NoLocal:           opts.NoLocal,
			RetainAsPublished: opts.RetainAsPublished,
			RetainHandling:    opts.RetainHandling,
			ConsumerGroup:     opts.ConsumerGroup,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Filter < result[j].Filter
	})

	return result
}

func storedSubscriptions(subs []*storage.Subscription) []SessionSubscription {
	if len(subs) == 0 {
		return nil
	}

	result := make([]SessionSubscription, 0, len(subs))
	for _, sub := range subs {
		result = append(result, SessionSubscription{
			Filter:            sub.Filter,
			QoS:               sub.QoS,
			NoLocal:           sub.Options.NoLocal,
			RetainAsPublished: sub.Options.RetainAsPublished,
			RetainHandling:    sub.Options.RetainHandling,
			ConsumerGroup:     sub.Options.ConsumerGroup,
			SubscriptionID:    sub.SubscriptionID,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Filter < result[j].Filter
	})

	return result
}

func (b *Broker) subscriptionsForClient(clientID string, live *session.Session) ([]SessionSubscription, error) {
	if b.stores.subscriptions != nil {
		subs, err := b.stores.subscriptions.GetForClient(clientID)
		if err == nil {
			return storedSubscriptions(subs), nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	if live == nil {
		return nil, nil
	}
	return liveSubscriptions(live), nil
}
