// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"sort"
	"strings"
)

const (
	sessionStateConnected    = "connected"
	sessionStateDisconnected = "disconnected"
)

// SubscriptionListFilter controls subscription listing.
type SubscriptionListFilter struct {
	Prefix    string
	State     string
	Limit     int
	PageToken string
}

// SubscriptionSnapshot is an aggregated view of a topic filter.
type SubscriptionSnapshot struct {
	Filter          string
	SubscriberCount int
	MaxQoS          byte
}

// SubscriptionClientSnapshot is a per-client view for a filter.
type SubscriptionClientSnapshot struct {
	ClientID string
	QoS      byte
}

// ListSubscriptions returns aggregated subscriptions for the current node.
//
// The result is eventually consistent because it merges storage and live
// session state non-atomically (same consistency model as ListSessions).
func (b *Broker) ListSubscriptions(filter SubscriptionListFilter) ([]SubscriptionSnapshot, string, error) {
	entries, err := b.collectSessionListEntries()
	if err != nil {
		return nil, "", err
	}

	state := normalizeSessionState(filter.State)
	aggregates := make(map[string]map[string]byte) // filter -> clientID -> qos

	for _, entry := range entries {
		if state != "" {
			if state == sessionStateConnected && !entry.connected {
				continue
			}
			if state == sessionStateDisconnected && entry.connected {
				continue
			}
		}

		live := b.sessionsMap.Get(entry.clientID)
		subs, err := b.subscriptionsForClient(entry.clientID, live)
		if err != nil {
			return nil, "", err
		}

		for _, sub := range subs {
			if filter.Prefix != "" && !strings.HasPrefix(sub.Filter, filter.Prefix) {
				continue
			}

			clients, ok := aggregates[sub.Filter]
			if !ok {
				clients = make(map[string]byte)
				aggregates[sub.Filter] = clients
			}
			if existingQoS, exists := clients[entry.clientID]; !exists || sub.QoS > existingQoS {
				clients[entry.clientID] = sub.QoS
			}
		}
	}

	filters := make([]string, 0, len(aggregates))
	for f := range aggregates {
		filters = append(filters, f)
	}
	sort.Strings(filters)

	start := 0
	if filter.PageToken != "" {
		start = len(filters)
		for i, f := range filters {
			if f > filter.PageToken {
				start = i
				break
			}
		}
	}

	end := len(filters)
	if filter.Limit > 0 && start+filter.Limit < end {
		end = start + filter.Limit
	}

	pageFilters := filters[start:end]
	result := make([]SubscriptionSnapshot, 0, len(pageFilters))
	for _, topicFilter := range pageFilters {
		clientQoS := aggregates[topicFilter]
		maxQoS := byte(0)
		for _, qos := range clientQoS {
			if qos > maxQoS {
				maxQoS = qos
			}
		}
		result = append(result, SubscriptionSnapshot{
			Filter:          topicFilter,
			SubscriberCount: len(clientQoS),
			MaxQoS:          maxQoS,
		})
	}

	nextPageToken := ""
	if end < len(filters) && len(pageFilters) > 0 {
		nextPageToken = pageFilters[len(pageFilters)-1]
	}

	return result, nextPageToken, nil
}

// ListSubscriptionClients returns clients subscribed to an exact filter.
//
// The filter must be the canonical subscription filter (wildcards allowed).
// Uses a direct store lookup via GetByFilter instead of scanning all sessions.
func (b *Broker) ListSubscriptionClients(
	filter string,
	state string,
	prefix string,
	limit int,
	pageToken string,
) ([]SubscriptionClientSnapshot, string, error) {
	if b.stores.subscriptions == nil {
		return nil, "", nil
	}

	subs, err := b.stores.subscriptions.GetByFilter(filter)
	if err != nil {
		return nil, "", err
	}

	normalizedState := normalizeSessionState(state)
	clients := make(map[string]byte, len(subs))

	for _, sub := range subs {
		if prefix != "" && !strings.HasPrefix(sub.ClientID, prefix) {
			continue
		}

		if normalizedState != "" {
			connected := false
			if live := b.sessionsMap.Get(sub.ClientID); live != nil {
				connected = live.IsConnected()
			}
			if normalizedState == "connected" && !connected {
				continue
			}
			if normalizedState == "disconnected" && connected {
				continue
			}
		}

		if existing, ok := clients[sub.ClientID]; !ok || sub.QoS > existing {
			clients[sub.ClientID] = sub.QoS
		}
	}

	clientIDs := make([]string, 0, len(clients))
	for clientID := range clients {
		clientIDs = append(clientIDs, clientID)
	}
	sort.Strings(clientIDs)

	start := 0
	if pageToken != "" {
		start = len(clientIDs)
		for i, clientID := range clientIDs {
			if clientID > pageToken {
				start = i
				break
			}
		}
	}

	end := len(clientIDs)
	if limit > 0 && start+limit < end {
		end = start + limit
	}

	pageClientIDs := clientIDs[start:end]
	result := make([]SubscriptionClientSnapshot, 0, len(pageClientIDs))
	for _, clientID := range pageClientIDs {
		result = append(result, SubscriptionClientSnapshot{
			ClientID: clientID,
			QoS:      clients[clientID],
		})
	}

	nextPageToken := ""
	if end < len(clientIDs) && len(pageClientIDs) > 0 {
		nextPageToken = pageClientIDs[len(pageClientIDs)-1]
	}

	return result, nextPageToken, nil
}
