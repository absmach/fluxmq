// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	amqpbroker "github.com/absmach/fluxmq/amqp/broker"
	mqttbroker "github.com/absmach/fluxmq/mqtt/broker"
)

type subscriptionResponse struct {
	Filter          string `json:"filter"`
	SubscriberCount int    `json:"subscriber_count"`
	MaxQoS          byte   `json:"max_qos"`
}

type listSubscriptionsResponse struct {
	Subscriptions []subscriptionResponse `json:"subscriptions"`
	NextPageToken string                 `json:"next_page_token,omitempty"`
}

type subscriptionClientResponse struct {
	ClientID string `json:"client_id"`
	QoS      byte   `json:"qos"`
}

type listSubscriptionClientsResponse struct {
	Filter        string                       `json:"filter"`
	Clients       []subscriptionClientResponse `json:"clients"`
	NextPageToken string                       `json:"next_page_token,omitempty"`
}

func (s *Server) handleSubscriptions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.broker == nil && s.amqpBroker == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "broker not available")
		return
	}

	if isSubscriptionsListPath(r.URL.Path) {
		s.handleSubscriptionsList(w, r)
		return
	}

	filter, ok := subscriptionFilterFromPath(r)
	if !ok {
		writeAPIError(w, http.StatusNotFound, "subscription not found")
		return
	}

	s.handleSubscriptionClients(w, r, filter)
}

func (s *Server) handleSubscriptionsList(w http.ResponseWriter, r *http.Request) {
	state, valid := subscriptionsStateParam(r)
	if !valid {
		writeAPIError(w, http.StatusBadRequest, "state must be one of: connected, disconnected, all")
		return
	}

	limit, ok := parseNonNegativeInt(r.URL.Query().Get("limit"))
	if !ok {
		writeAPIError(w, http.StatusBadRequest, "limit must be a non-negative integer")
		return
	}

	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	pageToken := strings.TrimSpace(r.URL.Query().Get("page_token"))

	snapshots, nextPageToken, err := s.listSubscriptions(state, prefix, limit, pageToken)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, listSubscriptionsResponse{
		Subscriptions: snapshots,
		NextPageToken: nextPageToken,
	})
}

func (s *Server) handleSubscriptionClients(w http.ResponseWriter, r *http.Request, filter string) {
	state, valid := subscriptionsStateParam(r)
	if !valid {
		writeAPIError(w, http.StatusBadRequest, "state must be one of: connected, disconnected, all")
		return
	}

	limit, ok := parseNonNegativeInt(r.URL.Query().Get("limit"))
	if !ok {
		writeAPIError(w, http.StatusBadRequest, "limit must be a non-negative integer")
		return
	}

	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	pageToken := strings.TrimSpace(r.URL.Query().Get("page_token"))

	clients, nextPageToken, err := s.listSubscriptionClients(filter, state, prefix, limit, pageToken)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, listSubscriptionClientsResponse{
		Filter:        filter,
		Clients:       clients,
		NextPageToken: nextPageToken,
	})
}

func (s *Server) listSubscriptions(
	state string,
	prefix string,
	limit int,
	pageToken string,
) ([]subscriptionResponse, string, error) {
	aggregated := make(map[string]subscriptionResponse)

	// Fetch all MQTT subscriptions without server-side pagination (Limit: 0)
	// so we can merge with AMQP data and paginate the combined result.
	if s.broker != nil {
		mqttSubs, _, err := s.broker.ListSubscriptions(mqttbroker.SubscriptionListFilter{
			Prefix: prefix,
			State:  state,
			Limit:  0,
		})
		if err != nil {
			return nil, "", err
		}

		for _, snap := range mqttSubs {
			aggregated[snap.Filter] = subscriptionResponse{
				Filter:          snap.Filter,
				SubscriberCount: snap.SubscriberCount,
				MaxQoS:          snap.MaxQoS,
			}
		}
	}

	if s.amqpBroker != nil && stateAllowsConnected(state) {
		mergeAMQPSubscriptionResponses(aggregated, s.amqpBroker.ListSubscriptionSnapshots(), prefix)
	}

	filters := make([]string, 0, len(aggregated))
	for filter := range aggregated {
		filters = append(filters, filter)
	}
	slices.Sort(filters)

	start := 0
	if pageToken != "" {
		start = len(filters)
		for i, filter := range filters {
			if filter > pageToken {
				start = i
				break
			}
		}
	}

	end := len(filters)
	if limit > 0 && start+limit < end {
		end = start + limit
	}

	pageFilters := filters[start:end]
	result := make([]subscriptionResponse, 0, len(pageFilters))
	for _, filter := range pageFilters {
		result = append(result, aggregated[filter])
	}

	nextPageToken := ""
	if end < len(filters) && len(pageFilters) > 0 {
		nextPageToken = pageFilters[len(pageFilters)-1]
	}

	return result, nextPageToken, nil
}

func mergeAMQPSubscriptionResponses(
	aggregated map[string]subscriptionResponse,
	snapshots []amqpbroker.SubscriptionSnapshot,
	prefix string,
) {
	for _, snap := range snapshots {
		if prefix != "" && !strings.HasPrefix(snap.Filter, prefix) {
			continue
		}

		existing, ok := aggregated[snap.Filter]
		if !ok {
			aggregated[snap.Filter] = subscriptionResponse{
				Filter:          snap.Filter,
				SubscriberCount: 1,
				MaxQoS:          snap.QoS,
			}
			continue
		}

		existing.SubscriberCount++
		if snap.QoS > existing.MaxQoS {
			existing.MaxQoS = snap.QoS
		}
		aggregated[snap.Filter] = existing
	}
}

func (s *Server) listSubscriptionClients(
	filter string,
	state string,
	prefix string,
	limit int,
	pageToken string,
) ([]subscriptionClientResponse, string, error) {
	clients := make(map[string]byte)

	// Fetch all MQTT clients without server-side pagination so we can merge
	// with AMQP consumers and paginate the combined result.
	if s.broker != nil {
		mqttClients, _, err := s.broker.ListSubscriptionClients(filter, state, prefix, 0, "")
		if err != nil {
			return nil, "", err
		}

		for _, client := range mqttClients {
			if existing, ok := clients[client.ClientID]; !ok || client.QoS > existing {
				clients[client.ClientID] = client.QoS
			}
		}
	}

	if s.amqpBroker != nil && stateAllowsConnected(state) {
		mergeAMQPSubscriptionClients(clients, s.amqpBroker.ListSubscriptionSnapshots(), filter, prefix)
	}

	clientIDs := make([]string, 0, len(clients))
	for clientID := range clients {
		clientIDs = append(clientIDs, clientID)
	}
	slices.Sort(clientIDs)

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
	result := make([]subscriptionClientResponse, 0, len(pageClientIDs))
	for _, clientID := range pageClientIDs {
		result = append(result, subscriptionClientResponse{
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

func mergeAMQPSubscriptionClients(
	clients map[string]byte,
	snapshots []amqpbroker.SubscriptionSnapshot,
	filter string,
	prefix string,
) {
	for _, snap := range snapshots {
		if snap.Filter != filter {
			continue
		}
		if prefix != "" && !strings.HasPrefix(snap.ClientID, prefix) {
			continue
		}
		if existing, ok := clients[snap.ClientID]; !ok || snap.QoS > existing {
			clients[snap.ClientID] = snap.QoS
		}
	}
}

func subscriptionsStateParam(r *http.Request) (string, bool) {
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	if state == "" {
		// Subscriptions endpoint defaults to active subscriptions.
		state = "connected"
	}
	if !validSessionState(state) {
		return "", false
	}
	return state, true
}

func parseNonNegativeInt(raw string) (int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, true
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return 0, false
	}
	return v, true
}

func isSubscriptionsListPath(path string) bool {
	return path == "/api/v1/subscriptions" || path == "/api/v1/subscriptions/"
}

func subscriptionFilterFromPath(r *http.Request) (string, bool) {
	const prefix = "/api/v1/subscriptions/"
	const suffix = "/clients"

	escaped := r.URL.EscapedPath()
	if !strings.HasPrefix(escaped, prefix) {
		return "", false
	}

	raw := strings.TrimPrefix(escaped, prefix)
	if raw == "" {
		return "", false
	}
	if !strings.HasSuffix(raw, suffix) {
		return "", false
	}

	rawFilter := strings.TrimSuffix(raw, suffix)
	if rawFilter == "" || strings.Contains(rawFilter, "/") {
		return "", false
	}

	filter, err := url.PathUnescape(rawFilter)
	if err != nil || filter == "" {
		return "", false
	}
	return filter, true
}
