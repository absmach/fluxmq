// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

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
	if s.broker == nil {
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

	snapshots, nextPageToken, err := s.broker.ListSubscriptions(mqttbroker.SubscriptionListFilter{
		Prefix:    prefix,
		State:     state,
		Limit:     limit,
		PageToken: pageToken,
	})
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := make([]subscriptionResponse, 0, len(snapshots))
	for _, snap := range snapshots {
		resp = append(resp, subscriptionResponse{
			Filter:          snap.Filter,
			SubscriberCount: snap.SubscriberCount,
			MaxQoS:          snap.MaxQoS,
		})
	}

	writeJSON(w, http.StatusOK, listSubscriptionsResponse{
		Subscriptions: resp,
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

	clients, nextPageToken, err := s.broker.ListSubscriptionClients(filter, state, prefix, limit, pageToken)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := make([]subscriptionClientResponse, 0, len(clients))
	for _, client := range clients {
		resp = append(resp, subscriptionClientResponse{
			ClientID: client.ClientID,
			QoS:      client.QoS,
		})
	}

	writeJSON(w, http.StatusOK, listSubscriptionClientsResponse{
		Filter:        filter,
		Clients:       resp,
		NextPageToken: nextPageToken,
	})
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
