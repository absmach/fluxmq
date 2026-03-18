// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	mqttbroker "github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/storage"
)

type sessionSubscriptionResponse struct {
	Filter            string  `json:"filter"`
	QoS               byte    `json:"qos"`
	NoLocal           bool    `json:"no_local"`
	RetainAsPublished bool    `json:"retain_as_published"`
	RetainHandling    byte    `json:"retain_handling"`
	ConsumerGroup     string  `json:"consumer_group,omitempty"`
	SubscriptionID    *uint32 `json:"subscription_id,omitempty"`
}

type sessionResponse struct {
	ClientID          string                        `json:"client_id"`
	State             string                        `json:"state"`
	Connected         bool                          `json:"connected"`
	Protocol          string                        `json:"protocol"`
	Version           int                           `json:"version"`
	CleanStart        bool                          `json:"clean_start"`
	ExpiryInterval    uint32                        `json:"expiry_interval"`
	ConnectedAt       *time.Time                    `json:"connected_at,omitempty"`
	DisconnectedAt    *time.Time                    `json:"disconnected_at,omitempty"`
	ReceiveMaximum    uint16                        `json:"receive_maximum"`
	MaxPacketSize     uint32                        `json:"max_packet_size"`
	TopicAliasMax     uint16                        `json:"topic_alias_max"`
	RequestResponse   bool                          `json:"request_response"`
	RequestProblem    bool                          `json:"request_problem"`
	HasWill           bool                          `json:"has_will"`
	SubscriptionCount int                           `json:"subscription_count"`
	InflightCount     int                           `json:"inflight_count"`
	OfflineQueueDepth int                           `json:"offline_queue_depth"`
	Subscriptions     []sessionSubscriptionResponse `json:"subscriptions,omitempty"`
}

type listSessionsResponse struct {
	Sessions      []sessionResponse `json:"sessions"`
	NextPageToken string            `json:"next_page_token,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func (s *Server) handleSessions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.broker == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "broker not available")
		return
	}

	state := strings.TrimSpace(r.URL.Query().Get("state"))
	if !validSessionState(state) {
		writeAPIError(w, http.StatusBadRequest, "state must be one of: connected, disconnected, all")
		return
	}

	limit := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeAPIError(w, http.StatusBadRequest, "limit must be a non-negative integer")
			return
		}
		limit = parsed
	}

	sessions, nextPageToken, err := s.broker.ListSessions(mqttbroker.SessionListFilter{
		Prefix:    strings.TrimSpace(r.URL.Query().Get("prefix")),
		State:     state,
		Limit:     limit,
		PageToken: strings.TrimSpace(r.URL.Query().Get("page_token")),
	})
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := listSessionsResponse{
		Sessions:      make([]sessionResponse, 0, len(sessions)),
		NextPageToken: nextPageToken,
	}
	for _, snapshot := range sessions {
		resp.Sessions = append(resp.Sessions, sessionToResponse(snapshot))
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/v1/sessions/" {
		s.handleSessions(w, r)
		return
	}

	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.broker == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "broker not available")
		return
	}

	clientID, ok := sessionIDFromPath(r)
	if !ok {
		writeAPIError(w, http.StatusNotFound, "session not found")
		return
	}

	snapshot, err := s.broker.GetSessionSnapshot(clientID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			writeAPIError(w, http.StatusNotFound, "session not found")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, sessionToResponse(*snapshot))
}

func sessionIDFromPath(r *http.Request) (string, bool) {
	const prefix = "/api/v1/sessions/"

	escaped := r.URL.EscapedPath()
	if !strings.HasPrefix(escaped, prefix) {
		return "", false
	}

	rawID := strings.TrimPrefix(escaped, prefix)
	if rawID == "" || strings.Contains(rawID, "/") {
		return "", false
	}

	clientID, err := url.PathUnescape(rawID)
	if err != nil || clientID == "" {
		return "", false
	}

	return clientID, true
}

func sessionToResponse(snapshot mqttbroker.SessionSnapshot) sessionResponse {
	resp := sessionResponse{
		ClientID:          snapshot.ClientID,
		State:             snapshot.State,
		Connected:         snapshot.Connected,
		Protocol:          protocolLabel(snapshot.Version),
		Version:           int(snapshot.Version),
		CleanStart:        snapshot.CleanStart,
		ExpiryInterval:    snapshot.ExpiryInterval,
		ConnectedAt:       timePtr(snapshot.ConnectedAt),
		DisconnectedAt:    timePtr(snapshot.DisconnectedAt),
		ReceiveMaximum:    snapshot.ReceiveMaximum,
		MaxPacketSize:     snapshot.MaxPacketSize,
		TopicAliasMax:     snapshot.TopicAliasMax,
		RequestResponse:   snapshot.RequestResponse,
		RequestProblem:    snapshot.RequestProblem,
		HasWill:           snapshot.HasWill,
		SubscriptionCount: snapshot.SubscriptionCount,
		InflightCount:     snapshot.InflightCount,
		OfflineQueueDepth: snapshot.OfflineQueueDepth,
	}

	if len(snapshot.Subscriptions) > 0 {
		resp.Subscriptions = make([]sessionSubscriptionResponse, 0, len(snapshot.Subscriptions))
		for _, sub := range snapshot.Subscriptions {
			resp.Subscriptions = append(resp.Subscriptions, sessionSubscriptionResponse{
				Filter:            sub.Filter,
				QoS:               sub.QoS,
				NoLocal:           sub.NoLocal,
				RetainAsPublished: sub.RetainAsPublished,
				RetainHandling:    sub.RetainHandling,
				ConsumerGroup:     sub.ConsumerGroup,
				SubscriptionID:    sub.SubscriptionID,
			})
		}
	}

	return resp
}

func protocolLabel(version byte) string {
	switch version {
	case 3:
		return "mqtt3.1"
	case 4:
		return "mqtt3.1.1"
	case 5:
		return "mqtt5"
	default:
		return "unknown"
	}
}

func validSessionState(state string) bool {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "", "all", "connected", "disconnected":
		return true
	default:
		return false
	}
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
