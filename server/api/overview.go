// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import "net/http"

type sessionSummary struct {
	Connected int `json:"connected"`
	Total     int `json:"total"`
}

type overviewCluster struct {
	Nodes []nodeResponse `json:"nodes"`
}

type overviewResponse struct {
	NodeID        string          `json:"node_id"`
	ClusterMode   bool            `json:"cluster_mode"`
	IsLeader      bool            `json:"is_leader"`
	UptimeSeconds float64         `json:"uptime_seconds"`
	Sessions      sessionSummary  `json:"sessions"`
	Stats         statsResponse   `json:"stats"`
	Cluster       overviewCluster `json:"cluster"`
}

func (s *Server) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.broker == nil && s.amqpBroker == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "broker not available")
		return
	}

	stats := s.buildStatsResponse()
	cl := s.buildClusterResponse(r.Context())

	var sessions sessionSummary
	if s.broker != nil {
		sessions.Connected = int(s.broker.Stats().GetCurrentConnections())
		sessions.Total = s.broker.SessionCount()
	}

	resp := overviewResponse{
		NodeID:        cl.NodeID,
		ClusterMode:   cl.ClusterMode,
		IsLeader:      cl.IsLeader,
		UptimeSeconds: stats.UptimeSeconds,
		Sessions:      sessions,
		Stats:         stats,
		Cluster: overviewCluster{
			Nodes: cl.Nodes,
		},
	}

	writeJSON(w, http.StatusOK, resp)
}
