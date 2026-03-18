// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import "net/http"

type nodeResponse struct {
	ID            string  `json:"id"`
	Address       string  `json:"address"`
	Healthy       bool    `json:"healthy"`
	Leader        bool    `json:"leader"`
	UptimeSeconds float64 `json:"uptime_seconds"`
}

type clusterResponse struct {
	NodeID      string         `json:"node_id"`
	ClusterMode bool           `json:"cluster_mode"`
	IsLeader    bool           `json:"is_leader"`
	Nodes       []nodeResponse `json:"nodes"`
}

func (s *Server) buildClusterResponse() clusterResponse {
	if s.cluster == nil {
		return clusterResponse{
			NodeID:      "single-node",
			ClusterMode: false,
			IsLeader:    true,
			Nodes:       []nodeResponse{},
		}
	}

	nodes := s.cluster.Nodes()
	resp := clusterResponse{
		NodeID:      s.cluster.NodeID(),
		ClusterMode: true,
		IsLeader:    s.cluster.IsLeader(),
		Nodes:       make([]nodeResponse, 0, len(nodes)),
	}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, nodeResponse{
			ID:            n.ID,
			Address:       n.Address,
			Healthy:       n.Healthy,
			Leader:        n.Leader,
			UptimeSeconds: n.Uptime.Seconds(),
		})
	}
	return resp
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.broker == nil && s.amqpBroker == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "broker not available")
		return
	}

	writeJSON(w, http.StatusOK, s.buildClusterResponse())
}
