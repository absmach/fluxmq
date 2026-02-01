// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/broker"
)

// Config holds health check server configuration.
type Config struct {
	Address         string
	ShutdownTimeout time.Duration
}

// Server provides health check endpoints for monitoring and orchestration.
type Server struct {
	config   Config
	broker   *broker.Broker
	cluster  cluster.Cluster
	logger   *slog.Logger
	server   *http.Server
	listener net.Listener
}

// New creates a new health check server.
func New(cfg Config, b *broker.Broker, cl cluster.Cluster, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		config:  cfg,
		broker:  b,
		cluster: cl,
		logger:  logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/cluster/status", s.handleClusterStatus)

	s.server = &http.Server{
		Addr:         cfg.Address,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	return s
}

// Addr returns the listener's network address.
// Returns nil if server hasn't started listening yet.
func (s *Server) Addr() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

// Listen starts the health check server.
func (s *Server) Listen(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}
	s.listener = listener

	s.logger.Info("Starting health check server", "address", s.listener.Addr().String())

	errCh := make(chan error, 1)
	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.logger.Info("Health check server shutdown initiated")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("Health check server shutdown error", "error", err)
			return err
		}

		s.logger.Info("Health check server stopped")
		return nil
	}
}

// HealthResponse represents the liveness probe response.
type HealthResponse struct {
	Status string `json:"status"`
}

// handleHealth implements liveness probe.
// Returns 200 OK if the process is alive.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(HealthResponse{
		Status: "healthy",
	})
}

// ReadyResponse represents the readiness probe response.
type ReadyResponse struct {
	Status  string `json:"status"`
	Details string `json:"details,omitempty"`
}

// handleReady implements readiness probe.
// Returns 200 OK if the node is ready to accept traffic.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	// Check if broker is ready (not shutting down)
	if s.broker == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ReadyResponse{
			Status:  "not_ready",
			Details: "broker not initialized",
		})
		return
	}

	// If clustered, check cluster connectivity
	if s.cluster != nil {
		nodeID := s.cluster.NodeID()
		if nodeID == "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(ReadyResponse{
				Status:  "not_ready",
				Details: "cluster not initialized",
			})
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ReadyResponse{
		Status: "ready",
	})
}

// ClusterStatusResponse represents cluster health information.
type ClusterStatusResponse struct {
	NodeID      string `json:"node_id"`
	IsLeader    bool   `json:"is_leader"`
	ClusterMode bool   `json:"cluster_mode"`
	NodeCount   int    `json:"node_count,omitempty"`
	Sessions    int    `json:"sessions"`
	Details     string `json:"details,omitempty"`
}

// handleClusterStatus returns cluster membership and health information.
func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	response := ClusterStatusResponse{
		ClusterMode: false,
	}

	// Single-node mode
	if s.cluster == nil {
		response.NodeID = "single-node"
		response.Sessions = int(s.broker.Stats().GetCurrentConnections())
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
		return
	}

	// Cluster mode
	response.ClusterMode = true
	response.NodeID = s.cluster.NodeID()
	response.IsLeader = s.cluster.IsLeader()
	response.Sessions = int(s.broker.Stats().GetCurrentConnections())

	// Get node count if available (would need to add GetNodeCount to cluster interface)
	// For now, just return what we have

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
