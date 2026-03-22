// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/storage"
)

// Operating modes reported by health and readiness endpoints.
const (
	ModeNominal  = "nominal"
	ModeDegraded = "degraded"
)

// Check status values.
const (
	StatusUp   = "up"
	StatusDown = "down"
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
	store    storage.Store
	logger   *slog.Logger
	server   *http.Server
	listener net.Listener
}

// New creates a new health check server.
func New(cfg Config, b *broker.Broker, cl cluster.Cluster, st storage.Store, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		config:  cfg,
		broker:  b,
		cluster: cl,
		store:   st,
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
// Returns empty string if server hasn't started listening yet.
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout) //nolint:contextcheck // intentionally creates new context for graceful shutdown
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil { //nolint:contextcheck // intentionally creates new context for graceful shutdown
			s.logger.Error("Health check server shutdown error", "error", err)
			return err
		}

		s.logger.Info("Health check server stopped")
		return nil
	}
}

// CheckResult holds the status of an individual health check component.
type CheckResult struct {
	Status  string `json:"status"`
	Details string `json:"details,omitempty"`
}

// HealthResponse represents the liveness probe response.
type HealthResponse struct {
	Status string `json:"status"`
}

// ReadyResponse represents the readiness probe response.
type ReadyResponse struct {
	Status  string                  `json:"status"`
	Mode    string                  `json:"mode,omitempty"`
	Details string                  `json:"details,omitempty"`
	Checks  map[string]*CheckResult `json:"checks,omitempty"`
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
	json.NewEncoder(w).Encode(HealthResponse{ //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
		Status: "healthy",
	})
}

// handleReady implements readiness probe.
//
// The endpoint evaluates three components and returns a composite result:
//
//   - broker:  fails (503) when the broker has not been initialized.
//   - storage: fails (503) when storage is not initialized or Ping() fails.
//   - cluster: in clustered mode, returns degraded (200) when some peers are
//     unreachable but the local node is operational; fails (503) when the
//     cluster has not finished initializing (empty NodeID).
//
// A degraded response still returns HTTP 200 so that load-balancers keep
// routing traffic — the node can serve local clients even when some peers
// are unreachable. The "mode" field distinguishes nominal from degraded.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	checks := make(map[string]*CheckResult, 3)
	mode := ModeNominal

	// --- Broker ---
	if s.broker == nil {
		checks["broker"] = &CheckResult{Status: StatusDown, Details: "not initialized"}
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ReadyResponse{ //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
			Status:  "not_ready",
			Details: "broker not initialized",
			Checks:  checks,
		})
		return
	}
	checks["broker"] = &CheckResult{Status: StatusUp}

	// --- Storage ---
	if s.store == nil {
		checks["storage"] = &CheckResult{Status: StatusDown, Details: "not initialized"}
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ReadyResponse{ //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
			Status:  "not_ready",
			Details: "storage not initialized",
			Checks:  checks,
		})
		return
	}
	if err := s.store.Ping(); err != nil {
		checks["storage"] = &CheckResult{Status: StatusDown, Details: err.Error()}
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(ReadyResponse{ //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
			Status:  "not_ready",
			Details: "storage unavailable",
			Checks:  checks,
		})
		return
	}
	checks["storage"] = &CheckResult{Status: StatusUp}

	// --- Cluster ---
	if s.clusterEnabled() {
		nodeID := s.cluster.NodeID()
		if nodeID == "" {
			checks["cluster"] = &CheckResult{Status: StatusDown, Details: "not initialized"}
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(ReadyResponse{ //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
				Status:  "not_ready",
				Details: "cluster not initialized",
				Checks:  checks,
			})
			return
		}

		nodes := s.cluster.Nodes()
		total, healthy := countPeers(nodeID, nodes)
		if total > 0 && healthy < total {
			mode = ModeDegraded
			checks["cluster"] = &CheckResult{
				Status:  StatusUp,
				Details: fmt.Sprintf("%d/%d peers reachable", healthy, total),
			}
		} else {
			checks["cluster"] = &CheckResult{Status: StatusUp}
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ReadyResponse{ //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
		Status: "ready",
		Mode:   mode,
		Checks: checks,
	})
}

// clusterEnabled reports whether true distributed clustering is enabled.
func (s *Server) clusterEnabled() bool {
	if s.cluster == nil {
		return false
	}
	_, isNoop := s.cluster.(*cluster.NoopCluster)
	return !isNoop
}

// countPeers returns the total number of peers (excluding self) and how many
// of those report as healthy.
func countPeers(selfID string, nodes []cluster.NodeInfo) (total, healthy int) {
	for _, n := range nodes {
		if n.ID == selfID {
			continue
		}
		total++
		if n.Healthy {
			healthy++
		}
	}
	return total, healthy
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
	if !s.clusterEnabled() {
		response.NodeID = "single-node"
		response.Sessions = int(s.broker.Stats().GetCurrentConnections())
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response) //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
		return
	}

	// Cluster mode
	response.ClusterMode = true
	response.NodeID = s.cluster.NodeID()
	response.IsLeader = s.cluster.IsLeader()
	response.Sessions = int(s.broker.Stats().GetCurrentConnections())

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response) //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
}
