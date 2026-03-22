// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/storage"
)

type Config struct {
	Address         string
	ShutdownTimeout time.Duration
	TLSConfig       *tls.Config
}

type Server struct {
	config Config
	broker *broker.Broker
	logger *slog.Logger
	server *http.Server
}

func New(cfg Config, b *broker.Broker, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		config: cfg,
		broker: b,
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/publish", s.handlePublish)
	mux.HandleFunc("/health", s.handleHealth)

	s.server = &http.Server{
		Addr:      cfg.Address,
		Handler:   mux,
		TLSConfig: cfg.TLSConfig,
	}

	return s
}

func (s *Server) Listen(ctx context.Context) error {
	s.logger.Info("http_bridge_starting", slog.String("addr", s.config.Address))

	errCh := make(chan error, 1)
	go func() {
		if s.config.TLSConfig != nil {
			if err := s.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
				errCh <- err
			}
			return
		}
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.logger.Info("http_bridge_shutdown_initiated")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout) //nolint:contextcheck // intentionally creates new context for graceful shutdown
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil { //nolint:contextcheck // intentionally creates new context for graceful shutdown
			s.logger.Error("http_bridge_shutdown_error", slog.String("error", err.Error()))
			return err
		}

		s.logger.Info("http_bridge_stopped")
		return nil
	}
}

type publishRequest struct {
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	QoS     byte   `json:"qos"`
	Retain  bool   `json:"retain"`
}

func authFromRequest(r *http.Request) (clientID, username, password string) {
	clientID = strings.TrimSpace(r.Header.Get("X-FluxMQ-Client-ID"))
	if clientID == "" {
		clientID = corebroker.HTTPClientPrefix + r.RemoteAddr
	}

	if user, pass, ok := r.BasicAuth(); ok {
		username = user
		password = pass
	}
	if username == "" {
		username = strings.TrimSpace(r.Header.Get("X-FluxMQ-Username"))
	}
	if password == "" {
		password = strings.TrimSpace(r.Header.Get("X-FluxMQ-Password"))
	}
	if password == "" {
		authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
		lower := strings.ToLower(authHeader)
		if strings.HasPrefix(lower, "bearer ") {
			password = strings.TrimSpace(authHeader[len("Bearer "):])
		} else {
			password = authHeader
		}
	}

	return clientID, username, password
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Warn("http_publish_invalid_request", slog.String("error", err.Error()))
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	if req.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}

	if req.QoS > 2 {
		http.Error(w, "qos must be 0, 1, or 2", http.StatusBadRequest)
		return
	}

	// Placeholder bridge-level auth hook. A concrete SuperMQ-backed auth
	// implementation will be wired in a follow-up step.
	clientID, username, password := authFromRequest(r)
	authenticated, err := s.broker.Authenticate(clientID, username, password)
	if err != nil || !authenticated {
		s.logger.Warn("http_publish_auth_failed",
			slog.String("client_id", clientID),
			slog.String("topic", req.Topic))
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.broker.CanPublish(clientID, req.Topic) {
		s.logger.Warn("http_publish_forbidden",
			slog.String("client_id", clientID),
			slog.String("topic", req.Topic))
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	msg := &storage.Message{
		Topic:   req.Topic,
		Payload: req.Payload,
		QoS:     req.QoS,
		Retain:  req.Retain,
	}

	s.logger.Debug("http_publish",
		slog.String("topic", req.Topic),
		slog.Int("qos", int(req.QoS)),
		slog.Int("payload_size", len(req.Payload)))

	if err := s.broker.Publish(msg); err != nil { //nolint:contextcheck // context propagation would require API changes across the call chain
		s.logger.Error("http_publish_failed", slog.String("error", err.Error()))
		http.Error(w, fmt.Sprintf("publish failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}) //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
}
