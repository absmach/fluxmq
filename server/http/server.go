// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package http

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	corebroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/storage"
)

func buildPublishMessage(topic string, payload []byte, qos byte, retain bool, clientID, externalID, contentType string) *storage.Message {
	props := map[string]string{
		corebroker.ProtocolProperty: corebroker.ProtocolHTTP,
	}
	if externalID != "" {
		props[corebroker.ExternalIDProperty] = externalID
	}
	return &storage.Message{
		Topic:       topic,
		Payload:     payload,
		QoS:         qos,
		Retain:      retain,
		ClientID:    clientID,
		ContentType: contentType,
		Properties:  props,
	}
}

const maxBodySize = 100 << 20 // 100 MB

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
	mux.HandleFunc("/", s.handleLegacyPublish)

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

func clientIDFromRequest(r *http.Request) string {
	clientID := strings.TrimSpace(r.Header.Get("X-FluxMQ-Client-ID"))
	if clientID == "" {
		return corebroker.HTTPClientPrefix + r.RemoteAddr
	}
	return clientID
}

func authFromRequest(r *http.Request) (clientID, username, password string, ok bool) {
	clientID = clientIDFromRequest(r)

	if user, pass, basicOK := r.BasicAuth(); basicOK {
		user, pass = strings.TrimSpace(user), strings.TrimSpace(pass)
		if user != "" && pass != "" {
			return clientID, user, pass, true
		}
	}

	username = strings.TrimSpace(r.Header.Get("X-FluxMQ-Username"))
	password = parseAuthorizationToken(r.Header.Get("Authorization"))
	if username == "" || password == "" {
		return clientID, username, password, false
	}

	return clientID, username, password, true
}

func authForTopic(r *http.Request, topic string) (clientID, username, password string, ok bool) {
	clientID, username, password, ok = authFromRequest(r)
	if ok {
		return clientID, username, password, true
	}

	// authFromRequest already parsed the Authorization token into password.
	// If it failed only because username was missing, try domain ID from topic.
	if password == "" {
		return clientID, "", "", false
	}

	domainID, domainOK := extractDomainIDFromTopic(topic)
	if !domainOK {
		return clientID, "", "", false
	}

	return clientID, domainID, password, true
}

func extractDomainIDFromTopic(topic string) (string, bool) {
	topic = strings.Trim(topic, "/")
	parts := strings.Split(topic, "/")
	if len(parts) < 4 {
		return "", false
	}
	if parts[0] != "m" || parts[2] != "c" {
		return "", false
	}
	if parts[1] == "" || parts[3] == "" {
		return "", false
	}

	return parts[1], true
}

func parseAuthorizationToken(authHeader string) string {
	authHeader = strings.TrimSpace(authHeader)
	if authHeader == "" {
		return ""
	}

	parts := strings.Fields(authHeader)
	if len(parts) == 2 {
		switch strings.ToLower(parts[0]) {
		case "bearer", "client":
			return strings.TrimSpace(parts[1])
		case "basic":
			// Basic credentials should be supplied via HTTP Basic auth.
			return ""
		}
	}

	return authHeader
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Warn("http_publish_invalid_request", slog.String("error", err.Error()))
		http.Error(w, "invalid request body", http.StatusBadRequest)
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

	s.publish(w, r, req.Topic, req.Payload, req.QoS, req.Retain)
}

func (s *Server) handleLegacyPublish(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := strings.Trim(r.URL.Path, "/")
	if !strings.HasPrefix(topic, "m/") || !strings.Contains(topic, "/c/") {
		http.NotFound(w, r)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		s.logger.Warn("http_publish_invalid_request", slog.String("error", err.Error()))
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	qos, err := parseQoS(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	retain, err := parseRetain(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.publish(w, r, topic, payload, qos, retain)
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request, topic string, payload []byte, qos byte, retain bool) {
	clientID, username, password, ok := authForTopic(r, topic)
	if !ok {
		s.logger.Warn("http_publish_auth_missing",
			slog.String("client_id", clientID),
			slog.String("topic", topic))
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	authenticated, externalID, err := s.broker.Authenticate(clientID, username, password)
	if err != nil || !authenticated {
		s.logger.Warn("http_publish_auth_failed",
			slog.String("client_id", clientID),
			slog.String("topic", topic))
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !s.broker.CanPublish(clientID, topic) {
		s.logger.Warn("http_publish_forbidden",
			slog.String("client_id", clientID),
			slog.String("topic", topic))
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	msg := buildPublishMessage(topic, payload, qos, retain, clientID, externalID, r.Header.Get("Content-Type"))

	s.logger.Debug("http_publish",
		slog.String("topic", topic),
		slog.Int("qos", int(qos)),
		slog.Int("payload_size", len(payload)))

	if err := s.broker.Publish(r.Context(), msg); err != nil {
		s.logger.Error("http_publish_failed", slog.String("error", err.Error()))
		http.Error(w, "publish failed", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
}

func parseQoS(r *http.Request) (byte, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("qos"))
	if raw == "" {
		return 0, nil
	}
	q, err := strconv.ParseUint(raw, 10, 8)
	if err != nil || q > 2 {
		return 0, fmt.Errorf("qos must be 0, 1, or 2")
	}
	return byte(q), nil
}

func parseRetain(r *http.Request) (bool, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("retain"))
	if raw == "" {
		return false, nil
	}
	val, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("retain must be true or false")
	}
	return val, nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"}) //nolint:errcheck,errchkjson // HTTP response write; client disconnect is non-fatal
}
