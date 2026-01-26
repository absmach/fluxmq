// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package coap

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/storage"
	piondtls "github.com/pion/dtls/v3"
	"github.com/plgd-dev/go-coap/v3/dtls"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
	"github.com/plgd-dev/go-coap/v3/net"
	"github.com/plgd-dev/go-coap/v3/options"
	"github.com/plgd-dev/go-coap/v3/udp"
)

// Config holds the CoAP server configuration.
type Config struct {
	Address         string
	ShutdownTimeout time.Duration

	// DTLS configuration (if nil, runs plain UDP)
	TLSConfig *piondtls.Config
}

// Server is a CoAP server that bridges CoAP to MQTT.
type Server struct {
	config Config
	broker *broker.Broker
	logger *slog.Logger
	mux    *mux.Router
}

// New creates a new CoAP server.
func New(cfg Config, b *broker.Broker, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	s := &Server{
		config: cfg,
		broker: b,
		logger: logger,
		mux:    mux.NewRouter(),
	}

	s.mux.Handle("/mqtt/publish/{topic}", mux.HandlerFunc(s.handlePublish))
	s.mux.Handle("/health", mux.HandlerFunc(s.handleHealth))

	return s
}

// Listen starts the CoAP server and blocks until the context is cancelled.
func (s *Server) Listen(ctx context.Context) error {
	if s.config.TLSConfig != nil {
		return s.listenDTLS(ctx)
	}
	return s.listenUDP(ctx)
}

// listenUDP starts a plain UDP CoAP server.
func (s *Server) listenUDP(ctx context.Context) error {
	s.logger.Info("coap_udp_server_starting", slog.String("addr", s.config.Address))

	conn, err := net.NewListenUDP("udp", s.config.Address)
	if err != nil {
		return fmt.Errorf("failed to create UDP listener: %w", err)
	}

	server := udp.NewServer(options.WithMux(s.mux))

	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(conn); err != nil {
			errCh <- err
		}
	}()

	s.logger.Info("coap_udp_server_started", slog.String("addr", s.config.Address))

	select {
	case err := <-errCh:
		return fmt.Errorf("CoAP UDP server error: %w", err)
	case <-ctx.Done():
		s.logger.Info("coap_udp_server_shutdown_initiated")
		server.Stop()
		s.logger.Info("coap_udp_server_stopped")
		return nil
	}
}

// listenDTLS starts a DTLS-secured CoAP server.
func (s *Server) listenDTLS(ctx context.Context) error {
	if s.config.TLSConfig == nil {
		return fmt.Errorf("dtls config is nil")
	}
	isMTLS := s.config.TLSConfig.ClientAuth == piondtls.RequireAndVerifyClientCert
	s.logger.Info("coap_dtls_server_starting",
		slog.String("addr", s.config.Address),
		slog.Bool("mtls", isMTLS))

	listener, err := net.NewDTLSListener("udp", s.config.Address, s.config.TLSConfig)
	if err != nil {
		return fmt.Errorf("failed to create DTLS listener: %w", err)
	}

	server := dtls.NewServer(options.WithMux(s.mux))

	errCh := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil {
			errCh <- err
		}
	}()

	s.logger.Info("coap_dtls_server_started",
		slog.String("addr", s.config.Address),
		slog.Bool("mtls", isMTLS))

	select {
	case err := <-errCh:
		return fmt.Errorf("CoAP DTLS server error: %w", err)
	case <-ctx.Done():
		s.logger.Info("coap_dtls_server_shutdown_initiated")
		server.Stop()
		listener.Close()
		s.logger.Info("coap_dtls_server_stopped")
		return nil
	}
}

func (s *Server) handlePublish(w mux.ResponseWriter, r *mux.Message) {
	path, err := r.Options().Path()
	if err != nil {
		s.logger.Warn("coap_publish_path_error", slog.String("error", err.Error()))
		s.sendResponse(w, r, codes.BadRequest, "invalid path")
		return
	}

	topic := strings.TrimPrefix(path, "/mqtt/publish/")
	if topic == "" {
		s.logger.Warn("coap_publish_missing_topic")
		s.sendResponse(w, r, codes.BadRequest, "topic is required in path")
		return
	}

	payload, err := r.ReadBody()
	if err != nil {
		s.logger.Warn("coap_publish_read_body_error", slog.String("error", err.Error()))
		s.sendResponse(w, r, codes.BadRequest, fmt.Sprintf("failed to read body: %v", err))
		return
	}

	msg := &storage.Message{
		Topic:   topic,
		Payload: payload,
		QoS:     0,
		Retain:  false,
	}

	s.logger.Debug("coap_publish",
		slog.String("topic", topic),
		slog.Int("payload_size", len(payload)))

	if err := s.broker.Publish(msg); err != nil {
		s.logger.Error("coap_publish_failed", slog.String("error", err.Error()))
		s.sendResponse(w, r, codes.InternalServerError, fmt.Sprintf("publish failed: %v", err))
		return
	}

	s.sendResponse(w, r, codes.Changed, "ok")
}

func (s *Server) handleHealth(w mux.ResponseWriter, r *mux.Message) {
	s.sendResponse(w, r, codes.Content, "healthy")
}

func (s *Server) sendResponse(w mux.ResponseWriter, r *mux.Message, code codes.Code, body string) {
	resp := w.Conn().AcquireMessage(r.Context())
	defer w.Conn().ReleaseMessage(resp)
	resp.SetCode(code)
	resp.SetBody(bytes.NewReader([]byte(body)))
	if err := w.Conn().WriteMessage(resp); err != nil {
		s.logger.Error("coap_send_response_error", slog.String("error", err.Error()))
	}
}
