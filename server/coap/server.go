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

	"github.com/absmach/mqtt/broker"
	"github.com/absmach/mqtt/storage"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
)

type Config struct {
	Address         string
	ShutdownTimeout time.Duration
}

type Server struct {
	config Config
	broker *broker.Broker
	logger *slog.Logger
	mux    *mux.Router
}

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

	s.mux.Handle("/mqtt/publish/*topic", mux.HandlerFunc(s.handlePublish))
	s.mux.Handle("/health", mux.HandlerFunc(s.handleHealth))

	return s
}

func (s *Server) Listen(ctx context.Context) error {
	s.logger.Info("coap_server_starting", slog.String("addr", s.config.Address))

	s.logger.Warn("coap_server_not_implemented",
		slog.String("reason", "CoAP implementation requires additional setup"))

	<-ctx.Done()
	s.logger.Info("coap_server_stopped")
	return nil
}

func (s *Server) handlePublish(w mux.ResponseWriter, r *mux.Message) {
	path, err := r.Options().Path()
	if err != nil {
		s.logger.Warn("coap_publish_path_error", slog.String("error", err.Error()))
		customResp := w.Conn().AcquireMessage(r.Context())
		defer w.Conn().ReleaseMessage(customResp)
		customResp.SetCode(codes.BadRequest)
		customResp.SetBody(bytes.NewReader([]byte("invalid path")))
		w.Conn().WriteMessage(customResp)
		return
	}

	topic := strings.TrimPrefix(path, "/mqtt/publish/")
	if topic == "" {
		s.logger.Warn("coap_publish_missing_topic")
		customResp := w.Conn().AcquireMessage(r.Context())
		defer w.Conn().ReleaseMessage(customResp)
		customResp.SetCode(codes.BadRequest)
		customResp.SetBody(bytes.NewReader([]byte("topic is required in path")))
		w.Conn().WriteMessage(customResp)
		return
	}

	payload, err := r.ReadBody()
	if err != nil {
		s.logger.Warn("coap_publish_read_body_error", slog.String("error", err.Error()))
		customResp := w.Conn().AcquireMessage(r.Context())
		defer w.Conn().ReleaseMessage(customResp)
		customResp.SetCode(codes.BadRequest)
		customResp.SetBody(bytes.NewReader([]byte(fmt.Sprintf("failed to read body: %v", err))))
		w.Conn().WriteMessage(customResp)
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
		customResp := w.Conn().AcquireMessage(r.Context())
		defer w.Conn().ReleaseMessage(customResp)
		customResp.SetCode(codes.InternalServerError)
		customResp.SetBody(bytes.NewReader([]byte(fmt.Sprintf("publish failed: %v", err))))
		w.Conn().WriteMessage(customResp)
		return
	}

	customResp := w.Conn().AcquireMessage(r.Context())
	defer w.Conn().ReleaseMessage(customResp)
	customResp.SetCode(codes.Changed)
	customResp.SetBody(bytes.NewReader([]byte("ok")))
	w.Conn().WriteMessage(customResp)
}

func (s *Server) handleHealth(w mux.ResponseWriter, r *mux.Message) {
	customResp := w.Conn().AcquireMessage(r.Context())
	defer w.Conn().ReleaseMessage(customResp)
	customResp.SetCode(codes.Content)
	customResp.SetBody(bytes.NewReader([]byte("healthy")))
	w.Conn().WriteMessage(customResp)
}
