// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/absmach/fluxmq/pkg/proto/queue/v1/queuev1connect"
	"github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/storage"
	serverqueue "github.com/absmach/fluxmq/server/queue"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// Config holds configuration for the API server.
type Config struct {
	Address         string
	ShutdownTimeout time.Duration
	TLSCertFile     string
	TLSKeyFile      string
}

// Server provides the HTTP/gRPC API server using Connect protocol.
type Server struct {
	config     Config
	httpServer *http.Server
	logger     *slog.Logger
}

// New creates a new API server.
func New(config Config, manager *queue.Manager, streamStore storage.QueueStore, groupStore storage.ConsumerGroupStore, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	mux := http.NewServeMux()

	queueHandler := serverqueue.NewHandler(manager, streamStore, groupStore, logger)
	path, handler := queuev1connect.NewQueueServiceHandler(queueHandler)
	mux.Handle(path, handler)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	})

	h2s := &http2.Server{}
	httpServer := &http.Server{
		Addr:         config.Address,
		Handler:      h2c.NewHandler(mux, h2s),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	return &Server{
		config:     config,
		httpServer: httpServer,
		logger:     logger,
	}
}

// Listen starts the API server.
func (s *Server) Listen(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		var err error
		if s.config.TLSCertFile != "" && s.config.TLSKeyFile != "" {
			s.logger.Info("Starting API server with TLS",
				slog.String("address", s.config.Address))
			err = s.httpServer.ListenAndServeTLS(s.config.TLSCertFile, s.config.TLSKeyFile)
		} else {
			s.logger.Info("Starting API server (h2c)",
				slog.String("address", s.config.Address))
			err = s.httpServer.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		s.logger.Info("Shutting down API server")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	case err := <-errCh:
		return fmt.Errorf("API server error: %w", err)
	}
}
