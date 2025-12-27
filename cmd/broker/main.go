// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/absmach/mqtt/broker"
	"github.com/absmach/mqtt/broker/webhook"
	"github.com/absmach/mqtt/cluster"
	"github.com/absmach/mqtt/config"
	"github.com/absmach/mqtt/server/coap"
	"github.com/absmach/mqtt/server/health"
	"github.com/absmach/mqtt/server/http"
	"github.com/absmach/mqtt/server/otel"
	"github.com/absmach/mqtt/server/tcp"
	"github.com/absmach/mqtt/server/websocket"
	"github.com/absmach/mqtt/storage"
	"github.com/absmach/mqtt/storage/badger"
	"github.com/absmach/mqtt/storage/memory"
	oteltrace "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	// Parse command-line flags
	configFile := flag.String("config", "", "Path to configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configFile)
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	// Setup logging
	logLevel := slog.LevelInfo
	switch cfg.Log.Level {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	}

	var handler slog.Handler
	if cfg.Log.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)

	slog.Info("Starting MQTT broker", "version", "0.1.0")
	slog.Info("Configuration loaded",
		"tcp_addr", cfg.Server.TCPAddr,
		"http_enabled", cfg.Server.HTTPEnabled,
		"ws_enabled", cfg.Server.WSEnabled,
		"coap_enabled", cfg.Server.CoAPEnabled,
		"health_enabled", cfg.Server.HealthEnabled,
		"cluster_enabled", cfg.Cluster.Enabled,
		"log_level", cfg.Log.Level)

	// Initialize storage backend
	var store storage.Store
	switch cfg.Storage.Type {
	case "memory":
		store = memory.New()
		slog.Info("Using in-memory storage")
	case "badger":
		badgerStore, err := badger.New(badger.Config{
			Dir: cfg.Storage.BadgerDir,
		})
		if err != nil {
			slog.Error("Failed to initialize BadgerDB storage", "error", err)
			os.Exit(1)
		}
		store = badgerStore
		defer store.Close()
		slog.Info("Using BadgerDB persistent storage", "dir", cfg.Storage.BadgerDir)
	default:
		slog.Error("Unknown storage type", "type", cfg.Storage.Type)
		os.Exit(1)
	}

	// Initialize cluster coordination
	var cl cluster.Cluster
	var etcdCluster *cluster.EtcdCluster
	if cfg.Cluster.Enabled {
		// Create embedded etcd cluster
		etcdCfg := &cluster.EtcdConfig{
			NodeID:         cfg.Cluster.NodeID,
			DataDir:        cfg.Cluster.Etcd.DataDir,
			BindAddr:       cfg.Cluster.Etcd.BindAddr,
			ClientAddr:     cfg.Cluster.Etcd.ClientAddr,
			AdvertiseAddr:  cfg.Cluster.Etcd.BindAddr, // Use bind addr as advertise for now
			InitialCluster: cfg.Cluster.Etcd.InitialCluster,
			Bootstrap:      cfg.Cluster.Etcd.Bootstrap,
			TransportAddr:  cfg.Cluster.Transport.BindAddr,
			PeerTransports: cfg.Cluster.Transport.Peers,
		}

		ec, err := cluster.NewEtcdCluster(etcdCfg, logger)
		if err != nil {
			slog.Error("Failed to initialize etcd cluster", "error", err)
			os.Exit(1)
		}
		etcdCluster = ec
		cl = etcdCluster
		defer cl.Stop()

		// Start cluster (campaign for leadership)
		if err := cl.Start(); err != nil {
			slog.Error("Failed to start cluster", "error", err)
			os.Exit(1)
		}

		slog.Info("Running in cluster mode",
			"node_id", cfg.Cluster.NodeID,
			"etcd_data_dir", cfg.Cluster.Etcd.DataDir,
			"etcd_bind", cfg.Cluster.Etcd.BindAddr)
	} else {
		// Single-node mode with noop cluster
		cl = cluster.NewNoopCluster(cfg.Cluster.NodeID)
		slog.Info("Running in single-node mode", "node_id", cfg.Cluster.NodeID)
	}

	// Initialize webhooks if enabled
	var webhooks broker.Notifier
	if cfg.Webhook.Enabled {
		// Create HTTP sender
		sender := webhook.NewHTTPSender()

		// Create notifier with HTTP sender
		wh, err := webhook.NewNotifier(cfg.Webhook, cfg.Cluster.NodeID, sender, logger)
		if err != nil {
			slog.Error("Failed to initialize webhooks", "error", err)
			os.Exit(1)
		}
		webhooks = wh
		slog.Info("Webhooks enabled",
			"type", "http",
			"endpoints", len(cfg.Webhook.Endpoints),
			"workers", cfg.Webhook.Workers,
			"queue_size", cfg.Webhook.QueueSize)
	} else {
		slog.Info("Webhooks disabled")
	}

	// Initialize OpenTelemetry if metrics enabled
	var otelShutdown func(context.Context) error
	var metrics *otel.Metrics
	var tracer trace.Tracer

	if cfg.Server.MetricsEnabled {
		// Initialize OTel SDK (traces and metrics)
		shutdown, err := otel.InitProvider(cfg.Server, cfg.Cluster.NodeID)
		if err != nil {
			slog.Error("Failed to initialize OpenTelemetry", "error", err)
			os.Exit(1)
		}
		otelShutdown = shutdown
		slog.Info("OpenTelemetry initialized", "endpoint", cfg.Server.MetricsAddr)

		// Create metrics instance
		if cfg.Server.OtelMetricsEnabled {
			m, err := otel.NewMetrics()
			if err != nil {
				slog.Error("Failed to create metrics", "error", err)
				os.Exit(1)
			}
			metrics = m
			slog.Info("OTel metrics enabled")
		}

		// Get tracer if tracing is enabled
		if cfg.Server.OtelTracesEnabled {
			tracer = oteltrace.Tracer("mqtt-broker")
			slog.Info("Distributed tracing enabled", "sample_rate", cfg.Server.OtelTraceSampleRate)
		} else {
			slog.Info("Distributed tracing disabled (zero overhead)")
		}
	} else {
		slog.Info("OpenTelemetry disabled")
	}

	// Create core broker with storage, cluster, logger, metrics, and webhooks
	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, logger, stats, webhooks, metrics, tracer)
	defer b.Close()

	// Set message handler on cluster if it's an etcd cluster
	// MessageHandler interface now includes both message routing and session management
	if etcdCluster != nil {
		etcdCluster.SetMessageHandler(b)
	}

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	serverErr := make(chan error, 10)

	// Start TCP server (always enabled)
	tcpCfg := tcp.Config{
		Address:         cfg.Server.TCPAddr,
		ShutdownTimeout: cfg.Server.ShutdownTimeout,
		MaxConnections:  cfg.Server.TCPMaxConn,
		ReadTimeout:     cfg.Server.TCPReadTimeout,
		WriteTimeout:    cfg.Server.TCPWriteTimeout,
		Logger:          logger,
	}
	tcpServer := tcp.New(tcpCfg, b)

	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("Starting TCP server", "address", cfg.Server.TCPAddr)
		if err := tcpServer.Listen(ctx); err != nil {
			serverErr <- err
		}
	}()

	// Start HTTP-MQTT bridge if enabled
	if cfg.Server.HTTPEnabled {
		httpCfg := http.Config{
			Address:         cfg.Server.HTTPAddr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
		}
		httpServer := http.New(httpCfg, b, logger)

		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("Starting HTTP-MQTT bridge", "address", cfg.Server.HTTPAddr)
			if err := httpServer.Listen(ctx); err != nil {
				serverErr <- err
			}
		}()
	}

	// Start WebSocket server if enabled
	if cfg.Server.WSEnabled {
		wsCfg := websocket.Config{
			Address:         cfg.Server.WSAddr,
			Path:            cfg.Server.WSPath,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
		}
		wsServer := websocket.New(wsCfg, b, logger)

		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("Starting WebSocket server", "address", cfg.Server.WSAddr, "path", cfg.Server.WSPath)
			if err := wsServer.Listen(ctx); err != nil {
				serverErr <- err
			}
		}()
	}

	// Start CoAP server if enabled
	if cfg.Server.CoAPEnabled {
		coapCfg := coap.Config{
			Address:         cfg.Server.CoAPAddr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
		}
		coapServer := coap.New(coapCfg, b, logger)

		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("Starting CoAP server", "address", cfg.Server.CoAPAddr)
			if err := coapServer.Listen(ctx); err != nil {
				serverErr <- err
			}
		}()
	}

	// Start health check server if enabled
	if cfg.Server.HealthEnabled {
		healthCfg := health.Config{
			Address:         cfg.Server.HealthAddr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
		}
		healthServer := health.New(healthCfg, b, cl, logger)

		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("Starting health check server", "address", cfg.Server.HealthAddr)
			if err := healthServer.Listen(ctx); err != nil {
				serverErr <- err
			}
		}()
	}

	slog.Info("MQTT broker started successfully")

	// Wait for shutdown signal or server error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		slog.Info("Received shutdown signal", "signal", sig)
	case err := <-serverErr:
		slog.Error("Server error", "error", err)
	}

	// Perform shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	// Shutdown broker (drain connections, transfer sessions)
	if err := b.Shutdown(shutdownCtx, cfg.Server.ShutdownTimeout); err != nil {
		slog.Error("Error during shutdown", "error", err)
	}

	// Shutdown OpenTelemetry
	if otelShutdown != nil {
		otelShutdownCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer otelCancel()
		if err := otelShutdown(otelShutdownCtx); err != nil {
			slog.Error("Failed to shutdown OpenTelemetry", "error", err)
		} else {
			slog.Info("OpenTelemetry shutdown complete")
		}
	}

	// Cancel server contexts to stop accepting new connections
	cancel()

	// Wait for all servers to stop
	wg.Wait()
	slog.Info("MQTT broker stopped")
}
