// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/absmach/fluxmq/amqpbroker"
	"github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/broker/webhook"
	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	logStorage "github.com/absmach/fluxmq/logstorage"
	mqtttls "github.com/absmach/fluxmq/pkg/tls"
	"github.com/absmach/fluxmq/queue"
	"github.com/absmach/fluxmq/queue/raft"
	queueTypes "github.com/absmach/fluxmq/queue/types"
	"github.com/absmach/fluxmq/ratelimit"
	amqpserver "github.com/absmach/fluxmq/server/amqp"
	"github.com/absmach/fluxmq/server/api"
	"github.com/absmach/fluxmq/server/coap"
	"github.com/absmach/fluxmq/server/health"
	"github.com/absmach/fluxmq/server/http"
	"github.com/absmach/fluxmq/server/otel"
	"github.com/absmach/fluxmq/server/tcp"
	"github.com/absmach/fluxmq/server/websocket"
	"github.com/absmach/fluxmq/storage"
	"github.com/absmach/fluxmq/storage/badger"
	"github.com/absmach/fluxmq/storage/memory"
	piondtls "github.com/pion/dtls/v3"
	oteltrace "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	configFile := flag.String("config", "", "Path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*configFile)
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

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
		"tcp_plain_listener", cfg.Server.TCP.Plain.Addr,
		"tcp_tls_listener", cfg.Server.TCP.TLS.Addr,
		"tcp_mtls_listener", cfg.Server.TCP.MTLS.Addr,
		"ws_plain_listener", cfg.Server.WebSocket.Plain.Addr,
		"ws_tls_listener", cfg.Server.WebSocket.TLS.Addr,
		"ws_mtls_listener", cfg.Server.WebSocket.MTLS.Addr,
		"http_plain_listener", cfg.Server.HTTP.Plain.Addr,
		"http_tls_listener", cfg.Server.HTTP.TLS.Addr,
		"http_mtls_listener", cfg.Server.HTTP.MTLS.Addr,
		"coap_plain_listener", cfg.Server.CoAP.Plain.Addr,
		"coap_dtls_listener", cfg.Server.CoAP.DTLS.Addr,
		"coap_mdtls_listener", cfg.Server.CoAP.MDTLS.Addr,
		"amqp_plain_listener", cfg.Server.AMQP.Plain.Addr,
		"amqp_tls_listener", cfg.Server.AMQP.TLS.Addr,
		"amqp_mtls_listener", cfg.Server.AMQP.MTLS.Addr,
		"health_enabled", cfg.Server.HealthEnabled,
		"cluster_enabled", cfg.Cluster.Enabled,
		"log_level", cfg.Log.Level)

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

	var cl cluster.Cluster
	var etcdCluster *cluster.EtcdCluster
	if cfg.Cluster.Enabled {
		// Build transport TLS config if enabled
		var transportTLS *cluster.TransportTLSConfig
		if cfg.Cluster.Transport.TLSEnabled {
			transportTLS = &cluster.TransportTLSConfig{
				CertFile: cfg.Cluster.Transport.TLSCertFile,
				KeyFile:  cfg.Cluster.Transport.TLSKeyFile,
				CAFile:   cfg.Cluster.Transport.TLSCAFile,
			}
		}

		etcdCfg := &cluster.EtcdConfig{
			NodeID:                      cfg.Cluster.NodeID,
			DataDir:                     cfg.Cluster.Etcd.DataDir,
			BindAddr:                    cfg.Cluster.Etcd.BindAddr,
			ClientAddr:                  cfg.Cluster.Etcd.ClientAddr,
			AdvertiseAddr:               cfg.Cluster.Etcd.BindAddr, // Use bind addr as advertise for now
			InitialCluster:              cfg.Cluster.Etcd.InitialCluster,
			Bootstrap:                   cfg.Cluster.Etcd.Bootstrap,
			TransportAddr:               cfg.Cluster.Transport.BindAddr,
			PeerTransports:              cfg.Cluster.Transport.Peers,
			HybridRetainedSizeThreshold: cfg.Cluster.Etcd.HybridRetainedSizeThreshold,
			TransportTLS:                transportTLS,
		}

		ec, err := cluster.NewEtcdCluster(etcdCfg, store, logger)
		if err != nil {
			slog.Error("Failed to initialize etcd cluster", "error", err)
			os.Exit(1)
		}
		etcdCluster = ec
		cl = etcdCluster
		defer cl.Stop()

		if err := cl.Start(); err != nil {
			slog.Error("Failed to start cluster", "error", err)
			os.Exit(1)
		}

		slog.Info("Running in cluster mode",
			"node_id", cfg.Cluster.NodeID,
			"etcd_data_dir", cfg.Cluster.Etcd.DataDir,
			"etcd_bind", cfg.Cluster.Etcd.BindAddr)
	} else {
		cl = cluster.NewNoopCluster(cfg.Cluster.NodeID)
		slog.Info("Running in single-node mode", "node_id", cfg.Cluster.NodeID)
	}

	var webhooks broker.Notifier
	if cfg.Webhook.Enabled {
		sender := webhook.NewHTTPSender()

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

	var otelShutdown func(context.Context) error
	var metrics *otel.Metrics
	var tracer trace.Tracer

	if cfg.Server.MetricsEnabled {
		shutdown, err := otel.InitProvider(cfg.Server, cfg.Cluster.NodeID)
		if err != nil {
			slog.Error("Failed to initialize OpenTelemetry", "error", err)
			os.Exit(1)
		}
		otelShutdown = shutdown
		slog.Info("OpenTelemetry initialized", "endpoint", cfg.Server.MetricsAddr)

		if cfg.Server.OtelMetricsEnabled {
			m, err := otel.NewMetrics()
			if err != nil {
				slog.Error("Failed to create metrics", "error", err)
				os.Exit(1)
			}
			metrics = m
			slog.Info("OTel metrics enabled")
		}

		if cfg.Server.OtelTracesEnabled {
			tracer = oteltrace.Tracer("mqtt-broker")
			slog.Info("Distributed tracing enabled", "sample_rate", cfg.Server.OtelTraceSampleRate)
		} else {
			slog.Info("Distributed tracing disabled (zero overhead)")
		}
	} else {
		slog.Info("OpenTelemetry disabled")
	}

	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, logger, stats, webhooks, metrics, tracer, cfg.Session)
	defer b.Close()

	// Configure maximum QoS level
	if cfg.Broker.MaxQoS >= 0 && cfg.Broker.MaxQoS <= 2 {
		b.SetMaxQoS(byte(cfg.Broker.MaxQoS))
	}

	// Initialize rate limiting
	var rateLimitManager *ratelimit.Manager
	if cfg.RateLimit.Enabled {
		rlConfig := ratelimit.Config{
			Enabled: true,
			Connection: ratelimit.ConnectionConfig{
				Enabled:         cfg.RateLimit.Connection.Enabled,
				Rate:            cfg.RateLimit.Connection.Rate,
				Burst:           cfg.RateLimit.Connection.Burst,
				CleanupInterval: cfg.RateLimit.Connection.CleanupInterval,
			},
			Message: ratelimit.MessageConfig{
				Enabled: cfg.RateLimit.Message.Enabled,
				Rate:    cfg.RateLimit.Message.Rate,
				Burst:   cfg.RateLimit.Message.Burst,
			},
			Subscribe: ratelimit.SubscribeConfig{
				Enabled: cfg.RateLimit.Subscribe.Enabled,
				Rate:    cfg.RateLimit.Subscribe.Rate,
				Burst:   cfg.RateLimit.Subscribe.Burst,
			},
		}
		rateLimitManager = ratelimit.NewManager(rlConfig)
		defer rateLimitManager.Stop()

		// Set client rate limiter on broker
		b.SetClientRateLimiter(rateLimitManager)

		slog.Info("Rate limiting enabled",
			slog.Bool("connection", cfg.RateLimit.Connection.Enabled),
			slog.Bool("message", cfg.RateLimit.Message.Enabled),
			slog.Bool("subscribe", cfg.RateLimit.Subscribe.Enabled))
	} else {
		slog.Info("Rate limiting disabled")
	}

	// Create AMQP broker (needs queue manager set later)
	amqpBroker := amqpbroker.New(nil, logger)
	defer amqpBroker.Close()

	// Initialize file-based log storage for queues
	{
		queueDir := cfg.Storage.BadgerDir
		if !strings.HasSuffix(queueDir, "/") {
			queueDir += "/"
		}
		queueDir += "queue"

		// Use file-based AOL storage (implements both LogStore and ConsumerGroupStore)
		logStore, err := logStorage.NewAdapter(queueDir, logStorage.DefaultAdapterConfig())
		if err != nil {
			slog.Error("Failed to initialize queue log storage", "error", err)
			os.Exit(1)
		}
		defer logStore.Close()

		// Convert queue configs from main config to queue types
		queueCfg := queue.DefaultConfig()
		for _, qc := range cfg.Queues {
			queueCfg.QueueConfigs = append(queueCfg.QueueConfigs, queueTypes.FromInput(queueTypes.QueueConfigInput{
				Name:           qc.Name,
				Topics:         qc.Topics,
				Reserved:       qc.Reserved,
				MaxMessageSize: qc.Limits.MaxMessageSize,
				MaxDepth:       qc.Limits.MaxDepth,
				MessageTTL:     qc.Limits.MessageTTL,
				MaxRetries:     qc.Retry.MaxRetries,
				InitialBackoff: qc.Retry.InitialBackoff,
				MaxBackoff:     qc.Retry.MaxBackoff,
				Multiplier:     qc.Retry.Multiplier,
				DLQEnabled:     qc.DLQ.Enabled,
				DLQTopic:       qc.DLQ.Topic,
			}))
		}

		// Delivery dispatcher: routes to AMQP or MQTT broker based on client ID prefix
		deliverFn := func(ctx context.Context, clientID string, msg any) error {
			if amqpbroker.IsAMQPClient(clientID) {
				return amqpBroker.DeliverToClient(ctx, clientID, msg)
			}
			return b.DeliverToSessionByID(ctx, clientID, msg)
		}

		// Create log-based queue manager with wildcard support
		qm := queue.NewManager(
			logStore,
			logStore,
			deliverFn,
			queueCfg,
			logger,
			cl,
		)

		// Initialize Raft replication if enabled
		if cfg.Cluster.Enabled && cfg.Cluster.Raft.Enabled {
			raftCfg := raft.ManagerConfig{
				Enabled:           true,
				ReplicationFactor: cfg.Cluster.Raft.ReplicationFactor,
				SyncMode:          cfg.Cluster.Raft.SyncMode,
				MinInSyncReplicas: cfg.Cluster.Raft.MinInSyncReplicas,
				AckTimeout:        cfg.Cluster.Raft.AckTimeout,
				HeartbeatTimeout:  cfg.Cluster.Raft.HeartbeatTimeout,
				ElectionTimeout:   cfg.Cluster.Raft.ElectionTimeout,
				SnapshotInterval:  cfg.Cluster.Raft.SnapshotInterval,
				SnapshotThreshold: cfg.Cluster.Raft.SnapshotThreshold,
			}

			raftManager := raft.NewManager(
				cfg.Cluster.NodeID,
				cfg.Cluster.Raft.BindAddr,
				cfg.Cluster.Raft.DataDir,
				logStore,
				logStore,
				cfg.Cluster.Raft.Peers,
				raftCfg,
				logger,
			)

			if err := raftManager.Start(context.Background()); err != nil {
				slog.Error("Failed to start Raft manager", "error", err)
				os.Exit(1)
			}

			qm.SetRaftManager(raftManager)

			slog.Info("Raft replication enabled",
				slog.String("node_id", cfg.Cluster.NodeID),
				slog.String("bind_addr", cfg.Cluster.Raft.BindAddr),
				slog.Int("replication_factor", cfg.Cluster.Raft.ReplicationFactor),
				slog.Bool("sync_mode", cfg.Cluster.Raft.SyncMode))
		}

		if err := b.SetQueueManager(qm); err != nil {
			slog.Error("Failed to set queue manager", "error", err)
			os.Exit(1)
		}

		// Set queue manager on AMQP broker
		amqpBroker.SetQueueManager(qm)

		// Set queue handler on cluster for cross-node message routing
		if etcdCluster != nil {
			etcdCluster.SetQueueHandler(qm)
		}

		slog.Info("Log-based queue initialized", "storage", "file", "dir", queueDir)
	}

	// Set message handler on cluster if it's an etcd cluster
	// MessageHandler interface now includes both message routing and session management
	if etcdCluster != nil {
		etcdCluster.SetMessageHandler(b)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	serverErr := make(chan error, 10)

	tcpSlots := []struct {
		name string
		cfg  config.TCPListenerConfig
	}{
		{name: "plain", cfg: cfg.Server.TCP.Plain},
		{name: "tls", cfg: cfg.Server.TCP.TLS},
		{name: "mtls", cfg: cfg.Server.TCP.MTLS},
	}

	for _, slot := range tcpSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		tlsCfg, err := mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
		if err != nil {
			slog.Error("Failed to build TCP TLS configuration", "listener", slot.name, "error", err)
			os.Exit(1)
		}

		tcpCfg := tcp.Config{
			Address:         slot.cfg.Addr,
			TLSConfig:       tlsCfg,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
			MaxConnections:  slot.cfg.MaxConnections,
			ReadTimeout:     slot.cfg.ReadTimeout,
			WriteTimeout:    slot.cfg.WriteTimeout,
			Logger:          logger,
		}
		if rateLimitManager != nil {
			tcpCfg.IPRateLimiter = rateLimitManager
		}
		tcpServer := tcp.New(tcpCfg, b)

		wg.Add(1)
		go func(name, addr string, server *tcp.Server) {
			defer wg.Done()
			slog.Info("Starting TCP server", "mode", name, "address", addr)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, tcpServer)
	}

	wsSlots := []struct {
		name string
		cfg  config.WSListenerConfig
	}{
		{name: "plain", cfg: cfg.Server.WebSocket.Plain},
		{name: "tls", cfg: cfg.Server.WebSocket.TLS},
		{name: "mtls", cfg: cfg.Server.WebSocket.MTLS},
	}

	for _, slot := range wsSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		tlsCfg, err := mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
		if err != nil {
			slog.Error("Failed to build WebSocket TLS configuration", "listener", slot.name, "error", err)
			os.Exit(1)
		}

		wsCfg := websocket.Config{
			Address:         slot.cfg.Addr,
			Path:            slot.cfg.Path,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
			TLSConfig:       tlsCfg,
			AllowedOrigins:  slot.cfg.AllowedOrigins,
		}
		if rateLimitManager != nil {
			wsCfg.IPRateLimiter = rateLimitManager
		}

		wsServer := websocket.New(wsCfg, b, logger)

		wg.Add(1)
		go func(name, addr, path string, server *websocket.Server) {
			defer wg.Done()
			slog.Info("Starting WebSocket server", "mode", name, "address", addr, "path", path)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, slot.cfg.Path, wsServer)
	}

	httpSlots := []struct {
		name string
		cfg  config.HTTPListenerConfig
	}{
		{name: "plain", cfg: cfg.Server.HTTP.Plain},
		{name: "tls", cfg: cfg.Server.HTTP.TLS},
		{name: "mtls", cfg: cfg.Server.HTTP.MTLS},
	}

	for _, slot := range httpSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		var tlsCfg *tls.Config
		if slot.name != "plain" {
			var err error
			tlsCfg, err = mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
			if err != nil {
				slog.Error("Failed to build HTTP TLS configuration", "listener", slot.name, "error", err)
				os.Exit(1)
			}
		}

		httpCfg := http.Config{
			Address:         slot.cfg.Addr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
			TLSConfig:       tlsCfg,
		}
		httpServer := http.New(httpCfg, b, logger)

		wg.Add(1)
		go func(name, addr string, server *http.Server) {
			defer wg.Done()
			slog.Info("Starting HTTP-MQTT bridge", "mode", name, "address", addr)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, httpServer)
	}

	coapSlots := []struct {
		name string
		cfg  config.CoAPListenerConfig
	}{
		{name: "plain", cfg: cfg.Server.CoAP.Plain},
		{name: "dtls", cfg: cfg.Server.CoAP.DTLS},
		{name: "mdtls", cfg: cfg.Server.CoAP.MDTLS},
	}

	for _, slot := range coapSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		var dtlsCfg *piondtls.Config
		if slot.name != "plain" {
			var err error
			dtlsCfg, err = mqtttls.LoadTLSConfig[*piondtls.Config](&slot.cfg.TLS)
			if err != nil {
				slog.Error("Failed to build CoAP DTLS configuration", "listener", slot.name, "error", err)
				os.Exit(1)
			}
		}

		coapCfg := coap.Config{
			Address:         slot.cfg.Addr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
			TLSConfig:       dtlsCfg,
		}
		coapServer := coap.New(coapCfg, b, logger)

		wg.Add(1)
		go func(name, addr string, server *coap.Server) {
			defer wg.Done()
			slog.Info("Starting CoAP server", "mode", name, "address", addr)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, coapServer)
	}

	// AMQP 1.0 servers
	amqpSlots := []struct {
		name string
		cfg  config.AMQPListenerConfig
	}{
		{name: "plain", cfg: cfg.Server.AMQP.Plain},
		{name: "tls", cfg: cfg.Server.AMQP.TLS},
		{name: "mtls", cfg: cfg.Server.AMQP.MTLS},
	}

	for _, slot := range amqpSlots {
		if strings.TrimSpace(slot.cfg.Addr) == "" {
			continue
		}

		tlsCfg, err := mqtttls.LoadTLSConfig[*tls.Config](&slot.cfg.TLS)
		if err != nil {
			slog.Error("Failed to build AMQP TLS configuration", "listener", slot.name, "error", err)
			os.Exit(1)
		}

		amqpCfg := amqpserver.Config{
			Address:         slot.cfg.Addr,
			TLSConfig:       tlsCfg,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
			MaxConnections:  slot.cfg.MaxConnections,
			Logger:          logger,
		}
		amqpSrv := amqpserver.New(amqpCfg, amqpBroker)

		wg.Add(1)
		go func(name, addr string, server *amqpserver.Server) {
			defer wg.Done()
			slog.Info("Starting AMQP server", "mode", name, "address", addr)
			if err := server.Listen(ctx); err != nil {
				serverErr <- err
			}
		}(slot.name, slot.cfg.Addr, amqpSrv)
	}

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

	// Start Queue API server (gRPC/HTTP via Connect protocol)
	if cfg.Server.APIEnabled {
		apiCfg := api.Config{
			Address:         cfg.Server.APIAddr,
			ShutdownTimeout: cfg.Server.ShutdownTimeout,
		}

		// Get the queue manager from the broker and log store
		queueManager := b.GetQueueManager()
		if qm, ok := queueManager.(*queue.Manager); ok {
			queueDir := cfg.Storage.BadgerDir
			if !strings.HasSuffix(queueDir, "/") {
				queueDir += "/"
			}
			queueDir += "queue"
			logStore, err := logStorage.NewAdapter(queueDir, logStorage.DefaultAdapterConfig())
			if err == nil {
				defer logStore.Close()
				apiServer := api.New(apiCfg, qm, logStore, logStore, logger)

				wg.Add(1)
				go func() {
					defer wg.Done()
					slog.Info("Starting Queue API server", "address", cfg.Server.APIAddr)
					if err := apiServer.Listen(ctx); err != nil {
						serverErr <- err
					}
				}()
			} else {
				slog.Warn("Failed to create API server log store", "error", err)
			}
		} else {
			slog.Warn("Queue manager not available or wrong type, API server disabled")
		}
	}

	slog.Info("MQTT broker started successfully")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		slog.Info("Received shutdown signal", "signal", sig)
	case err := <-serverErr:
		slog.Error("Server error", "error", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer shutdownCancel()

	if err := b.Shutdown(shutdownCtx, cfg.Server.ShutdownTimeout); err != nil {
		slog.Error("Error during shutdown", "error", err)
	}

	if otelShutdown != nil {
		otelShutdownCtx, otelCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer otelCancel()
		if err := otelShutdown(otelShutdownCtx); err != nil {
			slog.Error("Failed to shutdown OpenTelemetry", "error", err)
		} else {
			slog.Info("OpenTelemetry shutdown complete")
		}
	}

	cancel()

	wg.Wait()
	slog.Info("MQTT broker stopped")
}
