package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/absmach/mqtt/broker"
	"github.com/absmach/mqtt/config"
	"github.com/absmach/mqtt/server/coap"
	"github.com/absmach/mqtt/server/http"
	"github.com/absmach/mqtt/server/tcp"
	"github.com/absmach/mqtt/server/websocket"
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
		"log_level", cfg.Log.Level)

	// Create core broker with logger and metrics
	stats := broker.NewStats()
	b := broker.NewBroker(logger, stats)
	defer b.Close()

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

	slog.Info("MQTT broker started successfully")

	// Wait for shutdown signal or server error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		slog.Info("Received shutdown signal", "signal", sig)
		cancel()
	case err := <-serverErr:
		slog.Error("Server error", "error", err)
		cancel()
	}

	// Wait for all servers to stop
	wg.Wait()
	slog.Info("MQTT broker stopped")
}
