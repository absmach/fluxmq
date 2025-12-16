package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/absmach/mqtt/broker"
	"github.com/absmach/mqtt/config"
	"github.com/absmach/mqtt/server/tcp"
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
		"max_connections", cfg.Server.TCPMaxConn,
		"log_level", cfg.Log.Level)

	// Create core broker with logger and metrics
	stats := broker.NewStats()
	b := broker.NewBroker(logger, stats)
	defer b.Close()

	// Create TCP server with config
	serverCfg := tcp.Config{
		Address:         cfg.Server.TCPAddr,
		ShutdownTimeout: cfg.Server.ShutdownTimeout,
		MaxConnections:  cfg.Server.TCPMaxConn,
		ReadTimeout:     cfg.Server.TCPReadTimeout,
		WriteTimeout:    cfg.Server.TCPWriteTimeout,
		Logger:          logger,
	}
	server := tcp.New(serverCfg, b)

	// Context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start server in background
	serverErr := make(chan error, 1)
	go func() {
		if err := server.Listen(ctx); err != nil {
			serverErr <- err
		}
	}()

	slog.Info("MQTT broker started successfully", "address", server.Addr())

	// Wait for shutdown signal
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

	slog.Info("MQTT broker stopped")
}
