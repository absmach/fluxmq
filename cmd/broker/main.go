package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/config"
	"github.com/dborovcanin/mqtt/server/tcp"
)

func main() {
	// Load configuration
	cfg := config.Load()

	// Setup structured logging
	level := parseLogLevel(cfg.Log.Level)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)

	slog.Info("starting MQTT broker", "addr", cfg.Server.TCPAddr)

	// Create broker
	mqttBroker := broker.NewBroker()

	// Create TCP server configuration
	serverCfg := tcp.Config{
		Address:         cfg.Server.TCPAddr,
		ShutdownTimeout: 30 * time.Second,
		MaxConnections:  0, // unlimited for now
		ReadTimeout:     60 * time.Second,
		WriteTimeout:    60 * time.Second,
		TCPKeepAlive:    15 * time.Second,
		Logger:          logger,
	}

	// Create TCP server with broker as handler
	server := tcp.New(serverCfg, mqttBroker)

	// Setup graceful shutdown with context
	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.Info("MQTT broker started", "addr", cfg.Server.TCPAddr)

	// Start server (blocks until shutdown)
	if err := server.Listen(ctx); err != nil {
		if err == tcp.ErrShutdownTimeout {
			slog.Warn("shutdown timeout exceeded, some connections may have been forced closed")
		} else {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}

	// Shutdown broker
	if err := mqttBroker.Close(); err != nil {
		slog.Error("error during broker shutdown", "error", err)
	}

	slog.Info("broker stopped gracefully")
}

func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
