package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/dborovcanin/mqtt/broker"
	"github.com/dborovcanin/mqtt/config"
	"github.com/dborovcanin/mqtt/packets"
	"github.com/dborovcanin/mqtt/transport"
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

	// Create broker server
	server := broker.NewServer()

	// Create TCP frontend
	tcp, err := transport.NewTCPFrontend(cfg.Server.TCPAddr)
	if err != nil {
		slog.Error("failed to create TCP frontend", "error", err)
		os.Exit(1)
	}

	// Wrap with logging if debug level
	var frontend broker.Frontend = tcp
	if level == slog.LevelDebug {
		frontend = &loggingFrontend{Frontend: tcp, logger: logger}
	}

	// Add frontend to server
	if err := server.AddFrontend(frontend); err != nil {
		slog.Error("failed to add frontend", "error", err)
		os.Exit(1)
	}

	slog.Info("MQTT broker started", "addr", tcp.Addr().String())

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh

	slog.Info("shutting down", "signal", sig.String())

	if err := server.Close(); err != nil {
		slog.Error("error during shutdown", "error", err)
	}

	slog.Info("broker stopped")
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

// loggingFrontend wraps a Frontend to add packet logging.
type loggingFrontend struct {
	broker.Frontend
	logger *slog.Logger
}

func (f *loggingFrontend) Serve(handler broker.ConnectionHandler) error {
	return f.Frontend.Serve(&loggingHandler{
		handler: handler,
		logger:  f.logger,
	})
}

// loggingHandler wraps ConnectionHandler to log connections.
type loggingHandler struct {
	handler broker.ConnectionHandler
	logger  *slog.Logger
}

func (h *loggingHandler) HandleConnection(conn broker.Connection) {
	h.logger.Debug("new connection", "remote", conn.RemoteAddr().String())

	// Wrap connection for packet logging
	logged := &loggingConnection{
		Connection: conn,
		logger:     h.logger,
	}

	h.handler.HandleConnection(logged)
}

// loggingConnection wraps Connection to log packets.
type loggingConnection struct {
	broker.Connection
	logger *slog.Logger
}

func (c *loggingConnection) ReadPacket() (packets.ControlPacket, error) {
	pkt, err := c.Connection.ReadPacket()
	if err != nil {
		if err.Error() != "EOF" {
			c.logger.Debug("read error", "remote", c.RemoteAddr().String(), "error", err)
		}
		return nil, err
	}

	c.logger.Debug("packet received",
		"remote", c.RemoteAddr().String(),
		"type", packetTypeName(pkt.Type()),
		"packet", pkt.String(),
	)
	return pkt, nil
}

func (c *loggingConnection) WritePacket(pkt packets.ControlPacket) error {
	c.logger.Debug("packet sent",
		"remote", c.RemoteAddr().String(),
		"type", packetTypeName(pkt.Type()),
		"packet", pkt.String(),
	)
	return c.Connection.WritePacket(pkt)
}

func (c *loggingConnection) Close() error {
	c.logger.Debug("connection closed", "remote", c.RemoteAddr().String())
	return c.Connection.Close()
}

// packetTypeName returns a human-readable packet type name.
func packetTypeName(t byte) string {
	names := []string{
		"RESERVED", "CONNECT", "CONNACK", "PUBLISH", "PUBACK",
		"PUBREC", "PUBREL", "PUBCOMP", "SUBSCRIBE", "SUBACK",
		"UNSUBSCRIBE", "UNSUBACK", "PINGREQ", "PINGRESP", "DISCONNECT", "AUTH",
	}
	if int(t) < len(names) {
		return names[t]
	}
	return "UNKNOWN"
}
