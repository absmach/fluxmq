// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/absmach/mqtt/broker"
	"github.com/absmach/mqtt/core"
	"github.com/absmach/mqtt/core/packets"
	v3 "github.com/absmach/mqtt/core/packets/v3"
	v5 "github.com/absmach/mqtt/core/packets/v5"
	"github.com/gorilla/websocket"
)

type Config struct {
	Address         string
	Path            string
	ShutdownTimeout time.Duration
}

type Server struct {
	config   Config
	broker   *broker.Broker
	logger   *slog.Logger
	server   *http.Server
	upgrader websocket.Upgrader
}

func New(cfg Config, b *broker.Broker, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}

	if cfg.Path == "" {
		cfg.Path = "/mqtt"
	}

	s := &Server{
		config: cfg,
		broker: b,
		logger: logger,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc(cfg.Path, s.handleWebSocket)

	s.server = &http.Server{
		Addr:    cfg.Address,
		Handler: mux,
	}

	return s
}

func (s *Server) Listen(ctx context.Context) error {
	s.logger.Info("websocket_server_starting",
		slog.String("addr", s.config.Address),
		slog.String("path", s.config.Path))

	errCh := make(chan error, 1)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		s.logger.Info("websocket_server_shutdown_initiated")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("websocket_server_shutdown_error", slog.String("error", err.Error()))
			return err
		}

		s.logger.Info("websocket_server_stopped")
		return nil
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Warn("websocket_upgrade_failed", slog.String("error", err.Error()))
		return
	}

	s.logger.Debug("websocket_connection_accepted", slog.String("remote_addr", r.RemoteAddr))

	conn := newWSConnection(ws, r.RemoteAddr)
	broker.HandleConnection(s.broker, conn)
}

// wsConnection implements core.Connection for WebSocket transport.
type wsConnection struct {
	ws           *websocket.Conn
	remoteAddr   string
	reader       io.Reader
	version      int
	mu           sync.RWMutex
	closed       bool
	lastActivity time.Time
	onDisconnect func(graceful bool)
}

func newWSConnection(ws *websocket.Conn, remoteAddr string) core.Connection {
	return &wsConnection{
		ws:         ws,
		remoteAddr: remoteAddr,
		closed:     false,
	}
}

func (c *wsConnection) ReadPacket() (packets.ControlPacket, error) {
	c.Touch()

	messageType, data, err := c.ws.ReadMessage()
	if err != nil {
		return nil, err
	}

	if messageType != websocket.BinaryMessage {
		return nil, errors.New("expected binary message")
	}

	reader := bytes.NewReader(data)

	if c.version == 0 {
		ver, restored, err := packets.DetectProtocolVersion(reader)
		if err != nil {
			return nil, err
		}
		c.version = ver
		c.reader = restored
	} else {
		c.reader = reader
	}

	var pkt packets.ControlPacket
	switch c.version {
	case 5:
		pkt, _, _, err = v5.ReadPacket(c.reader)
	case 3, 4:
		pkt, err = v3.ReadPacket(c.reader)
	default:
		err = errors.New("unsupported MQTT protocol version")
	}

	if err != nil {
		return nil, err
	}
	return pkt, nil
}

func (c *wsConnection) WritePacket(pkt packets.ControlPacket) error {
	if pkt == nil {
		return errors.New("cannot encode nil packet")
	}

	buf := &bytes.Buffer{}
	if err := pkt.Pack(buf); err != nil {
		return err
	}

	return c.ws.WriteMessage(websocket.BinaryMessage, buf.Bytes())
}

func (c *wsConnection) Read(b []byte) (n int, err error) {
	return 0, errors.New("Read not supported on WebSocket connection")
}

func (c *wsConnection) Write(b []byte) (n int, err error) {
	return 0, errors.New("Write not supported on WebSocket connection")
}

func (c *wsConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	if c.onDisconnect != nil {
		c.onDisconnect(false)
	}

	return c.ws.Close()
}

func (c *wsConnection) LocalAddr() net.Addr {
	return c.ws.LocalAddr()
}

func (c *wsConnection) RemoteAddr() net.Addr {
	return &wsAddr{addr: c.remoteAddr}
}

func (c *wsConnection) SetReadDeadline(t time.Time) error {
	return c.ws.SetReadDeadline(t)
}

func (c *wsConnection) SetWriteDeadline(t time.Time) error {
	return c.ws.SetWriteDeadline(t)
}

func (c *wsConnection) SetDeadline(t time.Time) error {
	if err := c.ws.SetReadDeadline(t); err != nil {
		return err
	}
	return c.ws.SetWriteDeadline(t)
}

func (c *wsConnection) SetKeepAlive(d time.Duration) error {
	// WebSocket has its own ping/pong mechanism
	// We can enable it if needed
	return nil
}

func (c *wsConnection) SetOnDisconnect(fn func(graceful bool)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.onDisconnect = fn
}

func (c *wsConnection) Touch() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastActivity = time.Now()
}

// wsAddr implements net.Addr for WebSocket connections.
type wsAddr struct {
	addr string
}

func (a *wsAddr) Network() string {
	return "websocket"
}

func (a *wsAddr) String() string {
	return a.addr
}
