// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/mqtt/packets"
	v3 "github.com/absmach/fluxmq/mqtt/packets/v3"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestWSServer starts the real handler on a test HTTP server, so upgrades go
// through the connection accounting and deadlines the server installs.
func newTestWSServer(t *testing.T, cfg Config) (*Server, string) {
	t.Helper()

	b := broker.NewBroker(nil, nil)
	t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown

	cfg.Path = defaultPath
	s := New(cfg, b, nil)

	ts := httptest.NewServer(http.HandlerFunc(s.handleWebSocket))
	t.Cleanup(ts.Close)

	return s, "ws" + strings.TrimPrefix(ts.URL, "http") + defaultPath
}

func dialWS(t *testing.T, url string) (*websocket.Conn, *http.Response) {
	t.Helper()
	conn, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil && resp == nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	if conn != nil {
		t.Cleanup(func() { conn.Close() }) //nolint:errcheck // best-effort teardown
	}
	return conn, resp
}

// TestWSConnectDeadlineDropsSilentClient covers the pre-authentication window on
// the WebSocket transport: a client that upgrades and sends no CONNECT must be
// dropped rather than holding a goroutine and a connection slot indefinitely.
func TestWSConnectDeadlineDropsSilentClient(t *testing.T) {
	s, url := newTestWSServer(t, Config{ReadTimeout: 150 * time.Millisecond})

	conn, _ := dialWS(t, url)
	require.NotNil(t, conn)

	// The handler owns the socket; when the deadline fires it closes it.
	conn.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck // test client
	_, _, err := conn.ReadMessage()
	require.Error(t, err, "silent client was never dropped: no connect deadline applied")

	require.Eventually(t, func() bool {
		s.activeMu.Lock()
		defer s.activeMu.Unlock()
		return len(s.active) == 0
	}, 5*time.Second, time.Millisecond, "dropped connection must be untracked")
}

// TestWSConnectionLimitRejectsUpgrade checks upgraded connections are capped, so
// a peer cannot open unbounded goroutines by upgrading repeatedly.
func TestWSConnectionLimitRejectsUpgrade(t *testing.T) {
	s, url := newTestWSServer(t, Config{MaxConnections: 1, ReadTimeout: 5 * time.Second})

	first, _ := dialWS(t, url)
	require.NotNil(t, first, "first connection must be accepted")

	require.Eventually(t, func() bool {
		s.activeMu.Lock()
		defer s.activeMu.Unlock()
		return len(s.active) == 1
	}, 5*time.Second, time.Millisecond)

	second, resp := dialWS(t, url)
	assert.Nil(t, second, "second connection must be refused at the limit")
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// TestWSShutdownClosesUpgradedConnections covers shutdown: http.Server.Shutdown
// neither closes nor waits for hijacked connections, so an upgraded WebSocket
// must be closed explicitly or its goroutine outlives the server.
func TestWSShutdownClosesUpgradedConnections(t *testing.T) {
	b := broker.NewBroker(nil, nil)
	t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown

	s := New(Config{
		Address:         "127.0.0.1:0",
		Path:            defaultPath,
		ShutdownTimeout: 100 * time.Millisecond,
	}, b, nil)

	// Bind explicitly so the port is known before serving races with the dial.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	url := "ws://" + listener.Addr().String() + defaultPath

	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.serveListener(ctx, listener) }()

	conn, _ := dialWS(t, url)
	require.NotNil(t, conn)

	require.Eventually(t, func() bool {
		s.activeMu.Lock()
		defer s.activeMu.Unlock()
		return len(s.active) == 1
	}, 5*time.Second, time.Millisecond, "upgraded connection was never tracked")

	cancel()

	select {
	case <-serveDone:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown never returned")
	}

	// The handler untracks as it unwinds, which happens after Shutdown returns.
	require.Eventually(t, func() bool {
		s.activeMu.Lock()
		defer s.activeMu.Unlock()
		return len(s.active) == 0
	}, 5*time.Second, time.Millisecond, "shutdown must close and untrack upgraded connections")

	conn.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck // test client
	_, _, readErr := conn.ReadMessage()
	assert.Error(t, readErr, "upgraded connection must be closed by shutdown")
}

// TestWSWriteTimeoutAppliedPerWrite covers a peer that stops reading: every
// socket write must carry a deadline, so a stalled client cannot park a broker
// goroutine on the write. An already-elapsed timeout makes that observable
// without having to fill the socket buffers.
func TestWSWriteTimeoutAppliedPerWrite(t *testing.T) {
	serverWS, clientWS := wsConnPair(t)
	t.Cleanup(func() { clientWS.Close() }) //nolint:errcheck // best-effort teardown

	conn := newWSConnection(serverWS, "127.0.0.1:9999", core.ProtocolV3, 0, time.Nanosecond)
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // best-effort teardown

	err := conn.WritePacket(&v3.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	})
	require.Error(t, err, "a write past its deadline must fail rather than block")
	assert.Contains(t, err.Error(), "timeout")
}

// TestWSWriteWithoutTimeoutHasNoDeadline documents the opposite configuration:
// with no write timeout the write is left unbounded.
func TestWSWriteWithoutTimeoutHasNoDeadline(t *testing.T) {
	serverWS, clientWS := wsConnPair(t)
	t.Cleanup(func() { clientWS.Close() }) //nolint:errcheck // best-effort teardown

	conn := newWSConnection(serverWS, "127.0.0.1:9999", core.ProtocolV3, 0, 0)
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // best-effort teardown

	require.NoError(t, conn.WritePacket(&v3.PingResp{
		FixedHeader: packets.FixedHeader{PacketType: packets.PingRespType},
	}))
}
