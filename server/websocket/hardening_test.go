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

// TestWSConnectionLimitAppliesToSockets checks the cap is enforced on accepted
// sockets rather than on completed upgrades. A peer that opens a socket and
// stalls before finishing its HTTP request must still consume quota, otherwise
// the limit protects nothing.
func TestWSConnectionLimitAppliesToSockets(t *testing.T) {
	b := broker.NewBroker(nil, nil)
	t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown

	s := New(Config{Path: defaultPath, MaxConnections: 1, ReadTimeout: 5 * time.Second}, b, nil)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	serveDone := make(chan error, 1)
	go func() { serveDone <- s.serveListener(ctx, s.limitListener(listener)) }()

	first, _, err := websocket.DefaultDialer.Dial("ws://"+addr+defaultPath, nil)
	require.NoError(t, err, "first connection must be accepted")
	t.Cleanup(func() { first.Close() }) //nolint:errcheck // best-effort teardown

	require.Eventually(t, func() bool {
		s.activeMu.Lock()
		defer s.activeMu.Unlock()
		return len(s.active) == 1
	}, 5*time.Second, time.Millisecond)

	// At the cap the listener stops accepting, so the second handshake cannot
	// complete while the first connection holds the slot.
	dialer := &websocket.Dialer{HandshakeTimeout: 300 * time.Millisecond}
	second, _, err := dialer.Dial("ws://"+addr+defaultPath, nil)
	if second != nil {
		second.Close() //nolint:errcheck // only reached when the assertion below fails
	}
	require.Error(t, err, "second connection must not be served while the limit is held")

	// Releasing the first connection frees the slot for the next one.
	first.Close() //nolint:errcheck // handing the slot back

	require.Eventually(t, func() bool {
		conn, _, err := dialer.Dial("ws://"+addr+defaultPath, nil)
		if err != nil {
			return false
		}
		conn.Close() //nolint:errcheck // best-effort teardown
		return true
	}, 5*time.Second, 20*time.Millisecond, "a freed slot must admit the next connection")
}

// TestWSReadHeaderTimeoutDropsStalledRequest covers the phase before the
// upgrade: a peer that opens a socket and dribbles HTTP headers never reaches
// the handler, so only an HTTP-level deadline can evict it.
func TestWSReadHeaderTimeoutDropsStalledRequest(t *testing.T) {
	b := broker.NewBroker(nil, nil)
	t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown

	s := New(Config{Path: defaultPath, ReadTimeout: 150 * time.Millisecond}, b, nil)
	require.Equal(t, 150*time.Millisecond, s.server.ReadHeaderTimeout,
		"the read timeout must bound the pre-upgrade request phase")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { s.serveListener(ctx, listener) }() //nolint:errcheck // shutdown asserted elsewhere

	raw, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { raw.Close() }) //nolint:errcheck // best-effort teardown

	// A request line with no terminating blank line: headers never complete.
	_, err = raw.Write([]byte("GET " + defaultPath + " HTTP/1.1\r\nHost: localhost\r\n"))
	require.NoError(t, err)

	raw.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck // test client
	_, err = raw.Read(make([]byte, 1))
	assert.Error(t, err, "stalled request was never dropped: no header deadline applied")
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
