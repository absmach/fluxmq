// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	core "github.com/absmach/fluxmq/mqtt"
	"github.com/gorilla/websocket"
)

func TestNewWSConnectionProtocolVersion(t *testing.T) {
	conn := newWSConnection(nil, "127.0.0.1:1111", core.ProtocolV5)

	wsConn, ok := conn.(*wsConnection)
	if !ok {
		t.Fatalf("expected *wsConnection, got %T", conn)
	}

	if wsConn.version != core.ProtocolV5 {
		t.Fatalf("expected protocol version %d, got %d", core.ProtocolV5, wsConn.version)
	}
}

func TestSubprotocolNegotiation(t *testing.T) {
	// Use the same upgrader config as the real server to test subprotocol negotiation
	// without requiring a full broker.
	upgrader := websocket.Upgrader{
		CheckOrigin:  func(r *http.Request) bool { return true },
		Subprotocols: []string{"mqtt"},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		ws.Close()
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/mqtt"

	tests := []struct {
		name         string
		subprotocols []string
		wantProtocol string
	}{
		{
			name:         "mqtt subprotocol negotiated",
			subprotocols: []string{"mqtt"},
			wantProtocol: "mqtt",
		},
		{
			name:         "mqtt selected from multiple",
			subprotocols: []string{"graphql-ws", "mqtt"},
			wantProtocol: "mqtt",
		},
		{
			name:         "no subprotocol requested",
			subprotocols: nil,
			wantProtocol: "",
		},
		{
			name:         "unsupported subprotocol not echoed",
			subprotocols: []string{"graphql-ws"},
			wantProtocol: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer := websocket.Dialer{Subprotocols: tt.subprotocols}
			ws, resp, err := dialer.Dial(wsURL, nil)
			if err != nil {
				t.Fatalf("dial error: %v", err)
			}
			defer ws.Close()

			got := resp.Header.Get("Sec-WebSocket-Protocol")
			if got != tt.wantProtocol {
				t.Fatalf("Sec-WebSocket-Protocol = %q, want %q", got, tt.wantProtocol)
			}
		})
	}
}

func TestWebSocketPingPong(t *testing.T) {
	serverWS, clientWS := wsConnPair(t)
	defer clientWS.Close()

	conn := &wsConnection{
		ws:         serverWS,
		remoteAddr: "127.0.0.1:9999",
	}

	if err := conn.SetKeepAlive(2 * time.Second); err != nil {
		t.Fatalf("SetKeepAlive: %v", err)
	}
	defer conn.Close()

	// The ping goroutine sends pings every 1s (keepalive/2).
	// The client's default ping handler auto-replies with pong, but only when
	// the client is actively reading. The server's pong handler (set by
	// SetKeepAlive) calls Touch(), but only when the server is reading.
	//
	// Both sides need active readers for the ping/pong cycle to complete.

	// Client reader: processes incoming pings and auto-replies with pong.
	go func() {
		for {
			if _, _, err := clientWS.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Server reader: processes incoming pongs and fires the pong handler.
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.ws.ReadMessage() //nolint:errcheck
	}()

	// Wait for at least one full ping/pong cycle.
	time.Sleep(1500 * time.Millisecond)

	conn.mu.RLock()
	activity := conn.lastActivity
	conn.mu.RUnlock()

	if activity.IsZero() {
		t.Fatal("expected lastActivity to be set by pong handler, got zero")
	}

	elapsed := time.Since(activity)
	if elapsed > 2*time.Second {
		t.Fatalf("lastActivity too old: %v ago", elapsed)
	}

	// Close should stop the ping goroutine and unblock ReadMessage.
	conn.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("ReadMessage goroutine did not exit after Close")
	}
}

func TestSetKeepAliveZero(t *testing.T) {
	conn := &wsConnection{}
	if err := conn.SetKeepAlive(0); err != nil {
		t.Fatalf("SetKeepAlive(0) = %v, want nil", err)
	}
	if conn.pingStop != nil {
		t.Fatal("expected pingStop to be nil for zero keep-alive")
	}
}

// wsConnPair creates a connected pair of WebSocket connections using an in-process
// httptest server. The server-side conn is returned first.
func wsConnPair(t *testing.T) (server *websocket.Conn, client *websocket.Conn) {
	t.Helper()
	serverCh := make(chan *websocket.Conn, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("server upgrade: %v", err)
			return
		}
		serverCh <- ws
	}))
	t.Cleanup(ts.Close)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/"
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("client dial: %v", err)
	}

	serverConn := <-serverCh
	return serverConn, clientConn
}
