// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package websocket

import (
	"testing"

	core "github.com/absmach/fluxmq/mqtt"
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
