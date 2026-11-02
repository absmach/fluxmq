// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package tcp

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	fluxbroker "github.com/absmach/fluxmq/broker"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/stretchr/testify/require"
)

type rejectAllAuthenticator struct{}

func (rejectAllAuthenticator) Authenticate(context.Context, string, string, string) (*fluxbroker.AuthnResult, error) {
	return &fluxbroker.AuthnResult{Authenticated: false}, nil
}

// TestConnectRefusalReachesClient covers the CONNECT refusal over a real TCP
// socket: a well-formed CONNECT with rejected credentials must receive a
// non-zero CONNACK before the broker closes the connection, with or without the
// asynchronous send queue.
func TestConnectRefusalReachesClient(t *testing.T) {
	// CONNECT, flags 0xC2 (username, password, clean session), keep-alive 60,
	// client ID "c1", username "u", password "p".
	v3Connect := []byte{
		0x10, 0x14,
		0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0xC2, 0x00, 0x3C,
		0x00, 0x02, 'c', '1', 0x00, 0x01, 'u', 0x00, 0x01, 'p',
	}
	v5Connect := []byte{
		0x10, 0x15,
		0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0xC2, 0x00, 0x3C, 0x00,
		0x00, 0x02, 'c', '1', 0x00, 0x01, 'u', 0x00, 0x01, 'p',
	}

	cases := []struct {
		name      string
		queueSize int
		connect   []byte
		want      []byte
	}{
		{"v3/sync-writes", 0, v3Connect, []byte{0x20, 0x02, 0x00, 0x04}},
		{"v3/send-queue", 1000, v3Connect, []byte{0x20, 0x02, 0x00, 0x04}},
		{"v5/sync-writes", 0, v5Connect, []byte{0x20, 0x03, 0x00, 0x86, 0x00}},
		{"v5/send-queue", 1000, v5Connect, []byte{0x20, 0x03, 0x00, 0x86, 0x00}},
	}

	const attempts = 20
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := broker.NewBroker(nil, nil)
			t.Cleanup(func() { b.Close() }) //nolint:errcheck // best-effort teardown
			b.SetAuthEngine(fluxbroker.NewAuthEngine(rejectAllAuthenticator{}, nil))

			server := New(Config{
				ShutdownTimeout: 5 * time.Second,
				ReadTimeout:     5 * time.Second,
				SendQueueSize:   tc.queueSize,
			}, b)

			for range attempts {
				got := connectAndReadUntilEOF(t, server, tc.connect)
				require.Equal(t, tc.want, got, "refusal CONNACK must arrive before EOF")
			}
		})
	}
}

// connectAndReadUntilEOF runs one connection through the server's handler,
// sends connect, and returns everything the broker writes before closing.
func connectAndReadUntilEOF(t *testing.T, server *Server, connect []byte) []byte {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close() //nolint:errcheck // listener only needed for one accept

	handled := make(chan struct{})
	go func() {
		defer close(handled)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		server.wg.Add(1)
		server.handleConnection(context.Background(), conn)
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer client.Close() //nolint:errcheck // best-effort teardown

	_, err = client.Write(connect)
	require.NoError(t, err)

	require.NoError(t, client.SetReadDeadline(time.Now().Add(5*time.Second)))
	got, err := io.ReadAll(client)
	require.NoError(t, err, "broker must close the connection after refusing it")

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("connection handler did not return")
	}
	return got
}
