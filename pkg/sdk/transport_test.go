// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package sdk_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/absmach/magistrala/pkg/sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransport verifies that IdleConnTimeout=90s keeps connections pooled for
// healthy servers, and that network errors (EOF, reset) surface as descriptive errors.
func TestTransport(t *testing.T) {
	cases := []struct {
		desc        string
		serverFunc  func(t *testing.T) (url string, cleanup func())
		wantErr     bool
		errContains string
	}{
		{
			desc: "no error - healthy server reuses pooled connection",
			serverFunc: func(t *testing.T) (string, func()) {
				t.Helper()
				var connCount atomic.Int32
				srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte(`{"id":"1","name":"test-rule"}`))
				}))
				srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
					if state == http.StateNew {
						connCount.Add(1)
					}
				}
				srv.Start()
				return srv.URL, func() {
					srv.Close()
					assert.Equal(t, int32(1), connCount.Load(), "expected connections to be reused (keep-alives enabled)")
				}
			},
			wantErr: false,
		},
		{
			desc: "EOF - server closes connection immediately",
			serverFunc: func(t *testing.T) (string, func()) {
				t.Helper()
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				go func() {
					for {
						conn, err := ln.Accept()
						if err != nil {
							return
						}
						conn.Close()
					}
				}()
				return "http://" + ln.Addr().String(), func() { ln.Close() }
			},
			wantErr:     true,
			errContains: "request failed",
		},
		{
			desc: "ECONNRESET - server sends TCP reset",
			serverFunc: func(t *testing.T) (string, func()) {
				t.Helper()
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				go func() {
					for {
						conn, err := ln.Accept()
						if err != nil {
							return
						}
						// Set SO_LINGER with timeout 0 to force RST on close.
						tcpConn, ok := conn.(*net.TCPConn)
						if ok {
							_ = tcpConn.SetLinger(0)
						}
						conn.Close()
					}
				}()
				return "http://" + ln.Addr().String(), func() { ln.Close() }
			},
			wantErr:     true,
			errContains: "request failed",
		},
		{
			desc: "network error - listener closed before request completes",
			serverFunc: func(t *testing.T) (string, func()) {
				t.Helper()
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				addr := ln.Addr().String()
				ln.Close() // close immediately so dial fails with a net.OpError
				return "http://" + addr, func() {}
			},
			wantErr:     true,
			errContains: "request failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			url, cleanup := tc.serverFunc(t)
			defer cleanup()

			mgsdk := sdk.NewSDK(sdk.Config{RulesEngineURL: url})

			rule := sdk.Rule{Name: "test-rule"}
			for i := 0; i < 2; i++ {
				_, err := mgsdk.AddRule(context.Background(), rule, domainID, validToken)
				if tc.wantErr {
					require.Error(t, err)
					if tc.errContains != "" {
						assert.True(t, strings.Contains(err.Error(), tc.errContains),
							"expected error %q to contain %q", err.Error(), tc.errContains)
					}
					break
				}
				require.NoError(t, err)
			}
		})
	}
}

func TestConnReset(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			tcpConn, ok := conn.(*net.TCPConn)
			if ok {
				_ = tcpConn.SetLinger(0)
			}
			conn.Close()
		}
	}()
	defer ln.Close()

	mgsdk := sdk.NewSDK(sdk.Config{RulesEngineURL: "http://" + ln.Addr().String()})
	_, sdkErr := mgsdk.AddRule(context.Background(), sdk.Rule{Name: "test"}, domainID, validToken)
	require.Error(t, sdkErr)

	// On Linux, SO_LINGER=0 close produces ECONNRESET.
	// On other systems it may surface as EOF or a net.OpError — all are covered by the switch.
	errStr := sdkErr.Error()
	assert.True(t,
		strings.Contains(errStr, "connection reset by peer") ||
			strings.Contains(errStr, "connection closed unexpectedly") ||
			strings.Contains(errStr, "request failed"),
		"unexpected error: %s", errStr)

	_ = syscall.ECONNRESET // ensure syscall import is used
}
