// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package sdk_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/absmach/magistrala/pkg/sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransport verifies that DisableKeepAlives=true forces a new TCP connection
// per request, ensuring EOF always reflects a genuine failure and not a stale pool.
func TestTransport(t *testing.T) {
	cases := []struct {
		desc       string
		serverFunc func(t *testing.T) (url string, cleanup func())
		wantErr    bool
	}{
		{
			desc: "no EOF - healthy server returns a new connection per request",
			serverFunc: func(t *testing.T) (string, func()) {
				t.Helper()
				var connCount atomic.Int32
				srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))
				srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
					if state == http.StateNew {
						connCount.Add(1)
					}
				}
				srv.Start()
				return srv.URL, func() {
					srv.Close()
					assert.Equal(t, int32(2), connCount.Load(), "expected one TCP connection per request (DisableKeepAlives=true)")
				}
			},
			wantErr: false,
		},
		{
			desc: "EOF - server closes connection immediately surfaces as error",
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
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			url, cleanup := tc.serverFunc(t)
			defer cleanup()

			mgsdk := sdk.NewSDK(sdk.Config{RulesEngineURL: url})

			for i := 0; i < 2; i++ {
				_, err := mgsdk.ListRules(context.Background(), sdk.PageMetadata{Limit: 10}, domainID, validToken)
				if tc.wantErr {
					assert.Error(t, err, "expected error on closed connection, not silent success")
					break
				}
			}
		})
	}
}
