// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/absmach/fluxmq/pkg/proto/auth/v1/authv1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingCallout serves a callout endpoint that never answers on its own. It
// returns only when the caller walks away or the test releases it, so a client
// that comes back early can only have done so by honouring its context.
//
// The caller must close release before the server is torn down: httptest's
// Close waits for in-flight handlers, and its cleanup runs before any
// registered later.
func blockingCallout(release <-chan struct{}) http.HandlerFunc {
	return func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}
}

// TestHTTPClient_Authorize_HonoursCallerCancellation is the point of giving the
// authorization interfaces a context. The callout runs synchronously on the
// publish path, so a client that disconnects mid-decision used to leave the
// broker waiting out the full timeout and retry budget for an answer nobody
// would read.
func TestHTTPClient_Authorize_HonoursCallerCancellation(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv := newHTTPTestServer(t, nil, blockingCallout(release))

	client := NewHTTPClient(srv.Client(), srv.URL,
		WithLogger(discardLogger()),
		WithTimeout(30*time.Second),
		WithRetries(3),
		WithRetryBackoff(10*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	t.Cleanup(cancel)

	done := make(chan bool, 1)
	start := time.Now()
	go func() { done <- client.CanPublish(ctx, "client-1", "telemetry/room1") }()

	select {
	case allowed := <-done:
		assert.False(t, allowed, "a cancelled authorization must not be treated as a grant")
		assert.Less(t, time.Since(start), time.Second,
			"cancellation must abandon the callout instead of waiting out timeout and backoff")
	case <-time.After(3 * time.Second):
		t.Fatal("CanPublish ignored the cancelled context and waited on the server")
	}
}

// TestHTTPClient_Authenticate_HonoursCallerCancellation covers the same
// guarantee on the CONNECT path.
func TestHTTPClient_Authenticate_HonoursCallerCancellation(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv := newHTTPTestServer(t, blockingCallout(release), nil)

	client := NewHTTPClient(srv.Client(), srv.URL,
		WithLogger(discardLogger()),
		WithTimeout(30*time.Second),
		WithRetries(3),
		WithRetryBackoff(10*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	t.Cleanup(cancel)

	type outcome struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	start := time.Now()
	go func() {
		_, err := client.Authenticate(ctx, "client-1", "user", "pass")
		done <- outcome{err: err, elapsed: time.Since(start)}
	}()

	select {
	case got := <-done:
		require.Error(t, got.err, "a cancelled authentication must not report success")
		assert.Less(t, got.elapsed, time.Second)
	case <-time.After(3 * time.Second):
		t.Fatal("Authenticate ignored the cancelled context and waited on the server")
	}
}

// blockingAuthServer is the Connect equivalent of blockingCallout: it answers
// nothing until the caller gives up or the test releases it.
type blockingAuthServer struct {
	release <-chan struct{}
}

func (b *blockingAuthServer) Authenticate(ctx context.Context, _ *connect.Request[authv1.AuthnReq]) (*connect.Response[authv1.AuthnRes], error) {
	b.wait(ctx)
	return connect.NewResponse(&authv1.AuthnRes{}), nil
}

func (b *blockingAuthServer) Authorize(ctx context.Context, _ *connect.Request[authv1.AuthzReq]) (*connect.Response[authv1.AuthzRes], error) {
	b.wait(ctx)
	return connect.NewResponse(&authv1.AuthzRes{}), nil
}

func (b *blockingAuthServer) wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-b.release:
	}
}

// TestGRPCClient_Authorize_HonoursCallerCancellation is the same guarantee on
// the Connect/gRPC transport, which carries its own copy of the retry and
// timeout logic.
func TestGRPCClient_Authorize_HonoursCallerCancellation(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	mux := http.NewServeMux()
	path, handler := authv1connect.NewAuthServiceHandler(&blockingAuthServer{release: release})
	mux.Handle(path, handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := NewGRPCClient(srv.Client(), srv.URL,
		WithLogger(discardLogger()),
		WithTimeout(30*time.Second),
		WithRetries(3),
		WithRetryBackoff(10*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	t.Cleanup(cancel)

	done := make(chan bool, 1)
	start := time.Now()
	go func() { done <- client.CanSubscribe(ctx, "client-1", "telemetry/#") }()

	select {
	case allowed := <-done:
		assert.False(t, allowed, "a cancelled authorization must not be treated as a grant")
		assert.Less(t, time.Since(start), time.Second)
	case <-time.After(3 * time.Second):
		t.Fatal("CanSubscribe ignored the cancelled context and waited on the server")
	}
}
