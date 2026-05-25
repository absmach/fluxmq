// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package authcallout

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	authv1 "github.com/absmach/fluxmq/pkg/proto/auth/v1"
	"github.com/absmach/fluxmq/pkg/proto/auth/v1/authv1connect"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type flakyAuthServer struct {
	failuresLeft atomic.Int32
	authnResult  *authv1.AuthnRes
	calls        atomic.Int32
}

func (f *flakyAuthServer) Authenticate(_ context.Context, _ *connect.Request[authv1.AuthnReq]) (*connect.Response[authv1.AuthnRes], error) {
	f.calls.Add(1)
	if f.failuresLeft.Add(-1) >= 0 {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient"))
	}
	return connect.NewResponse(f.authnResult), nil
}

func (f *flakyAuthServer) Authorize(context.Context, *connect.Request[authv1.AuthzReq]) (*connect.Response[authv1.AuthzRes], error) {
	return connect.NewResponse(&authv1.AuthzRes{Authorized: true}), nil
}

func startRetryServer(t *testing.T, handler authv1connect.AuthServiceHandler, opts ...Option) (*httptest.Server, *GRPCClient) {
	t.Helper()
	mux := http.NewServeMux()
	path, h := authv1connect.NewAuthServiceHandler(handler)
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	opts = append(opts, WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))))
	client := NewGRPCClient(srv.Client(), srv.URL, opts...)
	return srv, client
}

func TestGRPCClient_RetriesTransientErrors(t *testing.T) {
	fake := &flakyAuthServer{authnResult: &authv1.AuthnRes{Authenticated: true, Id: "ext"}}
	fake.failuresLeft.Store(2)

	_, client := startRetryServer(t, fake,
		WithRetries(3),
		WithRetryBackoff(time.Millisecond),
		WithRetryMaxBackoff(2*time.Millisecond),
	)

	result, err := client.Authenticate("c", "u", "p")
	require.NoError(t, err)
	assert.True(t, result.Authenticated)
	assert.Equal(t, int32(3), fake.calls.Load(), "expected 2 failures + 1 success")
}

func TestGRPCClient_StopsRetryingWhenBudgetExhausted(t *testing.T) {
	fake := &flakyAuthServer{authnResult: &authv1.AuthnRes{Authenticated: true, Id: "ext"}}
	fake.failuresLeft.Store(10)

	_, client := startRetryServer(t, fake,
		WithRetries(2),
		WithRetryBackoff(time.Millisecond),
	)

	_, err := client.Authenticate("c", "u", "p")
	require.Error(t, err)
	assert.Equal(t, int32(3), fake.calls.Load(), "initial + 2 retries")
}

func TestRetriableError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"breaker open", gobreaker.ErrOpenState, false},
		{"breaker too many", gobreaker.ErrTooManyRequests, false},
		{"context canceled", context.Canceled, false},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"transport error", errors.New("connection refused"), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, retriableError(tc.err))
		})
	}
}

func TestOptions_BackoffDelay(t *testing.T) {
	o := Options{RetryBackoff: 10 * time.Millisecond, RetryMaxBackoff: 100 * time.Millisecond}
	// Full jitter means delay is in [0, base*2^attempt+1).
	for attempt := range 5 {
		d := o.backoffDelay(attempt)
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.LessOrEqual(t, d, o.RetryMaxBackoff+time.Nanosecond)
	}
}
