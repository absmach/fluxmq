// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package coap

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/absmach/fluxmq/cluster"
	"github.com/absmach/fluxmq/config"
	"github.com/absmach/fluxmq/mqtt/broker"
	"github.com/absmach/fluxmq/storage/memory"
	piondtls "github.com/pion/dtls/v3"
	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/message/pool"
	"github.com/plgd-dev/go-coap/v3/mux"
)

type stubConn struct {
	last *pool.Message
	done chan struct{}
}

func newStubConn() *stubConn {
	return &stubConn{done: make(chan struct{})}
}

func (c *stubConn) AcquireMessage(ctx context.Context) *pool.Message {
	return pool.NewMessage(ctx)
}

func (c *stubConn) ReleaseMessage(*pool.Message) {}

func (c *stubConn) Ping(ctx context.Context) error { return nil }
func (c *stubConn) Get(ctx context.Context, path string, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}
func (c *stubConn) Delete(ctx context.Context, path string, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}
func (c *stubConn) Post(ctx context.Context, path string, contentFormat message.MediaType, payload io.ReadSeeker, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}
func (c *stubConn) Put(ctx context.Context, path string, contentFormat message.MediaType, payload io.ReadSeeker, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}
func (c *stubConn) Observe(ctx context.Context, path string, observeFunc func(notification *pool.Message), opts ...message.Option) (mux.Observation, error) {
	return nil, nil
}
func (c *stubConn) RemoteAddr() net.Addr { return stubAddr("coap-stub") }
func (c *stubConn) NetConn() net.Conn    { return nil }
func (c *stubConn) Context() context.Context {
	return context.Background()
}
func (c *stubConn) SetContextValue(key interface{}, val interface{}) {}
func (c *stubConn) WriteMessage(req *pool.Message) error {
	c.last = req
	return nil
}
func (c *stubConn) Do(req *pool.Message) (*pool.Message, error) { return nil, nil }
func (c *stubConn) DoObserve(req *pool.Message, observeFunc func(req *pool.Message)) (mux.Observation, error) {
	return nil, nil
}
func (c *stubConn) Close() error {
	close(c.done)
	return nil
}
func (c *stubConn) Sequence() uint64 { return 0 }
func (c *stubConn) Done() <-chan struct{} {
	return c.done
}
func (c *stubConn) AddOnClose(func()) {}
func (c *stubConn) NewGetRequest(ctx context.Context, path string, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}
func (c *stubConn) NewObserveRequest(ctx context.Context, path string, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}
func (c *stubConn) NewPutRequest(ctx context.Context, path string, contentFormat message.MediaType, payload io.ReadSeeker, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}
func (c *stubConn) NewPostRequest(ctx context.Context, path string, contentFormat message.MediaType, payload io.ReadSeeker, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}
func (c *stubConn) NewDeleteRequest(ctx context.Context, path string, opts ...message.Option) (*pool.Message, error) {
	return nil, nil
}

type stubResponseWriter struct {
	conn *stubConn
	msg  *pool.Message
}

func (w *stubResponseWriter) SetResponse(code codes.Code, contentFormat message.MediaType, d io.ReadSeeker, opts ...message.Option) error {
	resp := w.conn.AcquireMessage(context.Background())
	resp.SetCode(code)
	resp.SetBody(d)
	w.conn.last = resp
	return nil
}

func (w *stubResponseWriter) Conn() mux.Conn { return w.conn }
func (w *stubResponseWriter) SetMessage(m *pool.Message) {
	w.msg = m
}
func (w *stubResponseWriter) Message() *pool.Message { return w.msg }

type stubAddr string

func (a stubAddr) Network() string { return "stub" }
func (a stubAddr) String() string  { return string(a) }

func TestNew(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, slog.Default(), stats, nil, nil, nil, config.SessionConfig{})
	defer b.Close()

	cfg := Config{
		Address:         ":5683",
		ShutdownTimeout: 5 * time.Second,
	}

	server := New(cfg, b, nil)

	if server == nil {
		t.Fatal("Expected non-nil server")
	}
	if server.config.Address != ":5683" {
		t.Errorf("Expected address :5683, got %s", server.config.Address)
	}
	if server.broker == nil {
		t.Error("Expected broker to be set")
	}
	if server.mux == nil {
		t.Error("Expected mux router to be set")
	}
}

func TestConfig_TLSConfig(t *testing.T) {
	cfg := Config{
		Address: ":5684",
		TLSConfig: &piondtls.Config{
			ClientAuth: piondtls.RequireAndVerifyClientCert,
		},
	}

	if cfg.TLSConfig == nil {
		t.Fatal("Expected TLSConfig to be set")
	}
	if cfg.TLSConfig.ClientAuth != piondtls.RequireAndVerifyClientCert {
		t.Errorf("Expected RequireAndVerifyClientCert, got %v", cfg.TLSConfig.ClientAuth)
	}
}

func TestHandleHealth(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, slog.Default(), stats, nil, nil, nil, config.SessionConfig{})
	defer b.Close()

	server := New(Config{}, b, slog.Default())
	conn := newStubConn()
	writer := &stubResponseWriter{conn: conn}

	req := &mux.Message{Message: pool.NewMessage(context.Background())}
	server.handleHealth(writer, req)

	if conn.last == nil {
		t.Fatal("expected response message")
	}
	if conn.last.Code() != codes.Content {
		t.Fatalf("expected code %v, got %v", codes.Content, conn.last.Code())
	}
	body, err := conn.last.ReadBody()
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != "healthy" {
		t.Fatalf("expected body %q, got %q", "healthy", string(body))
	}
}

func TestHandlePublish(t *testing.T) {
	store := memory.New()
	cl := cluster.NewNoopCluster("test-node")
	stats := broker.NewStats()
	b := broker.NewBroker(store, cl, slog.Default(), stats, nil, nil, nil, config.SessionConfig{})
	defer b.Close()

	server := New(Config{}, b, slog.Default())

	t.Run("missing topic", func(t *testing.T) {
		conn := newStubConn()
		writer := &stubResponseWriter{conn: conn}

		reqMsg := pool.NewMessage(context.Background())
		reqMsg.MustSetPath("/mqtt/publish")
		req := &mux.Message{Message: reqMsg}

		server.handlePublish(writer, req)

		if conn.last == nil {
			t.Fatal("expected response message")
		}
		if conn.last.Code() != codes.BadRequest {
			t.Fatalf("expected code %v, got %v", codes.BadRequest, conn.last.Code())
		}
	})

	t.Run("ok", func(t *testing.T) {
		conn := newStubConn()
		writer := &stubResponseWriter{conn: conn}

		reqMsg := pool.NewMessage(context.Background())
		reqMsg.MustSetPath("/mqtt/publish/test/topic")
		reqMsg.SetBody(bytes.NewReader([]byte("payload")))
		req := &mux.Message{Message: reqMsg}

		server.handlePublish(writer, req)

		if conn.last == nil {
			t.Fatal("expected response message")
		}
		if conn.last.Code() != codes.Changed {
			t.Fatalf("expected code %v, got %v", codes.Changed, conn.last.Code())
		}
		body, err := conn.last.ReadBody()
		if err != nil {
			t.Fatalf("failed to read body: %v", err)
		}
		if string(body) != "ok" {
			t.Fatalf("expected body %q, got %q", "ok", string(body))
		}
	})
}
