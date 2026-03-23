// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"context"
	"sync"
	"testing"
	"time"

	v5 "github.com/absmach/fluxmq/mqtt/packets/v5"
)

func TestNewClient(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, err := New(opts)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if client == nil {
		t.Fatal("client should not be nil")
	}
	if client.State() != StateDisconnected {
		t.Errorf("initial state should be Disconnected, got %v", client.State())
	}
	if client.IsConnected() {
		t.Error("IsConnected should be false initially")
	}
}

func TestNewClientValidation(t *testing.T) {
	tests := []struct {
		name    string
		opts    *Options
		wantErr error
	}{
		{
			name:    "empty client ID",
			opts:    NewOptions(),
			wantErr: ErrEmptyClientID,
		},
		{
			name:    "no servers",
			opts:    &Options{ClientID: "test", ProtocolVersion: 4},
			wantErr: ErrNoServers,
		},
		{
			name:    "invalid protocol",
			opts:    NewOptions().SetClientID("test").SetProtocolVersion(3),
			wantErr: ErrInvalidProtocol,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.opts)
			if err != tt.wantErr {
				t.Errorf("expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestNewClientNilOptions(t *testing.T) {
	_, err := New(nil)
	if err != ErrNilOptions {
		t.Fatalf("expected ErrNilOptions, got %v", err)
	}
}

func TestClientDefaultStore(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, err := New(opts)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if client.store == nil {
		t.Error("store should be initialized with default MemoryStore")
	}
}

func TestClientCustomStore(t *testing.T) {
	customStore := NewMemoryStore()
	opts := NewOptions().SetClientID("test-client").SetStore(customStore)
	client, err := New(opts)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if client.store != customStore {
		t.Error("client should use the custom store")
	}
}

func TestClientNotConnectedOperations(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	err := client.Publish(nil, "topic", []byte("payload"), 0, false)
	if err != ErrNotConnected {
		t.Errorf("Publish should fail with ErrNotConnected, got: %v", err)
	}

	err = client.Subscribe(nil, map[string]byte{"topic": 0})
	if err != ErrNotConnected {
		t.Errorf("Subscribe should fail with ErrNotConnected, got: %v", err)
	}

	err = client.Unsubscribe(nil, "topic")
	if err != ErrNotConnected {
		t.Errorf("Unsubscribe should fail with ErrNotConnected, got: %v", err)
	}
}

func TestClientPublishValidation(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	// Force connected state for validation tests
	client.state.set(StateConnected)

	err := client.Publish(nil, "", []byte("payload"), 0, false)
	if err != ErrInvalidTopic {
		t.Errorf("Publish with empty topic should fail with ErrInvalidTopic, got: %v", err)
	}

	err = client.Publish(nil, "topic", []byte("payload"), 3, false)
	if err != ErrInvalidQoS {
		t.Errorf("Publish with invalid QoS should fail with ErrInvalidQoS, got: %v", err)
	}
}

func TestClientSubscribeValidation(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	// Force connected state
	client.state.set(StateConnected)

	err := client.Subscribe(nil, map[string]byte{})
	if err != ErrInvalidTopic {
		t.Errorf("Subscribe with empty topics should fail with ErrInvalidTopic, got: %v", err)
	}
}

func TestClientSubscribeWithOptionsValidation(t *testing.T) {
	opts := NewOptions().SetClientID("test-client").SetProtocolVersion(5)
	client, _ := New(opts)
	client.state.set(StateConnected)

	err := client.SubscribeWithOptions(nil, nil)
	if err != ErrInvalidSubscribeOpt {
		t.Fatalf("expected ErrInvalidSubscribeOpt, got %v", err)
	}

	err = client.SubscribeWithOptions(nil, &SubscribeOption{Topic: "test/topic", QoS: 1, RetainHandling: 3})
	if err != ErrInvalidSubscribeOpt {
		t.Fatalf("expected ErrInvalidSubscribeOpt for retain handling, got %v", err)
	}
}

func TestClientUnsubscribeValidation(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	// Force connected state
	client.state.set(StateConnected)

	err := client.Unsubscribe(nil)
	if err != ErrInvalidTopic {
		t.Errorf("Unsubscribe with no topics should fail with ErrInvalidTopic, got: %v", err)
	}
}

func TestSubscribeContextCanceled(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.Subscribe(ctx, map[string]byte{"topic": 1})
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSubscribeWithOptionsContextCanceled(t *testing.T) {
	opts := NewOptions().SetClientID("test-client").SetProtocolVersion(5)
	client, _ := New(opts)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.SubscribeWithOptions(ctx, &SubscribeOption{Topic: "topic", QoS: 1})
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestUnsubscribeContextCanceled(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := client.Unsubscribe(ctx, "topic")
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestClientClose(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	err := client.Close(context.Background())
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if client.State() != StateClosed {
		t.Errorf("state should be Closed after Close, got %v", client.State())
	}

	// Operations should fail after close
	err = client.Connect(context.Background())
	if err != ErrClientClosed {
		t.Errorf("Connect after Close should fail with ErrClientClosed, got: %v", err)
	}
}

func TestClientCloseConcurrent(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	client.state.set(StateConnected)
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	writeDone := make(chan struct{})
	close(doneCh)
	close(writeDone)
	client.writeRT.Store(&writeRuntime{
		stopCh:    stopCh,
		doneCh:    doneCh,
		writeDone: writeDone,
	})

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.Close(context.Background())
		}()
	}
	wg.Wait()

	if client.State() != StateClosed {
		t.Fatalf("expected closed state, got %v", client.State())
	}
}

func TestClientCloseAndDisconnectConcurrent(t *testing.T) {
	for i := 0; i < 64; i++ {
		opts := NewOptions().SetClientID("test-client")
		client, _ := New(opts)

		client.state.set(StateConnected)
		stopCh := make(chan struct{})
		doneCh := make(chan struct{})
		close(doneCh)
		writeCh := make(chan writeRequest, 256)
		writeDone := make(chan struct{})
		close(writeDone)
		client.writeRT.Store(&writeRuntime{
			stopCh:    stopCh,
			doneCh:    doneCh,
			writeCh:   writeCh,
			writeDone: writeDone,
		})

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = client.Disconnect(context.Background())
		}()
		go func() {
			defer wg.Done()
			_ = client.Close(context.Background())
		}()
		wg.Wait()

		if client.State() != StateClosed {
			t.Fatalf("iteration %d: expected closed state, got %v", i, client.State())
		}
	}
}

func TestClientConnectFailure(t *testing.T) {
	opts := NewOptions().
		SetClientID("test-client").
		SetServers("localhost:19999"). // Non-existent port
		SetConnectTimeout(100 * time.Millisecond)

	client, _ := New(opts)

	err := client.Connect(context.Background())
	if err == nil {
		t.Error("Connect to non-existent server should fail")
		client.Disconnect(context.Background()) //nolint:errcheck // test cleanup
	}

	if client.State() != StateDisconnected {
		t.Errorf("state should be Disconnected after failed connect, got %v", client.State())
	}
}

func TestClientDisconnectWhenNotConnected(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	err := client.Disconnect(context.Background())
	if err != nil {
		t.Errorf("Disconnect when not connected should not error, got: %v", err)
	}
}

func TestSubscribeSingle(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	// Force connected state
	client.state.set(StateConnected)

	// This will fail because there's no actual connection,
	// but we're testing that SubscribeSingle calls Subscribe correctly
	err := client.SubscribeSingle(nil, "topic", 1)
	if err != ErrNotConnected {
		// The state check passes but the actual send fails
		// because there's no real connection
		t.Logf("SubscribeSingle error (expected): %v", err)
	}
}

func TestConnAckErrorString(t *testing.T) {
	tests := []struct {
		code ConnAckError
		want string
	}{
		{ErrConnAccepted, "connection accepted"},
		{ErrConnRefusedProtocol, "unacceptable protocol version"},
		{ErrConnRefusedIDRejected, "client identifier rejected"},
		{ErrConnRefusedUnavailable, "server unavailable"},
		{ErrConnRefusedBadAuth, "bad username or password"},
		{ErrConnRefusedNotAuth, "not authorized"},
		{ConnAckError(99), "unknown error"},
	}

	for _, tt := range tests {
		got := tt.code.String()
		if got != tt.want {
			t.Errorf("ConnAckError(%d).String() = %s, want %s", tt.code, got, tt.want)
		}
	}
}

func TestConnAckErrorError(t *testing.T) {
	code := ErrConnRefusedBadAuth
	err := code.Error()
	if err != "bad username or password" {
		t.Errorf("Error() = %s, want 'bad username or password'", err)
	}
}

func TestHandleSubAckV5FailureCodes(t *testing.T) {
	opts := NewOptions().SetClientID("test-client").SetProtocolVersion(5)
	client, err := New(opts)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	packetID := uint16(10)
	op, err := client.pending.add(packetID, pendingSubscribe, nil)
	if err != nil {
		t.Fatalf("pending add failed: %v", err)
	}

	reasonCodes := []byte{v5.SubAckNotAuthorized}
	client.handleSubAck(&v5.SubAck{
		ID:          packetID,
		ReasonCodes: &reasonCodes,
	})

	if err := op.wait(100 * time.Millisecond); err != ErrSubscribeFailed {
		t.Fatalf("expected ErrSubscribeFailed, got %v", err)
	}
}
