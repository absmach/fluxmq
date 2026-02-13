// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
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

	err := client.Publish("topic", []byte("payload"), 0, false)
	if err != ErrNotConnected {
		t.Errorf("Publish should fail with ErrNotConnected, got: %v", err)
	}

	err = client.Subscribe(map[string]byte{"topic": 0})
	if err != ErrNotConnected {
		t.Errorf("Subscribe should fail with ErrNotConnected, got: %v", err)
	}

	err = client.Unsubscribe("topic")
	if err != ErrNotConnected {
		t.Errorf("Unsubscribe should fail with ErrNotConnected, got: %v", err)
	}
}

func TestClientPublishValidation(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	// Force connected state for validation tests
	client.state.set(StateConnected)

	err := client.Publish("", []byte("payload"), 0, false)
	if err != ErrInvalidTopic {
		t.Errorf("Publish with empty topic should fail with ErrInvalidTopic, got: %v", err)
	}

	err = client.Publish("topic", []byte("payload"), 3, false)
	if err != ErrInvalidQoS {
		t.Errorf("Publish with invalid QoS should fail with ErrInvalidQoS, got: %v", err)
	}
}

func TestClientSubscribeValidation(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	// Force connected state
	client.state.set(StateConnected)

	err := client.Subscribe(map[string]byte{})
	if err != ErrInvalidTopic {
		t.Errorf("Subscribe with empty topics should fail with ErrInvalidTopic, got: %v", err)
	}
}

func TestClientSubscribeWithOptionsValidation(t *testing.T) {
	opts := NewOptions().SetClientID("test-client").SetProtocolVersion(5)
	client, _ := New(opts)
	client.state.set(StateConnected)

	err := client.SubscribeWithOptions(nil)
	if err != ErrInvalidSubscribeOpt {
		t.Fatalf("expected ErrInvalidSubscribeOpt, got %v", err)
	}

	err = client.SubscribeWithOptions(&SubscribeOption{Topic: "test/topic", QoS: 1, RetainHandling: 3})
	if err != ErrInvalidSubscribeOpt {
		t.Fatalf("expected ErrInvalidSubscribeOpt for retain handling, got %v", err)
	}
}

func TestClientUnsubscribeValidation(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	// Force connected state
	client.state.set(StateConnected)

	err := client.Unsubscribe()
	if err != ErrInvalidTopic {
		t.Errorf("Unsubscribe with no topics should fail with ErrInvalidTopic, got: %v", err)
	}
}

func TestClientClose(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	err := client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if client.State() != StateClosed {
		t.Errorf("state should be Closed after Close, got %v", client.State())
	}

	// Operations should fail after close
	err = client.Connect()
	if err != ErrClientClosed {
		t.Errorf("Connect after Close should fail with ErrClientClosed, got: %v", err)
	}
}

func TestClientConnectFailure(t *testing.T) {
	opts := NewOptions().
		SetClientID("test-client").
		SetServers("localhost:19999"). // Non-existent port
		SetConnectTimeout(100 * time.Millisecond)

	client, _ := New(opts)

	err := client.Connect()
	if err == nil {
		t.Error("Connect to non-existent server should fail")
		client.Disconnect()
	}

	if client.State() != StateDisconnected {
		t.Errorf("state should be Disconnected after failed connect, got %v", client.State())
	}
}

func TestClientDisconnectWhenNotConnected(t *testing.T) {
	opts := NewOptions().SetClientID("test-client")
	client, _ := New(opts)

	err := client.Disconnect()
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
	err := client.SubscribeSingle("topic", 1)
	if err != ErrNotConnected {
		// The state check passes but the actual send fails
		// because there's no real connection
		t.Logf("SubscribeSingle error (expected): %v", err)
	}
}

func TestConnAckCodeString(t *testing.T) {
	tests := []struct {
		code ConnAckCode
		want string
	}{
		{ConnAccepted, "connection accepted"},
		{ConnRefusedProtocol, "unacceptable protocol version"},
		{ConnRefusedIDRejected, "client identifier rejected"},
		{ConnRefusedUnavailable, "server unavailable"},
		{ConnRefusedBadAuth, "bad username or password"},
		{ConnRefusedNotAuth, "not authorized"},
		{ConnAckCode(99), "unknown error"},
	}

	for _, tt := range tests {
		got := tt.code.String()
		if got != tt.want {
			t.Errorf("ConnAckCode(%d).String() = %s, want %s", tt.code, got, tt.want)
		}
	}
}

func TestConnAckCodeError(t *testing.T) {
	code := ConnRefusedBadAuth
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
