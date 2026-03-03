// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package mqtt

import (
	"crypto/tls"
	"testing"
	"time"
)

func TestNewOptions(t *testing.T) {
	opts := NewOptions()

	if len(opts.Servers) != 1 || opts.Servers[0] != "localhost:1883" {
		t.Errorf("expected default server localhost:1883, got %v", opts.Servers)
	}
	if opts.ProtocolVersion != 4 {
		t.Errorf("expected protocol version 4, got %d", opts.ProtocolVersion)
	}
	if !opts.CleanSession {
		t.Error("expected CleanSession to be true by default")
	}
	if opts.KeepAlive != DefaultKeepAlive {
		t.Errorf("expected KeepAlive %v, got %v", DefaultKeepAlive, opts.KeepAlive)
	}
	if opts.MaxInflight != DefaultMaxInflight {
		t.Errorf("expected MaxInflight %d, got %d", DefaultMaxInflight, opts.MaxInflight)
	}
	if !opts.AutoReconnect {
		t.Error("expected AutoReconnect to be true by default")
	}
	if opts.OutboundBackpressurePolicy != OutboundBackpressureBlock {
		t.Errorf("expected outbound backpressure policy %q, got %q", OutboundBackpressureBlock, opts.OutboundBackpressurePolicy)
	}
}

func TestOptionsBuilder(t *testing.T) {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	onConnect := func() {}
	onLost := func(error) {}
	onReconn := func(int) {}
	onReconnFailed := func(error) {}
	onDropped := func(*DroppedMessage) {}
	onMsg := func(string, []byte, byte) {}
	store := NewMemoryStore()

	opts := NewOptions().
		SetServers("broker1:1883", "broker2:1883").
		SetClientID("test-client").
		SetCredentials("user", "pass").
		SetTLSConfig(tlsConfig).
		SetCleanSession(false).
		SetKeepAlive(30*time.Second).
		SetConnectTimeout(5*time.Second).
		SetAckTimeout(20*time.Second).
		SetProtocolVersion(5).
		SetWill("will/topic", []byte("goodbye"), 1, true).
		SetAutoReconnect(false).
		SetReconnectJitter(250 * time.Millisecond).
		SetMaxReconnectAttempts(3).
		SetMaxInflight(50).
		SetMaxOutboundPendingMessages(64).
		SetMaxOutboundPendingBytes(4096).
		SetOutboundBackpressurePolicy(OutboundBackpressureDropNew).
		SetOutboundBlockTimeout(75 * time.Millisecond).
		SetOnConnect(onConnect).
		SetOnConnectionLost(onLost).
		SetOnReconnecting(onReconn).
		SetOnReconnectFailed(onReconnFailed).
		SetOnDroppedMessage(onDropped).
		SetSlowConsumerPolicy(SlowConsumerDropOldest).
		SetSlowConsumerBlockTimeout(250 * time.Millisecond).
		SetOnMessage(onMsg).
		SetStore(store)

	if len(opts.Servers) != 2 {
		t.Errorf("expected 2 servers, got %d", len(opts.Servers))
	}
	if opts.ClientID != "test-client" {
		t.Errorf("expected client ID 'test-client', got %s", opts.ClientID)
	}
	if opts.Username != "user" {
		t.Errorf("expected username 'user', got %s", opts.Username)
	}
	if opts.Password != "pass" {
		t.Errorf("expected password 'pass', got %s", opts.Password)
	}
	if opts.TLSConfig != tlsConfig {
		t.Error("TLS config not set correctly")
	}
	if opts.CleanSession {
		t.Error("expected CleanSession to be false")
	}
	if opts.KeepAlive != 30*time.Second {
		t.Errorf("expected KeepAlive 30s, got %v", opts.KeepAlive)
	}
	if opts.ProtocolVersion != 5 {
		t.Errorf("expected protocol version 5, got %d", opts.ProtocolVersion)
	}
	if opts.Will == nil {
		t.Fatal("expected Will to be set")
	}
	if opts.Will.Topic != "will/topic" {
		t.Errorf("expected will topic 'will/topic', got %s", opts.Will.Topic)
	}
	if opts.Will.QoS != 1 {
		t.Errorf("expected will QoS 1, got %d", opts.Will.QoS)
	}
	if !opts.Will.Retain {
		t.Error("expected will retain to be true")
	}
	if opts.AutoReconnect {
		t.Error("expected AutoReconnect to be false")
	}
	if opts.MaxInflight != 50 {
		t.Errorf("expected MaxInflight 50, got %d", opts.MaxInflight)
	}
	if opts.ReconnectJitter != 250*time.Millisecond {
		t.Errorf("expected reconnect jitter 250ms, got %v", opts.ReconnectJitter)
	}
	if opts.MaxReconnectAttempts != 3 {
		t.Errorf("expected max reconnect attempts 3, got %d", opts.MaxReconnectAttempts)
	}
	if opts.SlowConsumerPolicy != SlowConsumerDropOldest {
		t.Errorf("expected slow consumer policy %q, got %q", SlowConsumerDropOldest, opts.SlowConsumerPolicy)
	}
	if opts.SlowConsumerBlockTimeout != 250*time.Millisecond {
		t.Errorf("expected slow consumer block timeout 250ms, got %v", opts.SlowConsumerBlockTimeout)
	}
	if opts.MaxOutboundPendingMessages != 64 {
		t.Errorf("expected max outbound pending messages 64, got %d", opts.MaxOutboundPendingMessages)
	}
	if opts.MaxOutboundPendingBytes != 4096 {
		t.Errorf("expected max outbound pending bytes 4096, got %d", opts.MaxOutboundPendingBytes)
	}
	if opts.OutboundBackpressurePolicy != OutboundBackpressureDropNew {
		t.Errorf("expected outbound backpressure policy %q, got %q", OutboundBackpressureDropNew, opts.OutboundBackpressurePolicy)
	}
	if opts.OutboundBlockTimeout != 75*time.Millisecond {
		t.Errorf("expected outbound block timeout 75ms, got %v", opts.OutboundBlockTimeout)
	}
	if opts.Store != store {
		t.Error("Store not set correctly")
	}
}

func TestOptionsValidation(t *testing.T) {
	tests := []struct {
		name    string
		opts    *Options
		wantErr error
	}{
		{
			name: "valid options",
			opts: NewOptions().SetClientID("valid-client"),
		},
		{
			name:    "no servers",
			opts:    &Options{ClientID: "test", ProtocolVersion: 4},
			wantErr: ErrNoServers,
		},
		{
			name:    "empty client ID",
			opts:    NewOptions(),
			wantErr: ErrEmptyClientID,
		},
		{
			name:    "invalid protocol version",
			opts:    NewOptions().SetClientID("test").SetProtocolVersion(3),
			wantErr: ErrInvalidProtocol,
		},
		{
			name: "protocol version 5",
			opts: NewOptions().SetClientID("test").SetProtocolVersion(5),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if tt.wantErr != nil {
				if err != tt.wantErr {
					t.Errorf("expected error %v, got %v", tt.wantErr, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestOptionsValidationFixesMaxInflight(t *testing.T) {
	opts := NewOptions().SetClientID("test").SetMaxInflight(-1)
	if err := opts.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.MaxInflight != DefaultMaxInflight {
		t.Errorf("expected MaxInflight to be fixed to %d, got %d", DefaultMaxInflight, opts.MaxInflight)
	}
}

func TestOptionsValidationNormalizesReconnectAndSlowConsumer(t *testing.T) {
	opts := NewOptions().
		SetClientID("test").
		SetReconnectJitter(-1 * time.Second).
		SetMaxReconnectAttempts(-1).
		SetSlowConsumerPolicy(SlowConsumerPolicy("invalid")).
		SetSlowConsumerBlockTimeout(-1 * time.Second).
		SetMaxOutboundPendingMessages(-1).
		SetMaxOutboundPendingBytes(-1).
		SetOutboundBackpressurePolicy(OutboundBackpressurePolicy("invalid")).
		SetOutboundBlockTimeout(-1 * time.Second)

	if err := opts.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if opts.ReconnectJitter != 0 {
		t.Errorf("expected reconnect jitter normalized to 0, got %v", opts.ReconnectJitter)
	}
	if opts.MaxReconnectAttempts != 0 {
		t.Errorf("expected max reconnect attempts normalized to 0, got %d", opts.MaxReconnectAttempts)
	}
	if opts.SlowConsumerPolicy != SlowConsumerDropNew {
		t.Errorf("expected slow consumer policy normalized to %q, got %q", SlowConsumerDropNew, opts.SlowConsumerPolicy)
	}
	if opts.SlowConsumerBlockTimeout != 0 {
		t.Errorf("expected slow consumer block timeout normalized to 0, got %v", opts.SlowConsumerBlockTimeout)
	}
	if opts.MaxOutboundPendingMessages != 0 {
		t.Errorf("expected max outbound pending messages normalized to 0, got %d", opts.MaxOutboundPendingMessages)
	}
	if opts.MaxOutboundPendingBytes != 0 {
		t.Errorf("expected max outbound pending bytes normalized to 0, got %d", opts.MaxOutboundPendingBytes)
	}
	if opts.OutboundBackpressurePolicy != OutboundBackpressureBlock {
		t.Errorf("expected outbound backpressure policy normalized to %q, got %q", OutboundBackpressureBlock, opts.OutboundBackpressurePolicy)
	}
	if opts.OutboundBlockTimeout != 0 {
		t.Errorf("expected outbound block timeout normalized to 0, got %v", opts.OutboundBlockTimeout)
	}
}
