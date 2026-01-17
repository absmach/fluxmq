// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

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
}

func TestOptionsBuilder(t *testing.T) {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	onConnect := func() {}
	onLost := func(error) {}
	onReconn := func(int) {}
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
		SetMaxInflight(50).
		SetOnConnect(onConnect).
		SetOnConnectionLost(onLost).
		SetOnReconnecting(onReconn).
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
