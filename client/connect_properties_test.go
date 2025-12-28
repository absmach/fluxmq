// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"testing"
)

func TestOptionsV5ConnectProperties(t *testing.T) {
	opts := NewOptions().
		SetClientID("test-client").
		SetProtocolVersion(5).
		SetSessionExpiry(3600).
		SetReceiveMaximum(100).
		SetMaximumPacketSize(65536).
		SetTopicAliasMaximum(10).
		SetRequestResponseInfo(true).
		SetRequestProblemInfo(false)

	if opts.ProtocolVersion != 5 {
		t.Errorf("Expected ProtocolVersion 5, got %d", opts.ProtocolVersion)
	}
	if opts.SessionExpiry != 3600 {
		t.Errorf("Expected SessionExpiry 3600, got %d", opts.SessionExpiry)
	}
	if opts.ReceiveMaximum != 100 {
		t.Errorf("Expected ReceiveMaximum 100, got %d", opts.ReceiveMaximum)
	}
	if opts.MaximumPacketSize != 65536 {
		t.Errorf("Expected MaximumPacketSize 65536, got %d", opts.MaximumPacketSize)
	}
	if opts.TopicAliasMaximum != 10 {
		t.Errorf("Expected TopicAliasMaximum 10, got %d", opts.TopicAliasMaximum)
	}
	if !opts.RequestResponseInfo {
		t.Error("Expected RequestResponseInfo true")
	}
	if opts.RequestProblemInfo {
		t.Error("Expected RequestProblemInfo false")
	}
}

func TestOptionsV5DefaultRequestProblemInfo(t *testing.T) {
	opts := NewOptions()

	if !opts.RequestProblemInfo {
		t.Error("Expected RequestProblemInfo to default to true")
	}
}

func TestOptionsV5ZeroValues(t *testing.T) {
	opts := NewOptions().
		SetClientID("test-client").
		SetProtocolVersion(5).
		SetReceiveMaximum(0).
		SetMaximumPacketSize(0).
		SetTopicAliasMaximum(0)

	if opts.ReceiveMaximum != 0 {
		t.Errorf("Expected ReceiveMaximum 0, got %d", opts.ReceiveMaximum)
	}
	if opts.MaximumPacketSize != 0 {
		t.Errorf("Expected MaximumPacketSize 0, got %d", opts.MaximumPacketSize)
	}
	if opts.TopicAliasMaximum != 0 {
		t.Errorf("Expected TopicAliasMaximum 0, got %d", opts.TopicAliasMaximum)
	}
}

func TestOptionsBuilderChaining(t *testing.T) {
	opts := NewOptions().
		SetServers("mqtt.example.com:1883").
		SetClientID("chained-client").
		SetProtocolVersion(5).
		SetReceiveMaximum(200).
		SetMaximumPacketSize(131072).
		SetTopicAliasMaximum(20).
		SetRequestResponseInfo(true).
		SetSessionExpiry(7200)

	if len(opts.Servers) != 1 || opts.Servers[0] != "mqtt.example.com:1883" {
		t.Error("Server not set correctly in chain")
	}
	if opts.ClientID != "chained-client" {
		t.Error("ClientID not set correctly in chain")
	}
	if opts.ProtocolVersion != 5 {
		t.Error("ProtocolVersion not set correctly in chain")
	}
	if opts.ReceiveMaximum != 200 {
		t.Error("ReceiveMaximum not set correctly in chain")
	}
	if opts.MaximumPacketSize != 131072 {
		t.Error("MaximumPacketSize not set correctly in chain")
	}
	if opts.TopicAliasMaximum != 20 {
		t.Error("TopicAliasMaximum not set correctly in chain")
	}
	if !opts.RequestResponseInfo {
		t.Error("RequestResponseInfo not set correctly in chain")
	}
	if opts.SessionExpiry != 7200 {
		t.Error("SessionExpiry not set correctly in chain")
	}
}
