// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"net"
	"testing"
	"time"
)

func TestIPRateLimiter_Allow(t *testing.T) {
	// Create limiter with 5 requests per second, burst of 2
	limiter := NewIPRateLimiter(5, 2, time.Minute)
	defer limiter.Stop()

	addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1234}

	// First 2 requests should succeed (burst)
	if !limiter.Allow(addr) {
		t.Error("First request should be allowed")
	}
	if !limiter.Allow(addr) {
		t.Error("Second request (within burst) should be allowed")
	}

	// Third request should be rate limited (burst exhausted, no tokens yet)
	if limiter.Allow(addr) {
		t.Error("Third request should be rate limited (burst exhausted)")
	}

	// Wait for token refill
	time.Sleep(250 * time.Millisecond)

	// Should be allowed now (token refilled)
	if !limiter.Allow(addr) {
		t.Error("Request after token refill should be allowed")
	}
}

func TestIPRateLimiter_DifferentIPs(t *testing.T) {
	limiter := NewIPRateLimiter(1, 1, time.Minute)
	defer limiter.Stop()

	addr1 := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1234}
	addr2 := &net.TCPAddr{IP: net.ParseIP("192.168.1.2"), Port: 1234}

	// First request from each IP should succeed
	if !limiter.Allow(addr1) {
		t.Error("First request from IP1 should be allowed")
	}
	if !limiter.Allow(addr2) {
		t.Error("First request from IP2 should be allowed")
	}

	// Second request from IP1 should be rate limited
	if limiter.Allow(addr1) {
		t.Error("Second request from IP1 should be rate limited")
	}
	// Second request from IP2 should also be rate limited
	if limiter.Allow(addr2) {
		t.Error("Second request from IP2 should be rate limited")
	}
}

func TestIPRateLimiter_NilAddr(t *testing.T) {
	limiter := NewIPRateLimiter(1, 1, time.Minute)
	defer limiter.Stop()

	// Nil address should always be allowed
	if !limiter.Allow(nil) {
		t.Error("Nil address should be allowed")
	}
}

func TestClientRateLimiter_AllowPublish(t *testing.T) {
	// 5 messages per second, burst of 2
	limiter := NewClientRateLimiter(5, 2, 10, 2)

	clientID := "test-client"

	// First 2 publishes should succeed (burst)
	if !limiter.AllowPublish(clientID) {
		t.Error("First publish should be allowed")
	}
	if !limiter.AllowPublish(clientID) {
		t.Error("Second publish (within burst) should be allowed")
	}

	// Third publish should be rate limited
	if limiter.AllowPublish(clientID) {
		t.Error("Third publish should be rate limited")
	}
}

func TestClientRateLimiter_AllowSubscribe(t *testing.T) {
	// 10 messages/s, burst 2; 5 subscriptions/s, burst of 2
	limiter := NewClientRateLimiter(10, 2, 5, 2)

	clientID := "test-client"

	// First 2 subscriptions should succeed (burst)
	if !limiter.AllowSubscribe(clientID) {
		t.Error("First subscribe should be allowed")
	}
	if !limiter.AllowSubscribe(clientID) {
		t.Error("Second subscribe (within burst) should be allowed")
	}

	// Third subscription should be rate limited
	if limiter.AllowSubscribe(clientID) {
		t.Error("Third subscribe should be rate limited")
	}
}

func TestClientRateLimiter_DifferentClients(t *testing.T) {
	limiter := NewClientRateLimiter(1, 1, 1, 1)

	client1 := "client-1"
	client2 := "client-2"

	// First request from each client should succeed
	if !limiter.AllowPublish(client1) {
		t.Error("First publish from client1 should be allowed")
	}
	if !limiter.AllowPublish(client2) {
		t.Error("First publish from client2 should be allowed")
	}

	// Second request from each should be rate limited
	if limiter.AllowPublish(client1) {
		t.Error("Second publish from client1 should be rate limited")
	}
	if limiter.AllowPublish(client2) {
		t.Error("Second publish from client2 should be rate limited")
	}
}

func TestClientRateLimiter_RemoveClient(t *testing.T) {
	limiter := NewClientRateLimiter(1, 1, 1, 1)

	clientID := "test-client"

	// Use up the burst
	if !limiter.AllowPublish(clientID) {
		t.Error("First publish should be allowed")
	}
	if limiter.AllowPublish(clientID) {
		t.Error("Second publish should be rate limited")
	}

	// Remove the client
	limiter.RemoveClient(clientID)

	// Client should get a fresh limiter
	if !limiter.AllowPublish(clientID) {
		t.Error("First publish after removal should be allowed (fresh limiter)")
	}
}

func TestManager_Disabled(t *testing.T) {
	cfg := Config{Enabled: false}
	manager := NewManager(cfg)

	addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1234}

	// All checks should pass when disabled
	if !manager.AllowConnection(addr) {
		t.Error("AllowConnection should return true when disabled")
	}
	if !manager.AllowPublish("client") {
		t.Error("AllowPublish should return true when disabled")
	}
	if !manager.AllowSubscribe("client") {
		t.Error("AllowSubscribe should return true when disabled")
	}
}

func TestManager_Enabled(t *testing.T) {
	cfg := Config{
		Enabled: true,
		Connection: ConnectionConfig{
			Enabled: true,
			Rate:    1,
			Burst:   1,
			CleanupInterval: time.Minute,
		},
		Message: MessageConfig{
			Enabled: true,
			Rate:    1,
			Burst:   1,
		},
		Subscribe: SubscribeConfig{
			Enabled: true,
			Rate:    1,
			Burst:   1,
		},
	}
	manager := NewManager(cfg)
	defer manager.Stop()

	addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1234}
	clientID := "test-client"

	// First requests should succeed
	if !manager.AllowConnection(addr) {
		t.Error("First connection should be allowed")
	}
	if !manager.AllowPublish(clientID) {
		t.Error("First publish should be allowed")
	}
	if !manager.AllowSubscribe(clientID) {
		t.Error("First subscribe should be allowed")
	}

	// Second requests should be rate limited
	if manager.AllowConnection(addr) {
		t.Error("Second connection should be rate limited")
	}
	if manager.AllowPublish(clientID) {
		t.Error("Second publish should be rate limited")
	}
	if manager.AllowSubscribe(clientID) {
		t.Error("Second subscribe should be rate limited")
	}
}

func TestManager_SelectiveEnable(t *testing.T) {
	// Only enable connection rate limiting
	cfg := Config{
		Enabled: true,
		Connection: ConnectionConfig{
			Enabled: true,
			Rate:    1,
			Burst:   1,
			CleanupInterval: time.Minute,
		},
		Message: MessageConfig{
			Enabled: false,
		},
		Subscribe: SubscribeConfig{
			Enabled: false,
		},
	}
	manager := NewManager(cfg)
	defer manager.Stop()

	addr := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1234}
	clientID := "test-client"

	// Connection should be rate limited
	if !manager.AllowConnection(addr) {
		t.Error("First connection should be allowed")
	}
	if manager.AllowConnection(addr) {
		t.Error("Second connection should be rate limited")
	}

	// Message and subscribe should always pass (disabled)
	for i := 0; i < 10; i++ {
		if !manager.AllowPublish(clientID) {
			t.Errorf("Publish %d should be allowed (rate limiting disabled)", i)
		}
		if !manager.AllowSubscribe(clientID) {
			t.Errorf("Subscribe %d should be allowed (rate limiting disabled)", i)
		}
	}
}

func TestExtractIP(t *testing.T) {
	tests := []struct {
		name     string
		addr     net.Addr
		expected string
	}{
		{
			name:     "TCPAddr",
			addr:     &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 1234},
			expected: "192.168.1.1",
		},
		{
			name:     "UDPAddr",
			addr:     &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5678},
			expected: "10.0.0.1",
		},
		{
			name:     "Nil",
			addr:     nil,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractIP(tt.addr)
			if result != tt.expected {
				t.Errorf("extractIP(%v) = %q, want %q", tt.addr, result, tt.expected)
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Enabled {
		t.Error("Default config should have Enabled=false")
	}
	if !cfg.Connection.Enabled {
		t.Error("Connection rate limiting should be enabled by default")
	}
	if !cfg.Message.Enabled {
		t.Error("Message rate limiting should be enabled by default")
	}
	if !cfg.Subscribe.Enabled {
		t.Error("Subscribe rate limiting should be enabled by default")
	}
}
